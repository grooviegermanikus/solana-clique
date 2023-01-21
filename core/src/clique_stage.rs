use std::{
    collections::{hash_map::DefaultHasher, HashMap, HashSet},
    convert::TryFrom,
    hash::{Hash, Hasher},
    net::UdpSocket,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    task::{Context, Poll},
    thread::{Builder, JoinHandle, self},
    time::{Duration, Instant}, iter::repeat,
};

use crossbeam_channel::{Receiver, Sender};
use futures::task::noop_waker;
use futures_lite::stream::StreamExt;
use itertools::{Itertools, izip};
use libp2p::{
    core, gossipsub, identify, identity, noise, ping, swarm::NetworkBehaviour, swarm::SwarmEvent,
    tcp, yamux, Multiaddr, PeerId, Swarm, Transport,
};
use log::info;
use lru::LruCache;
use rayon::{ThreadPoolBuilder, prelude::{IntoParallelIterator, ParallelIterator}};
use solana_gossip::{cluster_info::ClusterInfo, legacy_contact_info::LegacyContactInfo};
use solana_ledger::{shred::{ShredId, layout::get_shred_id}, leader_schedule_cache::LeaderScheduleCache};
use solana_perf::packet::{Meta, Packet, PacketBatch, PacketFlags, PACKET_DATA_SIZE};
use solana_rayon_threadlimit::get_thread_count;
use solana_runtime::bank_forks::BankForks;
use solana_sdk::pubkey::Pubkey;
use solana_streamer::{socket::SocketAddrSpace, sendmmsg::{SendPktsError, multi_target_send}};

use crate::{cluster_nodes::{ClusterNodesCache, new_cluster_nodes, ClusterNodes}, packet_hasher::PacketHasher, retransmit_stage::{maybe_reset_shreds_received_cache, should_skip_retransmit}};


const RECV_TIMEOUT: Duration = Duration::from_secs(1);
const DATA_PLANE_FANOUT: usize = 8;
const DEFAULT_LRU_SIZE: usize = 10_000;
// Minimum number of shreds to use rayon parallel iterators.
const PAR_ITER_MIN_NUM_SHREDS: usize = 2;
const METRICS_SUBMIT_CADENCE: Duration = Duration::from_secs(2);
const GOSSIP_HEARTBEAT_CADENCE: Duration = Duration::from_secs(10);
const CLIQUE_WARMUP_SLOTS: u64 = 10;

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
enum CliqueGossipMessage {
    Heartbeat(CliqueHeartbeatMessage)
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
struct CliqueHeartbeatMessage {
    boot_slot: u64,
    current_slot: u64,
}


struct CliqueStageStats {
    since: Instant,
    shreds_inbound: usize,
    shreds_skipped: usize,
    shreds_outbound: usize,
    transmit_attempts: usize,
    transmit_errors: usize,
}

impl CliqueStageStats {

    fn new() -> Self {
        Self {
            since: Instant::now(),
            shreds_inbound: 0usize,
            shreds_skipped: 0usize,
            shreds_outbound: 0usize,
            transmit_attempts: 0usize,
            transmit_errors: 0usize,
        }
    }

    fn maybe_submit(&mut self) {
        if self.since.elapsed() <= METRICS_SUBMIT_CADENCE {
            return;
        }
        datapoint_info!(
            "clique_stage",
            ("shreds_inbound", self.shreds_inbound, i64),
            ("shreds_skipped", self.shreds_skipped, i64),
            ("shreds_outbound", self.shreds_outbound, i64),
            ("transmit_attempts", self.transmit_attempts, i64),
            ("transmit_errors", self.transmit_errors, i64),
        );
        *self = Self::new();
    }
}

#[derive(NetworkBehaviour)]
struct SolanaCliqueBehaviour {
    gossipsub: gossipsub::Gossipsub,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
}

pub struct CliqueStage {
    gossip_thread_hdl: JoinHandle<()>,
    outbound_thread_hdl: JoinHandle<()>,
}

impl CliqueStage {
    pub fn new(
        bank_forks: Arc<RwLock<BankForks>>,
        cluster_info: Arc<ClusterInfo>,
        clique_outbound_receiver: Receiver<Vec</*shred:*/ Vec<u8>>>,
        clique_outbound_sockets: Arc<Vec<UdpSocket>>,
        exit: Arc<AtomicBool>,
        identity_keypair: Arc<solana_sdk::signature::Keypair>,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
    ) -> Self {
        let mut stats = CliqueStageStats::new();
        
        let boot_slot = bank_forks.read().unwrap().highest_slot();
        let clique_status = Arc::new(RwLock::new(HashMap::new()));
        
        let gossip_bank_forks = bank_forks.clone();
        let gossip_clique_status = clique_status.clone();
        let gossip_exit= exit.clone();
        let gossip_thread_hdl = Builder::new()
            .name("solCliqueGossip".to_string())
            .spawn(move || {

                // Derive peer id from solana keypair
                let mut copy = identity_keypair.secret().as_bytes().clone();
                let secret_key = identity::ed25519::SecretKey::from_bytes(copy)
                    .expect("CliqueStage solana_keypair is ed25519 compatible");
                let local_key = identity::Keypair::Ed25519(secret_key.into());
                let local_peer_id = PeerId::from(local_key.public());
                // println!("Local peer id: {local_peer_id}");

                // Set up an encrypted DNS-enabled TCP Transport over the Mplex protocol.
                let transport = tcp::async_io::Transport::default()
                    .upgrade(core::upgrade::Version::V1)
                    .authenticate(
                        noise::NoiseAuthenticated::xx(&local_key.clone())
                            .expect("CliqueStage noise authentication available"),
                    )
                    .multiplex(yamux::YamuxConfig::default())
                    .boxed();


                // To content-address message, we can take the hash of message and use it as an ID.
                let message_id_fn = |message: &gossipsub::GossipsubMessage| {
                    let mut s = DefaultHasher::new();
                    message.data.hash(&mut s);
                    let hash = s.finish().to_string();
                    gossipsub::MessageId::from(hash)
                };

                // Set a custom gossipsub configuration
                let gossipsub_config = gossipsub::GossipsubConfigBuilder::default()
                    .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
                    .validation_mode(gossipsub::ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message signing)
                    .message_id_fn(message_id_fn) 
                    .build()
                    .expect("CliqueStage valid gossipsub_config");

                // Build a gossipsub network behaviour
                let mut gossipsub = gossipsub::Gossipsub::new(
                    gossipsub::MessageAuthenticity::Signed(local_key.clone()),
                    gossipsub_config,
                )
                .expect("CliqueStage correct gossipsub_config");

                // Create a Gossipsub topic
                let topic: gossipsub::Topic<gossipsub::topic::Sha256Hash> =
                    gossipsub::Topic::new("solana-clique");

                // subscribes to our topic
                gossipsub
                    .subscribe(&topic)
                    .expect("CliqueStage subscribe to topic");

                // Build an identify network behaviour
                let identify = identify::Behaviour::new(identify::Config::new(
                    "/solana/1.15.0".into(),
                    local_key.public(),
                ));

                // Build a ping network behaviour
                let ping = ping::Behaviour::new(ping::Config::new());

                // Create a Swarm to manage peers and events from behaviours
                let mut swarm = {
                    let behaviour = SolanaCliqueBehaviour {
                        gossipsub,
                        identify,
                        ping,
                    };
                    Swarm::with_async_std_executor(transport, behaviour, local_peer_id)
                };

                // Reach out to other nodes if specified
                for to_dial in std::env::args().skip(1) {
                    if let Ok(addr) = Multiaddr::from_str(&to_dial) {
                        info!("dialing {}", to_dial);
                        swarm.dial(addr).expect("dial succeeds");
                    }
                }

                // Listen on all interfaces
                swarm
                    .listen_on(
                        "/ip4/0.0.0.0/tcp/0"
                            .parse()
                            .expect("CliqueStage valid listen_port"),
                    )
                    .expect("CliqueStage listen succeeds");

                let waker = noop_waker();
                let mut cx = Context::from_waker(&waker);
                let mut last_hearbeat = Instant::now();
                
                loop {
                    if gossip_exit.load(Ordering::Relaxed) {
                        break;
                    }

                    if last_hearbeat.elapsed() > GOSSIP_HEARTBEAT_CADENCE {   
                        let current_slot = gossip_bank_forks.read().unwrap().highest_slot();
                        let message = CliqueHeartbeatMessage {
                            boot_slot, current_slot
                        };

                        if let Err(e) = swarm
                            .behaviour_mut().gossipsub
                            .publish(topic.clone(), bincode::serialize(&CliqueGossipMessage::Heartbeat(message)).unwrap()) {
                            info!("CliqueStage Publish error: {e:?}");
                        }

                        gossip_clique_status.write().unwrap().insert(peer_id_to_solana_pubkey(local_peer_id), message);
                        last_hearbeat = Instant::now();    
                    }

                    if let Poll::Ready(Some(inbound)) = swarm.poll_next(&mut cx) {
                        match inbound {
                            SwarmEvent::NewListenAddr { address, .. } => {
                                info!("CliqueStage listening on {address:?}");
                            }
                            SwarmEvent::Behaviour(SolanaCliqueBehaviourEvent::Identify(event)) => {
                                debug!("CliqueStage identify: {event:?}");
                            }
                            SwarmEvent::Behaviour(SolanaCliqueBehaviourEvent::Gossipsub(event)) => {
                                match event {
                                    gossipsub::GossipsubEvent::Message {
                                        propagation_source,
                                        message_id,
                                        message,
                                    } => {
                                        if let Ok(message) = bincode::deserialize::<CliqueGossipMessage>(&message.data.as_slice()) {
                                            let peer_pk = peer_id_to_solana_pubkey(propagation_source);
                                            trace!("CliqueStage inbound gossipsub peer={} message={:?}", peer_pk.to_string(), message);

                                            match message {
                                                CliqueGossipMessage::Heartbeat(message) => {
                                                    gossip_clique_status.write().unwrap().insert(peer_pk, message);
                                                }
                                            }
                                        }
                                    }
                                    gossipsub::GossipsubEvent::Subscribed { peer_id, topic } => {
                                        let peer_pk = peer_id_to_solana_pubkey(peer_id);
                                        info!("CliqueStage gossipsub peer={} subscribed topic={}", peer_pk, topic)
                                    }
                                    gossipsub::GossipsubEvent::Unsubscribed { peer_id, topic } => {
                                        let peer_pk = peer_id_to_solana_pubkey(peer_id);
                                        info!("CliqueStage gossipsub peer={} unsubscribed topic={}", peer_pk, topic)
                                    }
                                    gossipsub::GossipsubEvent::GossipsubNotSupported { peer_id } => {
                                        let peer_pk = peer_id_to_solana_pubkey(peer_id);
                                        info!("CliqueStage gossipsub peer={} unsupported", peer_pk)
                                    }
                                }
                            }

                            SwarmEvent::Behaviour(SolanaCliqueBehaviourEvent::Ping(event)) => {
                                match event {
                                    ping::Event {
                                        peer,
                                        result: Result::Ok(ping::Success::Ping { rtt }),
                                    } => {
                                        debug!(
                                            "CliqueStage ping: rtt to {} is {} ms",
                                            peer.to_base58(),
                                            rtt.as_millis()
                                        );
                                    }
                                    ping::Event {
                                        peer,
                                        result: Result::Ok(ping::Success::Pong),
                                    } => {
                                        debug!("CliqueStage ping: pong from {}", peer.to_base58());
                                    }
                                    ping::Event {
                                        peer,
                                        result: Result::Err(ping::Failure::Timeout),
                                    } => {
                                        debug!("CliqueStage ping: timeout to {}", peer.to_base58());
                                    }
                                    ping::Event {
                                        peer,
                                        result: Result::Err(ping::Failure::Unsupported),
                                    } => {
                                        debug!(
                                            "CliqueStage ping: {} does not support ping protocol",
                                            peer.to_base58()
                                        );
                                    }
                                    ping::Event {
                                        peer,
                                        result: Result::Err(ping::Failure::Other { error }),
                                    } => {
                                        debug!(
                                            "CliqueStage ping: ping::Failure with {}: {error}",
                                            peer.to_base58()
                                        );
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
            })
            .unwrap();

        let mut hasher_reset_ts = Instant::now();
        let mut shreds_received = LruCache::<ShredId, _>::new(DEFAULT_LRU_SIZE);
        let mut packet_hasher = PacketHasher::default();
        let num_threads = get_thread_count().min(8).max(clique_outbound_sockets.len());
        let thread_pool = ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .thread_name(|i| format!("solCliqueRetransmitSender{i:02}"))
            .build()
            .unwrap();

        let outbound_thread_hdl = Builder::new()
            .name("solCliqueOutboundReceiver".to_string())
            .spawn(move || {
                while !exit.load(Ordering::Relaxed) {                
                    if let Ok(mut shreds) = clique_outbound_receiver.recv_timeout(RECV_TIMEOUT) {
                        shreds.extend(clique_outbound_receiver.try_iter().flatten());

                        stats.shreds_inbound += shreds.len();

                        let (working_bank, _root_bank) = {
                            let bank_forks = bank_forks.read().unwrap();
                            (bank_forks.working_bank(), bank_forks.root_bank())
                        };

                        maybe_reset_shreds_received_cache(
                            &mut shreds_received,
                            &mut packet_hasher,
                            &mut hasher_reset_ts,
                        );

                        // Lookup slot leader for each slot.
                        let shreds: Vec<_> = shreds
                            .into_iter()
                            .filter_map(|shred| {
                                let key = get_shred_id(&shred)?;
                                if should_skip_retransmit(key, &shred, &mut shreds_received, &packet_hasher) {
                                    stats.shreds_skipped += 1;
                                    None
                                } else {
                                    Some((key, shred))
                                }
                            })
                            .into_group_map_by(|(key, _)| key.slot())
                            .into_iter()
                            .filter_map(|(slot, shreds)| {
                                // TODO: consider using root-bank here for leader lookup!
                                // Shreds' signatures should be verified before they reach here,
                                // and if the leader is unknown they should fail signature check.
                                // So here we should expect to know the slot leader and otherwise
                                // skip the shred.
                                let slot_leader=Arc::new(leader_schedule_cache.slot_leader_at(slot, Some(&working_bank))?);
                                let active_clique_members: Vec<Pubkey> = clique_status.read().unwrap().iter().filter(|(_, m)| slot - m.boot_slot > CLIQUE_WARMUP_SLOTS).map(|(pk,_)|*pk).collect();
                                let cluster_nodes = Arc::new(ClusterNodes::<CliqueStage>::new(&cluster_info, &active_clique_members));
                            
                                Some(izip!(shreds, iter::repeat(slot_leader), iter::repeat(cluster_nodes)))
                            })
                            .flatten()
                            .collect();
                        stats.shreds_outbound += shreds.len();

                        let socket_addr_space = cluster_info.socket_addr_space();
                    
                        let init_stats = (0,0);
                        let merge_stats = |t1: (usize, usize), t2: (usize,usize)| (t1.0 + t2.0, t1.1 + t2.1);

                        let transmit_stats = if shreds.len() < PAR_ITER_MIN_NUM_SHREDS {
                            shreds
                                .into_iter()
                                .enumerate()
                                .map(|(index, ((key, shred), slot_leader, cluster_nodes))| {
                                    let (_root_distance, transmit_attempts, transmit_errors) = retransmit_shred(
                                        &key,
                                        &shred,
                                        &slot_leader,
                                        &cluster_nodes,
                                        socket_addr_space,
                                        &clique_outbound_sockets[index % clique_outbound_sockets.len()],
                                    );
                                    (transmit_attempts, transmit_errors)
                                })
                                .fold(init_stats,merge_stats)
                        } else {
                            thread_pool.install(|| {
                                shreds
                                    .into_par_iter()
                                    .map(|((key, shred), slot_leader, cluster_nodes)| {
                                        let index = thread_pool.current_thread_index().unwrap();
                                        let (_root_distance, transmit_attempts, transmit_errors) = retransmit_shred(
                                            &key,
                                            &shred,
                                            &slot_leader,
                                            &cluster_nodes,
                                            socket_addr_space,
                                            &clique_outbound_sockets[index % clique_outbound_sockets.len()],
                                        );
                                        (transmit_attempts, transmit_errors)
                                        
                                    })
                                    .fold(|| init_stats, merge_stats)
                                    .reduce(|| init_stats, merge_stats)
                            })
                        };
                        stats.transmit_attempts += transmit_stats.0;
                        stats.transmit_errors += transmit_stats.1;
                    } // -- END of recv
                    stats.maybe_submit();
                } // -- END thread
            })
            .unwrap();

        Self { gossip_thread_hdl, outbound_thread_hdl }
    }

    pub fn join(self) -> thread::Result<()> {
        self.outbound_thread_hdl.join()?;
        self.gossip_thread_hdl.join()?;
        Ok(())
    }
}




fn retransmit_shred(
    key: &ShredId,
    shred: &[u8],
    slot_leader: &Pubkey,
    cluster_nodes: &ClusterNodes<CliqueStage>,
    socket_addr_space: &SocketAddrSpace,
    socket: &UdpSocket,
) -> (/*root_distance:*/ usize, /*num_nodes:*/ usize, /*num_failed*/ usize) {
    let (root_distance, addrs) =
        cluster_nodes.get_retransmit_addrs(slot_leader, key, DATA_PLANE_FANOUT);
    let addrs: Vec<_> = addrs
        .into_iter()
        .filter(|addr| LegacyContactInfo::is_valid_address(addr, socket_addr_space))
        .collect();
    
    
    let num_failed = match multi_target_send(socket, shred, &addrs) {
        Ok(()) => 0,
        Err(SendPktsError::IoError(ioerr, num_failed)) => {
            error!(
                "retransmit_to multi_target_send error: {:?}, {}/{} packets failed",
                ioerr,
                num_failed,
                addrs.len(),
            );
            num_failed
        }
    };

    (root_distance, addrs.len(), num_failed)
}

fn peer_id_to_solana_pubkey(
    peer_id: PeerId,
) -> Pubkey {
    let bytes = peer_id.to_bytes();
    let split_index = bytes.len() - 32;
    let pk_bytes = <&[u8; 32]>::try_from(&bytes[split_index..]).unwrap();
    Pubkey::new_from_array(*pk_bytes)
}
