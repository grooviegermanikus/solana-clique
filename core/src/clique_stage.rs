use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
    thread::{Builder, JoinHandle},
    time::Duration,
};

use crossbeam_channel::{Receiver, Sender};
use futures::task::noop_waker;
use futures_lite::stream::StreamExt;
use libp2p::{
    core, gossipsub, identify, identity, noise, ping, swarm::NetworkBehaviour, swarm::SwarmEvent,
    tcp, yamux, Multiaddr, PeerId, Swarm, Transport,
};
use log::info;
use solana_perf::packet::{Meta, Packet, PacketBatch, PACKET_DATA_SIZE};

pub struct CliqueStageConfig {
    pub identity_keypair: Arc<solana_sdk::signature::Keypair>,
    pub exit: Arc<AtomicBool>,
}

#[derive(NetworkBehaviour)]
struct SolanaCliqueBehaviour {
    gossipsub: gossipsub::Gossipsub,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
}

pub struct CliqueStage {
    clique_thread_hdl: JoinHandle<()>,
}

impl CliqueStage {
    pub fn new(
        config: CliqueStageConfig,
        clique_outbound_receiver: Receiver<Vec</*shred:*/ Vec<u8>>>,
        clique_inbound_sender: Sender<PacketBatch>,
    ) -> Self {
        let clique_thread_hdl = Builder::new()
            .name("solClique".to_string())
            .spawn(move || {
                // Derive peer id from solana keypair
                let mut copy = config.identity_keypair.secret().as_bytes().clone();
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

                /*
                // To content-address message, we can take the embedded signature of the shred and use it as an ID.
                let message_id_fn = |message: &gossipsub::GossipsubMessage| {
                };
                */

                // To content-address message, we can take the hash of message and use it as an ID.
                let message_id_fn = |message: &gossipsub::GossipsubMessage| {
                    let mut s = DefaultHasher::new();
                    message.data.hash(&mut s);
                    // TODO: replace with signature data from shred
                    // gossipsub::MessageId::from(message.data.as_chunks::<24>().0[0])
                    gossipsub::MessageId::from(s.finish().to_string())
                };

                // Set a custom gossipsub configuration
                let gossipsub_config = gossipsub::GossipsubConfigBuilder::default()
                    .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
                    // .heartbeat_interval(Duration::from_millis(200)) // Heartbeat 2-3 times per block to make sure shreds are sent out quickly
                    .validation_mode(gossipsub::ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message signing)
                    // .validation_mode(gossipsub::ValidationMode::None) // This disables message validation by libp2p
                    // .validate_messages() // TODO: manually validate shreds
                    .message_id_fn(message_id_fn) // TODO: manually identify shreds
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
                        "/ip4/0.0.0.0/tcp/22334"
                            .parse()
                            .expect("CliqueStage valid listen Multiaddr"),
                    )
                    .expect("CliqueStage listen succeeds");

                let waker = noop_waker();
                let mut cx = Context::from_waker(&waker);

                loop {
                    if config.exit.load(Ordering::Relaxed) {
                        break;
                    }

                    if let Ok(outbound) = clique_outbound_receiver.try_recv() {
                        for shred in outbound.iter() {
                            if let Err(e) = swarm
                                .behaviour_mut()
                                .gossipsub
                                .publish(topic.clone(), shred.as_slice())
                            {
                                info!(
                                    "CliqueStage outbound publish error: {} shred.len {}",
                                    e,
                                    shred.len()
                                );
                                break;
                            }
                        }
                    }

                    if let Poll::Ready(Some(inbound)) = swarm.poll_next(&mut cx) {
                        match inbound {
                            SwarmEvent::NewListenAddr { address, .. } => {
                                info!("CliqueStage listening on {address:?}");
                            }
                            SwarmEvent::Behaviour(SolanaCliqueBehaviourEvent::Identify(event)) => {
                                info!("CliqueStage identify: {event:?}");
                            }
                            SwarmEvent::Behaviour(SolanaCliqueBehaviourEvent::Gossipsub(
                                gossipsub::GossipsubEvent::Message {
                                    propagation_source: _peer_id,
                                    message_id: _id,
                                    message,
                                },
                            )) => {
                                if let Ok(packet_bytes) =
                                    TryInto::<[u8; PACKET_DATA_SIZE]>::try_into(
                                        message.data.as_slice(),
                                    )
                                {
                                    let uni_batch = PacketBatch::new(vec![Packet::new(
                                        packet_bytes,
                                        Meta::default(),
                                    )]);
                                    clique_inbound_sender
                                        .send(uni_batch)
                                        .expect("CliqueStage send inbound");
                                }
                            }
                            SwarmEvent::Behaviour(SolanaCliqueBehaviourEvent::Ping(event)) => {
                                match event {
                                    ping::Event {
                                        peer,
                                        result: Result::Ok(ping::Success::Ping { rtt }),
                                    } => {
                                        info!(
                                            "CliqueStage ping: rtt to {} is {} ms",
                                            peer.to_base58(),
                                            rtt.as_millis()
                                        );
                                    }
                                    ping::Event {
                                        peer,
                                        result: Result::Ok(ping::Success::Pong),
                                    } => {
                                        info!("CliqueStage ping: pong from {}", peer.to_base58());
                                    }
                                    ping::Event {
                                        peer,
                                        result: Result::Err(ping::Failure::Timeout),
                                    } => {
                                        info!("CliqueStage ping: timeout to {}", peer.to_base58());
                                    }
                                    ping::Event {
                                        peer,
                                        result: Result::Err(ping::Failure::Unsupported),
                                    } => {
                                        info!(
                                            "CliqueStage ping: {} does not support ping protocol",
                                            peer.to_base58()
                                        );
                                    }
                                    ping::Event {
                                        peer,
                                        result: Result::Err(ping::Failure::Other { error }),
                                    } => {
                                        info!(
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

        Self { clique_thread_hdl }
    }
}
