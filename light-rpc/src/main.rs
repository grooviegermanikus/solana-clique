use bincode::de;
use lite_rpc::Lite;
use rayon::ThreadBuilder;
use solana_client::{rpc_client, thin_client::ThinClient, tpu_client::TpuClientConfig};
use solana_pubsub_client::pubsub_client::{
    BlockSubscription, PubsubBlockClientSubscription, PubsubClient,
};
use solana_sdk::client::AsyncClient;
use std::thread::{Builder, JoinHandle};

use {
    bincode::{config::Options, serialize},
    crossbeam_channel::{Receiver, Sender},
    jsonrpc_core::{
        futures::future, types::error, BoxFuture, Error, MetaIoHandler, Metadata, Result,
    },
    jsonrpc_derive::rpc,
    jsonrpc_http_server::{
        hyper, AccessControlAllowOrigin, CloseHandle, DomainsValidation, RequestMiddleware,
        RequestMiddlewareAction, ServerBuilder,
    },
    serde::{Deserialize, Serialize},
    solana_client::{rpc_client::RpcClient, rpc_config::RpcBlockConfig, tpu_client::TpuClient},
    solana_perf::packet::PACKET_DATA_SIZE,
    solana_rpc_client_api::{
        config::*,
        custom_error::RpcCustomError,
        filter::RpcFilterType,
        response::{Response as RpcResponse, *},
    },
    solana_sdk::{
        clock::{Slot, UnixTimestamp, MAX_RECENT_BLOCKHASHES},
        commitment_config::{CommitmentConfig, CommitmentLevel},
        exit::Exit,
        fee_calculator::FeeCalculator,
        hash::Hash,
        message::SanitizedMessage,
        pubkey::{Pubkey, PUBKEY_BYTES},
        signature::Signature,
        transaction::VersionedTransaction,
    },
    solana_streamer::socket::SocketAddrSpace,
    solana_tpu_client::connection_cache::ConnectionCache,
    solana_transaction_status::{
        BlockEncodingOptions, ConfirmedBlock, ConfirmedTransactionStatusWithSignature,
        ConfirmedTransactionWithStatusMeta, EncodedConfirmedTransactionWithStatusMeta, Reward,
        RewardType, TransactionBinaryEncoding, TransactionConfirmationStatus, TransactionStatus,
        UiConfirmedBlock, UiTransactionEncoding,
    },
    std::{
        any::type_name,
        cmp::{max, min},
        collections::{HashMap, HashSet},
        convert::TryFrom,
        net::SocketAddr,
        str::FromStr,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, Mutex, RwLock,
        },
        time::Duration,
    },
};

use solana_perf::thread::renice_this_thread;
mod cli;

#[derive(Clone)]
pub struct LightRpcRequestProcessor {
    pub rpc_client: Arc<RpcClient>,
    pub tpu_client: Arc<TpuClient>,
    pub last_valid_block_height: u64,
    pub ws_url: String,
    connection_cache: Arc<ConnectionCache>,
    pub current_slot: u64,
    pub signature_status: Arc<RwLock<HashMap<String, Option<CommitmentLevel>>>>,
    pub subscribed_clients: Arc<Vec<PubsubBlockClientSubscription>>,
    pub confirmation_slot: Arc<AtomicU64>,
    pub finalized_slot: Arc<AtomicU64>,
    joinables: Arc<Vec<JoinHandle<()>>>,
}

impl LightRpcRequestProcessor {
    pub fn new(
        json_rpc_url: &String,
        websocket_url: &String,
        pubsub_addr: SocketAddr,
    ) -> LightRpcRequestProcessor {
        let rpc_client = Arc::new(RpcClient::new(json_rpc_url.as_str()));
        let connection_cache = Arc::new(ConnectionCache::default());
        let tpu_client = Arc::new(
            TpuClient::new_with_connection_cache(
                rpc_client.clone(),
                websocket_url.as_str(),
                TpuClientConfig::default(),
                connection_cache.clone(),
            )
            .unwrap(),
        );

        let current_slot = rpc_client.get_slot().unwrap();

        println!("ws connection to {}", websocket_url);
        // subscribe for confirmed_blocks
        let (mut client_confirmed, receiver_confirmed) = PubsubClient::block_subscribe(
            websocket_url.as_str(),
            RpcBlockSubscribeFilter::All,
            Some(RpcBlockSubscribeConfig {
                commitment: Some(CommitmentConfig {
                    commitment: CommitmentLevel::Confirmed,
                }),
                encoding: None,
                transaction_details: Some(
                    solana_transaction_status::TransactionDetails::Signatures,
                ),
                show_rewards: None,
                max_supported_transaction_version: None,
            }),
        )
        .unwrap();

        // subscribe for finalized blocks
        let (mut client_finalized, receiver_finalized) = PubsubClient::block_subscribe(
            websocket_url.as_str(),
            RpcBlockSubscribeFilter::All,
            Some(RpcBlockSubscribeConfig {
                commitment: Some(CommitmentConfig {
                    commitment: CommitmentLevel::Finalized,
                }),
                encoding: None,
                transaction_details: Some(
                    solana_transaction_status::TransactionDetails::Signatures,
                ),
                show_rewards: None,
                max_supported_transaction_version: None,
            }),
        )
        .unwrap();

        let mut joinables = Vec::new();
        let signature_status = Arc::new(RwLock::new(HashMap::new()));

        let confirmation_slot = rpc_client
            .get_slot_with_commitment(CommitmentConfig {
                commitment: CommitmentLevel::Confirmed,
            })
            .unwrap();
        let finalized_slot = rpc_client
            .get_slot_with_commitment(CommitmentConfig {
                commitment: CommitmentLevel::Finalized,
            })
            .unwrap();

        let confirmation_slot = Arc::new(AtomicU64::new(confirmation_slot));
        let finalized_slot = Arc::new(AtomicU64::new(finalized_slot));

        // start thread which will listen to confirmed blocks
        {
            let signature_status = signature_status.clone();
            let confirmation_slot = confirmation_slot.clone();
            let finalized_slot = finalized_slot.clone();
            let join_handle = Builder::new()
                .name("thread working on confirmation block".to_string())
                .spawn(move || {
                    Self::process_block(
                        receiver_confirmed,
                        signature_status,
                        CommitmentLevel::Confirmed,
                        confirmation_slot,
                    );
                })
                .unwrap();
            joinables.push(join_handle);
        }
        // start thread which will listen to finalized blocks
        {
            let signature_status = signature_status.clone();
            let confirmation_slot = confirmation_slot.clone();
            let finalized_slot = finalized_slot.clone();
            let join_handle = Builder::new()
                .name("thread working on finalized block".to_string())
                .spawn(move || {
                    Self::process_block(
                        receiver_finalized,
                        signature_status,
                        CommitmentLevel::Finalized,
                        finalized_slot,
                    );
                })
                .unwrap();
            joinables.push(join_handle);
        }

        LightRpcRequestProcessor {
            rpc_client,
            tpu_client,
            last_valid_block_height: 0,
            ws_url: websocket_url.clone(),
            current_slot,
            signature_status,
            connection_cache: connection_cache,
            subscribed_clients: Arc::new(vec![client_confirmed, client_finalized]),
            joinables: Arc::new(joinables),
            confirmation_slot: confirmation_slot,
            finalized_slot: finalized_slot,
        }
    }

    fn process_block(
        reciever: Receiver<RpcResponse<RpcBlockUpdate>>,
        signature_status: Arc<RwLock<HashMap<String, Option<CommitmentLevel>>>>,
        commitment: CommitmentLevel,
        updated_slot: Arc<AtomicU64>,
    ) {
        println!("processing blocks for {}", commitment.to_string());
        let mut last_update_slot: u64 = 0;
        loop {
            let block_data = reciever.recv();

            match block_data {
                Ok(data) => {
                    let blockUpdate = data.value;
                    last_update_slot = blockUpdate.slot;
                    updated_slot.swap(last_update_slot, Ordering::Relaxed);
                    if let Some(block) = blockUpdate.block {
                        if let Some(signatures) = &block.signatures {
                            let mut lock = signature_status.write().unwrap();
                            for signature in signatures {
                                if lock.contains_key(signature) {
                                    println!("found signature {} for commitment {}", signature, commitment);
                                    lock.insert(signature.clone(), Some(commitment));
                                }
                            }
                        } else {
                            println!(
                                "Cannot get signatures at slot {} block hash {}",
                                last_update_slot,
                                block.blockhash,
                            );
                        }
                    } else {
                        println!("Cannot get a block at slot {}", last_update_slot);
                    }
                }
                Err(e) => {
                    println!("Got error when recieving the block ({})", e.to_string());
                }
            }
        }
        println!("stopped processing blocks for {}", commitment.to_string());
    }

}

impl Metadata for LightRpcRequestProcessor {}

pub mod lite_rpc {
    use {super::*, solana_sdk::message::VersionedMessage};
    #[rpc]
    pub trait Lite {
        type Metadata;

        #[rpc(meta, name = "sendTransaction")]
        fn send_transaction(
            &self,
            meta: Self::Metadata,
            data: String,
            config: Option<RpcSendTransactionConfig>,
        ) -> Result<String>;

        #[rpc(meta, name = "getRecentBlockhash")]
        fn get_recent_blockhash(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<RpcBlockhashFeeCalculator>>;

        #[rpc(meta, name = "confirmTransaction")]
        fn confirm_transaction(
            &self,
            meta: Self::Metadata,
            signature_str: String,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<bool>>;
    }
    pub struct LightRpc;
    impl Lite for LightRpc {
        type Metadata = LightRpcRequestProcessor;

        fn send_transaction(
            &self,
            meta: Self::Metadata,
            data: String,
            config: Option<RpcSendTransactionConfig>,
        ) -> Result<String> {
            let RpcSendTransactionConfig {
                skip_preflight,
                preflight_commitment,
                encoding,
                max_retries,
                min_context_slot,
            } = config.unwrap_or_default();
            let tx_encoding = encoding.unwrap_or(UiTransactionEncoding::Base58);
            let binary_encoding = tx_encoding.into_binary_encoding().ok_or_else(|| {
                Error::invalid_params(format!(
                    "unsupported encoding: {}. Supported encodings: base58, base64",
                    tx_encoding
                ))
            })?;
            let (wire_transaction, transaction) =
                decode_and_deserialize::<VersionedTransaction>(data.clone(), binary_encoding)?;

            {
                let mut lock = meta.signature_status.write().unwrap();
                lock.insert(transaction.signatures[0].to_string(), None);
                println!("added {} to map", transaction.signatures[0].to_string());
            }
            meta.tpu_client
                .send_wire_transaction(wire_transaction.clone());
            Ok(transaction.signatures[0].to_string())
        }

        fn get_recent_blockhash(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<RpcBlockhashFeeCalculator>> {
            let recent_block_hash = meta.rpc_client.get_latest_blockhash().unwrap();
            let lamport_per_signature = meta
                .rpc_client
                .get_fee_calculator_for_blockhash(&recent_block_hash)
                .unwrap();
            let slot = meta.rpc_client.get_slot().unwrap();
            Ok(RpcResponse {
                context: RpcResponseContext::new(slot),
                value: RpcBlockhashFeeCalculator {
                    blockhash: recent_block_hash.to_string(),
                    fee_calculator: lamport_per_signature.unwrap(),
                },
            })
        }

        fn confirm_transaction(
            &self,
            meta: Self::Metadata,
            signature_str: String,
            commitment_cfg: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<bool>> {
            let lock = meta.signature_status.read().unwrap();
            let k_value = lock.get_key_value(&signature_str);
            let commitment = match commitment_cfg {
                Some(x) => x.commitment,
                None => CommitmentLevel::Confirmed,
            };
            let confirmed_slot = meta.confirmation_slot.load(Ordering::Relaxed);
            let finalized_slot = meta.finalized_slot.load(Ordering::Relaxed);

            match k_value {
                Some(value) => match value.1 {
                    Some(commitmentForSignature) => {
                        let slot = if commitment == CommitmentLevel::Finalized {
                            finalized_slot
                        } else {
                            confirmed_slot
                        };
                        println!("found in cache");
                        return Ok(RpcResponse {
                            context: RpcResponseContext::new(slot),
                            value: if commitment.eq(&CommitmentLevel::Finalized) {
                                commitmentForSignature.eq(&CommitmentLevel::Finalized)
                            } else {
                                commitmentForSignature.eq(&CommitmentLevel::Finalized)
                                    || commitmentForSignature.eq(&CommitmentLevel::Confirmed)
                            },
                        });
                    }
                    None => {
                        return Ok(RpcResponse {
                            context: RpcResponseContext::new(confirmed_slot),
                            value: false,
                        })
                    }
                },
                None => {
                    let signature = Signature::from_str(signature_str.as_str()).unwrap();
                    let ans = match commitment_cfg {
                        None => meta.rpc_client.confirm_transaction(&signature).unwrap(),
                        Some(cfg) => meta.rpc_client.confirm_transaction_with_commitment(&signature, cfg).unwrap().value
                    };
                    return Ok(RpcResponse{
                        context: RpcResponseContext::new(confirmed_slot),
                        value : ans
                    })
                },
            };
            
        }
    }
}

const MAX_BASE58_SIZE: usize = 1683; // Golden, bump if PACKET_DATA_SIZE changes
const MAX_BASE64_SIZE: usize = 1644; // Golden, bump if PACKET_DATA_SIZE changes
fn decode_and_deserialize<T>(
    encoded: String,
    encoding: TransactionBinaryEncoding,
) -> Result<(Vec<u8>, T)>
where
    T: serde::de::DeserializeOwned,
{
    let wire_output = match encoding {
        TransactionBinaryEncoding::Base58 => {
            if encoded.len() > MAX_BASE58_SIZE {
                return Err(Error::invalid_params(format!(
                    "base58 encoded {} too large: {} bytes (max: encoded/raw {}/{})",
                    type_name::<T>(),
                    encoded.len(),
                    MAX_BASE58_SIZE,
                    PACKET_DATA_SIZE,
                )));
            }
            bs58::decode(encoded)
                .into_vec()
                .map_err(|e| Error::invalid_params(format!("invalid base58 encoding: {:?}", e)))?
        }
        TransactionBinaryEncoding::Base64 => {
            if encoded.len() > MAX_BASE64_SIZE {
                return Err(Error::invalid_params(format!(
                    "base64 encoded {} too large: {} bytes (max: encoded/raw {}/{})",
                    type_name::<T>(),
                    encoded.len(),
                    MAX_BASE64_SIZE,
                    PACKET_DATA_SIZE,
                )));
            }
            base64::decode(encoded)
                .map_err(|e| Error::invalid_params(format!("invalid base64 encoding: {:?}", e)))?
        }
    };
    if wire_output.len() > PACKET_DATA_SIZE {
        return Err(Error::invalid_params(format!(
            "decoded {} too large: {} bytes (max: {} bytes)",
            type_name::<T>(),
            wire_output.len(),
            PACKET_DATA_SIZE
        )));
    }
    bincode::options()
        .with_limit(PACKET_DATA_SIZE as u64)
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize_from(&wire_output[..])
        .map_err(|err| {
            Error::invalid_params(format!(
                "failed to deserialize {}: {}",
                type_name::<T>(),
                &err.to_string()
            ))
        })
        .map(|output| (wire_output, output))
}

pub fn main() {
    let matches = cli::build_args(solana_version::version!()).get_matches();
    let cli_config = cli::extract_args(&matches);

    let cli::Config {
        json_rpc_url,
        websocket_url,
        rpc_addr,
        subsription_port,
        ..
    } = &cli_config;

    let mut io = MetaIoHandler::default();
    let lite_rpc = lite_rpc::LightRpc;
    io.extend_with(lite_rpc.to_delegate());

    let request_processor =
        LightRpcRequestProcessor::new(json_rpc_url, websocket_url, subsription_port.clone());

    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .on_thread_start(move || renice_this_thread(0).unwrap())
            .thread_name("solRpcEl")
            .enable_all()
            .build()
            .expect("Runtime"),
    );
    let max_request_body_size: usize = 50 * (1 << 10);
    let socket_addr = rpc_addr.clone();

    let server =
        ServerBuilder::with_meta_extractor(io, move |_req: &hyper::Request<hyper::Body>| {
            request_processor.clone()
        })
        .event_loop_executor(runtime.handle().clone())
        .threads(1)
        .cors(DomainsValidation::AllowOnly(vec![
            AccessControlAllowOrigin::Any,
        ]))
        .cors_max_age(86400)
        .max_request_body_size(max_request_body_size)
        .start_http(&socket_addr);
    println!("Starting Lite RPC node");
    server.unwrap().wait();
}



// #[cfg(test)]
// mod tests {
//     use {
//         super::*,
//         solana_sdk::{
//             native_token::LAMPORTS_PER_SOL, signature::Signer, signer::keypair::Keypair,
//             system_instruction, transaction::Transaction,
//         },
//         std::{thread, time::Duration},
//     };

//     const RPC_ADDR: &str = "http://127.0.0.1:8899";
//     const WS_ADDR : &str = "ws://127.0.0.1:8900";
//     const LITE_RPC_PORT : SocketAddr = SocketAddr::from(([127,0,0,1], 10800));

//     const SUBSCRIPTION_RPC_PORT : SocketAddr = SocketAddr::from(([127,0,0,1], 10801));

//     fn start() {
//         let mut io = MetaIoHandler::default();
//         let lite_rpc = lite_rpc::LightRpc;
//         io.extend_with(lite_rpc.to_delegate());

//         let request_processor =
//             LightRpcRequestProcessor::new(&RPC_ADDR.to_string(), &WS_ADDR.to_string(), SUBSCRIPTION_RPC_PORT.clone());

//         let runtime = Arc::new(
//             tokio::runtime::Builder::new_multi_thread()
//                 .worker_threads(1)
//                 .on_thread_start(move || renice_this_thread(0).unwrap())
//                 .thread_name("solRpcEl")
//                 .enable_all()
//                 .build()
//                 .expect("Runtime"),
//         );
//         let max_request_body_size: usize = 50 * (1 << 10);
//         let socket_addr = LITE_RPC_PORT.clone();

//         let server =
//             ServerBuilder::with_meta_extractor(io, move |_req: &hyper::Request<hyper::Body>| {
//                 request_processor.clone()
//             })
//             .event_loop_executor(runtime.handle().clone())
//             .threads(1)
//             .cors(DomainsValidation::AllowOnly(vec![
//                 AccessControlAllowOrigin::Any,
//             ]))
//             .cors_max_age(86400)
//             .max_request_body_size(max_request_body_size)
//             .start_http(&socket_addr);
//         server.unwrap().wait();

//     }

//     // Should start rpc with the following configuration enabled for the moment `--rpc-pubsub-enable-block-subscription`
//     #[test]
//     fn initialize_light_rpc_http_sever() {
//         Builder::new().name("Server".to_string()).spawn(|| {
//             start();
//         })
//     }
//     #[test]
//     fn test_forward_transaction_confirm_transaction() {
        

//         let alice = Keypair::new();
//         let bob = Keypair::new();

//         let lamports = 1_000_000;

//         let sig = forward_transaction_sender(&light_rpc, lamports, 100);
//         let x = confirm_transaction_sender(&light_rpc, sig, 300);
//         println!("{:#?}", x);
//     }
// }
