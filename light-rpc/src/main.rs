use lite_rpc::Lite;
use solana_client::{
    pubsub_client::{BlockSubscription, PubsubClientError},
    tpu_client::TpuClientConfig,
};
use solana_pubsub_client::pubsub_client::{PubsubBlockClientSubscription, PubsubClient};
use std::thread::{Builder, JoinHandle};

use {
    bincode::config::Options,
    crossbeam_channel::Receiver,
    jsonrpc_core::{Error, MetaIoHandler, Metadata, Result},
    jsonrpc_derive::rpc,
    jsonrpc_http_server::{hyper, AccessControlAllowOrigin, DomainsValidation, ServerBuilder},
    solana_client::{rpc_client::RpcClient, tpu_client::TpuClient},
    solana_perf::packet::PACKET_DATA_SIZE,
    solana_rpc_client_api::{
        config::*,
        response::{Response as RpcResponse, *},
    },
    solana_sdk::{
        commitment_config::{CommitmentConfig, CommitmentLevel},
        signature::Signature,
        transaction::VersionedTransaction,
    },
    solana_tpu_client::connection_cache::ConnectionCache,
    solana_transaction_status::{TransactionBinaryEncoding, UiTransactionEncoding},
    std::{
        any::type_name,
        collections::HashMap,
        net::SocketAddr,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, RwLock,
        },
    },
};

use solana_perf::thread::renice_this_thread;
mod cli;

pub struct BlockInformation {
    pub block_hash: RwLock<String>,
    pub block_height: AtomicU64,
    pub slot: AtomicU64,
    pub confirmation_level: CommitmentLevel,
}

impl BlockInformation {
    pub fn new(rpc_client: Arc<RpcClient>, commitment: CommitmentLevel) -> Self {

        let slot = rpc_client
            .get_slot_with_commitment(CommitmentConfig {
                commitment: commitment,
            })
            .unwrap();

        let (blockhash, blockheight) = rpc_client.get_latest_blockhash_with_commitment(CommitmentConfig { commitment }).unwrap();

        BlockInformation{
            block_hash: RwLock::new(blockhash.to_string()),
            block_height: AtomicU64::new(blockheight),
            slot: AtomicU64::new(slot),
            confirmation_level : commitment,
        }
    }
}

#[derive(Clone)]
pub struct LightRpcRequestProcessor {
    pub rpc_client: Arc<RpcClient>,
    pub tpu_client: Arc<TpuClient>,
    pub last_valid_block_height: u64,
    pub ws_url: String,
    connection_cache: Arc<ConnectionCache>,
    pub signature_status: Arc<RwLock<HashMap<String, Option<CommitmentLevel>>>>,
    pub subscribed_clients: Arc<Vec<PubsubBlockClientSubscription>>,
    pub finalized_block_info: Arc<BlockInformation>,
    pub confirmed_block_info: Arc<BlockInformation>,
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

        // subscribe for confirmed_blocks
        let (client_confirmed, receiver_confirmed) =
            Self::subscribe_block(websocket_url, CommitmentLevel::Confirmed).unwrap();

        // subscribe for finalized blocks
        let (client_finalized, receiver_finalized) =
            Self::subscribe_block(websocket_url, CommitmentLevel::Finalized).unwrap();

        let signature_status = Arc::new(RwLock::new(HashMap::new()));

        let blockinfo_confirmed = Arc::new(BlockInformation::new(rpc_client.clone(), CommitmentLevel::Confirmed));
        let blockinfo_finalized = Arc::new(BlockInformation::new(rpc_client.clone(), CommitmentLevel::Confirmed));

        // create threads to listen for finalized and confrimed blocks
        let joinables = vec![
            Self::build_thread_to_process_blocks(receiver_confirmed, &signature_status, CommitmentLevel::Confirmed, &blockinfo_confirmed),
            Self::build_thread_to_process_blocks(receiver_finalized, &signature_status, CommitmentLevel::Finalized, &blockinfo_finalized),
         ];

        LightRpcRequestProcessor {
            rpc_client,
            tpu_client,
            last_valid_block_height: 0,
            ws_url: websocket_url.clone(),
            signature_status,
            connection_cache: connection_cache,
            subscribed_clients: Arc::new(vec![client_confirmed, client_finalized]),
            joinables: Arc::new(joinables),
            confirmed_block_info: blockinfo_confirmed,
            finalized_block_info: blockinfo_finalized, 
        }
    }

    fn subscribe_block(
        websocket_url: &String,
        commitment: CommitmentLevel,
    ) -> std::result::Result<BlockSubscription, PubsubClientError> {
        PubsubClient::block_subscribe(
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
    }

    fn build_thread_to_process_blocks(reciever: Receiver<RpcResponse<RpcBlockUpdate>>,
        signature_status: &Arc<RwLock<HashMap<String, Option<CommitmentLevel>>>>,
        commitment : CommitmentLevel, 
        block_information : &Arc<BlockInformation>) -> JoinHandle<()>{

        let signature_status = signature_status.clone();
        let block_information = block_information.clone();
        Builder::new()
            .name("thread working on confirmation block".to_string())
            .spawn(move || {
                Self::process_block(
                    reciever,
                    signature_status,
                    commitment,
                    block_information,
                );
            })
            .unwrap()
    }

    fn process_block(
        reciever: Receiver<RpcResponse<RpcBlockUpdate>>,
        signature_status: Arc<RwLock<HashMap<String, Option<CommitmentLevel>>>>,
        commitment: CommitmentLevel,
        block_information: Arc<BlockInformation>,
    ) {
        println!("processing blocks for {}", commitment.to_string());
        loop {
            let block_data = reciever.recv();

            match block_data {
                Ok(data) => {
                    let block_update = &data.value;
                    block_information.slot.store(block_update.slot, Ordering::Relaxed);
                    
                    if let Some(block) = &block_update.block {

                        block_information.block_height.store(block.block_height.unwrap(), Ordering::Relaxed);
                        // context to update blockhash
                        {
                            let mut lock = block_information.block_hash.write().unwrap();
                            *lock = block.blockhash.clone();
                        }
                        if let Some(signatures) = &block.signatures {
                            let mut lock = signature_status.write().unwrap();
                            for signature in signatures {
                                if lock.contains_key(signature) {
                                    println!(
                                        "found signature {} for commitment {}",
                                        signature, commitment
                                    );
                                    lock.insert(signature.clone(), Some(commitment));
                                }
                            }
                        } else {
                            println!(
                                "Cannot get signatures at slot {} block hash {}",
                                block_update.slot, block.blockhash,
                            );
                        }
                    } else {
                        println!("Cannot get a block at slot {}", block_update.slot);
                    }
                }
                Err(e) => {
                    println!("Got error when recieving the block ({})", e.to_string());
                }
            }
        }
    }
}

impl Metadata for LightRpcRequestProcessor {}

pub mod lite_rpc {
    use std::str::FromStr;

    use solana_sdk::fee_calculator::FeeCalculator;

    use super::*;
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
            let commitment = match commitment {
                Some(x) => x.commitment,
                None => CommitmentLevel::Confirmed,
            };
            let (block_hash, slot) = match commitment {
                CommitmentLevel::Finalized => { 
                    let slot = meta.finalized_block_info.slot.load(Ordering::Relaxed);
                    let lock = meta.finalized_block_info.block_hash.read().unwrap();
                    (lock.clone(), slot)
                },
                _ => {

                    let slot = meta.confirmed_block_info.slot.load(Ordering::Relaxed);
                    let lock = meta.confirmed_block_info.block_hash.read().unwrap();
                    (lock.clone(), slot)
                }
            };

            Ok(RpcResponse {
                context: RpcResponseContext::new(slot),
                value: RpcBlockhashFeeCalculator {
                    blockhash: block_hash,
                    fee_calculator: FeeCalculator::default(),
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
            let confirmed_slot = meta.confirmed_block_info.slot.load(Ordering::Relaxed);
            let finalized_slot = meta.finalized_block_info.slot.load(Ordering::Relaxed);

            let slot = if commitment == CommitmentLevel::Finalized {
                finalized_slot
            } else {
                confirmed_slot
            };

            match k_value {
                Some(value) => match value.1 {
                    Some(commitment_for_signature) => {
                        println!("found in cache");
                        return Ok(RpcResponse {
                            context: RpcResponseContext::new(slot),
                            value: if commitment.eq(&CommitmentLevel::Finalized) {
                                commitment_for_signature.eq(&CommitmentLevel::Finalized)
                            } else {
                                commitment_for_signature.eq(&CommitmentLevel::Finalized)
                                    || commitment_for_signature.eq(&CommitmentLevel::Confirmed)
                            },
                        });
                    }
                    None => {
                        return Ok(RpcResponse {
                            context: RpcResponseContext::new(slot),
                            value: false,
                        })
                    }
                },
                None => {
                    let signature = Signature::from_str(signature_str.as_str()).unwrap();
                    let ans = match commitment_cfg {
                        None => meta.rpc_client.confirm_transaction(&signature).unwrap(),
                        Some(cfg) => {
                            meta.rpc_client
                                .confirm_transaction_with_commitment(&signature, cfg)
                                .unwrap()
                                .value
                        }
                    };
                    return Ok(RpcResponse {
                        context: RpcResponseContext::new(slot),
                        value: ans,
                    });
                }
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
        subscription_port,
        ..
    } = &cli_config;

    let mut io = MetaIoHandler::default();
    let lite_rpc = lite_rpc::LightRpc;
    io.extend_with(lite_rpc.to_delegate());

    let request_processor =
        LightRpcRequestProcessor::new(json_rpc_url, websocket_url, subscription_port.clone());

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
