use lite_rpc::Lite;
use solana_client::{thin_client::ThinClient, rpc_client, tpu_client::TpuClientConfig};
use solana_sdk::client::AsyncClient;

use {
    solana_client::{rpc_client::RpcClient, rpc_config::RpcBlockConfig,
        tpu_client::TpuClient,
    },
    bincode::{config::Options, serialize},
    crossbeam_channel::{unbounded, Receiver, Sender},
    jsonrpc_core::{futures::future, types::error, BoxFuture, Error, Metadata, Result, MetaIoHandler},
    jsonrpc_derive::rpc,
    serde::{Deserialize, Serialize},
    
    solana_entry::entry::Entry,
    solana_faucet::faucet::request_airdrop_transaction,
    solana_gossip::{cluster_info::ClusterInfo, contact_info::ContactInfo},
    solana_ledger::{
        blockstore::{Blockstore, SignatureInfosForAddress},
        blockstore_db::BlockstoreError,
        get_tmp_ledger_path,
        leader_schedule_cache::LeaderScheduleCache,
    },
    solana_perf::packet::PACKET_DATA_SIZE,
    solana_rpc_client_api::{
        config::*,
        custom_error::RpcCustomError,
        deprecated_config::*,
        filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
        request::{
            TokenAccountsFilter, DELINQUENT_VALIDATOR_SLOT_DISTANCE,
            MAX_GET_CONFIRMED_BLOCKS_RANGE, MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS2_LIMIT,
            MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS_SLOT_RANGE, MAX_GET_PROGRAM_ACCOUNT_FILTERS,
            MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS, MAX_GET_SLOT_LEADERS, MAX_MULTIPLE_ACCOUNTS,
            MAX_RPC_VOTE_ACCOUNT_INFO_EPOCH_CREDITS_HISTORY, NUM_LARGEST_ACCOUNTS,
        },
        response::{Response as RpcResponse, *},
    },
    solana_runtime::{
        accounts::AccountAddressFilter,
        accounts_index::{AccountIndex, AccountSecondaryIndexes, IndexKey, ScanConfig},
        bank::{Bank, TransactionSimulationResult},
        bank_forks::BankForks,
        commitment::{BlockCommitmentArray, BlockCommitmentCache, CommitmentSlots},
        inline_spl_token::{SPL_TOKEN_ACCOUNT_MINT_OFFSET, SPL_TOKEN_ACCOUNT_OWNER_OFFSET},
        inline_spl_token_2022::{self, ACCOUNTTYPE_ACCOUNT},
        non_circulating_supply::calculate_non_circulating_supply,
        prioritization_fee_cache::PrioritizationFeeCache,
        snapshot_config::SnapshotConfig,
        snapshot_utils,
    },
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        account_utils::StateMut,
        clock::{Slot, UnixTimestamp, MAX_RECENT_BLOCKHASHES},
        commitment_config::{CommitmentConfig, CommitmentLevel},
        epoch_info::EpochInfo,
        epoch_schedule::EpochSchedule,
        exit::Exit,
        feature_set,
        fee_calculator::FeeCalculator,
        hash::Hash,
        message::SanitizedMessage,
        pubkey::{Pubkey, PUBKEY_BYTES},
        signature::{Keypair, Signature, Signer},
        stake::state::{StakeActivationStatus, StakeState},
        stake_history::StakeHistory,
        system_instruction,
        sysvar::stake_history,
        transaction::{
            self, AddressLoader, MessageHash, SanitizedTransaction, TransactionError,
            VersionedTransaction, MAX_TX_ACCOUNT_LOCKS,
        },
    },
    solana_send_transaction_service::{
        send_transaction_service::{SendTransactionService, TransactionInfo},
        tpu_info::NullTpuInfo,
    },
    solana_stake_program,
    solana_storage_bigtable::Error as StorageError,
    solana_streamer::socket::SocketAddrSpace,
    solana_tpu_client::connection_cache::ConnectionCache,
    solana_transaction_status::{
        BlockEncodingOptions, ConfirmedBlock, ConfirmedTransactionStatusWithSignature,
        ConfirmedTransactionWithStatusMeta, EncodedConfirmedTransactionWithStatusMeta, Reward,
        RewardType, TransactionBinaryEncoding, TransactionConfirmationStatus, TransactionStatus,
        UiConfirmedBlock, UiTransactionEncoding,
    },
    solana_vote_program::vote_state::{VoteState, MAX_LOCKOUT_HISTORY},
    spl_token_2022::{
        extension::StateWithExtensions,
        solana_program::program_pack::Pack,
        state::{Account as TokenAccount, Mint},
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
    jsonrpc_http_server::{
        hyper, AccessControlAllowOrigin, CloseHandle, DomainsValidation, RequestMiddleware,
        RequestMiddlewareAction, ServerBuilder,
    },
};

use solana_perf::thread::renice_this_thread;
mod cli;

#[derive(Clone)]
pub struct LightRpcRequestProcessor{
    pub rpc_client : Arc<RpcClient>,
    pub tpu_client : Arc<TpuClient>,
    pub last_valid_block_height : u64,
    pub ws_url : String,
}

impl LightRpcRequestProcessor {
    pub fn new(json_rpc_url : &String, websocket_url : &String) -> LightRpcRequestProcessor{

        let rpc_client = Arc::new(RpcClient::new(json_rpc_url.as_str()));
        let tpu_client = Arc::new(TpuClient::new(rpc_client.clone(), websocket_url.as_str(), TpuClientConfig::default()).unwrap());
        
        LightRpcRequestProcessor{
            rpc_client,
            tpu_client,
            last_valid_block_height : 0,   
            ws_url : websocket_url.clone(), 
        }
    }
}

impl Metadata for LightRpcRequestProcessor {}

pub mod lite_rpc {
    use {
        super::*,
        solana_sdk::message::{VersionedMessage},
    };
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

            if !meta.tpu_client.send_wire_transaction(wire_transaction.clone()) {
                // issue in transport layer / reset TPU client
                let tpu_client = TpuClient::new(meta.rpc_client, meta.ws_url.as_str(), TpuClientConfig::default()).unwrap();
                tpu_client.send_wire_transaction(wire_transaction);
            }
            Ok(transaction.signatures[0].to_string())
        }

        fn get_recent_blockhash(
            &self,
            meta: Self::Metadata,
            commitment: Option<CommitmentConfig>,
        ) -> Result<RpcResponse<RpcBlockhashFeeCalculator>> {
            let recent_block_hash = meta.rpc_client.get_latest_blockhash().unwrap();
            let lamport_per_signature = meta.rpc_client.get_fee_calculator_for_blockhash(&recent_block_hash).unwrap();
            let slot  = meta.rpc_client.get_slot().unwrap();
            Ok(
                RpcResponse {
                    context: RpcResponseContext::new(slot),
                    value : RpcBlockhashFeeCalculator {
                        blockhash: recent_block_hash.to_string(),
                        fee_calculator: lamport_per_signature.unwrap(),
                    },
                }
            )
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
        ..
    } = &cli_config;

    let mut io = MetaIoHandler::default();
    let lite_rpc = lite_rpc::LightRpc;
    io.extend_with(lite_rpc.to_delegate());

    let request_processor = LightRpcRequestProcessor::new(json_rpc_url, websocket_url);

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

    let server = ServerBuilder::with_meta_extractor(
            io,
            move |_req: &hyper::Request<hyper::Body>| request_processor.clone(),
        )
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
