use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_sdk::{commitment_config::CommitmentLevel, signature::Signature};

use {
    rayon::prelude::{IntoParallelRefIterator, ParallelIterator},
    solana_client::{
        client_error::ClientError, connection_cache::ConnectionCache, rpc_response::Response,
        thin_client::ThinClient,
    },
    solana_sdk::{commitment_config::CommitmentConfig, transaction::Transaction},
    std::{net::SocketAddr, sync::Arc},
};

pub enum ConfirmationStrategy {
    RpcConfirm,
}

pub struct LightRpc {
    pub connection_cache: Arc<ConnectionCache>,
    pub thin_client: ThinClient,
}

impl LightRpc {
    pub fn new(rpc_addr: SocketAddr, tpu_addr: SocketAddr, connection_pool_size: usize) -> Self {
        let connection_cache = Arc::new(ConnectionCache::new(connection_pool_size));
        let thin_client = ThinClient::new(rpc_addr, tpu_addr, connection_cache.clone());

        Self {
            connection_cache,
            thin_client,
        }
    }

    pub fn forward_transaction(&self, transaction: Transaction) -> Result<Signature, ClientError> {
        self.thin_client.rpc_client().send_transaction_with_config(
            &transaction,
            RpcSendTransactionConfig {
                skip_preflight: false,
                preflight_commitment: Some(CommitmentLevel::Processed),
                ..RpcSendTransactionConfig::default()
            },
        )
    }

    pub fn forward_transactions(
        &self,
        transactions: Vec<Transaction>,
    ) -> Vec<Result<Signature, ClientError>> {
        transactions
            .iter()
            .map(|tx| self.forward_transaction(tx.clone()))
            .collect()
    }

    pub fn confirm_transaction(
        &self,
        signature: &Signature,
        commitment_config: CommitmentConfig,
        confirmation_strategy: ConfirmationStrategy,
    ) -> Result<Response<bool>, ClientError> {
        match confirmation_strategy {
            ConfirmationStrategy::RpcConfirm => self
                .thin_client
                .rpc_client()
                .confirm_transaction_with_commitment(signature, commitment_config),
        }
    }
}

#[derive(Debug)]
pub struct ConfirmationStatus {
    pub signature: Signature,
    pub confirmation_status: bool,
}

pub fn confirm_transaction_sender(
    light_rpc: &LightRpc,
    signatures: Vec<Signature>,
    mut _retries: u16,
) -> Vec<ConfirmationStatus> {
    let x = signatures
        .par_iter()
        .map(|signature| {
            loop {
                let confirmed = light_rpc
                    .confirm_transaction(
                        signature,
                        CommitmentConfig::confirmed(),
                        ConfirmationStrategy::RpcConfirm,
                    )
                    .unwrap()
                    .value;
                if confirmed {
                    break ConfirmationStatus {
                        signature: *signature,
                        confirmation_status: confirmed,
                    };
                }
                //thread::sleep(Duration::from_millis(500))
            }
        })
        .collect();
    x
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_sdk::{
            native_token::LAMPORTS_PER_SOL, signature::Signer, signer::keypair::Keypair,
            system_instruction, transaction::Transaction,
        },
        std::{thread, time::Duration},
    };

    const RPC_ADDR: &str = "127.0.0.1:8899";
    const TPU_ADDR: &str = "127.0.0.1:1027";
    const CONNECTION_POOL_SIZE: usize = 1;

    #[test]
    fn initialize_light_rpc() {
        let _light_rpc = LightRpc::new(
            RPC_ADDR.parse().unwrap(),
            TPU_ADDR.parse().unwrap(),
            CONNECTION_POOL_SIZE,
        );
    }
}
