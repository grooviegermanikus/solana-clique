use futures::future::join_all;

use solana_client::{
    nonblocking::{rpc_client::RpcClient, tpu_client::TpuClient},
    rpc_response::RpcResult,
};
use solana_sdk::signature::Signature;

use {
    solana_client::connection_cache::ConnectionCache,
    solana_sdk::{commitment_config::CommitmentConfig, transaction::Transaction},
    std::sync::Arc,
};

pub enum ConfirmationStrategy {
    RpcConfirm,
}

pub struct LightRpc {
    pub rpc_client: Arc<RpcClient>,
    pub tpu_client: Arc<TpuClient>,
}

impl LightRpc {
    pub async fn new(rpc_url: String, websocket_url: String) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::processed(),
        ));
        let connection_cache = Arc::new(ConnectionCache::default());
        let tpu_client = Arc::new(
            TpuClient::new_with_connection_cache(
                rpc_client.clone(),
                &websocket_url,
                solana_client::tpu_client::TpuClientConfig::default(),
                connection_cache,
            )
            .await
            .unwrap(),
        );
        Self {
            rpc_client,
            tpu_client,
        }
    }

    pub async fn forward_transaction(&self, transaction: Transaction) -> Signature {
        self.tpu_client.send_transaction(&transaction).await;
        transaction.signatures[0]
    }

    pub async fn forward_transactions(&self, transactions: Vec<Transaction>) -> Vec<Signature> {
        join_all(
            transactions
                .iter()
                .map(|tx| self.forward_transaction(tx.clone())),
        )
        .await
    }

    pub async fn confirm_transaction(
        &self,
        signature: &Signature,
        commitment_config: CommitmentConfig,
        confirmation_strategy: ConfirmationStrategy,
    ) -> RpcResult<bool> {
        match confirmation_strategy {
            ConfirmationStrategy::RpcConfirm => {
                self.rpc_client
                    .confirm_transaction_with_commitment(signature, commitment_config)
                    .await
            }
        }
    }
}

#[derive(Debug)]
pub struct ConfirmationStatus {
    pub signature: Signature,
    pub confirmation_status: bool,
}

pub async fn confirm_transaction_sender(
    light_rpc: &LightRpc,
    signatures: Vec<Signature>,
    mut _retries: u16,
) -> Vec<ConfirmationStatus> {
    let rpc = Arc::new(light_rpc);
    join_all(signatures.iter().map(|signature| async {
        let rpc = rpc.clone();
        loop {
            let confirmed = rpc
                .confirm_transaction(
                    signature,
                    CommitmentConfig::confirmed(),
                    ConfirmationStrategy::RpcConfirm,
                )
                .await
                .unwrap()
                .value;
            if confirmed {
                return ConfirmationStatus {
                    signature: *signature,
                    confirmation_status: confirmed,
                };
            }
            //thread::sleep(Duration::from_millis(500))
        }
    }))
    .await
    .into()
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

    #[test]
    fn initialize_light_rpc() {
        let _light_rpc = LightRpc::new(RPC_ADDR.parse().unwrap(), TPU_ADDR.parse().unwrap());
    }
}
