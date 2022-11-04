use {
    rayon::prelude::{IntoParallelRefIterator, IntoParallelRefMutIterator, ParallelIterator},
    solana_client::{
        client_error::ClientError, connection_cache::ConnectionCache, rpc_response::Response,
        thin_client::ThinClient,
    },
    solana_sdk::{
        client::AsyncClient,
        commitment_config::CommitmentConfig,
        native_token::LAMPORTS_PER_SOL,
        signature::Signature,
        signer::{keypair::Keypair, Signer},
        system_instruction,
        transaction::Transaction,
        transport::TransportError,
    },
    std::{net::SocketAddr, sync::Arc, thread, time::Duration},
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

    pub fn forward_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<Signature, TransportError> {
        self.thin_client.async_send_transaction(transaction)
    }

    pub fn forward_transactions(
        &self,
        transactions: Vec<Transaction>,
    ) -> Vec<Result<Signature, TransportError>> {
        transactions
            .par_iter()
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

//Two helper methods to forward a transaction and confirm it
pub fn forward_transaction_sender(
    light_rpc: &LightRpc,
    lamports: u64,
    num_transactions: u64,
) -> Vec<Signature> {
    let mut txs = vec![];

    for i in 0..num_transactions {
        //generate new keypair for each transaction
        let alice = Keypair::new();
        let bob = Keypair::new();
        let frompubkey = Signer::pubkey(&alice);
        let topubkey = Signer::pubkey(&bob);

        light_rpc
            .thin_client
            .rpc_client()
            .request_airdrop(&frompubkey, LAMPORTS_PER_SOL)
            .unwrap();
        let ix = system_instruction::transfer(&frompubkey, &topubkey, lamports);
        let recent_blockhash = light_rpc
            .thin_client
            .rpc_client()
            .get_latest_blockhash()
            .expect("Failed to get latest blockhash.");
        let txn = Transaction::new_signed_with_payer(
            &[ix],
            Some(&frompubkey),
            &[&alice],
            recent_blockhash,
        );
        txs.push(txn);
    }

    let p = light_rpc.forward_transactions(txs);
    p.into_iter().map(|p| p.unwrap()).collect()
}
#[derive(Debug)]
pub struct ConfirmationStatus {
    signature: Signature,
    confirmation_status: bool,
}
pub fn confirm_transaction_sender(
    light_rpc: &LightRpc,
    signatures: Vec<Signature>,
    mut retries: u16,
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
    #[test]
    fn test_forward_transaction_confirm_transaction() {
        let light_rpc = LightRpc::new(
            RPC_ADDR.parse().unwrap(),
            TPU_ADDR.parse().unwrap(),
            CONNECTION_POOL_SIZE,
        );
        let alice = Keypair::new();
        let bob = Keypair::new();

        let lamports = 1_000_000;

        let sig = forward_transaction_sender(&light_rpc, lamports, 100);
        let x = confirm_transaction_sender(&light_rpc, sig, 300);
        println!("{:#?}", x);
    }
}
