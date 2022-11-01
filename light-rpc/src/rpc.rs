use {
    solana_client::{
        client_error::ClientError, connection_cache::ConnectionCache, rpc_response::Response,
        thin_client::ThinClient,
    },
    solana_sdk::{
        client::AsyncClient, commitment_config::CommitmentConfig, signature::Signature,
        transaction::Transaction, transport::TransportError,
    },
    std::{net::SocketAddr, sync::Arc},
};
// use solana_client::tpu_client::TpuSenderError;

// type Result<T> = std::result::Result<T, TpuSenderError>;

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

    pub fn confirm_transaction(
        &self,
        signature: &Signature,
        commitment_config: CommitmentConfig,
    ) -> Result<Response<bool>, ClientError> {
        self.thin_client
            .rpc_client()
            .confirm_transaction_with_commitment(signature, commitment_config)
    }
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

    fn forward_transaction_sender(
        light_rpc: &LightRpc,
        alice: &Keypair,
        bob: &Keypair,
        lamports: u64,
    ) -> Signature {
        let frompubkey = Signer::pubkey(alice);
        let topubkey = Signer::pubkey(bob);
        match light_rpc
            .thin_client
            .rpc_client()
            .request_airdrop(&frompubkey, LAMPORTS_PER_SOL)
        {
            Ok(sig) => {
                let confirmed = confirm_transaction_sender(&light_rpc, &sig, 300);
                println!("Transaction: {} Confirmed", sig);
            }
            Err(_) => println!("Error requesting airdrop"),
        };

        let ix = system_instruction::transfer(&frompubkey, &topubkey, lamports);
        let recent_blockhash = light_rpc
            .thin_client
            .rpc_client()
            .get_latest_blockhash()
            .expect("Failed to get latest blockhash.");
        let txn = Transaction::new_signed_with_payer(
            &[ix],
            Some(&frompubkey),
            &[alice],
            recent_blockhash,
        );

        let signature = light_rpc.forward_transaction(txn).unwrap();
        signature
    }

    fn confirm_transaction_sender(
        light_rpc: &LightRpc,
        signature: &Signature,
        mut retries: u16,
    ) -> bool {
        while retries > 0 {
            let confirmed = light_rpc
                .confirm_transaction(signature, CommitmentConfig::confirmed())
                .unwrap()
                .value;
            if confirmed {
                return true;
            }
            retries -= 1;
            thread::sleep(Duration::from_millis(500))
        }
        false
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

        let sig = forward_transaction_sender(&light_rpc, &alice, &bob, lamports);
        let res = confirm_transaction_sender(&light_rpc, &sig, 300);
        assert!(res)
    }
}
