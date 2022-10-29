use {
    solana_client::{
        connection_cache::ConnectionCache, thin_client::ThinClient,
    },
    solana_sdk::{
        client::AsyncClient,
        signature::Signature,
        transaction::Transaction,
        transport::TransportError,
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

    pub fn confirm_transaction(&self, signature: &Signature) -> bool {
        let x = self
            .thin_client
            .rpc_client()
            .confirm_transaction(signature)
            .unwrap();
        x
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_sdk::{
            native_token::LAMPORTS_PER_SOL, signature::Signer,
            signer::keypair::Keypair, system_instruction,
            transaction::Transaction,
        }
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
        let frompubkey = Signer::pubkey(&alice);

        let bob = Keypair::new();
        let topubkey = Signer::pubkey(&bob);

        let lamports = 1_000_000;
        match light_rpc
            .thin_client
            .rpc_client()
            .request_airdrop(&frompubkey, LAMPORTS_PER_SOL)
        {
            Ok(sig) => loop {
                let confirmed = light_rpc.confirm_transaction(&sig);
                if confirmed {
                    println!("Request Airdrop Transaction: {} Confirmed",sig);
                    break;
                }
            },
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
            &[&alice],
            recent_blockhash,
        );

        let sig = light_rpc.forward_transaction(txn).unwrap();
        loop {
            let confirmed = light_rpc.confirm_transaction(&sig);
            if confirmed {
                println!("Transaction: {} Confirmed",sig);
                break;
            }
        }
    }
}
