use {
    borsh::{BorshDeserialize, BorshSerialize},
    solana_client::{connection_cache::ConnectionCache, thin_client::ThinClient},
    solana_sdk::{
        client::AsyncClient,
        instruction::Instruction,
        message::Message,
        pubkey::Pubkey,
        signature::{Signature, Signer},
        signer::keypair::Keypair,
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
#[derive(BorshSerialize, BorshDeserialize)]
enum BankInstruction {
    Initialize,
    Deposit { lamports: u64 },
    Withdraw { lamports: u64 },
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
        program_id: Pubkey,
        payer: &Keypair,
    ) -> Result<Signature, TransportError> {
        let bankins = BankInstruction::Initialize;
        let instruction = Instruction::new_with_borsh(program_id, &bankins, vec![]);

        let message = Message::new(&[instruction], Some(&payer.pubkey()));
        let blockhash = self
            .thin_client
            .rpc_client()
            .get_latest_blockhash()
            .unwrap();
        let tx = Transaction::new(&[payer], message, blockhash);
        self.thin_client.async_send_transaction(tx)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::LightRpc,
        solana_sdk::{pubkey::Pubkey, signer::keypair::Keypair},
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
    fn test_forward_transaction() {
        let light_rpc = LightRpc::new(
            RPC_ADDR.parse().unwrap(),
            TPU_ADDR.parse().unwrap(),
            CONNECTION_POOL_SIZE,
        );
        let program_id = Pubkey::new_unique();
        let payer = Keypair::new();
        let x = light_rpc.forward_transaction(program_id, &payer).unwrap();
        println!("{}", x);
    }
}
