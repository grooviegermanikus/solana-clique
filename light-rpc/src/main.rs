use std::sync::Arc;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::nonblocking::tpu_client::TpuClient;

const RPC_ADDR: &str = "http://localhost:8899";
const WEBSOCKET_ADDR: &str = "ws://localhost:8900";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let rpc_client = Arc::new(RpcClient::new(RPC_ADDR.to_string()));
    let _tpu_client = TpuClient::new(rpc_client, WEBSOCKET_ADDR, Default::default()).await?;

    Ok(())
}
