use light_rpc::LightRpc;

const RPC_ADDR: &str = "127.0.0.1:8899";
const TPU_ADDR: &str = "127.0.0.1:1027";
const CONNECTION_POOL_SIZE: usize = 1;

#[tokio::main]
pub async fn main() {
    let _light_rpc = LightRpc::new(RPC_ADDR.parse().unwrap(), TPU_ADDR.parse().unwrap(), CONNECTION_POOL_SIZE);
}

