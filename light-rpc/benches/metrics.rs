#[cfg(test)]
use {
    light_rpc::LightRpc,
    solana_sdk::commitment_config::CommitmentConfig,
    solana_sdk::signature::Signature,
    solana_sdk::{
        native_token::LAMPORTS_PER_SOL, signature::Signer, signer::keypair::Keypair,
        system_instruction, transaction::Transaction,
    },
    std::time::SystemTime,
    std::{thread, time::Duration},
};

const RPC_ADDR: &str = "127.0.0.1:8899";
const TPU_ADDR: &str = "127.0.0.1:1027";
const CONNECTION_POOL_SIZE: usize = 1;

#[derive(serde::Serialize)]
struct Metrics {
    #[serde(rename = "start time(ns)")]
    start_time: u128,
    #[serde(rename = "end time(ns)")]
    end_time: u128,
    #[serde(rename = "duration(ns)")]
    duration: u128,
}

fn test_forward_transaction_confirm_transaction(tps: u64) {
    let light_rpc = LightRpc::new(
        RPC_ADDR.parse().unwrap(),
        TPU_ADDR.parse().unwrap(),
        CONNECTION_POOL_SIZE,
    );
    let alice = Keypair::new();
    let bob = Keypair::new();

    let lamports = 1_000_000;
    let mut data: Vec<Metrics> = vec![];

    let mut wtr = csv::Writer::from_path("metrics.csv").unwrap();
    let instant = SystemTime::now();
    for _ in 0..tps {
        let start_time = instant.elapsed().unwrap().as_nanos();

        let signatures = light_rpc::forward_transaction_sender(&light_rpc, &alice, &bob, lamports);
        let confirmed = light_rpc::confirm_transaction_sender(&light_rpc, signatures, 300);

        let end_time = instant.elapsed().unwrap().as_nanos();

        data.push(Metrics {
            start_time,
            end_time,
            duration: end_time - start_time,
        });
    }
    for d in data.into_iter() {
        wtr.serialize(d).unwrap();
    }
}
#[test]
fn dummy() {}

fn main() {
    test_forward_transaction_confirm_transaction(100);
}
