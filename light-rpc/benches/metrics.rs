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
    #[serde(rename = "start forward transaction time(ns)")]
    forward_start_time: u128,
    #[serde(rename = "end forward transaction time(ns)")]
    forward_end_time: u128,
    #[serde(rename = "Forward transaction duration(ns)")]
    forward_duration: u128,
    #[serde(rename = "start confirm transaction time(ns)")]
    confirm_start_time: u128,
    #[serde(rename = "end forward transaction time(ns)")]
    confirm_end_time: u128,
    #[serde(rename = "Confirm transaction duration(ns)")]
    confirm_duration: u128,
    #[serde(rename = "Total duration(ns)")]
    duration: u128,
}

fn test_forward_transaction_confirm_transaction(times: u64) {
    let light_rpc = LightRpc::new(
        RPC_ADDR.parse().unwrap(),
        TPU_ADDR.parse().unwrap(),
        CONNECTION_POOL_SIZE,
    );

    let lamports = 1_000_000;
    let mut data: Vec<Metrics> = vec![];

    let mut wtr = csv::Writer::from_path("metrics.csv").unwrap();
    let instant = SystemTime::now();
    for _ in 0..times {
        //generating a new keypair for each transaction
        let forward_start_time = instant.elapsed().unwrap().as_nanos();
        let signatures = light_rpc::forward_transaction_sender(&light_rpc, lamports, 10);
        let forward_end_time = instant.elapsed().unwrap().as_nanos();

        let confirm_start_time = instant.elapsed().unwrap().as_nanos();
        let confirmed = light_rpc::confirm_transaction_sender(&light_rpc, signatures, 300);
        let confirm_end_time = instant.elapsed().unwrap().as_nanos();

        let forward_duration = forward_end_time - forward_start_time;
        let confirm_duration = confirm_end_time - confirm_start_time;

        data.push(Metrics {
            forward_start_time,
            forward_end_time,
            forward_duration,
            confirm_start_time,
            confirm_end_time,
            confirm_duration,
            duration: forward_duration + confirm_duration,
        });
    }
    for d in data.into_iter() {
        wtr.serialize(d).unwrap();
    }
}
#[test]
fn dummy() {}

fn main() {
    test_forward_transaction_confirm_transaction(10);
}
