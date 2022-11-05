#[cfg(test)]
use {
    light_rpc::LightRpc,
    solana_sdk::{
        signature::Signer, signer::keypair::Keypair, system_instruction, transaction::Transaction,
    },
    std::time::*,
    std::{thread, time::Duration},
};

const RPC_ADDR: &str = "127.0.0.1:8899";
const TPU_ADDR: &str = "0.0.0.0:1027";
const CONNECTION_POOL_SIZE: usize = 1;
const NUM_KEYPAIRS: u64 = 16;
const RENT_EXEMPT_MINIMUM: u64 = 890_880;
const LAMPORTS_PER_TX: u64 = 5_000;

#[derive(serde::Serialize)]
struct Metrics {
    #[serde(rename = "start forward transaction time(ms)")]
    forward_start_time: u128,
    #[serde(rename = "end forward transaction time(ms)")]
    forward_end_time: u128,
    #[serde(rename = "Forward transaction duration(ms)")]
    forward_duration: u128,
    #[serde(rename = "start confirm transaction time(ms)")]
    confirm_start_time: u128,
    #[serde(rename = "end confirm transaction time(ms)")]
    confirm_end_time: u128,
    #[serde(rename = "Confirm transaction duration(ms)")]
    confirm_duration: u128,
    #[serde(rename = "Total duration(ms)")]
    duration: u128,
}

fn test_forward_transaction_confirm_transaction(times: u64) {
    let light_rpc = LightRpc::new(
        RPC_ADDR.parse().unwrap(),
        TPU_ADDR.parse().unwrap(),
        CONNECTION_POOL_SIZE,
    );

    let mut keypairs: Vec<Keypair> = vec![];
    for _ in 0..NUM_KEYPAIRS {
        let k = Keypair::new();
        let signature = light_rpc
            .thin_client
            .rpc_client()
            .request_airdrop(&k.pubkey(), RENT_EXEMPT_MINIMUM + LAMPORTS_PER_TX * times)
            .unwrap();
        println!(
            "airdrop keypair:{} signature:{}",
            k.pubkey().to_string(),
            signature.to_string()
        );
        keypairs.push(k);
    }

    println!("wait 30 seconds to start benchmark");
    thread::sleep(Duration::from_secs(30));

    let mut metrics: Vec<Metrics> = vec![];
    let instant = SystemTime::now();
    for _ in 0..times {
        let recent_blockhash = light_rpc
            .thin_client
            .rpc_client()
            .get_latest_blockhash()
            .expect("Failed to get latest blockhash.");

        println!("new blockhash:{}", recent_blockhash);

        // pre-create transactions to avoid signature costs
        let mut txs = vec![];
        for keypair in keypairs.iter() {
            let ix = system_instruction::transfer(&keypair.pubkey(), &keypair.pubkey(), 0);
            let tx = Transaction::new_signed_with_payer(
                &[ix],
                Some(&keypair.pubkey()),
                &[keypair],
                recent_blockhash,
            );
            txs.push(tx);
        }

        // forward whole batch
        let forward_start_time = instant.elapsed().unwrap().as_millis();
        let signatures = light_rpc
            .forward_transactions(txs)
            .into_iter()
            .map(|p| p.unwrap())
            .collect();
        let forward_end_time = instant.elapsed().unwrap().as_millis();

        // confirm whole bactch
        let confirm_start_time = instant.elapsed().unwrap().as_millis();
        let _confirmed = light_rpc::confirm_transaction_sender(&light_rpc, signatures, 300);
        let confirm_end_time = instant.elapsed().unwrap().as_millis();

        // collect metrics
        let forward_duration = forward_end_time - forward_start_time;
        let confirm_duration = confirm_end_time - confirm_start_time;
        metrics.push(Metrics {
            forward_start_time,
            forward_end_time,
            forward_duration,
            confirm_start_time,
            confirm_end_time,
            confirm_duration,
            duration: forward_duration + confirm_duration,
        });
    }

    // save metrics in file
    println!("saving metrics to metrics.csv");
    let mut wtr = csv::Writer::from_path("metrics.csv").unwrap();
    for d in metrics.into_iter() {
        wtr.serialize(d).unwrap();
    }
}
#[test]
fn dummy() {}

fn main() {
    test_forward_transaction_confirm_transaction(8);
}
