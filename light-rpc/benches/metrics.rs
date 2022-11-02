use std::time::SystemTime;
#[cfg(test)]
use {
    light_rpc::LightRpc,
    solana_sdk::commitment_config::CommitmentConfig,
    solana_sdk::signature::Signature,
    solana_sdk::{
        native_token::LAMPORTS_PER_SOL, signature::Signer, signer::keypair::Keypair,
        system_instruction, transaction::Transaction,
    },
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
        }
        Err(_) => println!("Error requesting airdrop"),
    };

    let ix = system_instruction::transfer(&frompubkey, &topubkey, lamports);
    let recent_blockhash = light_rpc
        .thin_client
        .rpc_client()
        .get_latest_blockhash()
        .expect("Failed to get latest blockhash.");
    let txn =
        Transaction::new_signed_with_payer(&[ix], Some(&frompubkey), &[alice], recent_blockhash);

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

        let signature = forward_transaction_sender(&light_rpc, &alice, &bob, lamports);

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
