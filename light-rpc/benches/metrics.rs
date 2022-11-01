use {
    bencher::{benchmark_group, benchmark_main, Bencher, TestDescAndFn},
    csv::Writer,
    light_rpc::LightRpc,
    serde::Serialize,
    solana_sdk::{
        commitment_config::CommitmentConfig,
        native_token::LAMPORTS_PER_SOL,
        pubkey::Pubkey,
        signature::{Signature, Signer},
        signer::keypair::Keypair,
        system_instruction,
        transaction::Transaction,
    },
    std::{fs::OpenOptions, io::Write},
};

const RPC_ADDR: &str = "127.0.0.1:8899";
const TPU_ADDR: &str = "127.0.0.1:1027";
const CONNECTION_POOL_SIZE: usize = 1;

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
    }
    false
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
            let confirmed = confirm_transaction_sender(&light_rpc, &sig, 240);
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
fn bench_forward_transaction_confirm_transaction(b: &mut Bencher) {
    let light_rpc = LightRpc::new(
        RPC_ADDR.parse().unwrap(),
        TPU_ADDR.parse().unwrap(),
        CONNECTION_POOL_SIZE,
    );
    let alice = Keypair::new();
    let frompubkey = Signer::pubkey(&alice);

    let bob = Keypair::new();
    let topubkey = Signer::pubkey(&bob);

    let lamports = 1_000_00;
    let mut sig = Signature::default();
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open("metrics.csv")
        .unwrap();
    let mut wtr = csv::Writer::from_writer(file);
    b.iter(|| {
        sig = forward_transaction_sender(&light_rpc, &alice, &bob, lamports);
    });
    let forwarder_time = (b.ns_per_iter() as f64) / 1000000 as f64;
    b.iter(|| {
        confirm_transaction_sender(&light_rpc, &sig, 240);
    });
    let confirm_time = (b.ns_per_iter() as f64) / 1000000 as f64;

    wtr.write_record(&[
        forwarder_time.to_string(),
        confirm_time.to_string(),
        (forwarder_time - confirm_time).abs().to_string(),
    ])
    .unwrap();
}
benchmark_group!(benches, bench_forward_transaction_confirm_transaction);
benchmark_main!(benches);
