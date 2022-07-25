use fixed::types::I80F48;
use log::*;
use mango::{
    instruction::{cancel_all_perp_orders, place_perp_order2},
    matching::Side,
    state::{MangoCache, MangoGroup, PerpMarket, QUOTE_INDEX},
};
use mango_common::Loadable;
use rand::prelude::*;

use serde::{Deserialize, Serialize};
use serde_json;

use solana_bench_mango::{
    cli,
    mango::{AccountKeys, MangoConfig},
};
use solana_client::{
    connection_cache::ConnectionCache, rpc_client::RpcClient, tpu_client::TpuClient,
};
use solana_sdk::{
    clock::{Slot, DEFAULT_MS_PER_SLOT},
    commitment_config::CommitmentConfig,
    hash::Hash,
    instruction::Instruction,
    message::Message,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    transaction::Transaction,
};

use std::{
    collections::HashMap,
    fs,
    ops::{Div, Mul},
    process::exit,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{channel, TryRecvError},
        Arc, RwLock,
    },
    thread::{sleep, Builder, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

fn load_from_rpc<T: Loadable>(rpc_client: &RpcClient, pk: &Pubkey) -> T {
    let acc = rpc_client.get_account(pk).unwrap();
    return T::load_from_bytes(acc.data.as_slice()).unwrap().clone();
}

fn get_latest_blockhash(rpc_client: &RpcClient) -> Hash {
    loop {
        match rpc_client.get_latest_blockhash() {
            Ok(blockhash) => return blockhash,
            Err(err) => {
                info!("Couldn't get last blockhash: {:?}", err);
                sleep(Duration::from_secs(1));
            }
        };
    }
}

fn get_new_latest_blockhash(client: &Arc<RpcClient>, blockhash: &Hash) -> Option<Hash> {
    let start = Instant::now();
    while start.elapsed().as_secs() < 5 {
        if let Ok(new_blockhash) = client.get_latest_blockhash() {
            if new_blockhash != *blockhash {
                debug!("Got new blockhash ({:?})", blockhash);
                return Some(new_blockhash);
            }
        }
        debug!("Got same blockhash ({:?}), will retry...", blockhash);

        // Retry ~twice during a slot
        sleep(Duration::from_millis(DEFAULT_MS_PER_SLOT / 2));
    }
    None
}

#[derive(Clone)]
struct TransactionSendRecord {
    pub signature: Signature,
    pub sent_at: Instant,
}

#[derive(Clone)]
struct TransactionConfirmRecord {
    pub signature: Signature,
    pub slot: Slot,
    pub sent_at: Instant,
    pub confirmed_at: Duration,
}

#[derive(Clone)]
struct PerpMarketCache {
    pub order_base_lots: i64,
    pub price: I80F48,
    pub price_quote_lots: i64,
    pub mango_program_pk: Pubkey,
    pub mango_group_pk: Pubkey,
    pub mango_cache_pk: Pubkey,
    pub perp_market_pk: Pubkey,
    pub perp_market: PerpMarket,
}

fn poll_blockhash(
    exit_signal: &Arc<AtomicBool>,
    blockhash: &Arc<RwLock<Hash>>,
    client: &Arc<RpcClient>,
    id: &Pubkey,
) {
    let mut blockhash_last_updated = Instant::now();
    let mut last_error_log = Instant::now();
    loop {
        let old_blockhash = *blockhash.read().unwrap();
        if exit_signal.load(Ordering::Relaxed) {
            break;
        }
        
        if let Some(new_blockhash) = get_new_latest_blockhash(client, &old_blockhash) {
            *blockhash.write().unwrap() = new_blockhash;
            blockhash_last_updated = Instant::now();
        } else {
            if blockhash_last_updated.elapsed().as_secs() > 120{
                break;
            }
        }

        sleep(Duration::from_millis(50));
    }
}

pub fn pk_from_str(str: &str) -> solana_program::pubkey::Pubkey {
    return solana_program::pubkey::Pubkey::from_str(str).unwrap();
}

pub fn pk_from_str_like<T: ToString>(str_like: T) -> solana_program::pubkey::Pubkey {
    return pk_from_str(&str_like.to_string());
}

fn main() {
    solana_logger::setup_with_default("solana=info");
    solana_metrics::set_panic_hook("bench-mango", /*version:*/ None);

    let matches = cli::build_args(solana_version::version!()).get_matches();
    let cli_config = cli::extract_args(&matches);

    let cli::Config {
        json_rpc_url,
        websocket_url,
        id,
        account_keys,
        mango_keys,
        duration,
        quotes_per_second,
        ..
    } = &cli_config;

    info!("Connecting to the cluster");

    let account_keys_json = fs::read_to_string(account_keys).expect("unable to read accounts file");
    let account_keys_parsed: Vec<AccountKeys> =
        serde_json::from_str(&account_keys_json).expect("accounts JSON was not well-formatted");

    let mango_keys_json = fs::read_to_string(mango_keys).expect("unable to read mango keys file");
    let mango_keys_parsed: MangoConfig =
        serde_json::from_str(&mango_keys_json).expect("mango JSON was not well-formatted");

    let mango_group_id = "testnet.0";
    let mango_group_config = mango_keys_parsed
        .groups
        .iter()
        .find(|g| g.name == mango_group_id)
        .unwrap();

    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        json_rpc_url.to_string(),
        CommitmentConfig::confirmed(),
    ));
    let connection_cache = Arc::new(ConnectionCache::default());
    let tpu_client = Arc::new(
        TpuClient::new_with_connection_cache(
            rpc_client.clone(),
            &websocket_url,
            solana_client::tpu_client::TpuClientConfig::default(),
            connection_cache,
        )
        .unwrap(),
    );

    info!(
        "accounts:{:?} markets:{:?} quotes_per_second:{:?} expected_tps:{:?} duration:{:?}",
        account_keys_parsed.len(),
        mango_group_config.perp_markets.len(),
        quotes_per_second,
        account_keys_parsed.len() * mango_group_config.perp_markets.len() * quotes_per_second.clone() as usize,
        duration
    );

    // continuosly fetch blockhash
    let exit_signal = Arc::new(AtomicBool::new(false));
    let blockhash = Arc::new(RwLock::new(get_latest_blockhash(&rpc_client.clone())));
    let blockhash_thread = {
        let exit_signal = exit_signal.clone();
        let blockhash = blockhash.clone();
        let client = rpc_client.clone();
        let id = id.pubkey();
        Builder::new()
            .name("solana-blockhash-poller".to_string())
            .spawn(move || {
                poll_blockhash(&exit_signal, &blockhash, &client, &id);
            })
            .unwrap()
    };

    // fetch group
    let mango_group_pk = Pubkey::from_str(mango_group_config.public_key.as_str()).unwrap();
    let mango_group = load_from_rpc::<MangoGroup>(&rpc_client, &mango_group_pk);
    let mango_program_pk = Pubkey::from_str(mango_group_config.mango_program_id.as_str()).unwrap();
    let mango_cache_pk = Pubkey::from_str(mango_group.mango_cache.to_string().as_str()).unwrap();
    let mango_cache = load_from_rpc::<MangoCache>(&rpc_client, &mango_cache_pk);

    let perp_market_caches: Vec<PerpMarketCache> = mango_group_config
        .perp_markets
        .iter()
        .enumerate()
        .map(|(market_index, perp_maket_config)| {
            let perp_market_pk = Pubkey::from_str(perp_maket_config.public_key.as_str()).unwrap();
            let perp_market = load_from_rpc::<PerpMarket>(&rpc_client, &perp_market_pk);

            // fetch price
            let base_decimals = mango_group_config.tokens[market_index + 1].decimals;
            let quote_decimals = mango_group_config.tokens[0].decimals;
            let base_unit = I80F48::from_num(10u64.pow(base_decimals as u32));
            let quote_unit = I80F48::from_num(10u64.pow(quote_decimals as u32));
            let price = mango_cache.price_cache[market_index].price;
            let price_quote_lots: i64 = price
                .mul(quote_unit)
                .mul(I80F48::from_num(perp_market.base_lot_size))
                .div(I80F48::from_num(perp_market.quote_lot_size))
                .div(base_unit)
                .to_num();
            let order_base_lots: i64 = base_unit
                .div(I80F48::from_num(perp_market.base_lot_size))
                .to_num();

            PerpMarketCache {
                order_base_lots,
                price,
                price_quote_lots,
                mango_program_pk,
                mango_group_pk,
                mango_cache_pk,
                perp_market_pk,
                perp_market,
            }
        })
        .collect();

    let (tx_record_sx, tx_record_rx) = channel::<TransactionSendRecord>();
    let tx_records: Arc<RwLock<Vec<TransactionConfirmRecord>>> = Arc::new(RwLock::new(Vec::new()));

    let mm_threads: Vec<JoinHandle<()>> = account_keys_parsed
        .iter()
        .map(|account_keys| {
            let exit_signal = exit_signal.clone();
            let tpu_client = tpu_client.clone();
            let blockhash = blockhash.clone();
            let duration = duration.clone();
            let quotes_per_second = quotes_per_second.clone();
            let tx_record_sx = tx_record_sx.clone();
            let perp_market_caches = perp_market_caches.clone();
            let mango_account_pk =
                Pubkey::from_str(account_keys.mango_account_pks[0].as_str()).unwrap();
            let mango_account_signer =
                Keypair::from_bytes(account_keys.secret_key.as_slice()).unwrap();

            info!(
                "wallet:{:?} https://testnet.mango.markets/account?pubkey={:?}",
                mango_account_signer.pubkey(),
                mango_account_pk
            );

            Builder::new()
                .name("solana-client-sender".to_string())
                .spawn(move || {
                    // update quotes 2x per second
                    for _ in 1..(duration.as_secs() * quotes_per_second) {
                        for c in perp_market_caches.iter() {

                            let offset = rand::random::<i8>() as i64;
                            let spread = rand::random::<u8>() as i64;
                            debug!(
                                "price:{:?} price_quote_lots:{:?} order_base_lots:{:?} offset:{:?} spread:{:?}",
                                c.price, c.price_quote_lots, c.order_base_lots, offset, spread
                            );
    
                            let cancel_ix: Instruction = serde_json::from_str(
                                &serde_json::to_string(
                                    &cancel_all_perp_orders(
                                        &pk_from_str_like(&c.mango_program_pk),
                                        &pk_from_str_like(&c.mango_group_pk),
                                        &pk_from_str_like(&mango_account_pk),
                                        &(pk_from_str_like(&mango_account_signer.pubkey())),
                                        &(pk_from_str_like(&c.perp_market_pk)),
                                        &c.perp_market.bids,
                                        &c.perp_market.asks,
                                        10,
                                    )
                                    .unwrap(),
                                )
                                .unwrap(),
                            )
                            .unwrap();
    
                            let place_bid_ix: Instruction = serde_json::from_str(
                                &serde_json::to_string(
                                    &place_perp_order2(
                                        &pk_from_str_like(&c.mango_program_pk),
                                        &pk_from_str_like(&c.mango_group_pk),
                                        &pk_from_str_like(&mango_account_pk),
                                        &(pk_from_str_like(&mango_account_signer.pubkey())),
                                        &pk_from_str_like(&c.mango_cache_pk),
                                        &(pk_from_str_like(&c.perp_market_pk)),
                                        &c.perp_market.bids,
                                        &c.perp_market.asks,
                                        &c.perp_market.event_queue,
                                        None,
                                        &[],
                                        Side::Bid,
                                        c.price_quote_lots + offset - spread,
                                        c.order_base_lots,
                                        i64::MAX,
                                        1,
                                        mango::matching::OrderType::Limit,
                                        false,
                                        None,
                                        64,
                                        mango::matching::ExpiryType::Absolute,
                                    )
                                    .unwrap(),
                                )
                                .unwrap(),
                            )
                            .unwrap();
    
                            let place_ask_ix: Instruction = serde_json::from_str(
                                &serde_json::to_string(
                                    &place_perp_order2(
                                        &pk_from_str_like(&c.mango_program_pk),
                                        &pk_from_str_like(&c.mango_group_pk),
                                        &pk_from_str_like(&mango_account_pk),
                                        &(pk_from_str_like(&mango_account_signer.pubkey())),
                                        &pk_from_str_like(&c.mango_cache_pk),
                                        &(pk_from_str_like(&c.perp_market_pk)),
                                        &c.perp_market.bids,
                                        &c.perp_market.asks,
                                        &c.perp_market.event_queue,
                                        None,
                                        &[],
                                        Side::Ask,
                                        c.price_quote_lots + offset + spread,
                                        c.order_base_lots,
                                        i64::MAX,
                                        2,
                                        mango::matching::OrderType::Limit,
                                        false,
                                        None,
                                        64,
                                        mango::matching::ExpiryType::Absolute,
                                    )
                                    .unwrap(),
                                )
                                .unwrap(),
                            )
                            .unwrap();
    
                            let mut tx = Transaction::new_unsigned(Message::new(
                                &[cancel_ix, place_bid_ix, place_ask_ix],
                                Some(&mango_account_signer.pubkey()),
                            ));
    
                            if let Ok(recent_blockhash) = blockhash.read() {
                                tx.sign(&[&mango_account_signer], *recent_blockhash);
                            }
    
                            tx_record_sx
                                .send(TransactionSendRecord {
                                    signature: tx.signatures[0],
                                    sent_at: Instant::now(),
                                })
                                .unwrap();
    
                            tpu_client.send_transaction(&tx);
                        }


                        sleep(Duration::from_millis(1000 / quotes_per_second));
                    }
                })
                .unwrap()
        })
        .collect();

    drop(tx_record_sx);
    let confirmed = tx_records.clone();
    let duration = duration.clone();
    let quotes_per_second = quotes_per_second.clone();
    let account_keys_parsed = account_keys_parsed.clone();
    let confirmation_thread = Builder::new()
        .name("solana-client-sender".to_string())
        .spawn(move || {
            let mut to_confirm: Vec<TransactionSendRecord> = Vec::new();

            const TIMEOUT: u64 = 30;
            let RECV_LIMIT = account_keys_parsed.len() * perp_market_caches.len() * 10 * quotes_per_second as usize;
            let mut recv_until_confirm = RECV_LIMIT;
            loop {
                if recv_until_confirm == 0 {
                    recv_until_confirm = RECV_LIMIT;

                     // collect all not confirmed records in a new buffer
                     let mut not_confirmed: Vec<TransactionSendRecord> = Vec::new();

                     const BATCH_SIZE: usize = 256;
                     info!(
                         "break from reading channel, try to confirm {} in {} batches",
                         to_confirm.len(),
                         (to_confirm.len() / BATCH_SIZE) + if to_confirm.len() % BATCH_SIZE > 0 { 1 } else { 0 }
                     );
                     for batch in to_confirm.rchunks(BATCH_SIZE) {
                         match rpc_client
                             .get_signature_statuses(&batch.iter().map(|t| t.signature).collect::<Vec<_>>())
                         {
                             Ok(statuses) => {
                                 trace!("batch result {:?}", statuses);
                                 for (i, s) in statuses.value.iter().enumerate() {
                                     let tx_record = &batch[i];
                                     match s {
                                         Some(s) => {
                                             if s.confirmation_status.is_none() {
                                                 not_confirmed.push(tx_record.clone());
                                             } else {
                                                 let mut lock = confirmed.write().unwrap();
                                                 (*lock).push(TransactionConfirmRecord {
                                                     signature: tx_record.signature,
                                                     slot: s.slot,
                                                     sent_at: tx_record.sent_at,
                                                     confirmed_at: tx_record.sent_at.elapsed(),
                                                 });

                                                 debug!(
                                                     "confirmed sig={} duration={:?}",
                                                     tx_record.signature,
                                                     tx_record.sent_at.elapsed()
                                                 );
                                             }
                                         }
                                         None => {
                                             if tx_record.sent_at.elapsed().as_secs() > TIMEOUT {
                                                 debug!(
                                                     "could not confirm tx {} within {} seconds, dropping it",
                                                     tx_record.signature, TIMEOUT
                                                 );
                                             } else {
                                                 not_confirmed.push(tx_record.clone());
                                             }
                                         }
                                     }
                                 }
                             }
                             Err(err) => {
                                 error!("could not confirm signatures err={}", err);
                                 not_confirmed.extend_from_slice(batch);
                                 sleep(Duration::from_millis(500));
                             }
                         }
                    }

                }

                match tx_record_rx.try_recv() {
                    Ok(tx_record) => {
                        debug!("add to queue len={} sig={}", to_confirm.len()+1, tx_record.signature);
                        to_confirm.push(tx_record);
                        recv_until_confirm -= 1;

                    }
                    Err(TryRecvError::Empty) => {
                        // collect all not confirmed records in a new buffer
                        let mut not_confirmed: Vec<TransactionSendRecord> = Vec::new();

                        const BATCH_SIZE: usize = 256;
                        info!(
                            "empty channel, try to confirm {} in {} batches",
                            to_confirm.len(),
                            (to_confirm.len() / BATCH_SIZE) + if to_confirm.len() % BATCH_SIZE > 0 { 1 } else { 0 }
                        );
                        for batch in to_confirm.rchunks(BATCH_SIZE) {
                            match rpc_client
                                .get_signature_statuses(&batch.iter().map(|t| t.signature).collect::<Vec<_>>())
                            {
                                Ok(statuses) => {
                                    trace!("batch result {:?}", statuses);
                                    for (i, s) in statuses.value.iter().enumerate() {
                                        let tx_record = &batch[i];
                                        match s {
                                            Some(s) => {
                                                if s.confirmation_status.is_none() {
                                                    not_confirmed.push(tx_record.clone());
                                                } else {
                                                    let mut lock = confirmed.write().unwrap();
                                                    (*lock).push(TransactionConfirmRecord {
                                                        signature: tx_record.signature,
                                                        slot: s.slot,
                                                        sent_at: tx_record.sent_at,
                                                        confirmed_at: tx_record.sent_at.elapsed(),
                                                    });

                                                    debug!(
                                                        "confirmed sig={} duration={:?}",
                                                        tx_record.signature,
                                                        tx_record.sent_at.elapsed()
                                                    );
                                                }
                                            }
                                            None => {
                                                if tx_record.sent_at.elapsed().as_secs() > TIMEOUT {
                                                    debug!(
                                                        "could not confirm tx {} within {} seconds, dropping it",
                                                        tx_record.signature, TIMEOUT
                                                    );
                                                } else {
                                                    not_confirmed.push(tx_record.clone());
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(err) => {
                                    error!("could not confirm signatures err={}", err);
                                    not_confirmed.extend_from_slice(batch);
                                    sleep(Duration::from_millis(500));
                                }
                            }
                        }

                        debug!("tried confirming {} signatures {} remaining", to_confirm.len(), not_confirmed.len());

                        // swap buffers
                        to_confirm = not_confirmed;
                    }
                    Err(TryRecvError::Disconnected) => {
                        if to_confirm.len() > 0 {
                            // collect all not confirmed records in a new buffer
                            let mut not_confirmed: Vec<TransactionSendRecord> = Vec::new();

                            const BATCH_SIZE: usize = 256;
                            info!(
                                "disconnected channel, try to confirm {} in {} batches",
                                to_confirm.len(),
                                (to_confirm.len() / BATCH_SIZE) + if to_confirm.len() % BATCH_SIZE > 0 { 1 } else { 0 }
                            );
                            for batch in to_confirm.rchunks(BATCH_SIZE) {
                                match rpc_client
                                    .get_signature_statuses(&batch.iter().map(|t| t.signature).collect::<Vec<_>>())
                                {
                                    Ok(statuses) => {
                                        trace!("batch result {:?}", statuses);
                                        for (i, s) in statuses.value.iter().enumerate() {
                                            let tx_record = &batch[i];
                                            match s {
                                                Some(s) => {
                                                    if s.confirmation_status.is_none() {
                                                        not_confirmed.push(tx_record.clone());
                                                    } else {
                                                        let mut lock = confirmed.write().unwrap();
                                                        (*lock).push(TransactionConfirmRecord {
                                                            signature: tx_record.signature,
                                                            slot: s.slot,
                                                            sent_at: tx_record.sent_at,
                                                            confirmed_at: tx_record.sent_at.elapsed(),
                                                        });
    
                                                        debug!(
                                                            "confirmed sig={} duration={:?}",
                                                            tx_record.signature,
                                                            tx_record.sent_at.elapsed()
                                                        );
                                                    }
                                                }
                                                None => {
                                                    if tx_record.sent_at.elapsed().as_secs() > TIMEOUT {
                                                        debug!(
                                                            "could not confirm tx {} within {} seconds, dropping it",
                                                            tx_record.signature, TIMEOUT
                                                        );
                                                    } else {
                                                        not_confirmed.push(tx_record.clone());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        error!("could not confirm signatures err={}", err);
                                        not_confirmed.extend_from_slice(batch);
                                        sleep(Duration::from_millis(500));
                                    }
                                }
                            }

                            debug!("tried confirming {} signatures {} remaining", to_confirm.len(), not_confirmed.len());

                            // swap buffers
                            to_confirm = not_confirmed;
                        }
                        else {
                            break;
                        }
                    }
                };
            }


            let confirmed: Vec<TransactionConfirmRecord> = {
                let confirmed = tx_records.clone();
                let lock = confirmed.write().unwrap();
                (*lock).clone()
            };
            let total_signed = account_keys_parsed.len() * perp_market_caches.len() * duration.as_secs() as usize * quotes_per_second as usize;
            info!("confirmed {} signatures of {} rate {}%", confirmed.len(), total_signed, (confirmed.len() * 100) / total_signed);

            let mut confirmation_times = confirmed.iter().map(|r| r.confirmed_at.as_millis()).collect::<Vec<_>>();
            confirmation_times.sort();
            info!("confirmation times min={} max={} median={}", confirmation_times.first().unwrap(), confirmation_times.last().unwrap(), confirmation_times[confirmation_times.len() / 2]);

            let mut slots = confirmed.iter().map(|r| r.slot).collect::<Vec<_>>();
            slots.sort();
            slots.dedup();
            info!("slots min={} max={} num={}", slots.first().unwrap(), slots.last().unwrap(), slots.len());

            let mut histogram = HashMap::new();
            for r in confirmed.iter(){
                *histogram.entry(r.slot).or_insert(0) += 1;
            }

            info!("histogram {:?}", histogram);


            /*
            while let Ok(tx_record) = tx_record_rx.recv() {
                while !rpc_client
                    .confirm_transaction(&tx_record.signature)
                    .unwrap()
                {
                    if tx_record.sent_at.elapsed().as_secs() > 120 {
                        warn!(
                            "could not confirm tx {} within 120 seconds",
                            tx_record.signature
                        );
                        break;
                    }
                    sleep(Duration::from_millis(500))
                }

                let mut lock = tx_records.write().unwrap();
                (*lock).push(TransactionConfirmRecord {
                    signature: tx_record.signature,
                    sent_at: tx_record.sent_at,
                    confirmed_at: tx_record.sent_at.elapsed(),
                });

                debug!(
                    "confirmed sig={} duration={:?}",
                    tx_record.signature,
                    tx_record.sent_at.elapsed()
                );
            }
             */
        })
        .unwrap();

    for t in mm_threads {
        if let Err(err) = t.join() {
            error!("mm join failed with: {:?}", err);
        }
    }

    info!("joined all mm_threads");

    exit_signal.store(true, Ordering::Relaxed);

    if let Err(err) = blockhash_thread.join() {
        error!("blockhash join failed with: {:?}", err);
    }

    if let Err(err) = confirmation_thread.join() {
        error!("confirmation join fialed with: {:?}", err);
    }
}
