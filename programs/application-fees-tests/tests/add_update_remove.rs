use assert_matches::assert_matches;
use solana_program_test::tokio;
use solana_sdk::{
    application_fees::ApplicationFeesInstuctions, signature::Signer, signer::keypair::Keypair,
    system_instruction::assign, transaction::Transaction,
};

mod common;
use crate::common::setup_test_context;

#[tokio::test]
async fn test_adding_write_lock_fees() {
    let mut context = setup_test_context().await;

    let client = &mut context.banks_client;
    let payer = &context.payer;
    let recent_blockhash = context.last_blockhash;
    let owner_keypair = Keypair::new();
    let writable_account_keypair = Keypair::new();

    {
        let change_authority = assign(&writable_account_keypair.pubkey(), &owner_keypair.pubkey());
        let transaction_change_auth = Transaction::new_signed_with_payer(
            &[change_authority.clone()],
            Some(&payer.pubkey()),
            &[payer, &writable_account_keypair],
            recent_blockhash,
        );

        assert_matches!(
            client.process_transaction(transaction_change_auth).await,
            Ok(())
        );
    }

    let add_ix = ApplicationFeesInstuctions::update(
        100,
        writable_account_keypair.pubkey(),
        owner_keypair.pubkey(),
        payer.pubkey(),
    );

    let transaction = Transaction::new_signed_with_payer(
        &[add_ix.clone()],
        Some(&payer.pubkey()),
        &[payer, &owner_keypair],
        recent_blockhash,
    );

    assert_matches!(client.process_transaction(transaction).await, Ok(()));
}
