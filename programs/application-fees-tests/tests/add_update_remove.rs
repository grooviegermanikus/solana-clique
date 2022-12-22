use {
    assert_matches::assert_matches,
    solana_program_test::tokio,
    solana_sdk::{
        application_fees::ApplicationFeesInstuctions, signature::Signer, signer::keypair::Keypair,
        system_instruction::assign, transaction::Transaction,
    },
};

mod common;
use crate::common::{create_owner_and_dummy_account, setup_test_context};

#[tokio::test]
async fn test_adding_write_lock_fees() {
    let mut context = setup_test_context().await;

    let (owner, writable_account) = create_owner_and_dummy_account(&mut context).await;

    {
        let client = &mut context.banks_client;
        let payer = &context.payer;
        let recent_blockhash = context.last_blockhash;
        let add_ix = ApplicationFeesInstuctions::update(
            100,
            writable_account,
            owner.pubkey(),
            payer.pubkey(),
        );

        let transaction = Transaction::new_signed_with_payer(
            &[add_ix.clone()],
            Some(&payer.pubkey()),
            &[payer, &owner],
            recent_blockhash,
        );

        println!("executing add transaction");
        assert_matches!(client.process_transaction(transaction).await, Ok(()));
    }
}
