use {
    assert_matches::assert_matches,
    solana_application_fees_program::instruction::update_fees,
    solana_program_test::tokio,
    solana_sdk::{
        signature::{Keypair, Signer},
        transaction::Transaction,
    },
};

mod common;
use {
    crate::common::{assert_error, create_owner_and_dummy_account, setup_test_context},
    solana_sdk::{
        application_fees::{self, ApplicationFeeStructure},
        instruction::InstructionError,
        pubkey::Pubkey,
    },
};

#[tokio::test]
async fn test_add_update_remove_write_lock_fees() {
    let mut context = setup_test_context().await;

    let (owner, writable_account) = create_owner_and_dummy_account(&mut context).await;

    {
        let client = &mut context.banks_client;
        let payer = &context.payer;
        let recent_blockhash = context.last_blockhash;
        let add_ix = update_fees(100, writable_account, owner.pubkey(), payer.pubkey());

        let transaction = Transaction::new_signed_with_payer(
            &[add_ix.clone()],
            Some(&payer.pubkey()),
            &[payer, &owner],
            recent_blockhash,
        );

        assert_matches!(client.process_transaction(transaction).await, Ok(()));

        let (pda, _bump) =
            Pubkey::find_program_address(&[&writable_account.to_bytes()], &application_fees::id());
        let account = client.get_account(pda).await.unwrap().unwrap();
        let fees_data: ApplicationFeeStructure =
            bincode::deserialize::<ApplicationFeeStructure>(account.data.as_slice()).unwrap();
        assert_eq!(fees_data.fee_lamports, 100);

        // test update

        let update_ix = update_fees(10000, writable_account, owner.pubkey(), payer.pubkey());

        let update_transaction = Transaction::new_signed_with_payer(
            &[update_ix.clone()],
            Some(&payer.pubkey()),
            &[payer, &owner],
            recent_blockhash,
        );

        assert_matches!(client.process_transaction(update_transaction).await, Ok(()));

        let account2 = client.get_account(pda).await.unwrap().unwrap();
        let fees_data2: ApplicationFeeStructure =
            bincode::deserialize::<ApplicationFeeStructure>(account2.data.as_slice()).unwrap();
        assert_eq!(fees_data2.fee_lamports, 10000);

        // test remove
        let remove_ix = update_fees(0, writable_account, owner.pubkey(), payer.pubkey());

        let remove_transaction = Transaction::new_signed_with_payer(
            &[remove_ix.clone()],
            Some(&payer.pubkey()),
            &[payer, &owner],
            recent_blockhash,
        );

        assert_matches!(client.process_transaction(remove_transaction).await, Ok(()));

        let account3 = client.get_account(pda).await.unwrap();
        assert_eq!(account3, None);
    }
}

#[tokio::test]
async fn test_adding_write_lock_fees_with_wrong_owner() {
    let mut context = setup_test_context().await;

    let (_owner, writable_account) = create_owner_and_dummy_account(&mut context).await;
    let (owner2, _writable_account2) = create_owner_and_dummy_account(&mut context).await;
    {
        let client = &mut context.banks_client;
        let payer = &context.payer;
        let recent_blockhash = context.last_blockhash;
        let add_ix = update_fees(100, writable_account, owner2.pubkey(), payer.pubkey());

        let transaction = Transaction::new_signed_with_payer(
            &[add_ix.clone()],
            Some(&payer.pubkey()),
            &[payer, &owner2],
            recent_blockhash,
        );

        assert_error(
            client.process_transaction(transaction).await,
            InstructionError::IllegalOwner,
        )
        .await;
    }
}

#[tokio::test]
#[should_panic]
async fn test_adding_write_lock_fees_without_signature_owner() {
    let mut context = setup_test_context().await;

    let (owner, writable_account) = create_owner_and_dummy_account(&mut context).await;

    {
        let client = &mut context.banks_client;
        let payer = &context.payer;
        let recent_blockhash = context.last_blockhash;
        let add_ix = update_fees(100, writable_account, owner.pubkey(), payer.pubkey());

        let transaction = Transaction::new_signed_with_payer(
            &[add_ix.clone()],
            Some(&payer.pubkey()),
            &[payer],
            recent_blockhash,
        );

        assert_error(
            client.process_transaction(transaction).await,
            InstructionError::MissingRequiredSignature,
        )
        .await;
    }
}

#[tokio::test]
#[should_panic]
async fn test_adding_write_lock_fees_without_signature_payer() {
    let mut context = setup_test_context().await;

    let (owner, writable_account) = create_owner_and_dummy_account(&mut context).await;

    {
        let client = &mut context.banks_client;
        let payer = &context.payer;
        let recent_blockhash = context.last_blockhash;
        let add_ix = update_fees(100, writable_account, owner.pubkey(), payer.pubkey());

        let transaction = Transaction::new_signed_with_payer(
            &[add_ix.clone()],
            Some(&payer.pubkey()),
            &[&owner],
            recent_blockhash,
        );

        assert_error(
            client.process_transaction(transaction).await,
            InstructionError::MissingRequiredSignature,
        )
        .await;
    }
}

#[tokio::test]
async fn test_add_update_remove_owner_same_as_writable_account() {
    let mut context = setup_test_context().await;

    let owner = Keypair::new();
    let writable_account = owner.pubkey();

    {
        let client = &mut context.banks_client;
        let payer = &context.payer;
        let recent_blockhash = context.last_blockhash;
        let add_ix = update_fees(100, writable_account, owner.pubkey(), payer.pubkey());

        let transaction = Transaction::new_signed_with_payer(
            &[add_ix.clone()],
            Some(&payer.pubkey()),
            &[payer, &owner],
            recent_blockhash,
        );

        assert_matches!(client.process_transaction(transaction).await, Ok(()));

        let (pda, _bump) =
            Pubkey::find_program_address(&[&writable_account.to_bytes()], &application_fees::id());
        let account = client.get_account(pda).await.unwrap().unwrap();
        let fees_data: ApplicationFeeStructure =
            bincode::deserialize::<ApplicationFeeStructure>(account.data.as_slice()).unwrap();
        assert_eq!(fees_data.fee_lamports, 100);

        // test update

        let update_ix = update_fees(10000, writable_account, owner.pubkey(), payer.pubkey());

        let update_transaction = Transaction::new_signed_with_payer(
            &[update_ix.clone()],
            Some(&payer.pubkey()),
            &[payer, &owner],
            recent_blockhash,
        );

        assert_matches!(client.process_transaction(update_transaction).await, Ok(()));

        let account2 = client.get_account(pda).await.unwrap().unwrap();
        let fees_data2: ApplicationFeeStructure =
            bincode::deserialize::<ApplicationFeeStructure>(account2.data.as_slice()).unwrap();
        assert_eq!(fees_data2.fee_lamports, 10000);

        // test remove
        let remove_ix = update_fees(0, writable_account, owner.pubkey(), payer.pubkey());

        let remove_transaction = Transaction::new_signed_with_payer(
            &[remove_ix.clone()],
            Some(&payer.pubkey()),
            &[payer, &owner],
            recent_blockhash,
        );

        assert_matches!(client.process_transaction(remove_transaction).await, Ok(()));

        let account3 = client.get_account(pda).await.unwrap();
        assert_eq!(account3, None);
    }
}
