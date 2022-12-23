use {
    assert_matches::assert_matches,
    solana_program_test::tokio,
    solana_sdk::{
        application_fees::ApplicationFeesInstuctions, signature::Signer, transaction::Transaction,
    },
};

mod common;
use solana_sdk::{pubkey::Pubkey, application_fees::{self, ApplicationFeeStructure}, instruction::InstructionError, native_token::LAMPORTS_PER_SOL, system_transaction, system_instruction, signature::Keypair};

use crate::common::{create_owner_and_dummy_account, setup_test_context, assert_error};

#[tokio::test]
async fn test_application_fees_are_applied_without_rebate() {
    let mut context = setup_test_context().await;

    let (owner, writable_account) = create_owner_and_dummy_account(&mut context).await;

    {
        let client = &mut context.banks_client;
        let payer = &context.payer;
        let recent_blockhash = context.last_blockhash;
        let add_ix = ApplicationFeesInstuctions::update(
            LAMPORTS_PER_SOL,
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

        assert_matches!(client.process_transaction(transaction).await, Ok(()));

        let (pda, _bump) = Pubkey::find_program_address(&[&writable_account.to_bytes()], &application_fees::id());
        let account = client.get_account(pda).await.unwrap().unwrap();
        let fees_data : ApplicationFeeStructure = bincode::deserialize::<ApplicationFeeStructure>(account.data.as_slice()).unwrap();
        assert_eq!(fees_data.fee_lamports, LAMPORTS_PER_SOL);
    }

    // transfer 1 lamport to the writable account / but payer has to pay 1SOL as application fee
    {
        let client = &mut context.banks_client;
        let payer = &context.payer;
        let balance_before_transaction = client.get_balance(payer.pubkey()).await.unwrap();

        let blockhash = client.get_latest_blockhash().await.unwrap();
        let transfer_ix = system_transaction::transfer(payer, &writable_account, 1, blockhash);
        assert_matches!(client.process_transaction(transfer_ix).await, Ok(()));

        let balance_after_transaction = client.get_balance(payer.pubkey()).await.unwrap();
        assert!(balance_before_transaction - balance_after_transaction > LAMPORTS_PER_SOL);
    }

    // check if the application fees are correcly dispatched to the correct account
    let balance_writable_account_before = context.banks_client.get_balance(writable_account).await.unwrap();
    let slot = context.banks_client.get_root_slot().await.unwrap();
    context.warp_to_slot(slot + 2).unwrap();
    let balance_writable_account_after = context.banks_client.get_balance(writable_account).await.unwrap();
    assert_eq!(balance_writable_account_after - balance_writable_account_before , LAMPORTS_PER_SOL);
}

#[tokio::test]
async fn test_application_fees_are_applied_without_rebate_for_failed_transactions() {
    let mut context = setup_test_context().await;

    let (owner, writable_account) = create_owner_and_dummy_account(&mut context).await;

    let payer2 = {
        let client = &mut context.banks_client;
        let payer = &context.payer;
        let recent_blockhash = context.last_blockhash;
        let add_ix = ApplicationFeesInstuctions::update(
            LAMPORTS_PER_SOL,
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

        assert_matches!(client.process_transaction(transaction).await, Ok(()));

        let (pda, _bump) = Pubkey::find_program_address(&[&writable_account.to_bytes()], &application_fees::id());
        let account = client.get_account(pda).await.unwrap().unwrap();
        let fees_data : ApplicationFeeStructure = bincode::deserialize::<ApplicationFeeStructure>(account.data.as_slice()).unwrap();
        assert_eq!(fees_data.fee_lamports, LAMPORTS_PER_SOL);

        let payer2 = Keypair::new();
        let transfer_ix = system_transaction::transfer(payer, &payer2.pubkey(), 10 * LAMPORTS_PER_SOL, recent_blockhash);
        assert_matches!(client.process_transaction(transfer_ix).await, Ok(()));
        payer2
    };

    // transfer 11 SOLs to the writable account which the payer clearly does not have / but payer has to pay 1SOL as application fee
    {
        let client = &mut context.banks_client;
        let payer = &payer2;
        let balance_before_transaction = client.get_balance(payer.pubkey()).await.unwrap();

        let blockhash = client.get_latest_blockhash().await.unwrap();
        let transfer_ix = system_transaction::transfer(payer, &writable_account, 11 * LAMPORTS_PER_SOL, blockhash);
        assert_error(client.process_transaction(transfer_ix).await, InstructionError::Custom(1)).await;

        let balance_after_transaction = client.get_balance(payer.pubkey()).await.unwrap();
        assert!(balance_before_transaction - balance_after_transaction > LAMPORTS_PER_SOL);
    }

    // check if the application fees are correcly dispatched to the correct account
    let balance_writable_account_before = context.banks_client.get_balance(writable_account).await.unwrap();
    let slot = context.banks_client.get_root_slot().await.unwrap();
    context.warp_to_slot(slot + 2).unwrap();
    let balance_writable_account_after = context.banks_client.get_balance(writable_account).await.unwrap();
    assert_eq!(balance_writable_account_after - balance_writable_account_before , LAMPORTS_PER_SOL);
}


#[tokio::test]
async fn test_application_fees_are_not_applied_if_rebated() {
    let mut context = setup_test_context().await;

    let (owner, writable_account) = create_owner_and_dummy_account(&mut context).await;

    {
        let client = &mut context.banks_client;
        let payer = &context.payer;
        let recent_blockhash = context.last_blockhash;
        let add_ix = ApplicationFeesInstuctions::update(
            LAMPORTS_PER_SOL,
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

        assert_matches!(client.process_transaction(transaction).await, Ok(()));

        let (pda, _bump) = Pubkey::find_program_address(&[&writable_account.to_bytes()], &application_fees::id());
        let account = client.get_account(pda).await.unwrap().unwrap();
        let fees_data : ApplicationFeeStructure = bincode::deserialize::<ApplicationFeeStructure>(account.data.as_slice()).unwrap();
        assert_eq!(fees_data.fee_lamports, LAMPORTS_PER_SOL);
    }

    // transfer 1 lamport to the writable account / but payer has to pay 1SOL as application fee
    {
        let client = &mut context.banks_client;
        let payer = &context.payer;
        let balance_before_transaction = client.get_balance(payer.pubkey()).await.unwrap();

        let blockhash = client.get_latest_blockhash().await.unwrap();
        let transfer_ix = system_instruction::transfer(&payer.pubkey(), &writable_account, 1);
        let rebate_ix = solana_sdk::application_fees::ApplicationFeesInstuctions::rebate(writable_account, owner.pubkey());
        let transaction = Transaction::new_signed_with_payer(
            &[transfer_ix.clone(), rebate_ix.clone()],
            Some(&payer.pubkey()),
            &[payer, &owner],
            blockhash,
        );
        assert_matches!(client.process_transaction(transaction).await, Ok(()));

        let balance_after_transaction = client.get_balance(payer.pubkey()).await.unwrap();
        assert!(balance_before_transaction - balance_after_transaction < LAMPORTS_PER_SOL );
    }

    // check if the application fees are correcly dispatched to the correct account
    let balance_writable_account_before = context.banks_client.get_balance(writable_account).await.unwrap();
    let slot = context.banks_client.get_root_slot().await.unwrap();
    context.warp_to_slot(slot + 2).unwrap();
    let balance_writable_account_after = context.banks_client.get_balance(writable_account).await.unwrap();
    assert_eq!(balance_writable_account_after - balance_writable_account_before , 0);
}
