use {
    assert_matches::assert_matches,
    solana_program_test::tokio,
    solana_sdk::{
        application_fees::ApplicationFeesInstuctions, signature::Signer, transaction::Transaction,
    },
};

mod common;
use {
    crate::common::{create_a_dummy_account, create_owner_and_dummy_account, setup_test_context},
    solana_sdk::{
        application_fees::{self, ApplicationFeeStructure},
        native_token::LAMPORTS_PER_SOL,
        pubkey::Pubkey,
        system_instruction,
    },
};

#[tokio::test]
async fn test_application_fees_are_not_applied_on_rebate_all() {
    let mut context = setup_test_context().await;

    let (owner, writable_account) = create_owner_and_dummy_account(&mut context).await;
    let (owner2, writable_account2) = create_owner_and_dummy_account(&mut context).await;
    let writable_account3 = create_a_dummy_account(&mut context, &owner.pubkey()).await;

    {
        // update fees for account 1
        let client = &mut context.banks_client;
        let payer = &context.payer;
        let recent_blockhash = context.last_blockhash;
        let add_ix = ApplicationFeesInstuctions::update_fees(
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

        let (pda, _bump) =
            Pubkey::find_program_address(&[&writable_account.to_bytes()], &application_fees::id());
        let account = client.get_account(pda).await.unwrap().unwrap();
        let fees_data: ApplicationFeeStructure =
            bincode::deserialize::<ApplicationFeeStructure>(account.data.as_slice()).unwrap();
        assert_eq!(fees_data.fee_lamports, LAMPORTS_PER_SOL);

        // update fees for account 2
        let add_ix = ApplicationFeesInstuctions::update_fees(
            LAMPORTS_PER_SOL * 2,
            writable_account2,
            owner2.pubkey(),
            payer.pubkey(),
        );

        let transaction = Transaction::new_signed_with_payer(
            &[add_ix.clone()],
            Some(&payer.pubkey()),
            &[payer, &owner2],
            recent_blockhash,
        );

        assert_matches!(client.process_transaction(transaction).await, Ok(()));

        let (pda, _bump) =
            Pubkey::find_program_address(&[&writable_account2.to_bytes()], &application_fees::id());
        let account = client.get_account(pda).await.unwrap().unwrap();
        let fees_data: ApplicationFeeStructure =
            bincode::deserialize::<ApplicationFeeStructure>(account.data.as_slice()).unwrap();
        assert_eq!(fees_data.fee_lamports, 2 * LAMPORTS_PER_SOL);

        // update fees for account 3
        let add_ix = ApplicationFeesInstuctions::update_fees(
            LAMPORTS_PER_SOL * 3,
            writable_account3,
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

        let (pda, _bump) =
            Pubkey::find_program_address(&[&writable_account3.to_bytes()], &application_fees::id());
        let account = client.get_account(pda).await.unwrap().unwrap();
        let fees_data: ApplicationFeeStructure =
            bincode::deserialize::<ApplicationFeeStructure>(account.data.as_slice()).unwrap();
        assert_eq!(fees_data.fee_lamports, 3 * LAMPORTS_PER_SOL);
    }

    // transfer 1 lamport to the writable accounts / rebate_all for owner (rebates 3+1 SOLs) / but payer has to pay 2SOL as application fee
    {
        let client = &mut context.banks_client;
        let payer = &context.payer;
        let balance_before_transaction = client.get_balance(payer.pubkey()).await.unwrap();

        let blockhash = client.get_latest_blockhash().await.unwrap();
        let transfer_ix1 = system_instruction::transfer(&payer.pubkey(), &writable_account, 1);
        let transfer_ix2 = system_instruction::transfer(&payer.pubkey(), &writable_account2, 1);
        let transfer_ix3 = system_instruction::transfer(&payer.pubkey(), &writable_account3, 1);
        let rebate_all =
            solana_sdk::application_fees::ApplicationFeesInstuctions::rebate_all(owner.pubkey());
        let transaction = Transaction::new_signed_with_payer(
            &[
                transfer_ix1.clone(),
                transfer_ix2.clone(),
                transfer_ix3.clone(),
                rebate_all.clone(),
            ],
            Some(&payer.pubkey()),
            &[payer, &owner],
            blockhash,
        );
        assert_matches!(client.process_transaction(transaction).await, Ok(()));

        let balance_after_transaction = client.get_balance(payer.pubkey()).await.unwrap();
        assert!(balance_before_transaction - balance_after_transaction < 3 * LAMPORTS_PER_SOL);
    }

    // check if the application fees are correcly dispatched to the correct account
    let balance_writable_account_before = context
        .banks_client
        .get_balance(writable_account)
        .await
        .unwrap();
    let balance_writable_account_2_before = context
        .banks_client
        .get_balance(writable_account2)
        .await
        .unwrap();
    let balance_writable_account_3_before = context
        .banks_client
        .get_balance(writable_account3)
        .await
        .unwrap();
    let slot = context.banks_client.get_root_slot().await.unwrap();
    context.warp_to_slot(slot + 2).unwrap();
    let balance_writable_account_after = context
        .banks_client
        .get_balance(writable_account)
        .await
        .unwrap();
    let balance_writable_account_2_after = context
        .banks_client
        .get_balance(writable_account2)
        .await
        .unwrap();
    let balance_writable_account_3_after = context
        .banks_client
        .get_balance(writable_account3)
        .await
        .unwrap();
    assert_eq!(
        balance_writable_account_after - balance_writable_account_before,
        0
    );
    assert_eq!(
        balance_writable_account_2_after - balance_writable_account_2_before,
        2 * LAMPORTS_PER_SOL
    );
    assert_eq!(
        balance_writable_account_3_after - balance_writable_account_3_before,
        0
    );
}
