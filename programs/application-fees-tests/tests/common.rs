use solana_program_test::BanksClientError;
use solana_sdk::{instruction::InstructionError, transaction::TransactionError};

use {
    assert_matches::assert_matches,
    solana_application_fees_program::{id, processor::process_instruction},
    solana_program_test::{ProgramTest, ProgramTestContext},
    solana_sdk::{
        pubkey::Pubkey,
        signer::{keypair::Keypair, Signer},
        system_instruction::create_account,
        transaction::Transaction,
    },
};
pub async fn setup_test_context() -> ProgramTestContext {
    let program_test = ProgramTest::new("", id(), Some(process_instruction));
    program_test.start_with_context().await
}

pub async fn create_owner_and_dummy_account(context: &mut ProgramTestContext) -> (Keypair, Pubkey) {
    let owner = Keypair::new();
    let account = Keypair::new();
    let payer = &context.payer;
    let client = &mut context.banks_client;
    let recent_blockhash = context.last_blockhash;
    let ix = create_account(
        &payer.pubkey(),
        &account.pubkey(),
        100_000_000,
        1,
        &owner.pubkey(),
    );
    let tx = Transaction::new_signed_with_payer(
        &[ix.clone()],
        Some(&payer.pubkey()),
        &[payer, &account],
        recent_blockhash,
    );

    assert_matches!(client.process_transaction(tx).await, Ok(()));

    (owner, account.pubkey())
}

pub async fn assert_error(
    res : Result<(), BanksClientError>,
    expected_err: InstructionError,
) {
    assert_eq!(
        res.unwrap_err().unwrap(),
        TransactionError::InstructionError(0, expected_err),
    );
}
