use solana_program_test::{ProgramTest, ProgramTestContext};

use solana_application_fees_program::{id, processor::process_instruction};

pub async fn setup_test_context() -> ProgramTestContext {
    let program_test = ProgramTest::new("", id(), Some(process_instruction));
    program_test.start_with_context().await
}
