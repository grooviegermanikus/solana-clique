use serde::{Deserialize, Serialize};
use solana_frozen_abi_macro::{AbiEnumVisitor, AbiExample};

use solana_program::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    system_program,
};

// application fees instructions
#[derive(AbiExample, AbiEnumVisitor, Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum ApplicationFeesInstuctions {
    // Add, Remove or change fees for a writable account
    // Set fees=0 to remove the fees
    UpdateFees { fees: u64 },
    // The write account owner i.e program usually can CPI this instruction to rebate the fees to good actors
    Rebate,
    // Rebase all fees beloning to the owner
    RebateAll,
}

pub fn update_fees(
    fees: u64,
    writable_account: Pubkey,
    owner: Pubkey,
    payer: Pubkey,
) -> Instruction {
    let (pda, _bump) = Pubkey::find_program_address(&[&writable_account.to_bytes()], &crate::id());
    Instruction::new_with_bincode(
        crate::id(),
        &ApplicationFeesInstuctions::UpdateFees { fees: fees },
        vec![
            AccountMeta::new_readonly(owner, true),
            AccountMeta::new(writable_account, false),
            AccountMeta::new(pda, false),
            AccountMeta::new(payer, true),
            AccountMeta::new_readonly(system_program::id(), false),
        ],
    )
}

pub fn rebate(writable_account: Pubkey, owner: Pubkey) -> Instruction {
    Instruction::new_with_bincode(
        crate::id(),
        &ApplicationFeesInstuctions::Rebate,
        vec![
            AccountMeta::new_readonly(owner, true),
            AccountMeta::new_readonly(writable_account, false),
        ],
    )
}

pub fn rebate_all(owner: Pubkey) -> Instruction {
    Instruction::new_with_bincode(
        crate::id(),
        &ApplicationFeesInstuctions::RebateAll,
        vec![AccountMeta::new_readonly(owner, true)],
    )
}
