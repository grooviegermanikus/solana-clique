use {
    crate::instruction::ApplicationFeesInstuctions,
    solana_program_runtime::{ic_msg, invoke_context::InvokeContext},
    solana_sdk::{
        account::ReadableAccount,
        application_fees::{ApplicationFeeStructure, APPLICATION_FEE_STRUCTURE_SIZE},
        instruction::InstructionError,
        program_utils::limited_deserialize,
        pubkey::Pubkey,
        system_instruction,
    },
};

pub fn process_instruction(
    _first_instruction_account: usize,
    instruction_data: &[u8],
    invoke_context: &mut InvokeContext,
) -> Result<(), InstructionError> {
    match limited_deserialize(instruction_data)? {
        ApplicationFeesInstuctions::UpdateFees { fees } => {
            Processor::add_or_update_fees(invoke_context, fees)
        }
        ApplicationFeesInstuctions::Rebate => Processor::rebate(invoke_context),
        ApplicationFeesInstuctions::RebateAll => Processor::rebate_all(invoke_context),
    }
}

pub struct Processor;

impl Processor {
    fn add_or_update_fees(
        invoke_context: &mut InvokeContext,
        fees: u64,
    ) -> Result<(), InstructionError> {
        let transaction_context = &invoke_context.transaction_context;
        let instruction_context = transaction_context.get_current_instruction_context()?;

        let owner = instruction_context.try_borrow_instruction_account(transaction_context, 0)?;

        if !owner.is_signer() {
            ic_msg!(invoke_context, "Authority account must be a signer");
            return Err(InstructionError::MissingRequiredSignature);
        }

        let writable_account = {
            let writable_account_key =
                instruction_context.get_instruction_account_key(transaction_context, 1)?;
            if writable_account_key.eq(owner.get_key()) {
                owner
            } else {
                let writable_account =
                    instruction_context.try_borrow_instruction_account(transaction_context, 1)?;

                if !writable_account.get_owner().eq(owner.get_key()) {
                    ic_msg!(
                        invoke_context,
                        "Invalid account owner {} instead of {}",
                        writable_account.get_owner().to_string(),
                        owner.get_key().to_string()
                    );
                    return Err(InstructionError::IllegalOwner);
                }

                drop(owner);
                writable_account
            }
        };
        let pda = instruction_context.try_borrow_instruction_account(transaction_context, 2)?;

        let writable_account_key = *writable_account.get_key();
        ic_msg!(
            invoke_context,
            "ApplicationFeesInstuctions::Update called for {} to change fees to {}",
            writable_account_key.to_string(),
            fees
        );

        let (calculated_pda, _bump) =
            Pubkey::find_program_address(&[&writable_account_key.to_bytes()], &crate::id());
        if !calculated_pda.eq(pda.get_key()) {
            ic_msg!(invoke_context, "Invalid pda to store fee info");
            return Err(InstructionError::InvalidArgument);
        }
        drop(writable_account);

        let pda_key = *pda.get_key();
        let is_pda_empty = pda.get_data().is_empty();
        let pda_lamports = pda.get_lamports();
        drop(pda);
        // allocate pda to store application fee strucutre
        if is_pda_empty {
            if fees == 0 {
                ic_msg!(invoke_context, "Removing fees which do not exist");
                return Err(InstructionError::InvalidArgument);
            }
            ic_msg!(
                invoke_context,
                "Creating new application fees account for writable account {}",
                writable_account_key.to_string()
            );
            let payer =
                instruction_context.try_borrow_instruction_account(transaction_context, 3)?;

            let payer_key = *payer.get_key();
            if !payer.is_signer() {
                ic_msg!(invoke_context, "Payer account must be a signer");
                return Err(InstructionError::MissingRequiredSignature);
            }
            drop(payer);

            let account_data_len = APPLICATION_FEE_STRUCTURE_SIZE;
            let rent = invoke_context.get_sysvar_cache().get_rent()?;
            let required_lamports = rent
                .minimum_balance(account_data_len)
                .max(1)
                .saturating_sub(pda_lamports);

            if required_lamports > 0 {
                invoke_context.native_invoke(
                    system_instruction::transfer(&payer_key, &pda_key, required_lamports),
                    &[payer_key],
                )?;
            }

            invoke_context.native_invoke(
                system_instruction::allocate(&pda_key, account_data_len as u64),
                &[pda_key],
            )?;

            invoke_context.native_invoke(
                system_instruction::assign(&pda_key, &crate::id()),
                &[pda_key],
            )?;
        }

        if fees == 0 {
            let transaction_context = &invoke_context.transaction_context;
            let instruction_context = transaction_context.get_current_instruction_context()?;
            ic_msg!(
                invoke_context,
                "Removing existing application fees account for writable account {}",
                writable_account_key.to_string()
            );

            // remove the fees associated with the writable account
            let mut payer =
                instruction_context.try_borrow_instruction_account(transaction_context, 3)?;
            if !payer.is_signer() {
                ic_msg!(invoke_context, "Payer account must be a signer");
                return Err(InstructionError::MissingRequiredSignature);
            }
            let mut pda =
                instruction_context.try_borrow_instruction_account(transaction_context, 2)?;

            // reimburse the payer
            let withdrawn_lamports = pda.get_lamports();
            payer.checked_add_lamports(withdrawn_lamports)?;
            drop(payer);
            // delete pda account
            pda.set_data_length(0);
            pda.set_lamports(0);
        } else {
            let transaction_context = &invoke_context.transaction_context;
            let instruction_context = transaction_context.get_current_instruction_context()?;
            let mut pda_account =
                instruction_context.try_borrow_instruction_account(transaction_context, 2)?;

            let application_fee_structure = ApplicationFeeStructure {
                fee_lamports: fees,
                version: 1,
                _padding: [0; 8],
            };
            let seralized = bincode::serialize(&application_fee_structure).unwrap();
            pda_account.set_data(seralized.as_slice());
        }

        Ok(())
    }

    fn rebate(invoke_context: &mut InvokeContext) -> Result<(), InstructionError> {
        let transaction_context = &invoke_context.transaction_context;
        let instruction_context = transaction_context.get_current_instruction_context()?;

        let owner = instruction_context.try_borrow_instruction_account(transaction_context, 0)?;
        if !owner.is_signer() {
            ic_msg!(invoke_context, "Authority account must be a signer");
            return Err(InstructionError::MissingRequiredSignature);
        }

        let writable_account_key =
            instruction_context.get_instruction_account_key(transaction_context, 1)?;
        let writable_account = {
            if writable_account_key.eq(owner.get_key()) {
                owner
            } else {
                let writable_account =
                    instruction_context.try_borrow_instruction_account(transaction_context, 1)?;

                if !writable_account.get_owner().eq(owner.get_key()) {
                    ic_msg!(
                        invoke_context,
                        "Invalid account owner {} instead of {}",
                        writable_account.get_owner().to_string(),
                        owner.get_key().to_string()
                    );
                    return Err(InstructionError::IllegalOwner);
                }

                drop(owner);
                writable_account
            }
        };
        drop(writable_account);
        // do rebate
        let lamports_rebated = {
            let lamports = invoke_context
                .application_fees
                .get_mut(&writable_account_key);
            if let Some(lamports) = lamports {
                let lamports_rebated = *lamports;
                *lamports = 0;
                lamports_rebated
            } else {
                0
            }
        };
        // log message
        if lamports_rebated > 0 {
            ic_msg!(
                invoke_context,
                "application fees rebated for writable account {} lamports {}",
                writable_account_key.to_string(),
                lamports_rebated
            );
        }
        Ok(())
    }

    fn rebate_all(invoke_context: &mut InvokeContext) -> Result<(), InstructionError> {
        let transaction_context = &invoke_context.transaction_context;
        let instruction_context = transaction_context.get_current_instruction_context()?;
        let owner = instruction_context.try_borrow_instruction_account(transaction_context, 0)?;
        let owner_key = *owner.get_key();
        if !owner.is_signer() {
            ic_msg!(invoke_context, "Authority account must be a signer");
            return Err(InstructionError::MissingRequiredSignature);
        }
        drop(owner);

        let number_of_accounts = transaction_context.get_number_of_accounts();
        for i in 0..number_of_accounts {
            let account = transaction_context.get_account_at_index(i)?;
            let key = transaction_context.get_key_of_account_at_index(i)?;
            let borrowed_account = account.try_borrow();
            if let Ok(borrowed_account) = borrowed_account {
                let account_owner = borrowed_account.owner();
                if owner_key.eq(account_owner) {
                    let lamports_rebated = {
                        let lamports = invoke_context.application_fees.get_mut(key);
                        if let Some(lamports) = lamports {
                            let lamports_rebated = *lamports;
                            *lamports = 0;
                            lamports_rebated
                        } else {
                            0
                        }
                    };
                    if lamports_rebated > 0 {
                        ic_msg!(
                            invoke_context,
                            "application fees rebated for writable account {} lamports {}",
                            key.to_string(),
                            lamports_rebated,
                        );
                    }
                }
            }
        }
        Ok(())
    }
}
