//! TODO

use solana_sdk_macro::CloneZeroed;

pub type Slot = u64;

/// TODO - document
#[repr(C)]
#[derive(Serialize, Deserialize, Debug, CloneZeroed, PartialEq, Eq)]
pub struct LastRestartSlot {
    /// The last restart `Slot`.
    pub last_restart_slot: Slot,
}

impl Default for LastRestartSlot {
    fn default() -> Self {
        Self {
            last_restart_slot: 0,
        }
    }
}

// TODO
// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn test_clone() {
//         let clock = Clock {
//             slot: 1,
//             epoch_start_timestamp: 2,
//             epoch: 3,
//             leader_schedule_epoch: 4,
//             unix_timestamp: 5,
//         };
//         let cloned_clock = clock.clone();
//         assert_eq!(cloned_clock, clock);
//     }
// }
