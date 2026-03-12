use storage::persist::control_file::{ControlFileOps, DBState};

use crate::{
    durability_ops::DurabilityOps,
    mutation::{addition_ops::InternalAdditionOps, MutationError},
};

pub trait RecoveryOps: DurabilityOps + InternalAdditionOps<Error = MutationError> {
    /// Recover from a crash if needed by replaying updates from the WAL.
    fn run_recovery(&self) -> Result<(), MutationError> {
        let transaction_manager = self.transaction_manager()?;
        let wal = self.wal()?;
        let control_file = self.control_file()?;

        match control_file.db_state() {
            DBState::Shutdown => {
                // DB was shutdown cleanly, no need for recovery.
                return Ok(());
            }
            DBState::Running => {
                todo!()
            }
            DBState::CrashRecovery => {
                todo!()
            }
        }

        Ok(())
    }
}
