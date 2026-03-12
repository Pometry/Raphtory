use storage::persist::control_file::{ControlFileOps, DBState};

use crate::{
    durability_ops::DurabilityOps,
    mutation::{addition_ops::InternalAdditionOps, MutationError},
};

pub trait RecoveryOps: DurabilityOps + InternalAdditionOps<Error = MutationError> {
    /// Recover from a crash if needed by replaying updates from the WAL.
    fn run_recovery(&self) -> Result<(), MutationError> {
        let wal = self.wal()?;
        let control_file = self.control_file()?;

        match control_file.db_state() {
            DBState::Shutdown => {
                let checkpoint_lsn = control_file.last_checkpoint();
                // let (record, next_lsn) = wal.read(checkpoint_lsn)?;
                // Make sure record is Checkpoint and record.is_shutdown() is true.
                // wal.set_next_lsn(next_lsn);
                return Ok(());
            }
            DBState::Running | DBState::CrashRecovery => {
                todo!()
            }
            DBState::NotSupported => {
                // Recovery is not supported, skip.
            }
        }

        Ok(())
    }
}
