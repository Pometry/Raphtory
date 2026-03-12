use storage::persist::control_file::{ControlFileOps, DBState};
use storage::wal::{GraphWalOps, WalOps};

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
                let end_of_wal_lsn = wal.read_shutdown_checkpoint(checkpoint_lsn)?;

                // LSN after the shutdown checkpoint points to the end of WAL stream.
                // Set this as the next LSN for future writes.
                wal.set_next_lsn(end_of_wal_lsn);

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
