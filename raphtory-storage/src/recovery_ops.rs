use storage::{
    persist::control_file::{ControlFileOps, DBState, LAST_CHECKPOINT_INIT},
    wal::{GraphWalOps, WalOps},
};

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
                wal.set_position(end_of_wal_lsn)?;
            }
            DBState::Running | DBState::CrashRecovery => {
                let checkpoint_lsn = control_file.last_checkpoint();

                let redo_lsn = if checkpoint_lsn == LAST_CHECKPOINT_INIT {
                    // No successful checkpoint has been written yet,
                    // replay from the start of the WAL stream.
                    0
                } else {
                    wal.read_checkpoint(checkpoint_lsn)?
                };

                // Set db state to indicate that recovery is in progress.
                control_file.set_db_state(DBState::CrashRecovery);
                control_file.save()?;

                let mut write_locked_graph = self.write_lock()?;
                let end_of_wal_lsn = wal.replay_to_graph(&mut write_locked_graph, redo_lsn)?;

                // Set the next LSN for future writes to the end of the WAL stream.
                wal.set_position(end_of_wal_lsn)?;
            }
            DBState::NotSupported => {
                // Recovery is not supported, skip.
            }
        }

        // Always set db state to Running after recovery completes.
        control_file.set_db_state(DBState::Running);
        control_file.save()?;

        Ok(())
    }
}
