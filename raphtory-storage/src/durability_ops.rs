use crate::{graph::graph::GraphStorage, mutation::MutationError};
use storage::{transaction::TransactionManager, ControlFile, Wal};

/// Accessor methods for supporting durability.
pub trait DurabilityOps {
    fn transaction_manager(&self) -> Result<&TransactionManager, MutationError>;

    fn wal(&self) -> Result<&Wal, MutationError>;

    fn control_file(&self) -> Result<&ControlFile, MutationError>;
}

impl DurabilityOps for GraphStorage {
    fn transaction_manager(&self) -> Result<&TransactionManager, MutationError> {
        self.mutable()?.transaction_manager()
    }

    fn wal(&self) -> Result<&Wal, MutationError> {
        self.mutable()?.wal()
    }

    fn control_file(&self) -> Result<&ControlFile, MutationError> {
        self.mutable()?.control_file()
    }
}
