use crate::{graph::graph::GraphStorage, mutation::MutationError};
use storage::{transaction::TransactionGuard, ControlFile, Wal};

/// Accessor methods for supporting durability.
pub trait DurabilityOps {
    /// Start a regular update. The guard excludes bulk loads until it is dropped, so keep it
    /// for the whole update. Never call this from inside a bulk load: it would deadlock.
    fn transaction_manager(&self) -> Result<TransactionGuard<'_>, MutationError>;

    fn wal(&self) -> Result<&Wal, MutationError>;

    fn control_file(&self) -> Result<&ControlFile, MutationError>;
}

impl DurabilityOps for GraphStorage {
    fn transaction_manager(&self) -> Result<TransactionGuard<'_>, MutationError> {
        self.mutable()?.transaction_manager()
    }

    fn wal(&self) -> Result<&Wal, MutationError> {
        self.mutable()?.wal()
    }

    fn control_file(&self) -> Result<&ControlFile, MutationError> {
        self.mutable()?.control_file()
    }
}
