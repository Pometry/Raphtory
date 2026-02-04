use crate::{graph::graph::GraphStorage, mutation::MutationError};
use storage::{transaction::TransactionManager, Wal};

/// Accessor methods for transactions and write-ahead logging.
pub trait DurabilityOps {
    fn transaction_manager(&self) -> Result<&TransactionManager, MutationError>;

    fn wal(&self) -> Result<&Wal, MutationError>;
}

impl DurabilityOps for GraphStorage {
    fn transaction_manager(&self) -> Result<&TransactionManager, MutationError> {
        self.mutable()?.transaction_manager()
    }

    fn wal(&self) -> Result<&Wal, MutationError> {
        self.mutable()?.wal()
    }
}
