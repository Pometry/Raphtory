use crate::graph::graph::GraphStorage;
use db4_graph::TemporalGraph;
use storage::{transaction::TransactionManager, Wal};
use crate::mutation::MutationError;

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
