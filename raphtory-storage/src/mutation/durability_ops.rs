use crate::graph::graph::GraphStorage;
use raphtory_api::inherit::Base;
use storage::{transaction::TransactionManager, Wal};

/// Accessor methods for transactions and write-ahead logging.
pub trait DurabilityOps {
    fn transaction_manager(&self) -> &TransactionManager;

    fn wal(&self) -> &Wal;
}

impl DurabilityOps for GraphStorage {
    fn transaction_manager(&self) -> &TransactionManager {
        self.mutable().unwrap().transaction_manager.as_ref()
    }

    fn wal(&self) -> &Wal {
        self.mutable().unwrap().wal()
    }
}

pub trait InheritDurabilityOps: Base {}

impl<G: InheritDurabilityOps> DurabilityOps for G
where
    G::Base: DurabilityOps,
{
    #[inline]
    fn transaction_manager(&self) -> &TransactionManager {
        self.base().transaction_manager()
    }

    #[inline]
    fn wal(&self) -> &Wal {
        self.base().wal()
    }
}
