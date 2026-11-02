use parking_lot::RwLockReadGuard;
use std::{
    ops::Deref,
    sync::atomic::{self, AtomicU64},
};

use crate::wal::TransactionID;

#[derive(Debug)]
pub struct TransactionManager {
    last_transaction_id: AtomicU64,
}

impl TransactionManager {
    const STARTING_TRANSACTION_ID: TransactionID = 1;

    pub fn new() -> Self {
        Self {
            last_transaction_id: AtomicU64::new(Self::STARTING_TRANSACTION_ID),
        }
    }

    /// Restores the last used transaction ID to the specified value.
    /// Intended for using during recovery.
    pub fn restore_transaction_id(&self, last_transaction_id: TransactionID) {
        self.last_transaction_id
            .store(last_transaction_id, atomic::Ordering::SeqCst)
    }

    pub fn begin_transaction(&self) -> TransactionID {
        self.last_transaction_id
            .fetch_add(1, atomic::Ordering::SeqCst)
    }

    pub fn end_transaction(&self, _transaction_id: TransactionID) {
        // No-op for now.
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// The transaction manager together with a bulk load read guard, so a regular update
/// cannot run while a bulk load holds the graph. Keep it until the update ends.
pub struct TransactionGuard<'a> {
    manager: &'a TransactionManager,
    _bulk_load: RwLockReadGuard<'a, ()>,
}

impl<'a> TransactionGuard<'a> {
    pub fn new(manager: &'a TransactionManager, bulk_load: RwLockReadGuard<'a, ()>) -> Self {
        Self {
            manager,
            _bulk_load: bulk_load,
        }
    }
}

impl Deref for TransactionGuard<'_> {
    type Target = TransactionManager;

    fn deref(&self) -> &Self::Target {
        self.manager
    }
}
