use std::sync::{
    Arc,
    atomic::{self, AtomicU64},
};

use crate::wal::{GraphWal, TransactionID};

#[derive(Debug)]
pub struct TransactionManager<W: GraphWal> {
    last_transaction_id: AtomicU64,
    wal: Arc<W>,
}

impl<W: GraphWal> TransactionManager<W> {
    const STARTING_TRANSACTION_ID: TransactionID = 1;

    pub fn new(wal: Arc<W>) -> Self {
        Self {
            last_transaction_id: AtomicU64::new(Self::STARTING_TRANSACTION_ID),
            wal,
        }
    }

    pub fn load(self, last_transaction_id: TransactionID) {
        self.last_transaction_id
            .store(last_transaction_id, atomic::Ordering::SeqCst)
    }

    pub fn begin_transaction(&self) -> TransactionID {
        let transaction_id = self
            .last_transaction_id
            .fetch_add(1, atomic::Ordering::SeqCst);
        self.wal.log_begin_transaction(transaction_id).unwrap();
        transaction_id
    }

    pub fn end_transaction(&self, transaction_id: TransactionID) {
        self.wal.log_end_transaction(transaction_id).unwrap();
    }
}
