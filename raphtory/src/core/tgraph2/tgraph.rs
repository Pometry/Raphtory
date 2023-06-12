use std::{
    hash::BuildHasherDefault,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};

use dashmap::DashMap;
use rustc_hash::FxHasher;

use crate::storage;

use super::{node_store::NodeStore, edge_store::EdgeStore};

type FxDashMap<K, V> = DashMap<K, V, BuildHasherDefault<FxHasher>>;

pub struct TemporalGraph<const N: usize, L: lock_api::RawRwLock> {
    // mapping between logical and physical ids
    logical_to_physical: FxDashMap<u64, usize>,

    // node storage with having (id, time_index, properties, adj list for each layer)
    nodes: Arc<storage::RawStorage<NodeStore<N>, L, N>>,

    // edge storage with having (src, dst, time_index, properties) for each layer
    edges: Arc<storage::RawStorage<EdgeStore<N>, L, N>>,

    //earliest time seen in this graph
    pub(crate) earliest_time: MinCounter,

    //latest time seen in this graph
    pub(crate) latest_time: MaxCounter,
}


trait TimeCounterTrait {
    fn cmp(a: i64, b: i64) -> bool;
    fn counter(&self) -> &AtomicI64;

    fn update(&self, time: i64) -> bool {
        let mut current = self.get();
        while Self::cmp(current, time) {
            match self.counter().compare_exchange_weak(
                current,
                time,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(x) => current = x,
            }
        }
        false
    }
    fn get(&self) -> i64;
}

pub(crate) struct MinCounter {
    counter: AtomicI64,
}

impl TimeCounterTrait for MinCounter {
    fn get(&self) -> i64 {
        self.counter.load(Ordering::Relaxed)
    }

    fn cmp(a: i64, b: i64) -> bool {
        a < b
    }

    fn counter(&self) -> &AtomicI64 {
        &self.counter
    }
}

pub(crate) struct MaxCounter {
    counter: AtomicI64,
}

impl TimeCounterTrait for MaxCounter {
    fn cmp(a: i64, b: i64) -> bool {
        a > b
    }
    fn get(&self) -> i64 {
        self.counter.load(Ordering::Relaxed)
    }

    fn counter(&self) -> &AtomicI64 {
        &self.counter
    }
}
