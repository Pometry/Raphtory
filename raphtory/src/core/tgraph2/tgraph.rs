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

use super::{
    edge_store::EdgeStore,
    node_store::NodeStore,
    timer::{MaxCounter, MinCounter},
};

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
