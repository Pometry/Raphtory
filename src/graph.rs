use std::{
    collections::BTreeMap,
    ops::RangeBounds,
    sync::{Arc, Mutex},
};

use dashmap::DashMap;
use guardian::ArcMutexGuardian;
use roaring::RoaringTreemap;

use crate::{TemporalGraphStorage, TVertexView};

#[derive(Debug)]
enum Adj {
    Empty,
    List { out: Vec<u64>, into: Vec<u64> },
}

type TsIndex = Arc<Mutex<BTreeMap<u64, RoaringTreemap>>>;

pub struct TemporalGraph {
    gs: Arc<DashMap<u64, Adj>>,
    t_index: ConcurrentTsIndex,
}

impl TemporalGraph {
    pub fn new_mem() -> Self {
        TemporalGraph {
            gs: Arc::new(DashMap::default()),
            t_index: ConcurrentTsIndex::new(BTreeMap::default()),
        }
    }
}

struct ConcurrentTsIndex {
    index: TsIndex,
}

impl ConcurrentTsIndex {
    fn new(val: BTreeMap<u64, RoaringTreemap>) -> Self {
        Self {
            index: Arc::new(Mutex::new(val)),
        }
    }

    fn iterable_lock<R: RangeBounds<u64>>(&self, r: R) -> TVertexView<R> {
        TVertexView {
            r,
            guard: ArcMutexGuardian::take(Arc::clone(&self.index)).unwrap(),
        }
    }
}

fn update_adj_list_and_ts_index(_adj: &mut Adj, t: u64, v: u64, ts_index: TsIndex) {
    if let Ok(mut index) = ts_index.lock() {
        index
            .entry(t)
            .and_modify(|set| {
                set.push(v);
            })
            .or_insert_with(|| {
                let mut bs = RoaringTreemap::default();
                bs.push(v);
                bs
            });
    }
}

impl TemporalGraphStorage for TemporalGraph {
    fn add_vertex(&self, v: u64, t: u64) -> &Self {
        let idx = self.t_index.index.clone();
        self.gs
            .entry(v)
            .and_modify(|adj| update_adj_list_and_ts_index(adj, t, v, idx.clone()))
            .or_insert_with(|| {
                update_adj_list_and_ts_index(&mut Adj::Empty, t, v, idx.clone());
                Adj::Empty
            });
        self
    }

    fn enumerate_vertices(&self) -> Vec<u64> {
        self.gs.iter().map(|entry| *entry.key()).collect()
    }

    fn enumerate_vs_at<R: RangeBounds<u64>>(&self, r: R) -> TVertexView<R>
    where
        R: RangeBounds<u64>,
    {
        self.t_index.iterable_lock(r)
    }
}
