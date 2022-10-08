use std::{
    collections::BTreeMap,
    ops::{Range, RangeBounds},
    sync::{Arc, RwLock},
};

use dashmap::DashMap;
use roaring::{RoaringBitmap, RoaringTreemap};

use crate::TemporalGraphStorage;

#[derive(Debug)]
enum Adj {
    Empty,
    List { out: Vec<u64>, into: Vec<u64> },
}
type TsIndex = RwLock<BTreeMap<u64, RoaringTreemap>>;

pub struct TemporalGraph {
    gs: Arc<DashMap<u64, Adj>>,
    t_index: Arc<TsIndex>,
}

impl TemporalGraph {
    pub fn new_mem() -> Self {
        TemporalGraph {
            gs: Arc::new(DashMap::default()),
            t_index: Arc::new(RwLock::new(BTreeMap::default())),
        }
    }
}

fn update_adj_list_and_ts_index(_adj: &mut Adj, t: u64, v: u64, ts_index: Arc<TsIndex>) {
    if let Ok(mut index) = ts_index.write() {
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
        self.gs
            .entry(v)
            .and_modify(|adj| update_adj_list_and_ts_index(adj, t, v, self.t_index.clone()))
            .or_insert_with(|| {
                update_adj_list_and_ts_index(&mut Adj::Empty, t, v, self.t_index.clone());
                Adj::Empty
            });
        self
    }

    fn enumerate_vertices(&self) -> Vec<u64> {
        self.gs.iter().map(|entry| *entry.key()).collect()
    }

    fn enumerate_vs_at<R>(&self, t: R) -> Vec<u64> 
    where R: RangeBounds<u64>
    {
        if let Ok(index) = self.t_index.read() {
            index.range(t).flat_map(|(_, vs)| vs.iter()).collect()
        } else {
            vec![]
        }
    }
}
