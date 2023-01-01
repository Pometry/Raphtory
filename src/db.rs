use std::{
    borrow::BorrowMut,
    sync::{Arc, Mutex},
};

use crate::{graph::TemporalGraph, Prop};

struct GraphDB {
    nr_shards: usize,
    shards: Vec<Arc<Mutex<TemporalGraph>>>,
}

#[derive(thiserror::Error, Debug)]
pub enum GraphError {
    #[error("Failed to acquire lock poisoned")]
    LockError(),
}

impl GraphDB {
    pub fn new(nr_shards: usize) -> Self {
        let mut v = vec![];
        for _ in 0..nr_shards {
            v.push(Arc::new(Mutex::new(TemporalGraph::default())))
        }
        GraphDB {
            nr_shards,
            shards: v,
        }
    }

    pub fn add_vertex(&self, v: u64, t: u64, props: Vec<Prop>) -> Result<(), GraphError> {
        let shard_id = self.shard_from_global_vid(v);

        if let Ok(mut shard) = self.shards[shard_id].lock() {
            shard.add_vertex_props(v, t, props);
            Ok(())
        } else {
            Err(GraphError::LockError())
        }
    }

    pub fn add_edge(&self, src: u64, dst: u64, props: Vec<Prop>) -> Result<(), GraphError> {
        // TODO: this is the part where one calls add_outbound edge on the src shard
        // TODO: and add inbound edge on the dst shard
        Ok(())
    }

    fn shard_from_global_vid(&self, v_gid: u64) -> usize {
        let a: usize = v_gid.try_into().unwrap();
        a % self.nr_shards
    }
}
