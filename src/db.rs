use std::sync::{Arc, Mutex};

use crate::{graph::TemporalGraph, Prop};

pub struct GraphDB {
    nr_shards: usize,
    shards: Vec<Arc<Mutex<TemporalGraph>>>,
}

#[derive(thiserror::Error, Debug)]
pub enum GraphError {
    #[error("Failed to acquire poisoned lock")]
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

    pub fn len(&self) -> usize {
        self.shards.iter().flat_map(|shard| shard.lock().map(|s| s.len()).ok()).sum()
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

    fn with_shard<A, F>(&self, shard_id: usize, f: F) -> Result<A, GraphError>
    where
        F: Fn(&mut TemporalGraph) -> A,
    {
        if let Ok(mut shard) = self.shards[shard_id].lock() {
            Ok(f(&mut shard))
        } else {
            Err(GraphError::LockError())
        }
    }

    pub fn add_edge(
        &self,
        src: u64,
        dst: u64,
        t: u64,
        props: &Vec<(String, Prop)>,
    ) -> Result<(), GraphError> {
        let src_shard_id = self.shard_from_global_vid(src);
        let dst_shard_id = self.shard_from_global_vid(dst);

        if src_shard_id == dst_shard_id {
            self.with_shard(src_shard_id, |shard| {
                shard.add_edge_props(src, dst, t, props);

            })
        } else {
            self.with_shard(src_shard_id, |shard| {
                shard.add_edge_remote_out(src, dst, t, props);
            })
            .and_then(|_| {
                self.with_shard(dst_shard_id, |shard| {
                    shard.add_edge_remote_into(src, dst, t, props);
                })
            })
            .and_then(|_| Ok(()))
        }
    }

    fn shard_from_global_vid(&self, v_gid: u64) -> usize {
        let a: usize = v_gid.try_into().unwrap();
        a % self.nr_shards
    }
}
