use std::{sync::Arc, error::Error};

use docbrown_core::{graph::TemporalGraph, Prop};
use parking_lot::RwLock;
use futures::executor::ThreadPool;
use flume::{unbounded, Receiver, Sender};

pub struct GraphDB {
    nr_shards: usize,
    shards: Vec<Arc<RwLock<TemporalGraph>>>,
}

enum Msg {
    AddVertex(u64, u64),
    AddEdge(u64, u64, Vec<(String, Prop)>, u64),
    AddOutEdge(u64, u64, Vec<(String, Prop)>, u64),
    AddIntoEdge(u64, u64, Vec<(String, Prop)>, u64),
    Batch(Vec<Msg>),
    Done,
}

impl GraphDB {
    pub fn new(nr_shards: usize) -> Self {
        let mut v = vec![];
        for _ in 0..nr_shards {
            v.push(Arc::new(RwLock::new(TemporalGraph::default())))
        }
        GraphDB {
            nr_shards,
            shards: v,
        }
    }

    pub fn add_vertex(&self, v: u64, t: u64, props: Vec<Prop>) -> () {
        let shard_id = self.shard_from_global_vid(v);

        self.write_shard(shard_id, move |shard| shard.add_vertex_props(v, t, &props));
    }

    pub fn add_edge(&self, src: u64, dst: u64, t: u64, props: &Vec<(String, Prop)>) {
        let src_shard_id = self.shard_from_global_vid(src);
        let dst_shard_id = self.shard_from_global_vid(dst);

        if src_shard_id == dst_shard_id {
            self.write_shard(src_shard_id, |shard| {
                shard.add_edge_props(src, dst, t, props);
            })
        } else {
            self.write_shard(src_shard_id, |shard| {
                shard.add_edge_remote_out(src, dst, t, props);
            });

            self.write_shard(dst_shard_id, |shard| {
                shard.add_edge_remote_into(src, dst, t, props);
            });
        }
    }

    pub fn load<SOURCE:Iterator<Item = Msg>>(&self, s: SOURCE) -> Result<(), Box<dyn Error>>{
        let n_threads = 2;
        let pool = ThreadPool::new()?;
        let mut senders = vec![];

        for _ in 0 .. n_threads {
        let (sender, receiver) = unbounded::<Msg>();
            senders.push(sender);
            let future = async {

            };
            pool.spawn_ok(future)
        }

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|lock_shard| {
                let shard = lock_shard.read();
                (*shard).len()
            })
            .sum()
    }

    #[inline(always)]
    fn shard_from_global_vid(&self, v_gid: u64) -> usize {
        let a: usize = v_gid.try_into().unwrap();
        a % self.nr_shards
    }

    #[inline(always)]
    fn write_shard<A, F>(&self, shard_id: usize, f: F) -> A
    where
        F: Fn(&mut TemporalGraph) -> A,
    {
        let mut shard = self.shards[shard_id].write();
        f(&mut shard)
    }

    #[inline(always)]
    fn read_shard<A, F>(&self, shard_id: usize, f: F) -> A
    where
        F: Fn(&TemporalGraph) -> A,
    {
        let shard = self.shards[shard_id].read();
        f(&shard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}
