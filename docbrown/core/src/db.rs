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
            shard.add_vertex_props(v, t, &props);
            Ok(())
        } else {
            Err(GraphError::LockError())
        }
    }

    #[inline(always)]
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


#[cfg(test)]
mod db_test {
    use csv::StringRecord;

    use crate::{db::GraphDB, Prop};

    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
        path::PathBuf,
    };

    #[test]
    fn db_lotr() {

        let g = GraphDB::new(4);

        fn calculate_hash<T: Hash>(t: &T) -> u64 {
            let mut s = DefaultHasher::new();
            t.hash(&mut s);
            s.finish()
        }

        fn parse_record(rec: &StringRecord) -> Option<(String, String, u64)> {
            let src = rec.get(0).and_then(|s| s.parse::<String>().ok())?;
            let dst = rec.get(1).and_then(|s| s.parse::<String>().ok())?;
            let t = rec.get(2).and_then(|s| s.parse::<u64>().ok())?;
            Some((src, dst, t))
        }

        let lotr_csv: PathBuf = [env!("CARGO_MANIFEST_DIR"), "resources/test/lotr.csv"]
            .iter()
            .collect();

        let empty:Vec<(String, Prop)> = vec![];

        if let Ok(mut reader) = csv::Reader::from_path(lotr_csv) {
            for rec_res in reader.records() {
                if let Ok(rec) = rec_res {
                    if let Some((src, dst, t)) = parse_record(&rec) {
                        let src_id = calculate_hash(&src);
                        let dst_id = calculate_hash(&dst);

                        g.add_vertex(src_id, t, vec![]).unwrap();
                        g.add_vertex(dst_id, t, vec![]).unwrap();
                        g.add_edge(src_id, dst_id, t, &empty).unwrap();
                    }
                }
            }
        }
    }
}
