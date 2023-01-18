#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

use std::sync::Arc;

use docbrown_core::{graph::TemporalGraph, Prop};
use parking_lot::RwLock;

#[derive(Debug, Clone)]
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

    pub fn add_vertex(&self, v: u64, t: u64, props: Vec<Prop>) {
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
mod db_tests {
    use itertools::Itertools;
    use csv::StringRecord;

    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
        path::PathBuf,
    };

    use super::*;

    #[quickcheck]
    fn add_vertex_to_graph_len_grows(vs: Vec<(u8, u8)>) {
        let g = GraphDB::new(2);

        let expected_len = vs.iter().map(|(v, _)| v).sorted().dedup().count();
        for (v, t) in vs {
            g.add_vertex(v.into(), t.into(), vec![]);
        }

        assert_eq!(g.len(), expected_len)

    }

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

                        g.add_vertex(src_id, t, vec![]);
                        g.add_vertex(dst_id, t, vec![]);
                        g.add_edge(src_id, dst_id, t, &empty);
                    }
                }
            }
        }
    }

}
