#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

pub mod loaders;

use docbrown_core::{tpartition::TemporalGraphPart, Prop};

#[derive(Debug, Clone)]
pub struct GraphDB {
    nr_shards: usize,
    shards: Vec<TemporalGraphPart>,
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
            v.push(TemporalGraphPart::default())
        }
        GraphDB {
            nr_shards,
            shards: v,
        }
    }

    pub fn add_vertex(&self, v: u64, t: u64, props: Vec<(String, Prop)>) {
        let shard_id = self.shard_from_global_vid(v);
        self.shards[shard_id].add_vertex(t, v, &props);
    }

    pub fn add_edge(&self, src: u64, dst: u64, t: u64, props: &Vec<(String, Prop)>) {
        let src_shard_id = self.shard_from_global_vid(src);
        let dst_shard_id = self.shard_from_global_vid(dst);

        if src_shard_id == dst_shard_id {
            self.shards[src_shard_id].add_edge(src, dst, t, props)
        } else {
            // FIXME these are sort of connected, we need to hold both locks for
            // the src partition and dst partition to add a remote edge between both
            self.shards[src_shard_id].add_edge_remote_out(src, dst, t, props);
            self.shards[dst_shard_id].add_edge_remote_into(src, dst, t, props);
        }
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.len()).sum()
    }

    #[inline(always)]
    fn shard_from_global_vid(&self, v_gid: u64) -> usize {
        let a: usize = v_gid.try_into().unwrap();
        a % self.nr_shards
    }
}

#[cfg(test)]
mod db_tests {
    use csv::StringRecord;
    use itertools::Itertools;

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

        let empty: Vec<(String, Prop)> = vec![]; // FIXME: add actual properties here

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
