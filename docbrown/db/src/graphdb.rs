use std::path::{Path, PathBuf};

use docbrown_core::{
    tpartition::{TEdge, TemporalGraphPart},
    utils, Direction, Prop,
};

use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphDB {
    nr_shards: usize,
    shards: Vec<TemporalGraphPart>,
}

impl GraphDB {
    pub fn new(nr_shards: usize) -> Self {
        GraphDB {
            nr_shards,
            shards: (0..nr_shards)
                .map(|_| TemporalGraphPart::default())
                .collect(),
        }
    }

    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<bincode::ErrorKind>> {
        // use BufReader for better performance

        println!("loading from {:?}", path.as_ref());
        let mut p = PathBuf::from(path.as_ref());
        p.push("graphdb_nr_shards");

        let f = std::fs::File::open(p).unwrap();
        let mut reader = std::io::BufReader::new(f);
        let nr_shards = bincode::deserialize_from(&mut reader)?;

        let mut shard_paths = vec![];
        for i in 0..nr_shards {
            let mut p = PathBuf::from(path.as_ref());
            p.push(format!("shard_{}", i));
            shard_paths.push((i, p));
        }
        let mut shards = shard_paths
            .par_iter()
            .map(|(i, path)| {
                let shard = TemporalGraphPart::load_from_file(path)?;
                Ok((*i, shard))
            })
            .collect::<Result<Vec<_>, Box<bincode::ErrorKind>>>()?;

        shards.sort_by_cached_key(|(i, _)| *i);

        let shards = shards.into_iter().map(|(_, shard)| shard).collect();

        Ok(GraphDB { nr_shards, shards })
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<bincode::ErrorKind>> {
        // write each shard to a different file

        // crate directory path if it doesn't exist
        std::fs::create_dir_all(path.as_ref())?;

        let mut shard_paths = vec![];
        for i in 0..self.nr_shards {
            let mut p = PathBuf::from(path.as_ref());
            p.push(format!("shard_{}", i));
            println!("saving shard {} to {:?}", i, p);
            shard_paths.push((i, p));
        }
        shard_paths
            .par_iter()
            .try_for_each(|(i, path)| self.shards[*i].save_to_file(path))?;

        let mut p = PathBuf::from(path.as_ref());
        p.push("graphdb_nr_shards");

        let f = std::fs::File::create(p)?;
        let writer = std::io::BufWriter::new(f);
        bincode::serialize_into(writer, &self.nr_shards)?;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.len()).sum()
    }

    pub fn edges_len(&self) -> usize {
        self.shards.iter().map(|shard| shard.out_edges_len()).sum()
    }

    pub fn contains(&self, v: u64) -> bool {
        self.shards.iter().any(|shard| shard.contains(v))
    }

    pub fn contains_window(&self, v: u64, t_start: i64, t_end: i64) -> bool {
        self.shards
            .iter()
            .any(|shard| shard.contains_window(v, t_start, t_end))
    }

    // TODO: Probably add vector reference here like add
    pub fn add_vertex(&self, v: u64, t: i64, props: &Vec<(String, Prop)>) {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        self.shards[shard_id].add_vertex(v, t, &props);
    }

    pub fn add_edge(&self, src: u64, dst: u64, t: i64, props: &Vec<(String, Prop)>) {
        let src_shard_id = utils::get_shard_id_from_global_vid(src, self.nr_shards);
        let dst_shard_id = utils::get_shard_id_from_global_vid(dst, self.nr_shards);

        if src_shard_id == dst_shard_id {
            self.shards[src_shard_id].add_edge(src, dst, t, props)
        } else {
            // FIXME these are sort of connected, we need to hold both locks for
            // the src partition and dst partition to add a remote edge between both
            self.shards[src_shard_id].add_edge_remote_out(src, dst, t, props);
            self.shards[dst_shard_id].add_edge_remote_into(src, dst, t, props);
        }
    }

    pub fn degree(&self, v: u64, d: Direction) -> usize {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        let iter = self.shards[shard_id].degree(v, d);
        iter
    }

    pub fn degree_window(&self, v: u64, t_start: i64, t_end: i64, d: Direction) -> usize {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        let iter = self.shards[shard_id].degree_window(v, t_start, t_end, d);
        iter
    }

    pub fn vertices(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        Box::new(self.shards.iter().flat_map(|shard| shard.vertices()))
    }

    pub fn neighbours(&self, v: u64, d: Direction) -> Box<dyn Iterator<Item = TEdge>> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);

        let iter = self.shards[shard_id].neighbours(v, d);

        Box::new(iter)
    }

    pub fn neighbours_window(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = TEdge>> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);

        let iter = self.shards[shard_id].neighbours_window(v, t_start, t_end, d);

        Box::new(iter)
    }

    pub fn neighbours_window_t(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = TEdge>> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);

        let iter = self.shards[shard_id].neighbours_window_t(v, t_start, t_end, d);

        Box::new(iter)
    }
}

#[cfg(test)]
mod db_tests {
    use csv::StringRecord;
    use docbrown_core::utils;
    use itertools::Itertools;
    use rayon::vec;

    use std::{path::PathBuf, sync::Arc};

    use super::*;

    #[test]
    fn cloning_vec() {
        let mut vs = vec![];
        for i in 0..10 {
            vs.push(Arc::new(i))
        }
        let should_be_10: usize = vs.iter().map(|arc| Arc::strong_count(arc)).sum();
        assert_eq!(should_be_10, 10);

        let vs2 = vs.clone();

        let should_be_10: usize = vs2.iter().map(|arc| Arc::strong_count(arc)).sum();
        assert_eq!(should_be_10, 20)
    }

    #[quickcheck]
    fn add_vertex_grows_graph_len(vs: Vec<(u8, u8)>) {
        let g = GraphDB::new(2);

        let expected_len = vs.iter().map(|(v, _)| v).sorted().dedup().count();
        for (v, t) in vs {
            g.add_vertex(v.into(), t.into(), &vec![]);
        }

        assert_eq!(g.len(), expected_len)
    }

    #[quickcheck]
    fn add_edge_grows_graph_edge_len(edges: Vec<(u64, u64, i64)>) {
        let nr_shards: usize = 2;

        let g = GraphDB::new(nr_shards);

        let unique_vertices_count = edges
            .iter()
            .map(|(src, dst, _)| vec![src, dst])
            .flat_map(|v| v)
            .sorted()
            .dedup()
            .count();

        let unique_edge_count = edges
            .iter()
            .map(|(src, dst, _)| (src, dst))
            .unique()
            .count();

        for (src, dst, t) in edges {
            g.add_edge(src, dst, t, &vec![]);
        }

        assert_eq!(g.len(), unique_vertices_count);
        assert_eq!(g.edges_len(), unique_edge_count);
    }

    #[test]
    fn db_lotr() {
        let g = GraphDB::new(4);

        fn parse_record(rec: &StringRecord) -> Option<(String, String, i64)> {
            let src = rec.get(0).and_then(|s| s.parse::<String>().ok())?;
            let dst = rec.get(1).and_then(|s| s.parse::<String>().ok())?;
            let t = rec.get(2).and_then(|s| s.parse::<i64>().ok())?;
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
                        let src_id = utils::calculate_hash(&src);
                        let dst_id = utils::calculate_hash(&dst);

                        g.add_vertex(src_id, t, &vec![]);
                        g.add_vertex(dst_id, t, &vec![]);
                        g.add_edge(src_id, dst_id, t, &empty);
                    }
                }
            }
        }
    }
}
