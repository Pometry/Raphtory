use crate::graph_window::WindowedGraph;
use std::{
    collections::HashMap,
    ops::Range,
    path::{Path, PathBuf},
    sync::{mpsc, Arc},
};

use docbrown_core::{
    tgraph::{EdgeView, VertexView},
    tgraph_shard::TGraphShard,
    utils, Direction, Prop,
};

use itertools::Itertools;
use rayon::prelude::*;
use tempdir::TempDir;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Graph {
    nr_shards: usize,
    pub(crate) shards: Vec<TGraphShard>,
}

impl Graph {
    pub fn new(nr_shards: usize) -> Self {
        Graph {
            nr_shards,
            shards: (0..nr_shards).map(|_| TGraphShard::default()).collect(),
        }
    }

    pub fn earliest_time(&self) -> Option<i64> {
        let min_from_shards = self.shards.iter().map(|shard|shard.earliest_time()).min();
        match min_from_shards {
            None => {None}
            Some(min) => {if min == i64::MAX {
                None
            }
            else {
               Some(min)
            }}
        }
    }

    pub fn latest_time(&self) -> Option<i64> {
        let max_from_shards = self.shards.iter().map(|shard|shard.latest_time()).max();
        match max_from_shards {
            None => {
                None
            },
            Some(max) => {if max == i64::MIN {
                None
            }
            else {
                Some(max)
            }}
        }
    }

    pub fn window(&self, t_start: i64, t_end: i64) -> WindowedGraph {
        WindowedGraph::new(self.clone(), t_start, t_end)
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
                let shard = TGraphShard::load_from_file(path)?;
                Ok((*i, shard))
            })
            .collect::<Result<Vec<_>, Box<bincode::ErrorKind>>>()?;

        shards.sort_by_cached_key(|(i, _)| *i);

        let shards = shards.into_iter().map(|(_, shard)| shard).collect();
        Ok(Graph { nr_shards,shards }) //TODO I need to put in the actual values here
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

    // TODO: Probably add vector reference here like add
    pub fn add_vertex(&self, t: i64, v: u64, props: &Vec<(String, Prop)>) {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        self.shards[shard_id].add_vertex(t, v, &props);
    }

    pub fn add_edge(&self, t: i64, src: u64, dst: u64, props: &Vec<(String, Prop)>) {
        let src_shard_id = utils::get_shard_id_from_global_vid(src, self.nr_shards);
        let dst_shard_id = utils::get_shard_id_from_global_vid(dst, self.nr_shards);

        if src_shard_id == dst_shard_id {
            self.shards[src_shard_id].add_edge(t, src, dst, props)
        } else {
            // FIXME these are sort of connected, we need to hold both locks for
            // the src partition and dst partition to add a remote edge between both
            self.shards[src_shard_id].add_edge_remote_out(t, src, dst, props);
            self.shards[dst_shard_id].add_edge_remote_into(t, src, dst, props);
        }
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.len()).sum()
    }

    pub fn edges_len(&self) -> usize {
        self.shards.iter().map(|shard| shard.out_edges_len()).sum()
    }

    pub fn has_edge(&self, src: u64, dst: u64) -> bool {
        let shard_id = utils::get_shard_id_from_global_vid(src, self.nr_shards);
        self.shards[shard_id].has_edge(src, dst)
    }

    pub fn has_edge_window(&self, src: u64, dst: u64, t_start: i64, t_end: i64) -> bool {
        let shard_id = utils::get_shard_id_from_global_vid(src, self.nr_shards);
        self.shards[shard_id].has_edge_window(src, dst, t_start..t_end)
    }

    pub fn has_vertex(&self, v: u64) -> bool {
        self.shards.iter().any(|shard| shard.has_vertex(v))
    }

    pub(crate) fn has_vertex_window(&self, v: u64, t_start: i64, t_end: i64) -> bool {
        self.shards
            .iter()
            .any(|shard| shard.has_vertex_window(v, t_start..t_end))
    }

    pub(crate) fn vertex(&self, v: u64) -> Option<VertexView> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        self.shards[shard_id].vertex(v)
    }

    pub(crate) fn vertex_window(&self, v: u64, t_start: i64, t_end: i64) -> Option<VertexView> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        self.shards[shard_id].vertex_window(v, t_start..t_end)
    }

    pub(crate) fn degree_window(&self, v: u64, t_start: i64, t_end: i64, d: Direction) -> usize {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        let iter = self.shards[shard_id].degree_window(v, t_start..t_end, d);
        iter
    }

    pub(crate) fn vertex_ids_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = u64> + Send> {
        let shards = self.shards.clone();
        Box::new(
            shards
                .into_iter()
                .map(move |shard| shard.vertex_ids_window(t_start..t_end))
                .into_iter()
                .flatten(),
        )
    }

    pub(crate) fn vertices_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexView> + Send> {
        let shards = self.shards.clone();
        Box::new(
            shards
                .into_iter()
                .map(move |shard| shard.vertices_window(t_start..t_end))
                .flatten(),
        )
    }

    pub(crate) fn fold_par<S, F, F2>(&self, t_start: i64, t_end: i64, f: F, agg: F2) -> Option<S>
    where
        S: Send,
        F: Fn(VertexView) -> S + Send + Sync + Copy,
        F2: Fn(S, S) -> S + Sync + Send + Copy,
    {
        let shards = self.shards.clone();

        let out = shards
            .into_par_iter()
            .map(|shard| {
                shard.read_shard(|tg_core| {
                    tg_core.vertices_window(t_start..t_end).par_bridge().map(f).reduce_with(agg)
                })
            })
            .flatten()
            .reduce_with(agg);

        out
    }

    pub(crate) fn vertex_window_par<O, F>(
        &self,
        t_start: i64,
        t_end: i64,
        f: F,
    ) -> Box<dyn Iterator<Item = O>>
    where
        O: Send + 'static,
        F: Fn(VertexView) -> O + Send + Sync + Copy,
    {
        let shards = self.shards.clone();
        let (tx, rx) = flume::unbounded();

        let arc_tx = Arc::new(tx);
        shards
            .into_par_iter()
            .map(|shard| shard.vertices_window(t_start..t_end).par_bridge().map(f))
            .flatten()
            .for_each(move |o| {
                arc_tx.send(o).unwrap();
            });

        Box::new(rx.into_iter())
    }

    pub(crate) fn edge(&self, v1: u64, v2: u64) -> Option<EdgeView> {
        let shard_id = utils::get_shard_id_from_global_vid(v1, self.nr_shards);
        self.shards[shard_id].edge(v1, v2)
    }

    pub(crate) fn edge_window(
        &self,
        src: u64,
        dst: u64,
        t_start: i64,
        t_end: i64,
    ) -> Option<EdgeView> {
        let shard_id = utils::get_shard_id_from_global_vid(src, self.nr_shards);
        self.shards[shard_id].edge_window(src, dst, t_start..t_end)
    }

    pub(crate) fn vertex_edges_window(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeView> + Send> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        let iter = self.shards[shard_id].vertex_edges_window(v, t_start..t_end, d);
        Box::new(iter)
    }

    pub(crate) fn vertex_edges_window_t(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeView> + Send> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        let iter = self.shards[shard_id].vertex_edges_window_t(v, t_start..t_end, d);
        Box::new(iter)
    }

    pub(crate) fn neighbours_window(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = VertexView> + Send> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        let iter = self.shards[shard_id].neighbours_window(v, t_start..t_end, d);
        Box::new(iter)
    }

    pub(crate) fn neighbours_ids_window(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send>
    where
        Self: Sized,
    {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        let iter = self.shards[shard_id]
            .neighbours_ids_window(v, t_start..t_end, d)
            .unique();
        Box::new(iter)
    }

    pub(crate) fn vertex_prop_vec(&self, v: u64, name: String) -> Vec<(i64, Prop)> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        self.shards[shard_id].vertex_prop_vec(v, name)
    }

    pub(crate) fn vertex_prop_vec_window(
        &self,
        v: u64,
        name: String,
        w: Range<i64>,
    ) -> Vec<(i64, Prop)> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        self.shards[shard_id].vertex_prop_vec_window(v, name, w)
    }

    pub(crate) fn vertex_props(&self, v: u64) -> HashMap<String, Vec<(i64, Prop)>> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        self.shards[shard_id].vertex_props(v)
    }

    pub(crate) fn vertex_props_window(
        &self,
        v: u64,
        w: Range<i64>,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        self.shards[shard_id].vertex_props_window(v, w)
    }

    pub fn edge_props_vec_window(
        &self,
        v: u64,
        e: usize,
        name: String,
        w: Range<i64>,
    ) -> Vec<(i64, Prop)> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        self.shards[shard_id].edge_props_vec_window(e, name, w)
    }
}

#[cfg(test)]
mod db_tests {
    use csv::StringRecord;
    use docbrown_core::utils;
    use itertools::Itertools;
    use quickcheck::{quickcheck, TestResult};
    use rand::Rng;
    use std::collections::HashMap;
    use std::{env, fs};
    use std::sync::Arc;
    use uuid::Uuid;
    use crate::graphgen::random_attachment::random_attachment;

    use crate::algorithms::local_triangle_count::local_triangle_count;

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
        let g = Graph::new(2);

        let expected_len = vs.iter().map(|(_, v)| v).sorted().dedup().count();
        for (t, v) in vs {
            g.add_vertex(t.into(), v.into(), &vec![]);
        }

        assert_eq!(g.len(), expected_len)
    }

    #[quickcheck]
    fn add_edge_grows_graph_edge_len(edges: Vec<(i64, u64, u64)>) {
        let nr_shards: usize = 2;

        let g = Graph::new(nr_shards);

        let unique_vertices_count = edges
            .iter()
            .map(|(_, src, dst)| vec![src, dst])
            .flat_map(|v| v)
            .sorted()
            .dedup()
            .count();

        let unique_edge_count = edges
            .iter()
            .map(|(_, src, dst)| (src, dst))
            .unique()
            .count();

        for (t, src, dst) in edges {
            g.add_edge(t, src, dst, &vec![]);
        }

        assert_eq!(g.len(), unique_vertices_count);
        assert_eq!(g.edges_len(), unique_edge_count);
    }

    #[test]
    fn graph_save_to_load_from_file() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = Graph::new(2);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let rand_dir = Uuid::new_v4();
        let tmp_docbrown_path: TempDir = TempDir::new("docbrown").unwrap();
        let shards_path = format!("{:?}/{}", tmp_docbrown_path.path()
            .display(), rand_dir).replace("\"", "");

        println!("shards_path: {}", shards_path);

        // Save to files
        let mut expected = vec![
            format!("{}/shard_1", shards_path),
            format!("{}/shard_0", shards_path),
            format!("{}/graphdb_nr_shards", shards_path),
        ].iter().map(Path::new).map(PathBuf::from).collect::<Vec<_>>();

        expected.sort();

        match g.save_to_file(&shards_path) {
            Ok(()) => {
                let mut actual = fs::read_dir(&shards_path)
                    .unwrap()
                    .map(|f| f.unwrap().path())
                    .collect::<Vec<_>>();

                actual.sort();

                assert_eq!(actual, expected);
            }
            Err(e) => panic!("{e}"),
        }

        // Load from files
        match Graph::load_from_file(Path::new(&shards_path)) {
            Ok(g) => {
                assert!(g.has_vertex(1));
                assert_eq!(g.nr_shards, 2);
            }
            Err(e) => panic!("{e}"),
        }

        tmp_docbrown_path.close();
    }

    #[test]
    fn has_edge() {
        let g = Graph::new(2);
        g.add_edge(1, 7, 8, &vec![]);

        assert_eq!(g.has_edge(8, 7), false);
        assert_eq!(g.has_edge(7, 8), true);

        g.add_edge(1, 7, 9, &vec![]);

        assert_eq!(g.has_edge(9, 7), false);
        assert_eq!(g.has_edge(7, 9), true);
    }

    #[test]
    fn graph_edge() {
        let g = Graph::new(2);
        let es = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];
        for (t, src, dst) in es {
            g.add_edge(t, src, dst, &vec![])
        }

        assert_eq!(
            g.edge_window(1, 3, i64::MIN, i64::MAX).unwrap().src_g_id,
            1u64
        );
        assert_eq!(
            g.edge_window(1, 3, i64::MIN, i64::MAX).unwrap().dst_g_id,
            3u64
        );
    }

    #[test]
    fn graph_degree_window() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = Graph::new(1);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let expected = vec![(2, 3, 1), (1, 0, 0), (1, 0, 0)];
        let actual = (1..=3)
            .map(|i| {
                (
                    g.degree_window(i, -1, 7, Direction::IN),
                    g.degree_window(i, 1, 7, Direction::OUT),
                    g.degree_window(i, 0, 1, Direction::BOTH),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);

        // Check results from multiple graphs with different number of shards
        let g = Graph::new(3);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let expected = (1..=3)
            .map(|i| {
                (
                    g.degree_window(i, -1, 7, Direction::IN),
                    g.degree_window(i, 1, 7, Direction::OUT),
                    g.degree_window(i, 0, 1, Direction::BOTH),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn graph_edges_window() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = Graph::new(1);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let expected = vec![(2, 3, 2), (1, 0, 0), (1, 0, 0)];
        let actual = (1..=3)
            .map(|i| {
                (
                    g.vertex_edges_window(i, -1, 7, Direction::IN)
                        .collect::<Vec<_>>()
                        .len(),
                    g.vertex_edges_window(i, 1, 7, Direction::OUT)
                        .collect::<Vec<_>>()
                        .len(),
                    g.vertex_edges_window(i, 0, 1, Direction::BOTH)
                        .collect::<Vec<_>>()
                        .len(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);

        // Check results from multiple graphs with different number of shards
        let g = Graph::new(10);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let expected = (1..=3)
            .map(|i| {
                (
                    g.vertex_edges_window(i, -1, 7, Direction::IN)
                        .collect::<Vec<_>>()
                        .len(),
                    g.vertex_edges_window(i, 1, 7, Direction::OUT)
                        .collect::<Vec<_>>()
                        .len(),
                    g.vertex_edges_window(i, 0, 1, Direction::BOTH)
                        .collect::<Vec<_>>()
                        .len(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn graph_edges_window_t() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = Graph::new(1);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let in_actual = (1..=3)
            .map(|i| {
                g.vertex_edges_window_t(i, -1, 7, Direction::IN)
                    .map(|e| e.time.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(vec![vec![-1, 0, 1], vec![1], vec![2]], in_actual);

        let out_actual = (1..=3)
            .map(|i| {
                g.vertex_edges_window_t(i, 1, 7, Direction::OUT)
                    .map(|e| e.time.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(vec![vec![1, 1, 2], vec![], vec![]], out_actual);

        let both_actual = (1..=3)
            .map(|i| {
                g.vertex_edges_window_t(i, 0, 1, Direction::BOTH)
                    .map(|e| e.time.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(vec![vec![0, 0], vec![], vec![]], both_actual);

        // Check results from multiple graphs with different number of shards
        let g = Graph::new(4);

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let in_expected = (1..=3)
            .map(|i| {
                let mut e = g
                    .vertex_edges_window_t(i, -1, 7, Direction::IN)
                    .map(|e| e.time.unwrap())
                    .collect::<Vec<_>>();
                e.sort();
                e
            })
            .collect::<Vec<_>>();
        assert_eq!(in_expected, in_actual);

        let out_expected = (1..=3)
            .map(|i| {
                let mut e = g
                    .vertex_edges_window_t(i, 1, 7, Direction::OUT)
                    .map(|e| e.time.unwrap())
                    .collect::<Vec<_>>();
                e.sort();
                e
            })
            .collect::<Vec<_>>();
        assert_eq!(out_expected, out_actual);

        let both_expected = (1..=3)
            .map(|i| {
                let mut e = g
                    .vertex_edges_window_t(i, 0, 1, Direction::BOTH)
                    .map(|e| e.time.unwrap())
                    .collect::<Vec<_>>();
                e.sort();
                e
            })
            .collect::<Vec<_>>();
        assert_eq!(both_expected, both_actual);
    }

    #[test]
    fn time_test() {
        let g = Graph::new(4);

        assert_eq!(g.latest_time(),None);
        assert_eq!(g.earliest_time(),None);

        g.add_vertex(5,1,&vec![]);

        assert_eq!(g.latest_time(),Some(5));
        assert_eq!(g.earliest_time(),Some(5));

        let g = Graph::new(4);

        g.add_edge(10,1, 2,&vec![]);
        assert_eq!(g.latest_time(),Some(10));
        assert_eq!(g.earliest_time(),Some(10));

        g.add_vertex(5,1,&vec![]);
        assert_eq!(g.latest_time(),Some(10));
        assert_eq!(g.earliest_time(),Some(5));

        g.add_edge(20,3, 4,&vec![]);
        assert_eq!(g.latest_time(),Some(20));
        assert_eq!(g.earliest_time(),Some(5));

        random_attachment(&g,100,10);
        assert_eq!(g.latest_time(),Some(126));
        assert_eq!(g.earliest_time(),Some(5));

    }

    #[test]
    fn graph_neighbours_window() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = Graph::new(2);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let expected = [
            (
                vec![
                    VertexView {
                        g_id: 1,
                        pid: Some(0),
                    },
                    VertexView { g_id: 2, pid: None },
                ],
                vec![
                    VertexView {
                        g_id: 1,
                        pid: Some(0),
                    },
                    VertexView {
                        g_id: 3,
                        pid: Some(1),
                    },
                    VertexView { g_id: 2, pid: None },
                ],
                vec![
                    VertexView {
                        g_id: 1,
                        pid: Some(0),
                    },
                    VertexView {
                        g_id: 1,
                        pid: Some(0),
                    },
                ],
            ),
            (vec![VertexView { g_id: 1, pid: None }], vec![], vec![]),
            (
                vec![VertexView {
                    g_id: 1,
                    pid: Some(0),
                }],
                vec![],
                vec![],
            ),
        ];
        let actual = (1..=3)
            .map(|i| {
                (
                    g.neighbours_window(i, -1, 7, Direction::IN)
                        .collect::<Vec<_>>(),
                    g.neighbours_window(i, 1, 7, Direction::OUT)
                        .collect::<Vec<_>>(),
                    g.neighbours_window(i, 0, 1, Direction::BOTH)
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn db_lotr() {
        let g = Graph::new(4);

        let data_dir =
            crate::graph_loader::lotr_graph::lotr_file().expect("Failed to get lotr.csv file");

        fn parse_record(rec: &StringRecord) -> Option<(String, String, i64)> {
            let src = rec.get(0).and_then(|s| s.parse::<String>().ok())?;
            let dst = rec.get(1).and_then(|s| s.parse::<String>().ok())?;
            let t = rec.get(2).and_then(|s| s.parse::<i64>().ok())?;
            Some((src, dst, t))
        }

        if let Ok(mut reader) = csv::Reader::from_path(data_dir) {
            for rec_res in reader.records() {
                if let Ok(rec) = rec_res {
                    if let Some((src, dst, t)) = parse_record(&rec) {
                        let src_id = utils::calculate_hash(&src);
                        let dst_id = utils::calculate_hash(&dst);

                        g.add_vertex(
                            t,
                            src_id,
                            &vec![("name".to_string(), Prop::Str("Character".to_string()))],
                        );
                        g.add_vertex(
                            t,
                            dst_id,
                            &vec![("name".to_string(), Prop::Str("Character".to_string()))],
                        );
                        g.add_edge(
                            t,
                            src_id,
                            dst_id,
                            &vec![(
                                "name".to_string(),
                                Prop::Str("Character Co-occurrence".to_string()),
                            )],
                        );
                    }
                }
            }
        }

        let gandalf = utils::calculate_hash(&"Gandalf");
        assert!(g.has_vertex(gandalf));
    }

    #[test]
    fn test_lotr_load_graph() {
        let g = crate::graph_loader::lotr_graph::lotr_graph(4);
        assert_eq!(g.edges_len(), 701);
    }

    //TODO: move this to integration tests or speed it up
    #[ignore]
    #[test]
    fn test_twitter_load_graph() {
        let g = crate::graph_loader::twitter_graph::twitter_graph(1);
        let windowed_graph = g.window(i64::MIN, i64::MAX);
        let mut i = 0;
        println!("Starting analysis");
        windowed_graph.vertex_ids().for_each(|v| {
            local_triangle_count(&windowed_graph, v);
            i += 1;
        });
        assert_eq!(g.edges_len(), 1089147);
        assert_eq!(g.len(), 49467);
    }
}
