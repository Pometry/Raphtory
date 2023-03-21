use crate::graph_window::{GraphWindowSet, WindowedGraph};
use crate::perspective::{Perspective, PerspectiveIterator, PerspectiveSet};
use std::{
    collections::HashMap,
    iter,
    ops::Range,
    path::{Path, PathBuf},
    sync::{mpsc, Arc},
};

use docbrown_core::{
    tgraph::{EdgeRef, VertexRef},
    tgraph_shard::TGraphShard,
    utils,
    vertex::InputVertex,
    Direction, Prop,
};

use crate::edge::EdgeView;
use crate::vertex::VertexView;
use crate::view_api::internal::GraphViewInternalOps;
use crate::view_api::*;
use itertools::Itertools;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use tempdir::TempDir;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Graph {
    nr_shards: usize,
    pub(crate) shards: Vec<TGraphShard>,
}

impl GraphViewInternalOps for Graph {
    fn vertices_len(&self) -> usize {
        self.shards.iter().map(|shard| shard.len()).sum()
    }

    fn vertices_len_window(&self, t_start: i64, t_end: i64) -> usize {
        //FIXME: This nees to be optimised ideally
        self.shards
            .iter()
            .map(|shard| shard.vertices_window(t_start..t_end).count())
            .sum()
    }

    fn edges_len(&self) -> usize {
        self.shards.iter().map(|shard| shard.out_edges_len()).sum()
    }

    fn edges_len_window(&self, t_start: i64, t_end: i64) -> usize {
        //FIXME: This nees to be optimised ideally
        self.shards
            .iter()
            .map(|shard| {
                shard
                    .vertices()
                    .flat_map(|v| shard.vertex_edges_window(v.g_id, t_start..t_end, Direction::OUT))
                    .count()
            })
            .sum()
    }

    fn has_edge_ref<V1: Into<VertexRef>, V2: Into<VertexRef>>(&self, src: V1, dst: V2) -> bool {
        let src: VertexRef = src.into();
        let dst: VertexRef = dst.into();
        self.get_shard_from_v(src).has_edge(src.g_id, dst.g_id)
    }

    fn has_edge_ref_window<V1: Into<VertexRef>, V2: Into<VertexRef>>(
        &self,
        src: V1,
        dst: V2,
        t_start: i64,
        t_end: i64,
    ) -> bool {
        let src: VertexRef = src.into();
        let dst: VertexRef = dst.into();
        self.get_shard_from_v(src)
            .has_edge_window(src.g_id, dst.g_id, t_start..t_end)
    }

    fn has_vertex_ref<V: Into<VertexRef>>(&self, v: V) -> bool {
        let v: VertexRef = v.into();
        self.get_shard_from_v(v).has_vertex(v.g_id)
    }

    fn has_vertex_ref_window<V: Into<VertexRef>>(&self, v: V, t_start: i64, t_end: i64) -> bool {
        let v: VertexRef = v.into();
        self.get_shard_from_v(v)
            .has_vertex_window(v.g_id, t_start..t_end)
    }

    fn degree(&self, v: VertexRef, d: Direction) -> usize {
        self.get_shard_from_v(v).degree(v.g_id, d)
    }

    fn degree_window(&self, v: VertexRef, t_start: i64, t_end: i64, d: Direction) -> usize {
        self.get_shard_from_v(v)
            .degree_window(v.g_id, t_start..t_end, d)
    }

    fn vertex_ref(&self, v: u64) -> Option<VertexRef> {
        self.get_shard_from_id(v).vertex(v)
    }

    fn vertex_ref_window(&self, v: u64, t_start: i64, t_end: i64) -> Option<VertexRef> {
        self.get_shard_from_id(v).vertex_window(v, t_start..t_end)
    }

    fn vertex_ids(&self) -> Box<dyn Iterator<Item = u64> + Send> {
        let shards = self.shards.clone();
        Box::new(shards.into_iter().flat_map(|s| s.vertex_ids()))
    }

    fn vertex_ids_window(&self, t_start: i64, t_end: i64) -> Box<dyn Iterator<Item = u64> + Send> {
        let shards = self.shards.clone();
        Box::new(
            shards
                .into_iter()
                .flat_map(move |s| s.vertex_ids_window(t_start..t_end)),
        )
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        let shards = self.shards.clone();
        Box::new(shards.into_iter().flat_map(|s| s.vertices()))
    }

    fn vertex_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        let shards = self.shards.clone();
        Box::new(
            shards
                .into_iter()
                .flat_map(move |s| s.vertices_window(t_start..t_end)),
        )
    }

    fn vertices_par<O, F>(&self, f: F) -> Box<dyn Iterator<Item = O>>
    where
        O: Send + 'static,
        F: Fn(VertexRef) -> O + Send + Sync + Copy,
    {
        let (tx, rx) = flume::unbounded();

        let arc_tx = Arc::new(tx);
        self.shards
            .par_iter()
            .flat_map(|shard| shard.vertices().par_bridge().map(f))
            .for_each(move |o| {
                arc_tx.send(o).unwrap();
            });

        Box::new(rx.into_iter())
    }

    fn fold_par<S, F, F2>(&self, f: F, agg: F2) -> Option<S>
    where
        S: Send + 'static,
        F: Fn(VertexRef) -> S + Send + Sync + Copy,
        F2: Fn(S, S) -> S + Sync + Send + Copy,
    {
        self.shards
            .par_iter()
            .flat_map(|shard| {
                shard.read_shard(|tg_core| tg_core.vertices().par_bridge().map(f).reduce_with(agg))
            })
            .reduce_with(agg)
    }

    fn vertices_window_par<O, F>(
        &self,
        t_start: i64,
        t_end: i64,
        f: F,
    ) -> Box<dyn Iterator<Item = O>>
    where
        O: Send + 'static,
        F: Fn(VertexRef) -> O + Send + Sync + Copy,
    {
        let (tx, rx) = flume::unbounded();

        let arc_tx = Arc::new(tx);
        self.shards
            .par_iter()
            .flat_map(|shard| shard.vertices_window(t_start..t_end).par_bridge().map(f))
            .for_each(move |o| {
                arc_tx.send(o).unwrap();
            });

        Box::new(rx.into_iter())
    }

    fn fold_window_par<S, F, F2>(&self, t_start: i64, t_end: i64, f: F, agg: F2) -> Option<S>
    where
        S: Send + 'static,
        F: Fn(VertexRef) -> S + Send + Sync + Copy,
        F2: Fn(S, S) -> S + Sync + Send + Copy,
    {
        self.shards
            .par_iter()
            .flat_map(|shard| {
                shard.read_shard(|tg_core| {
                    tg_core
                        .vertices_window(t_start..t_end)
                        .par_bridge()
                        .map(f)
                        .reduce_with(agg)
                })
            })
            .reduce_with(agg)
    }

    fn edge_ref<V1: Into<VertexRef>, V2: Into<VertexRef>>(
        &self,
        src: V1,
        dst: V2,
    ) -> Option<EdgeRef> {
        let src: VertexRef = src.into();
        let dst: VertexRef = dst.into();
        self.get_shard_from_v(src).edge(src.g_id, dst.g_id)
    }

    fn edge_ref_window<V1: Into<VertexRef>, V2: Into<VertexRef>>(
        &self,
        src: V1,
        dst: V2,
        t_start: i64,
        t_end: i64,
    ) -> Option<EdgeRef> {
        let src: VertexRef = src.into();
        let dst: VertexRef = dst.into();
        self.get_shard_from_v(src)
            .edge_window(src.g_id, dst.g_id, t_start..t_end)
    }

    fn edge_refs(&self) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        //FIXME: needs low-level primitive
        let g = self.clone();
        Box::new(
            self.vertex_refs()
                .flat_map(move |v| g.vertex_edges(v, Direction::OUT)),
        )
    }

    fn edge_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        //FIXME: needs low-level primitive
        let g = self.clone();
        Box::new(
            self.vertex_refs()
                .flat_map(move |v| g.vertex_edges_window(v, t_start, t_end, Direction::OUT)),
        )
    }

    fn vertex_edges(&self, v: VertexRef, d: Direction) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        Box::new(self.get_shard_from_v(v).vertex_edges(v.g_id, d))
    }

    fn vertex_edges_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        Box::new(
            self.get_shard_from_v(v)
                .vertex_edges_window(v.g_id, t_start..t_end, d),
        )
    }

    fn vertex_edges_window_t(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        Box::new(
            self.get_shard_from_v(v)
                .vertex_edges_window_t(v.g_id, t_start..t_end, d),
        )
    }

    fn neighbours(&self, v: VertexRef, d: Direction) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        Box::new(self.get_shard_from_v(v).neighbours(v.g_id, d))
    }

    fn neighbours_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        Box::new(
            self.get_shard_from_v(v)
                .neighbours_window(v.g_id, t_start..t_end, d),
        )
    }

    fn neighbours_ids(&self, v: VertexRef, d: Direction) -> Box<dyn Iterator<Item = u64> + Send> {
        Box::new(self.get_shard_from_v(v).neighbours_ids(v.g_id, d))
    }

    fn neighbours_ids_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send> {
        Box::new(
            self.get_shard_from_v(v)
                .neighbours_ids_window(v.g_id, t_start..t_end, d),
        )
    }

    fn static_vertex_prop(&self, v: VertexRef, name: String) -> Option<Prop> {
        self.get_shard_from_v(v).static_vertex_prop(v.g_id, name)
    }

    fn static_vertex_prop_keys(&self, v: VertexRef) -> Vec<String> {
        self.get_shard_from_v(v).static_vertex_prop_keys(v.g_id)
    }

    fn temporal_vertex_prop_vec(&self, v: VertexRef, name: String) -> Vec<(i64, Prop)> {
        self.get_shard_from_v(v).temporal_vertex_prop_vec(v.g_id, name)
    }

    fn temporal_vertex_prop_vec_window(
        &self,
        v: VertexRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.get_shard_from_v(v)
            .temporal_vertex_prop_vec_window(v.g_id, name, t_start..t_end)
    }

    fn temporal_vertex_props(&self, v: VertexRef) -> HashMap<String, Vec<(i64, Prop)>> {
        self.get_shard_from_v(v).temporal_vertex_props(v.g_id)
    }

    fn temporal_vertex_props_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        self.get_shard_from_v(v)
            .temporal_vertex_props_window(v.g_id, t_start..t_end)
    }

    fn static_edge_prop(&self, e: EdgeRef, name: String) -> Option<Prop> {
        self.get_shard_from_e(e).static_edge_prop(e.edge_id, name)
    }

    fn static_edge_prop_keys(&self, e: EdgeRef) -> Vec<String> {
        self.get_shard_from_e(e).static_edge_prop_keys(e.edge_id)
    }

    fn temporal_edge_props_vec(&self, e: EdgeRef, name: String) -> Vec<(i64, Prop)> {
        self.get_shard_from_e(e).temporal_edge_prop_vec(e.edge_id, name)
    }

    fn temporal_edge_props_vec_window(
        &self,
        e: EdgeRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.get_shard_from_e(e)
            .temporal_edge_props_vec_window(e.edge_id, name, t_start..t_end)
    }

    fn temporal_edge_props(&self, e: EdgeRef) -> HashMap<String, Vec<(i64, Prop)>> {
        //FIXME: This needs to be implemented in core if we want it
        todo!()
    }

    fn temporal_edge_props_window(
        &self,
        e: EdgeRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        //FIXME: This needs to be implemented in core if we want it
        todo!()
    }
}

impl GraphViewOps for Graph {
    type Vertex = VertexView<Self>;
    type VertexIter = Self::Vertices;
    type Vertices = Box<dyn Iterator<Item = Self::Vertex> + Send>;
    type Edge = EdgeView<Self>;
    type Edges = Box<dyn Iterator<Item = Self::Edge> + Send>;

    fn num_vertices(&self) -> usize {
        self.vertices_len()
    }

    fn earliest_time(&self) -> Option<i64> {
        let min_from_shards = self.shards.iter().map(|shard| shard.earliest_time()).min();
        min_from_shards.filter(|&min| min != i64::MAX)
    }

    fn latest_time(&self) -> Option<i64> {
        let max_from_shards = self.shards.iter().map(|shard| shard.latest_time()).max();
        max_from_shards.filter(|&max| max != i64::MIN)
    }

    fn num_edges(&self) -> usize {
        GraphViewInternalOps::edges_len(self)
    }

    fn has_vertex<T: InputVertex>(&self, v: T) -> bool {
        GraphViewInternalOps::has_vertex_ref(self, v.id())
    }

    fn has_edge<T: InputVertex>(&self, src: T, dst: T) -> bool {
        GraphViewInternalOps::has_edge_ref(self, src.id(), dst.id())
    }

    fn vertex<T: InputVertex>(&self, v: T) -> Option<Self::Vertex> {
        self.vertex_ref(v.id())
            .map(|v| Self::Vertex::new(Arc::new(self.clone()), v))
    }

    fn vertices(&self) -> Self::Vertices {
        let g = Arc::new(self.clone());
        Box::new(
            GraphViewInternalOps::vertex_refs(self).map(move |v| Self::Vertex::new(g.clone(), v)),
        )
    }

    fn edge<T: InputVertex>(&self, src: T, dst: T) -> Option<Self::Edge> {
        self.edge_ref(
            VertexRef {
                g_id: src.id(),
                pid: None,
            },
            VertexRef {
                g_id: dst.id(),
                pid: None,
            },
        )
        .map(|e| Self::Edge::new(Arc::new(self.clone()), e))
    }

    fn edges(&self) -> Self::Edges {
        Box::new(self.vertices().flat_map(|v| v.out_edges()))
    }
}

impl Graph {
    fn shard_id(&self, g_id: u64) -> usize {
        utils::get_shard_id_from_global_vid(g_id, self.nr_shards)
    }

    fn get_shard_from_id(&self, g_id: u64) -> &TGraphShard {
        &self.shards[self.shard_id(g_id)]
    }

    fn get_shard_from_v(&self, v: VertexRef) -> &TGraphShard {
        &self.shards[self.shard_id(v.g_id)]
    }

    fn get_shard_from_e(&self, e: EdgeRef) -> &TGraphShard {
        &self.shards[self.shard_id(e.src_g_id)]
    }

    pub fn new(nr_shards: usize) -> Self {
        Graph {
            nr_shards,
            shards: (0..nr_shards).map(|_| TGraphShard::default()).collect(),
        }
    }

    pub fn window(&self, t_start: i64, t_end: i64) -> WindowedGraph {
        WindowedGraph::new(self.clone(), t_start, t_end)
    }

    pub fn at(&self, end: i64) -> WindowedGraph {
        self.window(i64::MIN, end.saturating_add(1))
    }

    pub fn through_perspectives(&self, mut perspectives: PerspectiveSet) -> GraphWindowSet {
        let iter = match (self.earliest_time(), self.latest_time()) {
            (Some(start), Some(end)) => perspectives.build_iter(start..end),
            _ => PerspectiveIterator::empty(),
        };
        GraphWindowSet::new(self.clone(), Box::new(iter))
    }

    pub fn through_iter(
        &self,
        perspectives: Box<dyn Iterator<Item = Perspective> + Send>,
    ) -> GraphWindowSet {
        let iter = match (self.earliest_time(), self.latest_time()) {
            (Some(start), Some(end)) => perspectives,
            _ => Box::new(iter::empty::<Perspective>()),
        };
        GraphWindowSet::new(self.clone(), iter)
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
        Ok(Graph { nr_shards, shards }) //TODO I need to put in the actual values here
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
    pub fn add_vertex<T: InputVertex>(&self, t: i64, v: T, props: &Vec<(String, Prop)>) {
        let shard_id = utils::get_shard_id_from_global_vid(v.id(), self.nr_shards);
        self.shards[shard_id].add_vertex(t, v, &props);
    }

    pub fn add_vertex_properties<T: InputVertex>(&self, v: T, data: &Vec<(String, Prop)>) {
        let shard_id = utils::get_shard_id_from_global_vid(v.id(), self.nr_shards);
        self.shards[shard_id].add_vertex_properties(v.id(), data)
    }

    // TODO: Vertex.name which gets ._id property else numba as string

    pub fn add_edge<T: InputVertex>(&self, t: i64, src: T, dst: T, props: &Vec<(String, Prop)>) {
        // TODO: Problem: if the vertex already exists, then this
        // TODO: wont create a property name if the vertex is a string
        let src_shard_id = utils::get_shard_id_from_global_vid(src.id(), self.nr_shards);
        let dst_shard_id = utils::get_shard_id_from_global_vid(dst.id(), self.nr_shards);

        if src_shard_id == dst_shard_id {
            self.shards[src_shard_id].add_edge(t, src.id(), dst.id(), props)
        } else {
            // FIXME these are sort of connected, we need to hold both locks for
            // the src partition and dst partition to add a remote edge between both
            self.shards[src_shard_id].add_edge_remote_out(t, src.id(), dst.id(), props);
            self.shards[dst_shard_id].add_edge_remote_into(t, src.id(), dst.id(), props);
        }
    }

    pub fn add_edge_properties<T: InputVertex>(&self, src: T, dst: T, props: &Vec<(String, Prop)>) {
        let src_shard_id = utils::get_shard_id_from_global_vid(src.id(), self.nr_shards);
        let dst_shard_id = utils::get_shard_id_from_global_vid(dst.id(), self.nr_shards);

        if src_shard_id == dst_shard_id {
            self.shards[src_shard_id].add_edge_properties(src.id(), dst.id(), props)
        } else {
            // TODO: we don't add properties to dst shard, but may need to depending on the plans
            self.shards[src_shard_id].add_remote_out_properties(src.id(), dst.id(), props);
        }
    }
}

#[cfg(test)]
mod db_tests {
    use crate::graphgen::random_attachment::random_attachment;
    use csv::StringRecord;
    use docbrown_core::utils;
    use itertools::Itertools;
    use quickcheck::{quickcheck, TestResult};
    use rand::Rng;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::{env, fs};
    use uuid::Uuid;

    use crate::algorithms::local_triangle_count::local_triangle_count;
    use crate::view_api::GraphViewOps;

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
    fn add_vertex_grows_graph_len(vs: Vec<(u8, u64)>) {
        let g = Graph::new(2);

        let expected_len = vs.iter().map(|(_, v)| v).sorted().dedup().count();
        for (t, v) in vs {
            g.add_vertex(t.into(), v, &vec![]);
        }

        assert_eq!(g.num_vertices(), expected_len)
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

        assert_eq!(g.num_vertices(), unique_vertices_count);
        assert_eq!(g.num_edges(), unique_edge_count);
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
        let shards_path =
            format!("{:?}/{}", tmp_docbrown_path.path().display(), rand_dir).replace("\"", "");

        println!("shards_path: {}", shards_path);

        // Save to files
        let mut expected = vec![
            format!("{}/shard_1", shards_path),
            format!("{}/shard_0", shards_path),
            format!("{}/graphdb_nr_shards", shards_path),
        ]
        .iter()
        .map(Path::new)
        .map(PathBuf::from)
        .collect::<Vec<_>>();

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
                assert!(g.has_vertex_ref(1));
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

        assert_eq!(g.has_edge_ref(8, 7), false);
        assert_eq!(g.has_edge_ref(7, 8), true);

        g.add_edge(1, 7, 9, &vec![]);

        assert_eq!(g.has_edge_ref(9, 7), false);
        assert_eq!(g.has_edge_ref(7, 9), true);

        g.add_edge(2, "haaroon", "northLondon", &vec![]);
        assert_eq!(g.has_edge("haaroon", "northLondon"), true);
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
            g.edge_ref_window(1, 3, i64::MIN, i64::MAX)
                .unwrap()
                .src_g_id,
            1u64
        );
        assert_eq!(
            g.edge_ref_window(1, 3, i64::MIN, i64::MAX)
                .unwrap()
                .dst_g_id,
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
                let i = VertexRef::new_remote(i);
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
                let i = VertexRef::new_remote(i);
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
                let i = VertexRef { g_id: i, pid: None };
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
                let i = VertexRef { g_id: i, pid: None };
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
                g.vertex_edges_window_t(i.into(), -1, 7, Direction::IN)
                    .map(|e| e.time.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(vec![vec![-1, 0, 1], vec![1], vec![2]], in_actual);

        let out_actual = (1..=3)
            .map(|i| {
                g.vertex_edges_window_t(i.into(), 1, 7, Direction::OUT)
                    .map(|e| e.time.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(vec![vec![1, 1, 2], vec![], vec![]], out_actual);

        let both_actual = (1..=3)
            .map(|i| {
                g.vertex_edges_window_t(i.into(), 0, 1, Direction::BOTH)
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
                    .vertex_edges_window_t(i.into(), -1, 7, Direction::IN)
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
                    .vertex_edges_window_t(i.into(), 1, 7, Direction::OUT)
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
                    .vertex_edges_window_t(i.into(), 0, 1, Direction::BOTH)
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

        assert_eq!(g.latest_time(), None);
        assert_eq!(g.earliest_time(), None);

        g.add_vertex(5, 1, &vec![]);

        assert_eq!(g.latest_time(), Some(5));
        assert_eq!(g.earliest_time(), Some(5));

        let g = Graph::new(4);

        g.add_edge(10, 1, 2, &vec![]);
        assert_eq!(g.latest_time(), Some(10));
        assert_eq!(g.earliest_time(), Some(10));

        g.add_vertex(5, 1, &vec![]);
        assert_eq!(g.latest_time(), Some(10));
        assert_eq!(g.earliest_time(), Some(5));

        g.add_edge(20, 3, 4, &vec![]);
        assert_eq!(g.latest_time(), Some(20));
        assert_eq!(g.earliest_time(), Some(5));

        random_attachment(&g,100,10);
        assert_eq!(g.latest_time(),Some(126));
        assert_eq!(g.earliest_time(),Some(5));
    }

    #[test]
    fn static_properties() {
        let g = Graph::new(100); // big enough so all edges are very likely remote
        g.add_edge(0, 11, 22, &vec![]);
        g.add_edge(0, 11, 11, &vec![("temp".to_string(), Prop::Bool(true))]);
        g.add_edge(0, 22, 33, &vec![]);
        g.add_edge(0, 33, 11, &vec![]);
        g.add_vertex(0, 11, &vec![("temp".to_string(), Prop::Bool(true))]);

        let edges11 = g.vertex_edges_window(11.into(), 0, 1, Direction::OUT).collect_vec();
        let edge1122 = *edges11.iter().find(|e| e.dst_g_id == 22).unwrap();
        let edge1111 = *edges11.iter().find(|e| e.dst_g_id == 11).unwrap();
        let edge2233 = g.vertex_edges_window(22.into(), 0, 1, Direction::OUT).next().unwrap();
        let edge3311 = g.vertex_edges_window(33.into(), 0, 1, Direction::OUT).next().unwrap();

        g.add_vertex_properties(11, &vec![("a".to_string(), Prop::U64(11)), ("b".to_string(), Prop::I64(11))]);
        g.add_vertex_properties(11, &vec![("c".to_string(), Prop::U32(11))]);
        g.add_vertex_properties(22, &vec![("b".to_string(), Prop::U64(22))]);
        g.add_edge_properties(11, 11, &vec![("d".to_string(), Prop::U64(1111))]);
        g.add_edge_properties(33, 11, &vec![("a".to_string(), Prop::U64(3311))]);

        assert_eq!(g.static_vertex_prop_keys(11.into()), vec!["a", "b", "c"]);
        assert_eq!(g.static_vertex_prop_keys(22.into()), vec!["b"]);
        assert!(g.static_vertex_prop_keys(33.into()).is_empty());
        assert_eq!(g.static_edge_prop_keys(edge1111), vec!["d"]);
        assert_eq!(g.static_edge_prop_keys(edge3311), vec!["a"]);
        assert!(g.static_edge_prop_keys(edge2233).is_empty());

        assert_eq!(g.static_vertex_prop(11.into(), "a".to_string()), Some(Prop::U64(11)));
        assert_eq!(g.static_vertex_prop(11.into(), "b".to_string()), Some(Prop::I64(11)));
        assert_eq!(g.static_vertex_prop(11.into(), "c".to_string()), Some(Prop::U32(11)));
        assert_eq!(g.static_vertex_prop(22.into(), "b".to_string()), Some(Prop::U64(22)));
        assert_eq!(g.static_vertex_prop(22.into(), "a".to_string()), None);
        assert_eq!(g.static_edge_prop(edge1111, "d".to_string()), Some(Prop::U64(1111)));
        assert_eq!(g.static_edge_prop(edge3311, "a".to_string()), Some(Prop::U64(3311)));
        assert_eq!(g.static_edge_prop(edge2233, "a".to_string()), None);
    }

    #[test]
    #[should_panic]
    fn changing_property_type_for_vertex_panics() {
        let g = Graph::new(4);
        g.add_vertex(0, 11, &vec![("test".to_string(), Prop::Bool(true))]);
        g.add_vertex_properties(11, &vec![("test".to_string(), Prop::Bool(true))]);
    }

    #[test]
    #[should_panic]
    fn changing_property_type_for_edge_panics() {
        let g = Graph::new(4);
        g.add_edge(0, 11, 22, &vec![("test".to_string(), Prop::Bool(true))]);
        g.add_edge_properties(11, 22, &vec![("test".to_string(), Prop::Bool(true))]);
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
                    VertexRef {
                        g_id: 1,
                        pid: Some(0),
                    },
                    VertexRef { g_id: 2, pid: None },
                ],
                vec![
                    VertexRef {
                        g_id: 1,
                        pid: Some(0),
                    },
                    VertexRef {
                        g_id: 3,
                        pid: Some(1),
                    },
                    VertexRef { g_id: 2, pid: None },
                ],
                vec![VertexRef {
                    g_id: 1,
                    pid: Some(0),
                }],
            ),
            (vec![VertexRef { g_id: 1, pid: None }], vec![], vec![]),
            (
                vec![VertexRef {
                    g_id: 1,
                    pid: Some(0),
                }],
                vec![],
                vec![],
            ),
        ];
        let actual = (1..=3)
            .map(|i| {
                let i = i.into();
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
        assert!(g.has_vertex_ref(gandalf));
    }

    #[test]
    fn test_through_on_empty_graph() {
        let g = Graph::new(1);

        let perspectives = Perspective::rolling(1, Some(1), Some(-100), Some(100));
        let first_view = g.through_perspectives(perspectives).next();
        assert!(first_view.is_none());

        let perspectives = vec![Perspective::new(Some(-10), Some(10))].into_iter();
        let first_view = g.through_iter(Box::new(perspectives)).next();
        assert!(first_view.is_none());
    }

    #[test]
    fn test_lotr_load_graph() {
        let g = crate::graph_loader::lotr_graph::lotr_graph(4);
        assert_eq!(g.num_edges(), 701);
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
        assert_eq!(g.num_edges(), 1089147);
        assert_eq!(g.num_vertices(), 49467);
    }

    #[test]
    fn test_graph_at() {
        let g = crate::graph_loader::lotr_graph::lotr_graph(1);

        let g_at_empty = g.at(1);
        let g_at_start = g.at(7059);
        let g_at_another = g.at(28373);
        let g_at_max = g.at(i64::MAX);
        let g_at_min = g.at(i64::MIN);

        assert_eq!(g_at_empty.num_vertices(), 0);
        assert_eq!(g_at_start.num_vertices(), 70);
        assert_eq!(g_at_another.num_vertices(), 123);
        assert_eq!(g_at_max.num_vertices(), 139);
        assert_eq!(g_at_min.num_vertices(), 0);
    }

    #[test]
    fn test_add_vertex_with_strings() {
        let g = Graph::new(1);

        g.add_vertex(0, "haaroon", &vec![]);
        g.add_vertex(1, "hamza", &vec![]);
        g.add_vertex(1, 831, &vec![]);

        assert!(g.has_vertex(831));
        assert!(g.has_vertex("haaroon"));
        assert!(g.has_vertex("hamza"));

        assert_eq!(g.num_vertices(), 3);
    }
}
