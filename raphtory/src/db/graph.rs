//! Defines the `Graph` struct, which represents a raphtory graph in memory.
//!
//! This is the base class used to create a temporal graph, add vertices and edges,
//! create windows, and query the graph with a variety of algorithms.
//! It is a wrapper around a set of shards, which are the actual graph data structures.
//!
//! # Examples
//!
//! ```rust
//! use raphtory::db::graph::Graph;
//! use raphtory::db::mutation_api::AdditionOps;
//! use raphtory::db::view_api::*;
//! let graph = Graph::new(2);
//! graph.add_vertex(0, "Alice", []).unwrap();
//! graph.add_vertex(1, "Bob", []).unwrap();
//! graph.add_edge(2, "Alice", "Bob", [], None).unwrap();
//! graph.num_edges();
//! ```
//!

use crate::core::tgraph::TemporalGraph;
use crate::core::tgraph_shard::{LockedView, TGraphShard};
use crate::core::{
    edge_ref::EdgeRef, tgraph_shard::errors::GraphError, utils, vertex::InputVertex,
    vertex_ref::VertexRef, Direction, Prop, PropUnwrap,
};

use crate::core::timeindex::{TimeIndex, TimeIndexOps};
use crate::core::tprop::TProp;
use crate::core::vertex_ref::LocalVertexRef;
use crate::db::graph_immutable::ImmutableGraph;
use crate::db::mutation_api::internal::{
    InheritAdditionOps, InheritPropertyAdditionOps, InternalAdditionOps, InternalDeletionOps,
    InternalPropertyAdditionOps,
};
use crate::db::view_api::internal::time_semantics::TimeSemantics;
use crate::db::view_api::internal::{
    Base, CoreDeletionOps, CoreGraphOps, DynamicGraph, GraphOps, InheritViewOps,
    InternalMaterialize, IntoDynamic, MaterializedGraph,
};
use crate::db::view_api::*;
use itertools::Itertools;
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::{
    iter,
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalGraph {
    /// The number of shards in the graph.
    pub(crate) nr_shards: usize,
    /// A vector of `TGraphShard<TemporalGraph>` representing the shards in the graph.
    pub(crate) shards: Vec<TGraphShard<TemporalGraph>>,
    /// Translates layer names to layer ids
    pub(crate) layer_ids: Arc<parking_lot::RwLock<FxHashMap<String, usize>>>,
}

pub fn graph_equal<G1: GraphViewOps, G2: GraphViewOps>(g1: &G1, g2: &G2) -> bool {
    if g1.num_vertices() == g2.num_vertices() && g1.num_edges() == g2.num_edges() {
        g1.vertices().id().all(|v| g2.has_vertex(v)) && // all vertices exist in other 
            g1.edges().explode().count() == g2.edges().explode().count() && // same number of exploded edges
            g1.edges().explode().all(|e| { // all exploded edges exist in other
                g2
                    .edge(e.src().id(), e.dst().id(), None)
                    .filter(|ee| ee.active(e.time().expect("exploded")))
                    .is_some()
            })
    } else {
        false
    }
}

/// A temporal graph composed of multiple shards.
///
/// This is the public facing struct used to create a temporal graph, add vertices and edges,
/// create windows, and query the graph with a variety of algorithms.
/// It is a wrapper around a set of shards, which are the actual graph data structures.
#[repr(transparent)]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Graph(Arc<InternalGraph>);

impl<G: GraphViewOps> PartialEq<G> for Graph {
    fn eq(&self, other: &G) -> bool {
        graph_equal(self, other)
    }
}

impl Display for Graph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<InternalGraph> for Graph {
    fn from(value: InternalGraph) -> Self {
        Self(Arc::new(value))
    }
}

impl Base for Graph {
    type Base = InternalGraph;

    fn base(&self) -> &InternalGraph {
        &self.0
    }
}

impl InheritAdditionOps for Graph {}
impl InheritPropertyAdditionOps for Graph {}
impl InheritViewOps for Graph {}

impl Graph {
    /// Create a new graph with the specified number of shards
    ///
    /// # Arguments
    ///
    /// * `nr_shards` - The number of shards
    ///
    /// # Returns
    ///
    /// A raphtory graph
    ///
    /// # Example
    ///
    /// ```
    /// use raphtory::db::graph::Graph;
    /// let g = Graph::new(4);
    /// ```
    pub fn new(nr_shards: usize) -> Self {
        Self(Arc::new(InternalGraph::new(nr_shards)))
    }

    pub(crate) fn new_from_frozen(
        nr_shards: usize,
        shards: Vec<TGraphShard<TemporalGraph>>,
        layer_ids: Arc<parking_lot::RwLock<FxHashMap<String, usize>>>,
    ) -> Self {
        Self(Arc::new(InternalGraph {
            nr_shards,
            shards,
            layer_ids,
        }))
    }

    /// Save a graph to a directory
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the directory
    ///
    /// # Returns
    ///
    /// A raphtory graph
    ///
    /// # Example
    ///
    /// ```
    /// use raphtory::db::graph::InternalGraph;
    /// use std::fs::File;
    /// use raphtory::db::mutation_api::AdditionOps;
    /// let g = InternalGraph::new(4);
    /// g.add_vertex(1, 1, []).unwrap();
    /// // g.save_to_file("path_str");
    /// ```
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), GraphError> {
        self.0.save_to_file(path)
    }

    /// Load a graph from a directory
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the directory
    ///
    /// # Returns
    ///
    /// A raphtory graph
    ///
    /// # Example
    ///
    /// ```
    /// use raphtory::db::graph::InternalGraph;
    /// // let g = Graph::load_from_file("path/to/graph");
    /// ```
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, GraphError> {
        Ok(Self(Arc::new(InternalGraph::load_from_file(path)?)))
    }

    /// Freezes the current mutable graph into an immutable graph.
    ///
    /// This removes the internal locks, allowing the graph to be queried in
    /// a read-only fashion.
    ///
    /// # Returns
    ///
    /// An `ImmutableGraph` which is an immutable copy of the current graph.
    ///
    /// # Example
    /// ```
    /// use raphtory::db::view_api::*;
    /// use raphtory::db::graph::Graph;
    ///
    /// let mut mutable_graph = Graph::new(1);
    /// // ... add vertices and edges to the graph
    ///
    /// // Freeze the mutable graph into an immutable graph
    /// let immutable_graph = mutable_graph.freeze();
    /// ```
    pub fn freeze(self) -> ImmutableGraph {
        ImmutableGraph {
            nr_shards: self.0.nr_shards,
            shards: self.0.shards.iter().map(|s| s.freeze()).collect_vec(),
            layer_ids: Arc::new(self.0.layer_ids.read().clone()),
        }
    }
}

impl IntoDynamic for Graph {
    fn into_dynamic(self) -> DynamicGraph {
        self.0
    }
}

impl Default for InternalGraph {
    fn default() -> Self {
        InternalGraph::new(1)
    }
}

impl Display for InternalGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Graph(num_vertices={}, num_edges={})",
            self.num_vertices(),
            self.num_edges()
        )
    }
}

impl<G: GraphViewOps> PartialEq<G> for InternalGraph {
    fn eq(&self, other: &G) -> bool {
        if self.num_vertices() == other.num_vertices() && self.num_edges() == other.num_edges() {
            self.vertices().id().all(|v| other.has_vertex(v)) && // all vertices exist in other 
            self.edges().explode().count() == other.edges().explode().count() && // same number of exploded edges
            self.edges().explode().all(|e| { // all exploded edges exist in other
                other
                    .edge(e.src().id(), e.dst().id(), None)
                    .filter(|ee| ee.active(e.time().expect("exploded")))
                    .is_some()
            })
        } else {
            false
        }
    }
}

impl InternalMaterialize for InternalGraph {
    fn new_base_graph(&self, graph: InternalGraph) -> MaterializedGraph {
        MaterializedGraph::EventGraph(Graph(Arc::new(graph)))
    }

    fn include_deletions(&self) -> bool {
        false
    }
}

impl TimeSemantics for InternalGraph {
    fn vertex_earliest_time(&self, v: LocalVertexRef) -> Option<i64> {
        self.vertex_additions(v).first()
    }

    fn vertex_latest_time(&self, v: LocalVertexRef) -> Option<i64> {
        self.vertex_additions(v).last()
    }

    fn view_start(&self) -> Option<i64> {
        self.earliest_time_global()
    }

    fn view_end(&self) -> Option<i64> {
        self.latest_time_global().map(|t| t + 1) // so it is exclusive
    }

    fn earliest_time_global(&self) -> Option<i64> {
        let min_from_shards = self.shards.iter().map(|shard| shard.earliest_time()).min();
        min_from_shards.filter(|&min| min != i64::MAX)
    }

    fn latest_time_global(&self) -> Option<i64> {
        let max_from_shards = self.shards.iter().map(|shard| shard.latest_time()).max();
        max_from_shards.filter(|&max| max != i64::MIN)
    }

    fn earliest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.vertex_refs()
            .flat_map(|v| self.vertex_earliest_time_window(v, t_start, t_end))
            .min()
    }

    fn latest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.vertex_refs()
            .flat_map(|v| self.vertex_latest_time_window(v, t_start, t_end))
            .max()
    }

    fn vertex_earliest_time_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<i64> {
        self.vertex_additions(v).range(t_start..t_end).first()
    }

    fn vertex_latest_time_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<i64> {
        self.vertex_additions(v).range(t_start..t_end).last()
    }

    fn include_vertex_window(&self, v: LocalVertexRef, w: Range<i64>) -> bool {
        self.vertex_additions(v).active(w)
    }

    fn include_edge_window(&self, e: EdgeRef, w: Range<i64>) -> bool {
        self.edge_additions(e).active(w)
    }

    fn vertex_history(&self, v: LocalVertexRef) -> Vec<i64> {
        self.vertex_additions(v).iter().copied().collect()
    }

    fn vertex_history_window(&self, v: LocalVertexRef, w: Range<i64>) -> Vec<i64> {
        self.vertex_additions(v).range(w).iter().copied().collect()
    }

    fn edge_t(&self, e: EdgeRef) -> BoxedIter<EdgeRef> {
        Box::new(
            self.edge_additions(e)
                .iter()
                .map(|t| e.at(*t))
                .collect::<Vec<_>>()
                .into_iter(),
        )
    }

    fn edge_window_t(&self, e: EdgeRef, w: Range<i64>) -> BoxedIter<EdgeRef> {
        Box::new(
            self.edge_additions(e)
                .range(w)
                .iter()
                .map(|t| e.at(*t))
                .collect::<Vec<_>>()
                .into_iter(),
        )
    }

    fn edge_earliest_time(&self, e: EdgeRef) -> Option<i64> {
        e.time().or_else(|| self.edge_additions(e).first())
    }

    fn edge_earliest_time_window(&self, e: EdgeRef, w: Range<i64>) -> Option<i64> {
        e.time().or_else(|| self.edge_additions(e).range(w).first())
    }

    fn edge_latest_time(&self, e: EdgeRef) -> Option<i64> {
        e.time().or_else(|| self.edge_additions(e).last())
    }

    fn edge_latest_time_window(&self, e: EdgeRef, w: Range<i64>) -> Option<i64> {
        e.time().or_else(|| self.edge_additions(e).range(w).last())
    }

    fn edge_deletion_history(&self, e: EdgeRef) -> Vec<i64> {
        self.edge_deletions(e).iter().copied().collect()
    }

    fn edge_deletion_history_window(&self, e: EdgeRef, w: Range<i64>) -> Vec<i64> {
        self.edge_deletions(e).range(w).iter().copied().collect()
    }

    fn temporal_prop_vec(&self, name: &str) -> Vec<(i64, Prop)> {
        match self.temporal_prop(name) {
            Some(props) => props.iter().collect(),
            None => Default::default(),
        }
    }

    fn temporal_prop_vec_window(&self, name: &str, t_start: i64, t_end: i64) -> Vec<(i64, Prop)> {
        match self.temporal_prop(name) {
            Some(props) => props.iter_window(t_start..t_end).collect(),
            None => Default::default(),
        }
    }

    fn temporal_vertex_prop_vec(&self, v: LocalVertexRef, name: &str) -> Vec<(i64, Prop)> {
        match self.temporal_vertex_prop(v, name) {
            Some(tprop) => tprop.iter().collect(),
            None => Default::default(),
        }
    }

    fn temporal_vertex_prop_vec_window(
        &self,
        v: LocalVertexRef,
        name: &str,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        match self.temporal_vertex_prop(v, name) {
            Some(tprop) => tprop.iter_window(t_start..t_end).collect(),
            None => Default::default(),
        }
    }

    fn temporal_edge_prop_vec_window(
        &self,
        e: EdgeRef,
        name: &str,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        match self.temporal_edge_prop(e, name) {
            Some(tprop) => tprop.iter_window(t_start..t_end).collect(),
            None => Default::default(),
        }
    }

    fn temporal_edge_prop_vec(&self, e: EdgeRef, name: &str) -> Vec<(i64, Prop)> {
        match self.temporal_edge_prop(e, name) {
            Some(tprop) => tprop.iter().collect(),
            None => Default::default(),
        }
    }
}

impl CoreDeletionOps for InternalGraph {
    fn edge_deletions(&self, eref: EdgeRef) -> LockedView<TimeIndex> {
        self.get_shard_from_e(eref).edge_deletions(eref)
    }
}

impl CoreGraphOps for InternalGraph {
    fn get_layer_name_by_id(&self, layer_id: usize) -> String {
        let layer_ids = self.layer_ids.read();
        layer_ids
            .iter()
            .find_map(|(name, &id)| (layer_id == id).then_some(name))
            .unwrap_or_else(|| panic!("layer id '{layer_id}' doesn't exist"))
            .to_string()
    }
    fn vertex_id(&self, v: LocalVertexRef) -> u64 {
        self.shards[v.shard_id].vertex_id(v)
    }

    fn vertex_name(&self, v: LocalVertexRef) -> String {
        self.static_vertex_prop(v, "_id")
            .into_str()
            .unwrap_or(self.vertex_id(v).to_string())
    }

    fn edge_additions(&self, eref: EdgeRef) -> LockedView<TimeIndex> {
        self.get_shard_from_e(eref).edge_additions(eref)
    }

    fn vertex_additions(&self, v: LocalVertexRef) -> LockedView<TimeIndex> {
        self.get_shard_from_local_v(v).vertex_additions(v)
    }

    fn localise_vertex_unchecked(&self, v: VertexRef) -> LocalVertexRef {
        match v {
            VertexRef::Local(v) => v,
            VertexRef::Remote(id) => self
                .get_shard_from_id(id)
                .vertex(id)
                .expect("vertex should exist"),
        }
    }

    fn static_prop_names(&self) -> Vec<String> {
        self.shards[0].static_prop_names()
    }

    fn static_prop(&self, name: &str) -> Option<Prop> {
        self.shards[0].static_prop(name)
    }

    fn temporal_prop_names(&self) -> Vec<String> {
        self.shards[0].temporal_prop_names()
    }

    fn temporal_prop(&self, name: &str) -> Option<LockedView<TProp>> {
        self.shards[0].temporal_prop(name)
    }

    fn static_vertex_prop(&self, v: LocalVertexRef, name: &str) -> Option<Prop> {
        self.get_shard_from_local_v(v).static_vertex_prop(v, name)
    }

    fn static_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        self.get_shard_from_local_v(v).static_vertex_prop_names(v)
    }

    fn temporal_vertex_prop(&self, v: LocalVertexRef, name: &str) -> Option<LockedView<TProp>> {
        self.get_shard_from_local_v(v).temporal_vertex_prop(v, name)
    }

    fn temporal_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        self.get_shard_from_local_v(v).temporal_vertex_prop_names(v)
    }

    fn static_edge_prop(&self, e: EdgeRef, name: &str) -> Option<Prop> {
        self.get_shard_from_e(e).static_edge_prop(e, name)
    }

    fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.get_shard_from_e(e).static_edge_prop_names(e)
    }

    fn temporal_edge_prop(&self, e: EdgeRef, name: &str) -> Option<LockedView<TProp>> {
        self.get_shard_from_e(e).temporal_edge_prop(e, name)
    }

    fn temporal_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.get_shard_from_e(e).temporal_edge_prop_names(e)
    }

    fn num_shards_internal(&self) -> usize {
        self.nr_shards
    }
}

impl GraphOps for InternalGraph {
    fn local_vertex_ref(&self, v: VertexRef) -> Option<LocalVertexRef> {
        self.get_shard_from_v(v).local_vertex(v)
    }

    /// Return all the layer ids, included the id of the default layer, 0
    fn get_unique_layers_internal(&self) -> Vec<usize> {
        Box::new(iter::once(0).chain(self.layer_ids.read().values().copied())).collect_vec()
    }

    fn get_layer_id(&self, key: Option<&str>) -> Option<usize> {
        match key {
            None => Some(0),
            Some(key) => self.layer_ids.read().get(key).copied(),
        }
    }

    fn vertices_len(&self) -> usize {
        self.shards.iter().map(|shard| shard.len()).sum()
    }

    fn edges_len(&self, layer: Option<usize>) -> usize {
        let vs: Vec<usize> = self
            .shards
            .iter()
            .map(|shard| shard.out_edges_len(layer))
            .collect();
        vs.iter().sum()
    }

    fn has_edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> bool {
        let (shard, src, dst) = self.localise_edge(src, dst);
        self.shards[shard].has_edge(src, dst, layer)
    }

    fn has_vertex_ref(&self, v: VertexRef) -> bool {
        self.get_shard_from_v(v).has_vertex(v)
    }

    fn degree(&self, v: LocalVertexRef, d: Direction, layer: Option<usize>) -> usize {
        self.get_shard_from_local_v(v).degree(v, d, layer)
    }

    fn vertex_ref(&self, v: u64) -> Option<LocalVertexRef> {
        self.get_shard_from_id(v).vertex(v)
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        let shards = self.shards.clone();
        Box::new(shards.into_iter().flat_map(|s| s.vertices()))
    }

    fn vertex_refs_shard(&self, shard: usize) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        let shard = self.shards[shard].clone();
        Box::new(shard.vertices())
    }

    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> Option<EdgeRef> {
        let (shard_id, src, dst) = self.localise_edge(src, dst);
        self.shards[shard_id].edge(src, dst, layer)
    }

    fn edge_refs(&self, layer: Option<usize>) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        //FIXME: needs low-level primitive
        let g = self.clone();
        match layer {
            Some(layer) => Box::new(
                self.vertex_refs()
                    .flat_map(move |v| g.vertex_edges(v, Direction::OUT, Some(layer))),
            ),
            None => Box::new(
                self.vertex_refs()
                    .flat_map(move |v| g.vertex_edges(v, Direction::OUT, None)),
            ),
        }
    }

    fn vertex_edges(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        Box::new(self.get_shard_from_local_v(v).vertex_edges(v, d, layer))
    }

    fn neighbours(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        Box::new(self.get_shard_from_local_v(v).neighbours(v, d, layer))
    }
}

/// The implementation of a temporal graph composed of multiple shards.
impl InternalGraph {
    /// Freezes the current mutable graph into an immutable graph.
    ///
    /// This removes the internal locks, allowing the graph to be queried in
    /// a read-only fashion.
    ///
    /// # Returns
    ///
    /// An `ImmutableGraph` which is an immutable copy of the current graph.
    ///
    /// # Example
    /// ```
    /// use raphtory::db::view_api::*;
    /// use raphtory::db::graph::Graph;
    ///
    /// let mut mutable_graph = Graph::new(1);
    /// // ... add vertices and edges to the graph
    ///
    /// // Freeze the mutable graph into an immutable graph
    /// let immutable_graph = mutable_graph.freeze();
    /// ```
    pub fn freeze(self) -> ImmutableGraph {
        ImmutableGraph {
            nr_shards: self.nr_shards,
            shards: self.shards.iter().map(|s| s.freeze()).collect_vec(),
            layer_ids: Arc::new(self.layer_ids.read().clone()),
        }
    }

    fn localise_edge(&self, src: VertexRef, dst: VertexRef) -> (usize, VertexRef, VertexRef) {
        match src {
            VertexRef::Local(local_src) => match dst {
                VertexRef::Local(local_dst) => {
                    if local_src.shard_id == local_dst.shard_id {
                        (local_src.shard_id, src, dst)
                    } else {
                        (
                            local_src.shard_id,
                            src,
                            VertexRef::Remote(self.vertex_id(local_dst)),
                        )
                    }
                }
                VertexRef::Remote(_) => (local_src.shard_id, src, dst),
            },
            VertexRef::Remote(gid) => match dst {
                VertexRef::Local(local_dst) => (local_dst.shard_id, src, dst),
                VertexRef::Remote(_) => (self.shard_id(gid), src, dst),
            },
        }
    }

    /// Get the shard id from a global vertex id
    ///
    /// # Arguments
    ///
    /// * `g_id` - The global vertex id
    ///
    /// # Returns
    ///
    /// The shard id
    fn shard_id(&self, g_id: u64) -> usize {
        utils::get_shard_id_from_global_vid(g_id, self.nr_shards)
    }

    /// Get the shard from a global vertex id
    ///
    /// # Arguments
    ///
    /// * `g_id` - The global vertex id
    ///
    /// # Returns
    ///
    /// The shard reference
    fn get_shard_from_id(&self, g_id: u64) -> &TGraphShard<TemporalGraph> {
        &self.shards[self.shard_id(g_id)]
    }

    /// Get the shard from a vertex reference
    ///
    /// # Arguments
    ///
    /// * `g_id` - The global vertex id
    ///
    /// # Returns
    ///
    /// The shard reference
    fn get_shard_from_v(&self, v: VertexRef) -> &TGraphShard<TemporalGraph> {
        match v {
            VertexRef::Local(v) => self.get_shard_from_local_v(v),
            VertexRef::Remote(g_id) => self.get_shard_from_id(g_id),
        }
    }

    #[inline(always)]
    fn get_shard_from_local_v(&self, v: LocalVertexRef) -> &TGraphShard<TemporalGraph> {
        &self.shards[v.shard_id]
    }

    /// Get the shard from an edge reference
    ///
    /// # Arguments
    ///
    /// * `e` - The edge reference
    ///
    /// # Returns
    ///
    /// The shard reference
    fn get_shard_from_e(&self, e: EdgeRef) -> &TGraphShard<TemporalGraph> {
        &self.shards[e.shard()]
    }

    /// Create a new graph with the specified number of shards
    ///
    /// # Arguments
    ///
    /// * `nr_shards` - The number of shards
    ///
    /// # Returns
    ///
    /// A raphtory graph
    ///
    /// # Example
    ///
    /// ```
    /// use raphtory::db::graph::Graph;
    /// let g = Graph::new(4);
    /// ```
    pub fn new(nr_shards: usize) -> Self {
        InternalGraph {
            nr_shards,
            shards: (0..nr_shards).map(TGraphShard::new).collect(),
            layer_ids: Default::default(),
        }
    }

    /// Load a graph from a directory
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the directory
    ///
    /// # Returns
    ///
    /// A raphtory graph
    ///
    /// # Example
    ///
    /// ```
    /// use raphtory::db::graph::InternalGraph;
    /// // let g = Graph::load_from_file("path/to/graph");
    /// ```
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, GraphError> {
        // use BufReader for better performance

        //TODO turn to logging?
        println!("loading from {:?}", path.as_ref());
        let mut p = PathBuf::from(path.as_ref());
        p.push("graphdb_nr_shards");

        let f = std::fs::File::open(p).unwrap();
        let mut reader = std::io::BufReader::new(f);
        let (nr_shards, layer_ids) = bincode::deserialize_from(&mut reader)?;

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
        Ok(InternalGraph {
            nr_shards,
            shards,
            layer_ids,
        }) //TODO I need to put in the actual values here
    }

    /// Save a graph to a directory
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the directory
    ///
    /// # Returns
    ///
    /// A raphtory graph
    ///
    /// # Example
    ///
    /// ```
    /// use raphtory::db::graph::InternalGraph;
    /// use std::fs::File;
    /// use raphtory::db::mutation_api::AdditionOps;
    /// let g = InternalGraph::new(4);
    /// g.add_vertex(1, 1, []).unwrap();
    /// // g.save_to_file("path_str");
    /// ```
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), GraphError> {
        // write each shard to a different file

        // crate directory path if it doesn't exist
        std::fs::create_dir_all(path.as_ref())?;

        let mut shard_paths = vec![];
        for i in 0..self.nr_shards {
            let mut p = PathBuf::from(path.as_ref());
            p.push(format!("shard_{}", i));
            //TODO turn to logging?
            //println!("saving shard {} to {:?}", i, p);
            shard_paths.push((i, p));
        }
        shard_paths
            .par_iter()
            .try_for_each(|(i, path)| self.shards[*i].save_to_file(path))?;

        let mut p = PathBuf::from(path.as_ref());
        p.push("graphdb_nr_shards");

        let f = std::fs::File::create(p)?;
        let writer = std::io::BufWriter::new(f);
        bincode::serialize_into(writer, &(self.nr_shards, self.layer_ids.clone()))?;
        Ok(())
    }
}

impl InternalAdditionOps for InternalGraph {
    fn internal_add_vertex(
        &self,
        t: i64,
        v: u64,
        name: Option<&str>,
        props: Vec<(String, Prop)>,
    ) -> Result<(), GraphError> {
        self.get_shard_from_id(v).add_vertex(t, v, name, props)
    }

    fn internal_add_edge(
        &self,
        t: i64,
        src: u64,
        dst: u64,
        props: Vec<(String, Prop)>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let src_shard_id = utils::get_shard_id_from_global_vid(src.id(), self.nr_shards);
        let dst_shard_id = utils::get_shard_id_from_global_vid(dst.id(), self.nr_shards);

        let layer_id = self.get_or_allocate_layer(layer)?;

        if src_shard_id == dst_shard_id {
            self.shards[src_shard_id].add_edge(t, src, dst, props, layer_id)
        } else {
            // FIXME these are sort of connected, we need to hold both locks for
            // the src partition and dst partition to add a remote edge between both
            self.shards[src_shard_id].add_edge_remote_out(t, src, dst, props.clone(), layer_id)?;
            self.shards[dst_shard_id].add_edge_remote_into(t, src, dst, props, layer_id)?;
            Ok(())
        }
    }
}

impl InternalDeletionOps for InternalGraph {
    fn internal_delete_edge(
        &self,
        t: i64,
        src: u64,
        dst: u64,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let src_shard_id = self.shard_id(src);
        let dst_shard_id = self.shard_id(dst);
        let layer = self.get_or_allocate_layer(layer)?;
        if src_shard_id == dst_shard_id {
            self.shards[src_shard_id].delete_edge(t, src, dst, layer)?;
        } else {
            self.shards[src_shard_id].delete_edge_remote_out(t, src, dst, layer)?;
            self.shards[dst_shard_id].delete_edge_remote_into(t, src, dst, layer)?;
        }
        Ok(())
    }
}

impl InternalPropertyAdditionOps for InternalGraph {
    fn internal_add_vertex_properties(
        &self,
        v: u64,
        data: Vec<(String, Prop)>,
    ) -> Result<(), GraphError> {
        self.get_shard_from_id(v).add_vertex_properties(v, data)
    }

    fn internal_add_properties(
        &self,
        t: i64,
        props: Vec<(String, Prop)>,
    ) -> Result<(), GraphError> {
        self.shards[0].add_properties(t, props)
    }

    fn internal_add_static_properties(&self, props: Vec<(String, Prop)>) -> Result<(), GraphError> {
        self.shards[0].add_static_properties(props)
    }

    fn internal_add_edge_properties(
        &self,
        src: u64,
        dst: u64,
        props: Vec<(String, Prop)>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let layer = self.get_layer_id(layer).ok_or(GraphError::InvalidLayer)?;
        self.get_shard_from_id(src)
            .add_edge_properties(src, dst, props, layer)
    }
}

impl InternalGraph {
    fn get_or_allocate_layer(&self, key: Option<&str>) -> Result<usize, GraphError> {
        match self.get_layer_id(key) {
            Some(id) => Ok(id),
            None => {
                let mut layer_ids = self.layer_ids.write();
                let layer_id = layer_ids.len() + 1; // default layer not included in the hashmap
                layer_ids.insert(key.unwrap().to_string(), layer_id);
                for shard in &self.shards {
                    shard.allocate_layer(layer_id)?;
                }
                Ok(layer_id)
            }
        }
    }
}

#[cfg(test)]
mod db_tests {
    use super::*;
    use crate::core::time::TryIntoTime;
    use crate::db::edge::EdgeView;
    use crate::db::mutation_api::{AdditionOps, PropertyAdditionOps};
    use crate::db::path::PathFromVertex;
    use crate::db::view_api::internal::*;
    use crate::db::view_api::LayerOps;
    use crate::graphgen::random_attachment::random_attachment;
    use itertools::Itertools;
    use quickcheck::Arbitrary;
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Arc;
    use tempdir::TempDir;
    use uuid::Uuid;

    #[test]
    fn cloning_vec() {
        let mut vs = vec![];
        for i in 0..10 {
            vs.push(Arc::new(i))
        }
        let should_be_10: usize = vs.iter().map(Arc::strong_count).sum();
        assert_eq!(should_be_10, 10);

        let vs2 = vs.clone();

        let should_be_10: usize = vs2.iter().map(Arc::strong_count).sum();
        assert_eq!(should_be_10, 20)
    }

    #[quickcheck]
    fn add_vertex_grows_graph_len(vs: Vec<(i64, u64)>) {
        let g = Graph::new(2);

        let expected_len = vs.iter().map(|(_, v)| v).sorted().dedup().count();
        for (t, v) in vs {
            g.add_vertex(t, v, [])
                .map_err(|err| println!("{:?}", err))
                .ok();
        }

        assert_eq!(g.num_vertices(), expected_len)
    }

    #[quickcheck]
    fn add_edge_grows_graph_edge_len(edges: Vec<(i64, u64, u64)>) {
        let nr_shards: usize = 2;

        let g = InternalGraph::new(nr_shards);

        let unique_vertices_count = edges
            .iter()
            .flat_map(|(_, src, dst)| vec![src, dst])
            .sorted()
            .dedup()
            .count();

        let unique_edge_count = edges
            .iter()
            .map(|(_, src, dst)| (src, dst))
            .unique()
            .count();

        for (t, src, dst) in edges {
            g.add_edge(t, src, dst, [], None).unwrap();
        }

        assert_eq!(g.num_vertices(), unique_vertices_count);
        assert_eq!(g.num_edges(), unique_edge_count);
    }

    #[quickcheck]
    fn add_edge_works(edges: Vec<(i64, u64, u64)>) -> bool {
        let g = InternalGraph::new(3);
        for &(t, src, dst) in edges.iter() {
            g.add_edge(t, src, dst, [], None).unwrap();
        }

        edges
            .iter()
            .all(|&(_, src, dst)| g.has_edge(src, dst, None))
    }

    #[quickcheck]
    fn get_edge_works(edges: Vec<(i64, u64, u64)>) -> bool {
        let g = InternalGraph::new(100);
        for &(t, src, dst) in edges.iter() {
            g.add_edge(t, src, dst, [], None).unwrap();
        }

        edges
            .iter()
            .all(|&(_, src, dst)| g.edge(src, dst, None).is_some())
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

        let g = InternalGraph::new(2);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, [], None).unwrap();
        }

        let rand_dir = Uuid::new_v4();
        let tmp_raphtory_path: TempDir = TempDir::new("raphtory").unwrap();
        let shards_path =
            format!("{:?}/{}", tmp_raphtory_path.path().display(), rand_dir).replace('\"', "");

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
        match InternalGraph::load_from_file(Path::new(&shards_path)) {
            Ok(g) => {
                assert!(g.has_vertex_ref(1.into()));
                assert_eq!(g.nr_shards, 2);
            }
            Err(e) => panic!("{e}"),
        }

        let _ = tmp_raphtory_path.close();
    }

    #[test]
    fn has_edge() {
        let g = InternalGraph::new(2);
        g.add_edge(1, 7, 8, [], None).unwrap();

        assert!(!g.has_edge(8, 7, None));
        assert!(g.has_edge(7, 8, None));

        g.add_edge(1, 7, 9, [], None).unwrap();

        assert!(!g.has_edge(9, 7, None));
        assert!(g.has_edge(7, 9, None));

        g.add_edge(2, "haaroon", "northLondon", [], None).unwrap();
        assert!(g.has_edge("haaroon", "northLondon", None));
    }

    #[test]
    fn graph_edge() {
        let g = InternalGraph::new(2);
        let es = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];
        for (t, src, dst) in es {
            g.add_edge(t, src, dst, [], None).unwrap()
        }

        let e = g
            .edge_ref_window(1.into(), 3.into(), i64::MIN, i64::MAX, 0)
            .unwrap();
        assert_eq!(g.vertex_id(g.localise_vertex_unchecked(e.src())), 1u64);
        assert_eq!(g.vertex_id(g.localise_vertex_unchecked(e.dst())), 3u64);
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

        let g = InternalGraph::new(1);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, [], None).unwrap();
        }

        let expected = vec![(2, 3, 1), (1, 0, 0), (1, 0, 0)];
        let actual = (1..=3)
            .map(|i| {
                let i = g.vertex_ref(i).unwrap();
                (
                    g.degree_window(i, -1, 7, Direction::IN, None),
                    g.degree_window(i, 1, 7, Direction::OUT, None),
                    g.degree_window(i, 0, 1, Direction::BOTH, None),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);

        // Check results from multiple graphs with different number of shards
        let g = InternalGraph::new(3);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, [], None).unwrap();
        }

        let expected = (1..=3)
            .map(|i| {
                let i = g.vertex_ref(i).unwrap();
                (
                    g.degree_window(i, -1, 7, Direction::IN, None),
                    g.degree_window(i, 1, 7, Direction::OUT, None),
                    g.degree_window(i, 0, 1, Direction::BOTH, None),
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

        let g = InternalGraph::new(1);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, [], None).unwrap();
        }

        let expected = vec![(2, 3, 2), (1, 0, 0), (1, 0, 0)];
        let actual = (1..=3)
            .map(|i| {
                let i = g.vertex_ref(i).unwrap();
                (
                    g.vertex_edges_window(i, -1, 7, Direction::IN, None)
                        .collect::<Vec<_>>()
                        .len(),
                    g.vertex_edges_window(i, 1, 7, Direction::OUT, None)
                        .collect::<Vec<_>>()
                        .len(),
                    g.vertex_edges_window(i, 0, 1, Direction::BOTH, None)
                        .collect::<Vec<_>>()
                        .len(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);

        // Check results from multiple graphs with different number of shards
        let g = InternalGraph::new(10);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, [], None).unwrap();
        }

        let expected = (1..=3)
            .map(|i| {
                let i = g.vertex_ref(i).unwrap();
                (
                    g.vertex_edges_window(i, -1, 7, Direction::IN, None)
                        .collect::<Vec<_>>()
                        .len(),
                    g.vertex_edges_window(i, 1, 7, Direction::OUT, None)
                        .collect::<Vec<_>>()
                        .len(),
                    g.vertex_edges_window(i, 0, 1, Direction::BOTH, None)
                        .collect::<Vec<_>>()
                        .len(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn time_test() {
        let g = Graph::new(4);

        assert_eq!(g.latest_time(), None);
        assert_eq!(g.earliest_time(), None);

        g.add_vertex(5, 1, [])
            .map_err(|err| println!("{:?}", err))
            .ok();

        assert_eq!(g.latest_time(), Some(5));
        assert_eq!(g.earliest_time(), Some(5));

        let g = Graph::new(4);

        g.add_edge(10, 1, 2, [], None).unwrap();
        assert_eq!(g.latest_time(), Some(10));
        assert_eq!(g.earliest_time(), Some(10));

        g.add_vertex(5, 1, [])
            .map_err(|err| println!("{:?}", err))
            .ok();
        assert_eq!(g.latest_time(), Some(10));
        assert_eq!(g.earliest_time(), Some(5));

        g.add_edge(20, 3, 4, [], None).unwrap();
        assert_eq!(g.latest_time(), Some(20));
        assert_eq!(g.earliest_time(), Some(5));

        random_attachment(&g, 100, 10);
        assert_eq!(g.latest_time(), Some(126));
        assert_eq!(g.earliest_time(), Some(5));
    }

    #[test]
    fn static_properties() {
        let g = Graph::new(100); // big enough so all edges are very likely remote
        g.add_edge(0, 11, 22, [], None).unwrap();
        g.add_edge(0, 11, 11, [("temp".to_string(), Prop::Bool(true))], None)
            .unwrap();
        g.add_edge(0, 22, 33, [], None).unwrap();
        g.add_edge(0, 33, 11, [], None).unwrap();
        g.add_vertex(0, 11, [("temp".to_string(), Prop::Bool(true))])
            .unwrap();
        let v11 = g.vertex_ref(11).unwrap();
        let v22 = g.vertex_ref(22).unwrap();
        let v33 = g.vertex_ref(33).unwrap();
        let edge1111 = g.edge_ref(11.into(), 11.into(), 0).unwrap();
        let edge2233 = g.edge_ref(v22.into(), v33.into(), 0).unwrap();
        let edge3311 = g.edge_ref(v33.into(), v11.into(), 0).unwrap();

        g.add_vertex_properties(
            11,
            [
                ("a".to_string(), Prop::U64(11)),
                ("b".to_string(), Prop::I64(11)),
            ],
        )
        .unwrap();
        g.add_vertex_properties(11, [("c".to_string(), Prop::U32(11))])
            .unwrap();
        g.add_vertex_properties(22, [("b".to_string(), Prop::U64(22))])
            .unwrap();
        g.add_edge_properties(11, 11, [("d".to_string(), Prop::U64(1111))], None)
            .unwrap();
        g.add_edge_properties(33, 11, [("a".to_string(), Prop::U64(3311))], None)
            .unwrap();

        assert_eq!(g.static_vertex_prop_names(v11), vec!["a", "b", "c"]);
        assert_eq!(g.static_vertex_prop_names(v22), vec!["b"]);
        assert!(g.static_vertex_prop_names(v33).is_empty());
        assert_eq!(g.static_edge_prop_names(edge1111), vec!["d"]);
        assert_eq!(g.static_edge_prop_names(edge3311), vec!["a"]);
        assert!(g.static_edge_prop_names(edge2233).is_empty());

        assert_eq!(g.static_vertex_prop(v11, "a"), Some(Prop::U64(11)));
        assert_eq!(g.static_vertex_prop(v11, "b"), Some(Prop::I64(11)));
        assert_eq!(g.static_vertex_prop(v11, "c"), Some(Prop::U32(11)));
        assert_eq!(g.static_vertex_prop(v22, "b"), Some(Prop::U64(22)));
        assert_eq!(g.static_vertex_prop(v22, "a"), None);
        assert_eq!(g.static_edge_prop(edge1111, "d"), Some(Prop::U64(1111)));
        assert_eq!(g.static_edge_prop(edge3311, "a"), Some(Prop::U64(3311)));
        assert_eq!(g.static_edge_prop(edge2233, "a"), None);
    }

    #[test]
    #[should_panic]
    fn changing_property_type_for_vertex_panics() {
        let g = InternalGraph::new(4);
        g.add_vertex(0, 11, [("test".to_string(), Prop::Bool(true))])
            .unwrap();
        g.add_vertex_properties(11, [("test".to_string(), Prop::Bool(true))])
            .unwrap();
    }

    #[test]
    #[should_panic]
    fn changing_property_type_for_edge_panics() {
        let g = InternalGraph::new(4);
        g.add_edge(0, 11, 22, [("test".to_string(), Prop::Bool(true))], None)
            .unwrap();
        g.add_edge_properties(11, 22, [("test".to_string(), Prop::Bool(true))], None)
            .unwrap();
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

        let g = InternalGraph::new(2);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, [], None).unwrap();
        }

        let local_1 = VertexRef::new_local(0, 1);
        let remote_2 = VertexRef::Remote(2);
        let local_3 = VertexRef::new_local(1, 1);

        let expected = [
            (
                vec![local_1, remote_2],
                vec![local_1, local_3, remote_2],
                vec![local_1],
            ),
            (vec![VertexRef::Remote(1)], vec![], vec![]),
            (vec![local_1], vec![], vec![]),
        ];
        let actual = (1..=3)
            .map(|i| {
                let i = g.vertex_ref(i).unwrap();
                (
                    g.neighbours_window(i, -1, 7, Direction::IN, None)
                        .collect::<Vec<_>>(),
                    g.neighbours_window(i, 1, 7, Direction::OUT, None)
                        .collect::<Vec<_>>(),
                    g.neighbours_window(i, 0, 1, Direction::BOTH, None)
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_time_range_on_empty_graph() {
        let g = InternalGraph::new(1);

        let rolling = g.rolling(1, None).unwrap().collect_vec();
        assert!(rolling.is_empty());

        let expanding = g.expanding(1).unwrap().collect_vec();
        assert!(expanding.is_empty());
    }

    #[test]
    fn test_add_vertex_with_strings() {
        let g = InternalGraph::new(1);

        g.add_vertex(0, "haaroon", []).unwrap();
        g.add_vertex(1, "hamza", []).unwrap();
        g.add_vertex(1, 831, []).unwrap();

        assert!(g.has_vertex(831));
        assert!(g.has_vertex("haaroon"));
        assert!(g.has_vertex("hamza"));

        assert_eq!(g.num_vertices(), 3);
    }

    #[test]
    fn layers() {
        let g = InternalGraph::new(4);
        g.add_edge(0, 11, 22, [], None).unwrap();
        g.add_edge(0, 11, 33, [], None).unwrap();
        g.add_edge(0, 33, 11, [], None).unwrap();
        g.add_edge(0, 11, 22, [], Some("layer1")).unwrap();
        g.add_edge(0, 11, 33, [], Some("layer2")).unwrap();
        g.add_edge(0, 11, 44, [], Some("layer2")).unwrap();

        assert!(g.has_edge(11, 22, None));
        assert!(!g.has_edge(11, 44, None));
        assert!(!g.has_edge(11, 22, Some("layer2")));
        assert!(g.has_edge(11, 44, Some("layer2")));

        assert!(g.edge(11, 22, None).is_some());
        assert!(g.edge(11, 44, None).is_none());
        assert!(g.edge(11, 22, Some("layer2")).is_none());
        assert!(g.edge(11, 44, Some("layer2")).is_some());

        let dft_layer = g.default_layer();
        let layer1 = g.layer("layer1").unwrap();
        let layer2 = g.layer("layer2").unwrap();
        assert!(g.layer("missing layer").is_none());

        assert_eq!(g.num_edges(), 4);
        assert_eq!(dft_layer.num_edges(), 3);
        assert_eq!(layer1.num_edges(), 1);
        assert_eq!(layer2.num_edges(), 2);

        let vertex = g.vertex(11).unwrap();
        let vertex_dft = dft_layer.vertex(11).unwrap();
        let vertex1 = layer1.vertex(11).unwrap();
        let vertex2 = layer2.vertex(11).unwrap();

        assert_eq!(vertex.degree(), 3);
        assert_eq!(vertex_dft.degree(), 2);
        assert_eq!(vertex1.degree(), 1);
        assert_eq!(vertex2.degree(), 2);

        assert_eq!(vertex.out_degree(), 3);
        assert_eq!(vertex_dft.out_degree(), 2);
        assert_eq!(vertex1.out_degree(), 1);
        assert_eq!(vertex2.out_degree(), 2);

        assert_eq!(vertex.in_degree(), 1);
        assert_eq!(vertex_dft.in_degree(), 1);
        assert_eq!(vertex1.in_degree(), 0);
        assert_eq!(vertex2.in_degree(), 0);

        fn to_tuples<G: GraphViewOps, I: Iterator<Item = EdgeView<G>>>(
            edges: I,
        ) -> Vec<(u64, u64)> {
            edges
                .map(|e| (e.src().id(), e.dst().id()))
                .sorted()
                .collect_vec()
        }

        assert_eq!(
            to_tuples(vertex.edges()),
            vec![(11, 22), (11, 22), (11, 33), (11, 33), (11, 44), (33, 11)]
        );
        assert_eq!(
            to_tuples(vertex_dft.edges()),
            vec![(11, 22), (11, 33), (33, 11)]
        );
        assert_eq!(to_tuples(vertex1.edges()), vec![(11, 22)]);
        assert_eq!(to_tuples(vertex2.edges()), vec![(11, 33), (11, 44)]);

        assert_eq!(to_tuples(vertex.in_edges()), vec![(33, 11)]);
        assert_eq!(to_tuples(vertex_dft.in_edges()), vec![(33, 11)]);
        assert_eq!(to_tuples(vertex1.in_edges()), vec![]);
        assert_eq!(to_tuples(vertex2.in_edges()), vec![]);

        assert_eq!(
            to_tuples(vertex.out_edges()),
            vec![(11, 22), (11, 22), (11, 33), (11, 33), (11, 44)]
        );
        assert_eq!(to_tuples(vertex_dft.out_edges()), vec![(11, 22), (11, 33)]);
        assert_eq!(to_tuples(vertex1.out_edges()), vec![(11, 22)]);
        assert_eq!(to_tuples(vertex2.out_edges()), vec![(11, 33), (11, 44)]);

        fn to_ids<G: GraphViewOps>(neighbours: PathFromVertex<G>) -> Vec<u64> {
            neighbours.iter().map(|n| n.id()).sorted().collect_vec()
        }

        assert_eq!(to_ids(vertex.neighbours()), vec![22, 33, 44]);
        assert_eq!(to_ids(vertex_dft.neighbours()), vec![22, 33]);
        assert_eq!(to_ids(vertex1.neighbours()), vec![22]);
        assert_eq!(to_ids(vertex2.neighbours()), vec![33, 44]);

        assert_eq!(to_ids(vertex.out_neighbours()), vec![22, 33, 44]);
        assert_eq!(to_ids(vertex_dft.out_neighbours()), vec![22, 33]);
        assert_eq!(to_ids(vertex1.out_neighbours()), vec![22]);
        assert_eq!(to_ids(vertex2.out_neighbours()), vec![33, 44]);

        assert_eq!(to_ids(vertex.in_neighbours()), vec![33]);
        assert_eq!(to_ids(vertex_dft.in_neighbours()), vec![33]);
        assert!(to_ids(vertex1.in_neighbours()).is_empty());
        assert!(to_ids(vertex2.in_neighbours()).is_empty());
    }

    #[test]
    fn test_exploded_edge() {
        let g = InternalGraph::new(1);
        g.add_edge(0, 1, 2, [("weight".to_string(), Prop::I64(1))], None)
            .unwrap();
        g.add_edge(1, 1, 2, [("weight".to_string(), Prop::I64(2))], None)
            .unwrap();
        g.add_edge(2, 1, 2, [("weight".to_string(), Prop::I64(3))], None)
            .unwrap();

        let exploded = g.edge(1, 2, None).unwrap().explode();

        let res = exploded.map(|e| e.properties(false)).collect_vec();

        let mut expected = Vec::new();
        for i in 1..4 {
            let mut map = HashMap::new();
            map.insert("weight".to_string(), Prop::I64(i));
            expected.push(map);
        }

        assert_eq!(res, expected);

        let e = g
            .vertex(1)
            .unwrap()
            .edges()
            .explode()
            .map(|e| e.properties(false))
            .collect_vec();
        assert_eq!(e, expected);
    }

    #[test]
    fn test_edge_earliest_latest() {
        let g = InternalGraph::new(1);
        g.add_edge(0, 1, 2, [], None).unwrap();
        g.add_edge(1, 1, 2, [], None).unwrap();
        g.add_edge(2, 1, 2, [], None).unwrap();
        g.add_edge(0, 1, 3, [], None).unwrap();
        g.add_edge(1, 1, 3, [], None).unwrap();
        g.add_edge(2, 1, 3, [], None).unwrap();

        let mut res = g.edge(1, 2, None).unwrap().earliest_time().unwrap();
        assert_eq!(res, 0);

        res = g.edge(1, 2, None).unwrap().latest_time().unwrap();
        assert_eq!(res, 2);

        res = g.at(1).edge(1, 2, None).unwrap().earliest_time().unwrap();
        assert_eq!(res, 0);

        res = g.at(1).edge(1, 2, None).unwrap().latest_time().unwrap();
        assert_eq!(res, 1);

        let res_list: Vec<i64> = g
            .vertex(1)
            .unwrap()
            .edges()
            .earliest_time()
            .flatten()
            .collect();
        assert_eq!(res_list, vec![0, 0]);

        let res_list: Vec<i64> = g
            .vertex(1)
            .unwrap()
            .edges()
            .latest_time()
            .flatten()
            .collect();
        assert_eq!(res_list, vec![2, 2]);

        let res_list: Vec<i64> = g
            .vertex(1)
            .unwrap()
            .at(1)
            .edges()
            .earliest_time()
            .flatten()
            .collect();
        assert_eq!(res_list, vec![0, 0]);

        let res_list: Vec<i64> = g
            .vertex(1)
            .unwrap()
            .at(1)
            .edges()
            .latest_time()
            .flatten()
            .collect();
        assert_eq!(res_list, vec![1, 1]);
    }

    #[test]
    fn check_vertex_history() {
        let g = InternalGraph::new(1);

        g.add_vertex(1, 1, []).unwrap();
        g.add_vertex(2, 1, []).unwrap();
        g.add_vertex(3, 1, []).unwrap();
        g.add_vertex(4, 1, []).unwrap();
        g.add_vertex(8, 1, []).unwrap();

        g.add_vertex(4, "Lord Farquaad", []).unwrap();
        g.add_vertex(6, "Lord Farquaad", []).unwrap();
        g.add_vertex(7, "Lord Farquaad", []).unwrap();
        g.add_vertex(8, "Lord Farquaad", []).unwrap();

        let times_of_one = g.vertex(1).unwrap().history();
        let times_of_farquaad = g.vertex("Lord Farquaad").unwrap().history();

        assert_eq!(times_of_one, [1, 2, 3, 4, 8]);
        assert_eq!(times_of_farquaad, [4, 6, 7, 8]);

        let view = g.window(1, 8);

        let windowed_times_of_one = view.vertex(1).unwrap().history();
        let windowed_times_of_farquaad = view.vertex("Lord Farquaad").unwrap().history();
        assert_eq!(windowed_times_of_one, [1, 2, 3, 4]);
        assert_eq!(windowed_times_of_farquaad, [4, 6, 7]);
    }

    #[test]
    fn check_edge_history() {
        let g = InternalGraph::new(1);

        g.add_edge(1, 1, 2, [], None).unwrap();
        g.add_edge(2, 1, 3, [], None).unwrap();
        g.add_edge(3, 1, 2, [], None).unwrap();
        g.add_edge(4, 1, 4, [], None).unwrap();

        let times_of_onetwo = g.edge(1, 2, None).unwrap().history();
        let times_of_four = g.edge(1, 4, None).unwrap().window(1, 5).history();
        let view = g.window(2, 5);
        let windowed_times_of_four = view.edge(1, 4, None).unwrap().window(2, 4).history();

        assert_eq!(times_of_onetwo, [1, 3]);
        assert_eq!(times_of_four, [4]);
        assert!(windowed_times_of_four.is_empty());
    }

    #[test]
    fn check_edge_history_on_multiple_shards() {
        let g = InternalGraph::new(10);

        g.add_edge(1, 1, 2, [], None).unwrap();
        g.add_edge(2, 1, 3, [], None).unwrap();
        g.add_edge(3, 1, 2, [], None).unwrap();
        g.add_edge(4, 1, 4, [], None).unwrap();
        g.add_edge(5, 1, 4, [], None).unwrap();
        g.add_edge(6, 1, 4, [], None).unwrap();
        g.add_edge(7, 1, 4, [], None).unwrap();
        g.add_edge(8, 1, 4, [], None).unwrap();
        g.add_edge(9, 1, 4, [], None).unwrap();
        g.add_edge(10, 1, 4, [], None).unwrap();

        let times_of_onetwo = g.edge(1, 2, None).unwrap().history();
        let times_of_four = g.edge(1, 4, None).unwrap().window(1, 5).history();
        let times_of_outside_window = g.edge(1, 4, None).unwrap().window(1, 4).history();
        let times_of_four_higher = g.edge(1, 4, None).unwrap().window(6, 11).history();

        let view = g.window(1, 11);
        let windowed_times_of_four = view.edge(1, 4, None).unwrap().window(2, 5).history();
        let windowed_times_of_four_higher = view.edge(1, 4, None).unwrap().window(8, 11).history();

        assert_eq!(times_of_onetwo, [1, 3]);
        assert_eq!(times_of_four, [4]);
        assert_eq!(times_of_four_higher, [6, 7, 8, 9, 10]);
        assert!(times_of_outside_window.is_empty());
        assert_eq!(windowed_times_of_four, [4]);
        assert_eq!(windowed_times_of_four_higher, [8, 9, 10]);
    }

    #[test]
    fn check_vertex_history_multiple_shards() {
        let g = InternalGraph::new(10);

        g.add_vertex(1, 1, []).unwrap();
        g.add_vertex(2, 1, []).unwrap();
        g.add_vertex(3, 1, []).unwrap();
        g.add_vertex(4, 1, []).unwrap();
        g.add_vertex(5, 2, []).unwrap();
        g.add_vertex(6, 2, []).unwrap();
        g.add_vertex(7, 2, []).unwrap();
        g.add_vertex(8, 1, []).unwrap();
        g.add_vertex(9, 2, []).unwrap();
        g.add_vertex(10, 2, []).unwrap();

        g.add_vertex(4, "Lord Farquaad", []).unwrap();
        g.add_vertex(6, "Lord Farquaad", []).unwrap();
        g.add_vertex(7, "Lord Farquaad", []).unwrap();
        g.add_vertex(8, "Lord Farquaad", []).unwrap();

        let times_of_one = g.vertex(1).unwrap().history();
        let times_of_farquaad = g.vertex("Lord Farquaad").unwrap().history();
        let times_of_upper = g.vertex(2).unwrap().history();

        assert_eq!(times_of_one, [1, 2, 3, 4, 8]);
        assert_eq!(times_of_farquaad, [4, 6, 7, 8]);
        assert_eq!(times_of_upper, [5, 6, 7, 9, 10]);

        let view = g.window(1, 8);
        let windowed_times_of_one = view.vertex(1).unwrap().history();
        let windowed_times_of_two = view.vertex(2).unwrap().history();
        let windowed_times_of_farquaad = view.vertex("Lord Farquaad").unwrap().history();

        assert_eq!(windowed_times_of_one, [1, 2, 3, 4]);
        assert_eq!(windowed_times_of_farquaad, [4, 6, 7]);
        assert_eq!(windowed_times_of_two, [5, 6, 7]);
    }

    #[test]
    fn test_ingesting_timestamps() {
        let earliest_time = "2022-06-06 12:34:00".try_into_time().unwrap();
        let latest_time = "2022-06-07 12:34:00".try_into_time().unwrap();

        let g = InternalGraph::new(4);
        g.add_vertex("2022-06-06T12:34:00.000", 0, []).unwrap();
        g.add_edge("2022-06-07T12:34:00", 1, 2, [], None).unwrap();
        assert_eq!(g.earliest_time().unwrap(), earliest_time);
        assert_eq!(g.latest_time().unwrap(), latest_time);

        let g = InternalGraph::new(4);
        let fmt = "%Y-%m-%d %H:%M";
        g.add_vertex_with_custom_time_format("2022-06-06 12:34", fmt, 0, [])
            .unwrap();
        g.add_edge_with_custom_time_format("2022-06-07 12:34", fmt, 1, 2, [], None)
            .unwrap();
        assert_eq!(g.earliest_time().unwrap(), earliest_time);
        assert_eq!(g.latest_time().unwrap(), latest_time);
    }

    #[test]
    fn test_prop_display_str() {
        let mut prop = Prop::Str(String::from("hello"));
        assert_eq!(format!("{}", prop), "hello");

        prop = Prop::I32(42);
        assert_eq!(format!("{}", prop), "42");

        prop = Prop::I64(9223372036854775807);
        assert_eq!(format!("{}", prop), "9223372036854775807");

        prop = Prop::U32(4294967295);
        assert_eq!(format!("{}", prop), "4294967295");

        prop = Prop::U64(18446744073709551615);
        assert_eq!(format!("{}", prop), "18446744073709551615");

        prop = Prop::F32(3.14159);
        assert_eq!(format!("{}", prop), "3.14159");

        prop = Prop::F64(3.141592653589793);
        assert_eq!(format!("{}", prop), "3.141592653589793");

        prop = Prop::Bool(true);
        assert_eq!(format!("{}", prop), "true");
    }

    #[test]
    fn test_temporral_edge_props_window() {
        let g = Graph::new(1);
        g.add_edge(1, 1, 2, [("weight".to_string(), Prop::I64(1))], None)
            .unwrap();
        g.add_edge(2, 1, 2, [("weight".to_string(), Prop::I64(2))], None)
            .unwrap();
        g.add_edge(3, 1, 2, [("weight".to_string(), Prop::I64(3))], None)
            .unwrap();

        let e = g.vertex(1).unwrap().out_edges().next().unwrap();

        let res = g.temporal_edge_props_window(EdgeRef::from(e), 1, 3);
        let mut exp = HashMap::new();
        exp.insert(
            "weight".to_string(),
            vec![(1, Prop::I64(1)), (2, Prop::I64(2))],
        );
        assert_eq!(res, exp);
    }

    #[test]
    fn test_vertex_early_late_times() {
        let g = InternalGraph::new(1);
        g.add_vertex(1, 1, []).unwrap();
        g.add_vertex(2, 1, []).unwrap();
        g.add_vertex(3, 1, []).unwrap();

        assert_eq!(g.vertex(1).unwrap().earliest_time(), Some(1));
        assert_eq!(g.vertex(1).unwrap().latest_time(), Some(3));

        assert_eq!(g.at(2).vertex(1).unwrap().earliest_time(), Some(1));
        assert_eq!(g.at(2).vertex(1).unwrap().latest_time(), Some(2));
    }

    #[test]
    fn test_vertex_ids() {
        let g = InternalGraph::new(1);
        g.add_vertex(1, 1, []).unwrap();
        g.add_vertex(1, 2, []).unwrap();
        g.add_vertex(2, 3, []).unwrap();

        assert_eq!(g.vertices().id().collect::<Vec<u64>>(), vec![1, 2, 3]);

        let g_at = g.at(1);
        assert_eq!(g_at.vertices().id().collect::<Vec<u64>>(), vec![1, 2]);
    }

    #[test]
    fn test_edge_layer_name() -> Result<(), GraphError> {
        let g = InternalGraph::new(4);
        g.add_edge(0, 0, 1, [], None)?;
        g.add_edge(0, 0, 1, [], Some("awesome name"))?;

        let layer_names = g.edges().map(|e| e.layer_name()).sorted().collect_vec();
        assert_eq!(layer_names, vec!["awesome name", "default layer"]);
        Ok(())
    }

    #[test]
    fn test_edge_from_single_layer() {
        let g = InternalGraph::new(4);
        g.add_edge(0, 1, 2, [], Some("layer")).unwrap();

        assert!(g.edge(1, 2, None).is_none());
        assert!(g.layer("layer").unwrap().edge(1, 2, None).is_some())
    }

    #[test]
    fn test_unique_layers() {
        let g = InternalGraph::new(4);
        g.add_edge(0, 1, 2, [], Some("layer1")).unwrap();
        g.add_edge(0, 1, 2, [], Some("layer2")).unwrap();
        assert_eq!(
            g.layer("layer2").unwrap().get_unique_layers(),
            vec!["layer2"]
        )
    }

    #[quickcheck]
    fn vertex_from_id_is_consistent(vertices: Vec<u64>) -> bool {
        let g = InternalGraph::new(1);
        for v in vertices.iter() {
            g.add_vertex(0, *v, []).unwrap();
        }
        g.vertices()
            .name()
            .map(|name| g.vertex(name))
            .all(|v| v.is_some())
    }

    #[quickcheck]
    fn exploded_edge_times_is_consistent(edges: Vec<(u64, u64, Vec<i64>)>, offset: i64) -> bool {
        let mut correct = true;
        let mut check = |condition: bool, message: String| {
            if !condition {
                println!("Failed: {}", message);
            }
            correct = correct && condition;
        };
        // checks that exploded edges are preserved with correct timestamps
        let mut edges: Vec<(u64, u64, Vec<i64>)> =
            edges.into_iter().filter(|e| !e.2.is_empty()).collect();
        // discard edges without timestamps
        for e in edges.iter_mut() {
            e.2.sort();
            // FIXME: Should not have to do this, see issue https://github.com/Pometry/Raphtory/issues/973
            e.2.dedup(); // add each timestamp only once (multi-edge per timestamp currently not implemented)
        }
        edges.sort();
        edges.dedup_by_key(|(src, dst, _)| (*src, *dst));

        let g = Graph::new(1);
        for (src, dst, times) in edges.iter() {
            for t in times.iter() {
                g.add_edge(*t, *src, *dst, [], None).unwrap();
            }
        }

        let mut actual_edges: Vec<(u64, u64, Vec<i64>)> = g
            .edges()
            .map(|e| {
                (
                    e.src().id(),
                    e.dst().id(),
                    e.explode()
                        .map(|ee| {
                            check(
                                ee.earliest_time() == ee.latest_time(),
                                format!("times mismatched for {:?}", ee),
                            ); // times are the same for exploded edge
                            let t = ee.earliest_time().unwrap();
                            check(
                                ee.active(t),
                                format!("exploded edge {:?} inactive at {}", ee, t),
                            );
                            if t < i64::MAX {
                                // window is broken at MAX!
                                check(e.active(t), format!("edge {:?} inactive at {}", e, t));
                            }
                            let t_test = t.saturating_add(offset);
                            if t_test != t && t_test < i64::MAX && t_test > i64::MIN {
                                check(
                                    !ee.active(t_test),
                                    format!("exploded edge {:?} active at {}", ee, t_test),
                                );
                            }
                            t
                        })
                        .collect(),
                )
            })
            .collect();

        for e in actual_edges.iter_mut() {
            e.2.sort();
        }
        actual_edges.sort();
        check(
            actual_edges == edges,
            format!(
                "actual edges didn't match input actual: {:?}, expected: {:?}",
                actual_edges, edges
            ),
        );
        correct
    }

    // non overlaping time intervals
    #[derive(Clone, Debug)]
    struct Intervals(Vec<(i64, i64)>);

    impl Arbitrary for Intervals {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            let mut some_nums = Vec::<i64>::arbitrary(g);
            some_nums.sort();
            let intervals = some_nums
                .into_iter()
                .tuple_windows()
                .filter(|(a, b)| a != b)
                .collect_vec();
            Intervals(intervals)
        }
    }
}
