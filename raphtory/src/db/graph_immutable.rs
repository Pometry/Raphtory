//! Defines the `ImmutableGraph` struct, which represents a raphtory graph in a frozen state.
//! This graph can be queried in a read-only format avoiding any locks placed when using a
//! non-immutable graph.
//!
//! # Examples
//!
//! ```rust
//! use raphtory::db::graph::Graph;
//! use raphtory::db::view_api::*;
//!
//! let graph = Graph::new(2);
//! // Add vertices and edges
//!
//! let immutable_graph = graph.freeze();
//! ```

use crate::core::edge_ref::EdgeRef;
use crate::core::tgraph::TemporalGraph;
use crate::core::tgraph_shard::ImmutableTGraphShard;
use crate::core::timeindex::TimeIndex;
use crate::core::tprop::TProp;
use crate::core::vertex_ref::{LocalVertexRef, VertexRef};
use crate::core::Direction;
use crate::core::{utils, Prop};
use crate::db::graph::Graph;
use crate::db::view_api::internal::time_semantics::TimeSemantics;
use crate::db::view_api::internal::CoreGraphOps;
use crate::db::view_api::BoxedIter;
use itertools::Itertools;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::cmp::{max, min};
use std::iter;
use std::ops::Range;
use std::sync::Arc;

use super::view_api::internal::GraphViewInternalOps;

/// A raphtory graph in a frozen state that is read-only.
/// This graph can be queried in a read-only format avoiding any locks placed when using a
/// non-immutable graph.
///
/// # Examples
///
/// ```rust
/// use raphtory::db::graph::Graph;
/// use raphtory::db::view_api::*;
///
/// let graph = Graph::new(2);
/// // Add vertices and edges
///
/// let immutable_graph = graph.freeze();
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImmutableGraph {
    pub(crate) nr_shards: usize,
    pub(crate) shards: Vec<ImmutableTGraphShard<TemporalGraph>>,
    pub(crate) layer_ids: Arc<FxHashMap<String, usize>>,
}

/// Failure if there is an issue with unfreezing a frozen graph
#[derive(Debug, PartialEq)]
pub struct UnfreezeFailure;

/// Implements the `ImmutableGraph` struct.
impl ImmutableGraph {
    /// Unfreeze the immutable graph and convert it to a mutable `Graph`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use raphtory::db::graph::Graph;
    /// use raphtory::db::view_api::*;
    ///
    /// let graph = Graph::new(2);
    /// // Add vertices and edges
    /// let immutable_graph = graph.freeze();
    /// // Unfreeze the graph
    /// let graph = immutable_graph.unfreeze().unwrap();
    /// ```
    pub fn unfreeze(self) -> Result<Graph, UnfreezeFailure> {
        let mut shards = Vec::with_capacity(self.shards.len());
        for shard in self.shards {
            match shard.unfreeze() {
                Ok(t) => shards.push(t),
                Err(_) => return Err(UnfreezeFailure),
            }
        }
        Ok(Graph::new_from_frozen(
            self.nr_shards,
            shards,
            Arc::new(parking_lot::RwLock::new((*self.layer_ids).clone())),
        ))
    }

    /// Get the shard id for a given global vertex id.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use raphtory::db::graph::Graph;
    /// use raphtory::db::view_api::*;
    ///
    /// let graph = Graph::new(2);
    /// graph.add_vertex(0, 1, &vec![]).unwrap();
    /// // ... Add vertices and edges ...
    /// let immutable_graph = graph.freeze();
    /// // Unfreeze the graph
    /// immutable_graph.shard_id(1);
    /// ```
    pub fn shard_id(&self, g_id: u64) -> usize {
        utils::get_shard_id_from_global_vid(g_id, self.nr_shards)
    }

    /// Get an immutable graph shard for a given global vertex id.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use raphtory::db::graph::Graph;
    /// use raphtory::db::view_api::*;
    ///
    /// let graph = Graph::new(2);
    /// graph.add_vertex(0, 1, &vec![]).unwrap();
    /// // ... Add vertices and edges ...
    /// let immutable_graph = graph.freeze();
    /// // Unfreeze the graph
    /// let shard = immutable_graph.get_shard_from_id(1);
    /// ```
    pub fn get_shard_from_id(&self, g_id: u64) -> &ImmutableTGraphShard<TemporalGraph> {
        &self.shards[self.shard_id(g_id)]
    }

    /// Get an immutable graph shard for a given vertex.
    ///
    pub fn get_shard_from_v(&self, v: VertexRef) -> &ImmutableTGraphShard<TemporalGraph> {
        match v {
            VertexRef::Local(local) => &self.shards[local.shard_id],
            VertexRef::Remote(g_id) => &self.shards[self.shard_id(g_id)],
        }
    }

    pub fn get_shard_from_local_v(
        &self,
        v: LocalVertexRef,
    ) -> &ImmutableTGraphShard<TemporalGraph> {
        &self.shards[v.shard_id]
    }

    /// Get an immutable graph shard for a given edge.
    ///
    pub fn get_shard_from_e(&self, e: EdgeRef) -> &ImmutableTGraphShard<TemporalGraph> {
        &self.shards[e.shard()]
    }

    // Get the earliest time in the graph.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use raphtory::db::graph::Graph;
    /// use raphtory::db::view_api::*;
    ///
    /// let graph = Graph::new(2);
    /// graph.add_vertex(0, 1, &vec![]).unwrap();
    /// // ... Add vertices and edges ...
    /// let immutable_graph = graph.freeze();
    /// // Unfreeze the graph
    /// let time = immutable_graph.earliest_time();
    /// ```
    pub fn earliest_time(&self) -> Option<i64> {
        let min_from_shards = self.shards.iter().map(|shard| shard.earliest_time()).min();
        min_from_shards.filter(|&min| min != i64::MAX)
    }

    // Get the latest time in the graph.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use raphtory::db::graph::Graph;
    /// use raphtory::db::view_api::*;
    ///
    /// let graph = Graph::new(2);
    /// graph.add_vertex(0, 1, &vec![]).unwrap();
    /// // ... Add vertices and edges ...
    /// let immutable_graph = graph.freeze();
    /// // Unfreeze the graph
    /// let time = immutable_graph.latest_time();
    /// ```
    pub fn latest_time(&self) -> Option<i64> {
        let max_from_shards = self.shards.iter().map(|shard| shard.latest_time()).max();
        max_from_shards.filter(|&max| max != i64::MIN)
    }

    /// Get the degree for a vertex in the graph given its direction.
    pub fn degree(&self, v: LocalVertexRef, d: Direction) -> usize {
        self.get_shard_from_local_v(v).degree(v, d, None)
    }

    /// Get all vertices in the graph.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use raphtory::db::graph::Graph;
    /// use raphtory::db::view_api::*;
    ///
    /// let graph = Graph::new(2);
    /// graph.add_vertex(0, 1, &vec![]).unwrap();
    /// // ... Add vertices and edges ...
    /// let immutable_graph = graph.freeze();
    /// // Unfreeze the graph
    /// let vertices = immutable_graph.vertices();
    /// ```
    pub fn vertices(&self) -> Box<dyn Iterator<Item = LocalVertexRef> + Send + '_> {
        Box::new(self.shards.iter().flat_map(|s| s.vertices()))
    }

    /// Get all edges in the graph.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use raphtory::db::graph::Graph;
    /// use raphtory::db::view_api::*;
    ///
    /// let graph = Graph::new(2);
    /// graph.add_edge(0, 1, 1, &vec![], None).unwrap();
    /// // ... Add vertices and edges ...
    /// let immutable_graph = graph.freeze();
    /// // Unfreeze the graph
    /// let edges = immutable_graph.edges();
    /// ```
    pub fn edges(&self) -> Box<dyn Iterator<Item = EdgeRef> + Send + '_> {
        Box::new(self.vertices().flat_map(|v| {
            self.get_shard_from_local_v(v)
                .vertex_edges(v, Direction::OUT, None)
        }))
    }

    /// Get number of edges in the graph.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use raphtory::db::graph::Graph;
    /// use raphtory::db::view_api::*;
    ///
    /// let graph = Graph::new(2);
    /// graph.add_edge(0, 1, 2, &vec![], None).unwrap();
    /// // ... Add vertices and edges ...
    /// let immutable_graph = graph.freeze();
    /// // Unfreeze the graph
    /// let num_edges = immutable_graph.num_edges();
    /// ```
    pub fn num_edges(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.out_edges_len(None))
            .sum()
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
}

impl CoreGraphOps for ImmutableGraph {
    fn edge_additions(&self, eref: EdgeRef) -> &TimeIndex {
        todo!()
    }

    fn edge_deletions(&self, eref: EdgeRef) -> &TimeIndex {
        todo!()
    }

    fn vertex_additions(&self, v: LocalVertexRef) -> &TimeIndex {
        todo!()
    }

    fn localise_vertex_unchecked(&self, v: VertexRef) -> LocalVertexRef {
        todo!()
    }

    fn static_vertex_prop(&self, v: LocalVertexRef, name: String) -> Option<crate::core::Prop> {
        self.get_shard_from_local_v(v).static_vertex_prop(v, name)
    }

    fn static_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        self.get_shard_from_local_v(v).static_vertex_prop_names(v)
    }

    fn temporal_vertex_prop(&self, v: LocalVertexRef, name: String) -> Option<&TProp> {
        todo!()
    }

    fn temporal_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        self.get_shard_from_local_v(v).temporal_vertex_prop_names(v)
    }

    fn static_edge_prop(&self, e: EdgeRef, name: String) -> Option<crate::core::Prop> {
        self.get_shard_from_e(e).static_edge_prop(e, name)
    }

    fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.get_shard_from_e(e).static_edge_prop_names(e)
    }

    fn temporal_edge_prop(&self, e: EdgeRef, name: String) -> Option<&TProp> {
        todo!()
    }

    fn temporal_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.get_shard_from_e(e).temporal_edge_prop_names(e)
    }

    fn num_shards(&self) -> usize {
        self.nr_shards
    }
}

impl GraphViewInternalOps for ImmutableGraph {
    fn local_vertex_ref(&self, v: VertexRef) -> Option<LocalVertexRef> {
        self.get_shard_from_v(v).local_vertex(v)
    }

    fn get_unique_layers_internal(&self) -> Vec<usize> {
        let a = iter::once(0);
        let b = self.layer_ids.values().copied();
        a.chain(b).collect_vec()
    }

    fn get_layer_name_by_id(&self, layer_id: usize) -> String {
        self.layer_ids
            .iter()
            .find_map(|(name, &id)| (layer_id == id).then_some(name))
            .expect(&format!("layer id '{layer_id}' doesn't exist"))
            .to_string()
    }

    fn get_layer_id(&self, key: Option<&str>) -> Option<usize> {
        match key {
            None => Some(0),
            Some(key) => self.layer_ids.get(key).copied(),
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

    fn vertex_id(&self, v: LocalVertexRef) -> u64 {
        self.shards[v.shard_id].vertex_id(v)
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

impl TimeSemantics for ImmutableGraph {
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

    fn vertex_earliest_time(&self, v: LocalVertexRef) -> Option<i64> {
        self.get_shard_from_local_v(v).vertex_earliest_time(v)
    }

    fn vertex_latest_time(&self, v: LocalVertexRef) -> Option<i64> {
        self.get_shard_from_local_v(v).vertex_latest_time(v)
    }

    fn earliest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        todo!()
    }

    fn latest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        todo!()
    }

    fn vertex_earliest_time_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<i64> {
        todo!()
    }

    fn vertex_latest_time_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<i64> {
        todo!()
    }

    fn include_vertex_window(&self, v: LocalVertexRef, w: Range<i64>) -> bool {
        todo!()
    }

    fn include_edge_window(&self, e: EdgeRef, w: Range<i64>) -> bool {
        todo!()
    }

    fn vertex_history(&self, v: LocalVertexRef) -> BoxedIter<i64> {
        todo!()
    }

    fn vertex_history_window(&self, v: LocalVertexRef, w: Range<i64>) -> BoxedIter<i64> {
        todo!()
    }

    fn edge_history(&self, e: EdgeRef) -> BoxedIter<(i64, i64)> {
        todo!()
    }

    fn edge_history_window(&self, e: EdgeRef, w: Range<i64>) -> BoxedIter<(i64, i64)> {
        todo!()
    }

    fn temporal_vertex_prop_vec(&self, v: LocalVertexRef, name: String) -> Vec<(i64, Prop)> {
        todo!()
    }

    fn temporal_vertex_prop_vec_window(
        &self,
        v: LocalVertexRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        todo!()
    }

    fn temporal_edge_prop_vec_window(
        &self,
        e: EdgeRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        todo!()
    }

    fn temporal_edge_prop_vec(&self, e: EdgeRef, name: String) -> Vec<(i64, Prop)> {
        todo!()
    }
}
