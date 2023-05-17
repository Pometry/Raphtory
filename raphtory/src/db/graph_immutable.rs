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
use crate::core::utils;
use crate::core::vertex_ref::{LocalVertexRef, VertexRef};
use crate::core::Direction;
use crate::db::graph::Graph;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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
        Ok(Graph {
            nr_shards: self.nr_shards,
            shards,
            layer_ids: Arc::new(parking_lot::RwLock::new((*self.layer_ids).clone())),
        })
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
}

// impl GraphViewInternalOps for ImmutableGraph {
//     fn get_layer(&self, key: Option<&str>) -> Option<usize> {
//         match key {
//             None => Some(0),
//             Some(key) => self.layer_ids.get(key).copied(),
//         }
//     }

//     fn view_start(&self) -> Option<i64> {
//         self.earliest_time_global()
//     }

//     fn view_end(&self) -> Option<i64> {
//         self.latest_time_global().map(|t| t + 1) // so it is exclusive
//     }

//     fn earliest_time_global(&self) -> Option<i64> {
//         let min_from_shards = self.shards.iter().map(|shard| shard.earliest_time()).min();
//         min_from_shards.filter(|&min| min != i64::MAX)
//     }

//     fn earliest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
//         //FIXME: this is not correct, should actually be the earliest activity in window
//         let earliest = self.earliest_time_global()?;
//         if earliest > t_end {
//             None
//         } else {
//             Some(max(earliest, t_start))
//         }
//     }

//     fn latest_time_global(&self) -> Option<i64> {
//         let max_from_shards = self.shards.iter().map(|shard| shard.latest_time()).max();
//         max_from_shards.filter(|&max| max != i64::MIN)
//     }

//     fn latest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
//         //FIXME: this is not correct, should actually be the latest activity in window
//         let latest = self.latest_time_global()?;
//         if latest < t_start {
//             None
//         } else {
//             Some(min(latest, t_end))
//         }
//     }

//     fn vertices_len(&self) -> usize {
//         let vs: Vec<usize> = self.shards.iter().map(|shard| shard.len()).collect();
//         vs.iter().sum()
//     }

//     fn vertices_len_window(&self, t_start: i64, t_end: i64) -> usize {
//         //FIXME: This nees to be optimised ideally
//         self.shards
//             .iter()
//             .map(|shard| shard.vertices_window(t_start..t_end).count())
//             .sum()
//     }

//     fn edges_len(&self, layer: Option<usize>) -> usize {
//         let vs: Vec<usize> = self
//             .shards
//             .iter()
//             .map(|shard| shard.out_edges_len(layer))
//             .collect();
//         vs.iter().sum()
//     }

//     fn edges_len_window(&self, t_start: i64, t_end: i64, layer: Option<usize>) -> usize {
//         self.shards
//             .iter()
//             .map(|shard| shard.out_edges_len_window(&(t_start..t_end), layer))
//             .sum()
//     }

//     fn has_edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> bool {
//         self.get_shard_from_v(src)
//             .has_edge(src.g_id, dst.g_id, layer)
//     }

//     fn has_edge_ref_window(
//         &self,
//         src: VertexRef,
//         dst: VertexRef,
//         t_start: i64,
//         t_end: i64,
//         layer: usize,
//     ) -> bool {
//         self.get_shard_from_v(src)
//             .has_edge_window(src.g_id, dst.g_id, t_start..t_end, layer)
//     }

//     fn has_vertex_ref(&self, v: VertexRef) -> bool {
//         self.get_shard_from_v(v).has_vertex(v.g_id)
//     }

//     fn has_vertex_ref_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> bool {
//         self.get_shard_from_v(v)
//             .has_vertex_window(v.g_id, t_start..t_end)
//     }

//     fn degree(&self, v: VertexRef, d: Direction, layer: Option<usize>) -> usize {
//         self.get_shard_from_v(v).degree(v.g_id, d, layer)
//     }

//     fn degree_window(
//         &self,
//         v: VertexRef,
//         t_start: i64,
//         t_end: i64,
//         d: Direction,
//         layer: Option<usize>,
//     ) -> usize {
//         self.get_shard_from_v(v)
//             .degree_window(v.g_id, t_start..t_end, d, layer)
//     }

//     fn vertex_ref(&self, v: u64) -> Option<VertexRef> {
//         self.get_shard_from_id(v).vertex(v)
//     }

//     fn lookup_by_pid_and_shard(&self, pid: usize, shard: usize) -> Option<VertexRef> {
//         todo!()
//     }

//     fn vertex_ref_window(&self, v: u64, t_start: i64, t_end: i64) -> Option<VertexRef> {
//         self.get_shard_from_id(v).vertex_window(v, t_start..t_end)
//     }

//     fn vertex_earliest_time(&self, v: VertexRef) -> Option<i64> {
//         self.get_shard_from_v(v).vertex_earliest_time(v)
//     }

//     fn vertex_earliest_time_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> Option<i64> {
//         self.get_shard_from_v(v)
//             .vertex_earliest_time_window(v, t_start..t_end)
//     }

//     fn vertex_latest_time(&self, v: VertexRef) -> Option<i64> {
//         self.get_shard_from_v(v).vertex_latest_time(v)
//     }

//     fn vertex_latest_time_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> Option<i64> {
//         todo!()
//     }

//     fn vertex_ids(&self) -> Box<dyn Iterator<Item = u64> + Send> {
//         todo!()
//     }

//     fn vertex_ids_window(&self, t_start: i64, t_end: i64) -> Box<dyn Iterator<Item = u64> + Send> {
//         todo!()
//     }

//     fn vertex_refs(&self) -> Box<dyn Iterator<Item = VertexRef> + Send> {
//         todo!()
//     }

//     fn vertex_refs_window(
//         &self,
//         t_start: i64,
//         t_end: i64,
//     ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
//         todo!()
//     }

//     fn vertex_refs_shard(&self, shard: usize) -> Box<dyn Iterator<Item = VertexRef> + Send> {
//         todo!()
//     }

//     fn vertex_refs_window_shard(
//         &self,
//         shard: usize,
//         t_start: i64,
//         t_end: i64,
//     ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
//         todo!()
//     }

//     fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> Option<EdgeRef> {
//         todo!()
//     }

//     fn edge_ref_window(
//         &self,
//         src: VertexRef,
//         dst: VertexRef,
//         t_start: i64,
//         t_end: i64,
//         layer: usize,
//     ) -> Option<EdgeRef> {
//         todo!()
//     }

//     fn edge_refs(&self, layer: Option<usize>) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
//         todo!()
//     }

//     fn edge_refs_window(
//         &self,
//         t_start: i64,
//         t_end: i64,
//         layer: Option<usize>,
//     ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
//         todo!()
//     }

//     fn vertex_edges_all_layers(
//         &self,
//         v: VertexRef,
//         d: Direction,
//     ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
//         todo!()
//     }

//     fn vertex_edges_single_layer(
//         &self,
//         v: VertexRef,
//         d: Direction,
//         layer: usize,
//     ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
//         todo!()
//     }

//     fn vertex_edges_t(
//         &self,
//         v: VertexRef,
//         d: Direction,
//         layer: Option<usize>,
//     ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
//         todo!()
//     }

//     fn vertex_edges_window(
//         &self,
//         v: VertexRef,
//         t_start: i64,
//         t_end: i64,
//         d: Direction,
//         layer: Option<usize>,
//     ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
//         todo!()
//     }

//     fn vertex_edges_window_t(
//         &self,
//         v: VertexRef,
//         t_start: i64,
//         t_end: i64,
//         d: Direction,
//         layer: Option<usize>,
//     ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
//         todo!()
//     }

//     fn neighbours(
//         &self,
//         v: VertexRef,
//         d: Direction,
//         layer: Option<usize>,
//     ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
//         todo!()
//     }

//     fn neighbours_window(
//         &self,
//         v: VertexRef,
//         t_start: i64,
//         t_end: i64,
//         d: Direction,
//         layer: Option<usize>,
//     ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
//         todo!()
//     }

//     fn neighbours_ids(
//         &self,
//         v: VertexRef,
//         d: Direction,
//         layer: Option<usize>,
//     ) -> Box<dyn Iterator<Item = u64> + Send> {
//         todo!()
//     }

//     fn neighbours_ids_window(
//         &self,
//         v: VertexRef,
//         t_start: i64,
//         t_end: i64,
//         d: Direction,
//         layer: Option<usize>,
//     ) -> Box<dyn Iterator<Item = u64> + Send> {
//         todo!()
//     }

//     fn static_vertex_prop(&self, v: VertexRef, name: String) -> Option<crate::core::Prop> {
//         todo!()
//     }

//     fn static_vertex_prop_names(&self, v: VertexRef) -> Vec<String> {
//         todo!()
//     }

//     fn temporal_vertex_prop_names(&self, v: VertexRef) -> Vec<String> {
//         todo!()
//     }

//     fn temporal_vertex_prop_vec(
//         &self,
//         v: VertexRef,
//         name: String,
//     ) -> Vec<(i64, crate::core::Prop)> {
//         todo!()
//     }

//     fn vertex_timestamps(&self, v: VertexRef) -> Vec<i64> {
//         todo!()
//     }

//     fn vertex_timestamps_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> Vec<i64> {
//         todo!()
//     }

//     fn temporal_vertex_prop_vec_window(
//         &self,
//         v: VertexRef,
//         name: String,
//         t_start: i64,
//         t_end: i64,
//     ) -> Vec<(i64, crate::core::Prop)> {
//         todo!()
//     }

//     fn temporal_vertex_props(
//         &self,
//         v: VertexRef,
//     ) -> std::collections::HashMap<String, Vec<(i64, crate::core::Prop)>> {
//         todo!()
//     }

//     fn temporal_vertex_props_window(
//         &self,
//         v: VertexRef,
//         t_start: i64,
//         t_end: i64,
//     ) -> std::collections::HashMap<String, Vec<(i64, crate::core::Prop)>> {
//         todo!()
//     }

//     fn static_edge_prop(&self, e: EdgeRef, name: String) -> Option<crate::core::Prop> {
//         todo!()
//     }

//     fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
//         todo!()
//     }

//     fn temporal_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
//         todo!()
//     }

//     fn temporal_edge_props_vec(&self, e: EdgeRef, name: String) -> Vec<(i64, crate::core::Prop)> {
//         todo!()
//     }

//     fn temporal_edge_props_vec_window(
//         &self,
//         e: EdgeRef,
//         name: String,
//         t_start: i64,
//         t_end: i64,
//     ) -> Vec<(i64, crate::core::Prop)> {
//         todo!()
//     }

//     fn edge_timestamps(&self, e: EdgeRef, window: Option<std::ops::Range<i64>>) -> Vec<i64> {
//         todo!()
//     }

//     fn temporal_edge_props(
//         &self,
//         e: EdgeRef,
//     ) -> std::collections::HashMap<String, Vec<(i64, crate::core::Prop)>> {
//         todo!()
//     }

//     fn temporal_edge_props_window(
//         &self,
//         e: EdgeRef,
//         t_start: i64,
//         t_end: i64,
//     ) -> std::collections::HashMap<String, Vec<(i64, crate::core::Prop)>> {
//         todo!()
//     }

//     fn num_shards(&self) -> usize {
//         todo!()
//     }

//     fn vertices_shard(&self, shard_id: usize) -> Box<dyn Iterator<Item = VertexRef> + Send> {
//         todo!()
//     }

//     fn vertices_shard_window(
//         &self,
//         shard_id: usize,
//         t_start: i64,
//         t_end: i64,
//     ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
//         todo!()
//     }
// }
