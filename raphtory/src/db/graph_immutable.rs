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

use crate::core::tgraph::TemporalGraph;
use crate::core::tgraph_shard::ImmutableTGraphShard;
use crate::core::Direction;
use crate::core::{
    tgraph::{EdgeRef, VertexRef},
    utils,
};
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
        &self.shards[self.shard_id(v.g_id)]
    }

    /// Get an immutable graph shard for a given edge.
    ///
    pub fn get_shard_from_e(&self, e: EdgeRef) -> &ImmutableTGraphShard<TemporalGraph> {
        &self.shards[self.shard_id(e.src_g_id)]
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
    pub fn degree(&self, v: VertexRef, d: Direction) -> usize {
        self.get_shard_from_v(v).degree(v, d, None)
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
    pub fn vertices(&self) -> Box<dyn Iterator<Item = VertexRef> + Send + '_> {
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
    pub fn edges(&self) -> Box<dyn Iterator<Item = (usize, EdgeRef)> + Send + '_> {
        Box::new(
            self.vertices()
                .flat_map(|v| self.get_shard_from_v(v).edges(v.g_id, Direction::OUT, None)),
        )
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
