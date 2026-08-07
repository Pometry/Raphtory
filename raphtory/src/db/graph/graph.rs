//! Defines the `Graph` struct, which represents a raphtory graph in memory.
//!
//! This is the base class used to create a temporal graph, add nodes and edges,
//! create windows, and query the graph with a variety of algorithms.
//! It is a wrapper around a set of shards, which are the actual graph data structures.
//!
//! # Examples
//!
//! ```rust
//! use raphtory::prelude::*;
//! let graph = Graph::new();
//! graph.add_node(0, "Alice", NO_PROPS, None, None).unwrap();
//! graph.add_node(1, "Bob", NO_PROPS, None, None).unwrap();
//! graph.add_edge(2, "Alice", "Bob", NO_PROPS, None).unwrap();
//! graph.count_edges();
//! ```
//!
use super::views::deletion_graph::PersistentGraph;
#[cfg(feature = "io")]
use crate::serialise::metadata::build_graph_metadata;
use crate::{
    db::api::{
        storage::storage::{PersistenceStrategy, Storage},
        view::internal::{
            InheritEdgeHistoryFilter, InheritNodeHistoryFilter, InheritStorageOps, InheritViewOps,
            Static,
        },
    },
    errors::GraphError,
    prelude::*,
};
#[cfg(feature = "io")]
use raphtory_api::core::storage::graph_folder::{GraphPaths, Metadata as GraphFolderMetadata};
use raphtory_api::inherit::Base;
use raphtory_storage::{
    core_ops::InheritCoreGraphOps, graph::graph::GraphStorage, layer_ops::InheritLayerOps,
    mutation::InheritMutationOps,
};
use rayon::prelude::*;
use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};
use storage::{Args, Extension};

#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct Graph {
    pub(crate) inner: Arc<Storage>,
}

impl From<Arc<Storage>> for Graph {
    fn from(inner: Arc<Storage>) -> Self {
        Self { inner }
    }
}

impl From<GraphStorage> for Graph {
    fn from(inner: GraphStorage) -> Self {
        Self {
            inner: Arc::new(Storage::from_inner(inner)),
        }
    }
}

impl Base for Graph {
    type Base = Storage;

    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.inner
    }
}

impl InheritMutationOps for Graph {}

impl InheritViewOps for Graph {}

impl InheritStorageOps for Graph {}

impl InheritNodeHistoryFilter for Graph {}

impl InheritEdgeHistoryFilter for Graph {}

impl InheritCoreGraphOps for Graph {}

impl InheritLayerOps for Graph {}

impl Static for Graph {}

impl Display for Graph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl<'graph, G: GraphViewOps<'graph>> PartialEq<G> for Graph
where
    Self: 'graph,
{
    fn eq(&self, other: &G) -> bool {
        graph_equal(self, other)
    }
}

impl Graph {
    /// Create a new graph
    ///
    /// Returns:
    ///
    /// A raphtory graph
    ///
    /// # Example
    ///
    /// ```
    /// use raphtory::prelude::Graph;
    /// let g = Graph::new();
    /// ```
    pub fn new() -> Self {
        // TODO: This should return a Result.
        Self::new_with_config(Args::default()).unwrap()
    }

    /// Create a new graph with config
    ///
    /// Returns:
    ///
    /// A raphtory graph
    ///
    /// # Example
    ///
    /// ```
    /// use raphtory::prelude::*;
    ///
    /// let g = Graph::new_with_config(Args::default().with_max_node_page_len(262144)).unwrap();
    /// ```
    pub fn new_with_config(args: Args) -> Result<Self, GraphError> {
        Ok(Self {
            inner: Arc::new(Storage::new_with_config(args)?),
        })
    }

    /// Create a new graph at a specific path
    ///
    /// # Arguments
    /// * `path` - The path to the storage location
    /// # Returns
    /// A raphtory graph with storage at the specified path
    /// # Example
    /// ```no_run
    /// use raphtory::prelude::Graph;
    /// let g = Graph::new_at_path("/path/to/storage");
    /// ```
    #[cfg(feature = "io")]
    pub fn new_at_path(path: &(impl GraphPaths + ?Sized)) -> Result<Self, GraphError> {
        Self::new_at_path_with_config(path, Args::default())
    }

    #[cfg(feature = "io")]
    pub fn new_at_path_with_config(
        path: &(impl GraphPaths + ?Sized),
        args: Args,
    ) -> Result<Self, GraphError> {
        if !Extension::disk_storage_enabled() {
            return Err(GraphError::DiskGraphNotEnabled);
        }

        path.init()?;

        let graph_path = path.graph_path()?;
        let graph = Self {
            inner: Arc::new(Storage::new_at_path_with_config(graph_path, args)?),
        };

        let meta = GraphFolderMetadata {
            path: path.relative_graph_path()?,
            meta: build_graph_metadata(&graph),
        };

        path.write_metadata(meta)?;
        Ok(graph)
    }

    /// Load a graph from a specific path
    /// # Arguments
    /// * `path` - The path to the storage location
    /// # Returns
    /// A raphtory graph loaded from the specified path
    /// # Example
    /// ```no_run
    /// use raphtory::prelude::Graph;
    /// let g = Graph::load("/path/to/storage");
    #[cfg(feature = "io")]
    pub fn load(path: &(impl GraphPaths + ?Sized)) -> Result<Self, GraphError> {
        // TODO: add support for loading indexes and vectors
        Ok(Self {
            inner: Arc::new(Storage::load(path.graph_path()?)?),
        })
    }

    /// Load a graph from a specific path, overriding config
    /// # Arguments
    /// * `path` - The path to the storage location
    /// * `config` - The new config (page sizes cannot be changed; providing them returns an error)
    /// # Returns
    /// A raphtory graph loaded from the specified path
    /// # Example
    /// ```no_run
    /// use raphtory::prelude::Graph;
    /// let g = Graph::load("/path/to/storage");
    #[cfg(feature = "io")]
    pub fn load_with_config(
        path: &(impl GraphPaths + ?Sized),
        args: Args,
    ) -> Result<Self, GraphError> {
        // TODO: add support for loading indexes and vectors
        Ok(Self {
            inner: Arc::new(Storage::load_with_config(path.graph_path()?, args)?),
        })
    }

    /// Load the graph as a read-only snapshot. Multiple processes can hold
    /// a read-only handle to the same graph directory concurrently. Mutating
    /// operations on the returned graph will fail.
    #[cfg(feature = "io")]
    pub fn load_read_only(path: &(impl GraphPaths + ?Sized)) -> Result<Self, GraphError> {
        Ok(Self {
            inner: Arc::new(Storage::load_read_only(path.graph_path()?)?),
        })
    }

    #[cfg(feature = "io")]
    pub fn load_read_only_with_config(
        path: &(impl GraphPaths + ?Sized),
        args: Args,
    ) -> Result<Self, GraphError> {
        Ok(Self {
            inner: Arc::new(Storage::load_read_only_with_config(
                path.graph_path()?,
                args,
            )?),
        })
    }

    pub(crate) fn from_storage(inner: Arc<Storage>) -> Self {
        Self { inner }
    }

    /// Return a read-only handle to this graph. Mutations on the returned
    /// graph fail with `Immutable::ReadLockedImmutable`. The underlying
    /// `TemporalGraph` is shared — this is not a snapshot.
    ///
    /// **Warning**: while a read-only handle is live, writes through the
    /// original `Graph` will block on the per-segment read locks the
    /// handle holds. Drop the read-only handle before mutating the
    /// original.
    pub fn read_only(&self) -> Self {
        Self {
            inner: Arc::new(self.inner.read_only()),
        }
    }

    pub(crate) fn from_internal_graph(graph_storage: GraphStorage) -> Self {
        let inner = Arc::new(Storage::from_inner(graph_storage));
        Self { inner }
    }

    pub fn event_graph(&self) -> Graph {
        self.clone()
    }

    /// Get persistent graph
    pub fn persistent_graph(&self) -> PersistentGraph {
        PersistentGraph::from_storage(self.inner.clone())
    }
}

pub fn graph_equal<'graph1, 'graph2, G1: GraphViewOps<'graph1>, G2: GraphViewOps<'graph2>>(
    g1: &G1,
    g2: &G2,
) -> bool {
    if g1.count_nodes() == g2.count_nodes() && g1.count_edges() == g2.count_edges() {
        g1.nodes().id().par_iter_values().all(|v| g2.has_node(v)) && // all nodes exist in other
            g1.count_temporal_edges() == g2.count_temporal_edges() && // same number of exploded edges
            g1.edges().explode().iter().all(|e| { // all exploded edges exist in other
                g2
                    .edge(e.src().id(), e.dst().id())
                    .filter(|ee| ee.at(e.time().expect("exploded")).is_valid())
                    .is_some()
            })
    } else {
        false
    }
}
