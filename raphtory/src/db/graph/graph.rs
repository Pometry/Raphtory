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
//! graph.add_node(0, "Alice", NO_PROPS, None).unwrap();
//! graph.add_node(1, "Bob", NO_PROPS, None).unwrap();
//! graph.add_edge(2, "Alice", "Bob", NO_PROPS, None).unwrap();
//! graph.count_edges();
//! ```
//!
use super::views::deletion_graph::PersistentGraph;
#[cfg(feature = "io")]
use crate::serialise::GraphPaths;
use crate::{
    db::{
        api::{
            storage::storage::Storage,
            view::{
                internal::{
                    InheritEdgeHistoryFilter, InheritNodeHistoryFilter, InheritStorageOps,
                    InheritViewOps, Static,
                },
                time::internal::InternalTimeOps,
            },
        },
        graph::{edges::Edges, node::NodeView, nodes::Nodes},
    },
    errors::GraphError,
    prelude::*,
};
use raphtory_api::inherit::Base;
use raphtory_storage::{
    core_ops::InheritCoreGraphOps, graph::graph::GraphStorage, layer_ops::InheritLayerOps,
    mutation::InheritMutationOps,
};
use rayon::prelude::*;
use std::{
    collections::HashSet,
    fmt::{Display, Formatter},
    hint::black_box,
    ops::Deref,
    sync::Arc,
};
use storage::{persist::strategy::PersistenceStrategy, Extension};

#[repr(transparent)]
#[derive(Debug, Clone, Default)]
pub struct Graph {
    pub(crate) inner: Arc<Storage>,
}

impl InheritCoreGraphOps for Graph {}
impl InheritLayerOps for Graph {}
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

impl Static for Graph {}

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

#[track_caller]
pub fn assert_node_equal<
    'graph,
    G1: GraphViewOps<'graph>,
    GH1: GraphViewOps<'graph>,
    G2: GraphViewOps<'graph>,
    GH2: GraphViewOps<'graph>,
>(
    n1: NodeView<'graph, G1, GH1>,
    n2: NodeView<'graph, G2, GH2>,
) {
    assert_node_equal_layer(n1, n2, "", false)
}

#[track_caller]
pub fn assert_node_equal_layer<
    'graph,
    G1: GraphViewOps<'graph>,
    GH1: GraphViewOps<'graph>,
    G2: GraphViewOps<'graph>,
    GH2: GraphViewOps<'graph>,
>(
    n1: NodeView<'graph, G1, GH1>,
    n2: NodeView<'graph, G2, GH2>,
    layer_tag: &str,
    persistent: bool,
) {
    assert_eq!(
        n1.id(),
        n2.id(),
        "mismatched node id{layer_tag}: left {:?}, right {:?}",
        n1.id(),
        n2.id()
    );
    assert_eq!(
        n1.name(),
        n2.name(),
        "mismatched node name{layer_tag}: left {:?}, right {:?}",
        n1.name(),
        n2.name()
    );
    assert_eq!(
        n1.node_type(),
        n2.node_type(),
        "mismatched node type{layer_tag}"
    );
    assert_eq!(
        n1.earliest_time(),
        n2.earliest_time(),
        "mismatched node earliest time for node {:?}{layer_tag}: left {:?}, right {:?}",
        n1.id(),
        n1.earliest_time(),
        n2.earliest_time()
    );
    // This doesn't hold for materialised windowed PersistentGraph (node is still present after the end of the window)
    assert_eq!(
        n1.latest_time(),
        n2.latest_time(),
        "mismatched node latest time for node {:?}{layer_tag}: left {:?}, right {:?}",
        n1.id(),
        n1.latest_time(),
        n2.latest_time()
    );
    assert_eq!(
        n1.metadata().as_map(),
        n2.metadata().as_map(),
        "mismatched metadata for node {:?}{layer_tag}: left {:?}, right {:?}",
        n1.id(),
        n1.metadata().as_map(),
        n2.metadata().as_map()
    );
    assert_eq!(
        n1.properties().temporal().as_map(),
        n2.properties().temporal().as_map(),
        "mismatched temporal properties for node {:?}{layer_tag}: left {:?}, right {:?}",
        n1.id(),
        n1.properties().temporal().as_map(),
        n2.properties().temporal().as_map()
    );
    assert_eq!(
        n1.out_degree(),
        n2.out_degree(),
        "mismatched out-degree for node {:?}{layer_tag}: left {}, right {}",
        n1.id(),
        n1.out_degree(),
        n2.out_degree(),
    );
    assert_eq!(
        n1.in_degree(),
        n2.in_degree(),
        "mismatched in-degree for node {:?}{layer_tag}: left {}, right {}",
        n1.id(),
        n1.in_degree(),
        n2.in_degree(),
    );
    assert_eq!(
        n1.degree(),
        n2.degree(),
        "mismatched degree for node {:?}{layer_tag}: left {}, right {}",
        n1.id(),
        n1.degree(),
        n2.degree(),
    );
    assert_eq!(
        n1.out_neighbours().id().collect::<HashSet<_>>(),
        n2.out_neighbours().id().collect::<HashSet<_>>(),
        "mismatched out-neighbours for node {:?}{layer_tag}: left {:?}, right {:?}",
        n1.id(),
        n1.out_neighbours().id().collect::<HashSet<_>>(),
        n2.out_neighbours().id().collect::<HashSet<_>>()
    );
    assert_eq!(
        n1.in_neighbours().id().collect::<HashSet<_>>(),
        n2.in_neighbours().id().collect::<HashSet<_>>(),
        "mismatched in-neighbours for node {:?}{layer_tag}: left {:?}, right {:?}",
        n1.id(),
        n1.in_neighbours().id().collect::<HashSet<_>>(),
        n2.in_neighbours().id().collect::<HashSet<_>>()
    );
    if persistent {
        let earliest = n1.timeline_start();
        match earliest {
            None => {
                assert!(
                    n2.timeline_end().is_none(),
                    "expected empty timeline for node {:?}{layer_tag}",
                    n1.id()
                );
            }
            Some(earliest) => {
                // persistent graph might have updates at start after materialize
                assert_eq!(
                    n1.after(earliest).edge_history_count(),
                    n2.after(earliest).edge_history_count(),
                    "mismatched edge_history_count for node {:?}{layer_tag}",
                    n1.id()
                );
                assert_eq!(
                    n1.after(earliest).history(),
                    n2.after(earliest).history(),
                    "mismatched history for node {:?}{layer_tag}",
                    n1.id()
                );
            }
        }
    } else {
        assert_eq!(
            n1.history(),
            n2.history(),
            "mismatched history for node {:?}{layer_tag}",
            n1.id()
        );
        assert_eq!(
            n1.edge_history_count(),
            n2.edge_history_count(),
            "mismatched edge_history_count for node {:?}{layer_tag}",
            n1.id()
        );
    }
}

#[track_caller]
pub fn assert_nodes_equal<
    'graph,
    G1: GraphViewOps<'graph>,
    GH1: GraphViewOps<'graph>,
    G2: GraphViewOps<'graph>,
    GH2: GraphViewOps<'graph>,
>(
    nodes1: &Nodes<'graph, G1, GH1>,
    nodes2: &Nodes<'graph, G2, GH2>,
) {
    assert_nodes_equal_layer(nodes1, nodes2, "", false);
}

#[track_caller]
pub fn assert_nodes_equal_layer<
    'graph,
    G1: GraphViewOps<'graph>,
    GH1: GraphViewOps<'graph>,
    G2: GraphViewOps<'graph>,
    GH2: GraphViewOps<'graph>,
>(
    nodes1: &Nodes<'graph, G1, GH1>,
    nodes2: &Nodes<'graph, G2, GH2>,
    layer_tag: &str,
    persistent: bool,
) {
    let mut nodes1: Vec<_> = nodes1.collect();
    nodes1.sort();
    let mut nodes2: Vec<_> = nodes2.collect();
    nodes2.sort();
    assert_eq!(
        nodes1.len(),
        nodes2.len(),
        "mismatched number of nodes{layer_tag}",
    );
    for (n1, n2) in nodes1.into_iter().zip(nodes2) {
        assert_node_equal_layer(n1, n2, layer_tag, persistent);
    }
}

#[track_caller]
pub fn assert_edges_equal<
    'graph1,
    'graph2,
    G1: GraphViewOps<'graph1>,
    GH1: GraphViewOps<'graph1>,
    G2: GraphViewOps<'graph2>,
    GH2: GraphViewOps<'graph2>,
>(
    edges1: &Edges<'graph1, G1, GH1>,
    edges2: &Edges<'graph2, G2, GH2>,
) {
    assert_edges_equal_layer(edges1, edges2, "", false);
}

#[track_caller]
pub fn assert_edges_equal_layer<
    'graph1,
    'graph2,
    G1: GraphViewOps<'graph1>,
    GH1: GraphViewOps<'graph1>,
    G2: GraphViewOps<'graph2>,
    GH2: GraphViewOps<'graph2>,
>(
    edges1: &Edges<'graph1, G1, GH1>,
    edges2: &Edges<'graph2, G2, GH2>,
    layer_tag: &str,
    persistent: bool,
) {
    let mut edges1: Vec<_> = edges1.collect();
    let mut edges2: Vec<_> = edges2.collect();
    assert_eq!(
        edges1.len(),
        edges2.len(),
        "mismatched number of edges{layer_tag}",
    );
    edges1.sort_by(|e1, e2| e1.id().cmp(&e2.id()));
    edges2.sort_by(|e1, e2| e1.id().cmp(&e2.id()));

    for (e1, e2) in edges1.into_iter().zip(edges2) {
        assert_eq!(e1.id(), e2.id(), "mismatched edge ids{layer_tag}");
        assert_eq!(
            e1.earliest_time(),
            e2.earliest_time(),
            "mismatched earliest time for edge {:?}{layer_tag}",
            e1.id(),
        );
        assert_eq!(
            e1.metadata().as_map(),
            e2.metadata().as_map(),
            "mismatched metadata for edge {:?}{layer_tag}",
            e1.id(),
        );
        assert_eq!(
            e1.properties().temporal().as_map(),
            e2.properties().temporal().as_map(),
            "mismatched temporal properties for edge {:?}{layer_tag}",
            e1.id(),
        );
        assert_eq!(
            e1.is_valid(),
            e2.is_valid(),
            "mismatched is_valid for edge {:?}{layer_tag}",
            e1.id()
        );
        if persistent {
            let earliest = e1.timeline_start();
            match earliest {
                None => {
                    assert!(
                        e2.timeline_start().is_none(),
                        "expected empty timeline for edge {:?}{layer_tag}",
                        e1.id()
                    )
                }
                Some(earliest) => {
                    assert_eq!(
                        e1.after(earliest).is_active(),
                        e2.after(earliest).is_active(),
                        "mismatched is_active for edge {:?}{layer_tag}",
                        e1.id()
                    );
                }
            }
        } else {
            assert_eq!(
                e1.is_active(),
                e2.is_active(),
                "mismatched is_active for edge {:?}{layer_tag}",
                e1.id()
            );
        }
        assert_eq!(
            e1.is_deleted(),
            e2.is_deleted(),
            "mismatched is_deleted for edge {:?}{layer_tag}",
            e1.id()
        );

        // FIXME: DiskGraph does not currently preserve secondary index

        let mut e1_updates: Vec<_> = e1
            .explode()
            .iter()
            .map(|e| (e.layer_name().unwrap(), e.time().unwrap()))
            .collect();
        e1_updates.sort();

        let mut e2_updates: Vec<_> = e2
            .explode()
            .iter()
            .map(|e| (e.layer_name().unwrap(), e.time().unwrap()))
            .collect();
        e2_updates.sort();
        assert_eq!(
            e1_updates,
            e2_updates,
            "mismatched updates for edge {:?}{layer_tag}",
            e1.id(),
        );
    }
}

#[track_caller]
fn assert_graph_equal_layer<'graph, G1: GraphViewOps<'graph>, G2: GraphViewOps<'graph>>(
    g1: &G1,
    g2: &G2,
    layer: Option<&str>,
    persistent: bool,
) {
    let layer_tag = match layer {
        None => "",
        Some(layer) => &format!(" for layer {layer}"),
    };
    assert_eq!(
        g1.count_nodes(),
        g2.count_nodes(),
        "mismatched number of nodes{layer_tag}",
    );
    assert_eq!(
        g1.count_edges(),
        g2.count_edges(),
        "mismatched number of edges{layer_tag}",
    );
    assert_eq!(
        g1.count_temporal_edges(),
        g2.count_temporal_edges(),
        "mismatched number of temporal edges{layer_tag}",
    );
    assert_eq!(
        g1.earliest_time(),
        g2.earliest_time(),
        "mismatched earliest time{layer_tag}",
    );
    assert_eq!(
        g1.latest_time(),
        g2.latest_time(),
        "mismatched latest time{layer_tag}",
    );
    assert_eq!(
        g1.metadata().as_map(),
        g2.metadata().as_map(),
        "mismatched graph metadata{layer_tag}",
    );
    assert_eq!(
        g1.properties().temporal().as_map(),
        g2.properties().temporal().as_map(),
        "mismatched graph temporal properties{layer_tag}",
    );
    assert_nodes_equal_layer(&g1.nodes(), &g2.nodes(), layer_tag, persistent);
    assert_edges_equal_layer(&g1.edges(), &g2.edges(), layer_tag, persistent);
}

#[track_caller]
fn assert_graph_equal_inner<'graph, G1: GraphViewOps<'graph>, G2: GraphViewOps<'graph>>(
    g1: &G1,
    g2: &G2,
    persistent: bool,
) {
    black_box({
        assert_graph_equal_layer(g1, g2, None, persistent);
        let left_layers: HashSet<_> = g1.unique_layers().collect();
        let right_layers: HashSet<_> = g2.unique_layers().collect();
        assert_eq!(
            left_layers, right_layers,
            "mismatched layers: left {:?}, right {:?}",
            left_layers, right_layers
        );

        for layer in left_layers {
            assert_graph_equal_layer(
                &g1.layers(layer.deref())
                    .unwrap_or_else(|_| panic!("Left graph missing layer {layer})")),
                &g2.layers(layer.deref())
                    .unwrap_or_else(|_| panic!("Right graph missing layer {layer}")),
                Some(&layer),
                persistent,
            );
        }
    })
}

#[track_caller]
pub fn assert_graph_equal<'graph, G1: GraphViewOps<'graph>, G2: GraphViewOps<'graph>>(
    g1: &G1,
    g2: &G2,
) {
    assert_graph_equal_inner(g1, g2, false)
}

/// Equality check for materialized persistent graph that ignores the updates generated by the materialise at graph.earliest_time()
#[track_caller]
pub fn assert_persistent_materialize_graph_equal<
    'graph,
    G1: GraphViewOps<'graph>,
    G2: GraphViewOps<'graph>,
>(
    g1: &G1,
    g2: &G2,
) {
    assert_graph_equal_inner(g1, g2, true)
}

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
        Self {
            inner: Arc::new(Storage::default()),
        }
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
        if !Extension::disk_storage_enabled() {
            return Err(GraphError::DiskGraphNotEnabled);
        }

        path.init()?;
        let graph_storage_path = path.graph_path()?;
        let storage = Storage::new_at_path(graph_storage_path)?;

        let graph = Self {
            inner: Arc::new(storage),
        };

        path.write_metadata(&graph)?;
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
    /// let g = Graph::load_from_path("/path/to/storage");
    ///
    #[cfg(feature = "io")]
    pub fn load_from_path(path: &(impl GraphPaths + ?Sized)) -> Result<Self, GraphError> {
        // TODO: add support for loading indexes and vectors
        Ok(Self {
            inner: Arc::new(Storage::load_from(path.graph_path()?)?),
        })
    }

    pub(crate) fn from_storage(inner: Arc<Storage>) -> Self {
        Self { inner }
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
