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
        n1.properties().constant().as_map(),
        n2.properties().constant().as_map(),
        "mismatched constant properties for node {:?}{layer_tag}: left {:?}, right {:?}",
        n1.id(),
        n1.properties().constant().as_map(),
        n2.properties().constant().as_map()
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
    }
}

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
            e1.properties().constant().as_map(),
            e2.properties().constant().as_map(),
            "mismatched constant properties for edge {:?}{layer_tag}",
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
        g1.properties().constant().as_map(),
        g2.properties().constant().as_map(),
        "mismatched graph constant properties{layer_tag}",
    );
    assert_eq!(
        g1.properties().temporal().as_map(),
        g2.properties().temporal().as_map(),
        "mismatched graph temporal properties{layer_tag}",
    );
    assert_nodes_equal_layer(&g1.nodes(), &g2.nodes(), layer_tag, persistent);
    assert_edges_equal_layer(&g1.edges(), &g2.edges(), layer_tag, persistent);
}

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

pub fn assert_graph_equal<'graph, G1: GraphViewOps<'graph>, G2: GraphViewOps<'graph>>(
    g1: &G1,
    g2: &G2,
) {
    assert_graph_equal_inner(g1, g2, false)
}

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

    /// Create a new graph with specified number of shards
    ///
    /// Returns:
    ///
    /// A raphtory graph
    pub fn new_with_shards(num_shards: usize) -> Self {
        Self {
            inner: Arc::new(Storage::new(num_shards)),
        }
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

#[cfg(test)]
mod db_tests {
    use super::*;
    #[cfg(feature = "proto")]
    use crate::serialise::StableDecode;
    use crate::{
        algorithms::components::weakly_connected_components,
        db::{
            api::{
                properties::internal::ConstantPropertiesOps,
                view::{
                    internal::{EdgeFilterOps, GraphTimeSemanticsOps},
                    time::internal::InternalTimeOps,
                    EdgeViewOps, LayerOps, NodeViewOps, TimeOps,
                },
            },
            graph::{edge::EdgeView, edges::Edges, path::PathFromNode},
        },
        errors::GraphError,
        graphgen::random_attachment::random_attachment,
        prelude::{AdditionOps, PropertyAdditionOps},
        test_storage,
        test_utils::{
            build_graph, build_graph_strat, test_graph, EdgeFixture, EdgeUpdatesFixture,
            GraphFixture, NodeFixture, PropUpdatesFixture,
        },
    };
    use bigdecimal::BigDecimal;
    use chrono::NaiveDateTime;
    use itertools::Itertools;
    use proptest::{arbitrary::any, proptest};
    use quickcheck_macros::quickcheck;
    use raphtory_api::core::{
        entities::{GID, VID},
        storage::{
            arc_str::{ArcStr, OptionAsStr},
            timeindex::TimeIndexEntry,
        },
        utils::logging::global_info_logger,
    };
    use raphtory_core::utils::time::{ParseTimeError, TryIntoTime};
    use raphtory_storage::{
        core_ops::CoreGraphOps, graph::nodes::node_storage_ops::NodeStorageOps,
        mutation::addition_ops::InternalAdditionOps,
    };
    use rayon::{join, vec};
    use std::{
        collections::{HashMap, HashSet},
        ops::Range,
    };
    #[cfg(feature = "proto")]
    use tempfile::TempDir;
    use tracing::{error, info};

    #[test]
    fn edge_const_props() -> Result<(), GraphError> {
        let g = Graph::new();

        g.add_edge(0, 0, 0, NO_PROPS, None)?;
        g.add_edge(0, 0, 1, NO_PROPS, None)?;

        g.edge(0, 0).unwrap().update_constant_properties(
            vec![("x".to_string(), Prop::map([("n", Prop::U64(23))]))],
            None,
        )?;
        g.edge(0, 1).unwrap().update_constant_properties(
            vec![(
                "a".to_string(),
                Prop::map([("a", Prop::U8(1)), ("b", Prop::str("baa"))]),
            )],
            None,
        )?;

        let e1 = g.edge(0, 0).unwrap();
        let actual = e1
            .properties()
            .constant()
            .iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect::<Vec<_>>();
        assert!(actual.contains(&("x".to_string(), Prop::map([("n", Prop::U64(23))]))));

        let e2 = g.edge(0, 1).unwrap();
        let actual = e2
            .properties()
            .constant()
            .iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![(
                "a".to_string(),
                Prop::map([("b", Prop::str("baa")), ("a", Prop::U8(1))])
            )]
        );
        Ok(())
    }

    #[test]
    fn test_empty_graph() {
        let graph = Graph::new();
        test_storage!(&graph, |graph| {
            assert!(!graph.has_edge(1, 2));

            let test_time = 42;
            let result = graph.at(test_time);
            assert!(result.start.is_some());
            assert!(result.end.is_some());

            let result = graph.after(test_time);
            assert!(result.start.is_some());
            assert!(result.end.is_none());

            let result = graph.before(test_time);
            assert!(result.start.is_none());
            assert!(result.end.is_some());

            assert_eq!(
                graph.const_prop_keys().collect::<Vec<_>>(),
                Vec::<ArcStr>::new()
            );
            assert_eq!(
                graph.const_prop_ids().collect::<Vec<_>>(),
                Vec::<usize>::new()
            );
            assert_eq!(
                graph.const_prop_values().collect::<Vec<_>>(),
                Vec::<Option<Prop>>::new()
            );
            assert!(graph.constant_prop(1).is_none());
            assert!(graph.get_const_prop_id("1").is_none());
            assert!(graph.get_const_prop(1).is_none());
            assert_eq!(graph.count_nodes(), 0);
            assert_eq!(graph.count_edges(), 0);
            assert_eq!(graph.count_temporal_edges(), 0);

            assert!(graph.start().is_none());
            assert!(graph.end().is_none());
            assert!(graph.earliest_date_time().is_none());
            assert_eq!(graph.earliest_time(), None);
            assert!(graph.end_date_time().is_none());
            assert!(graph.timeline_end().is_none());

            assert!(graph.is_empty());

            assert!(graph.nodes().collect().is_empty());
            assert_eq!(
                graph.edges().collect(),
                Vec::<EdgeView<Graph, Graph>>::new()
            );
            assert!(!graph.edges_filtered());
            assert!(graph.edge(1, 2).is_none());
            assert!(graph.latest_time_global().is_none());
            assert!(graph.latest_time_window(1, 2).is_none());
            assert!(graph.latest_time().is_none());
            assert!(graph.latest_date_time().is_none());
            assert!(graph.latest_time_global().is_none());
            assert!(graph.earliest_time_global().is_none());
        });
    }

    #[quickcheck]
    fn test_multithreaded_add_edge(edges: Vec<(u64, u64)>) -> bool {
        let g = Graph::new();
        edges.par_iter().enumerate().for_each(|(t, (i, j))| {
            g.add_edge(t as i64, *i, *j, NO_PROPS, None).unwrap();
        });
        edges.iter().all(|(i, j)| g.has_edge(*i, *j)) && g.count_temporal_edges() == edges.len()
    }

    #[quickcheck]
    fn add_node_grows_graph_len(vs: Vec<(i64, u64)>) {
        let g = Graph::new();

        let expected_len = vs.iter().map(|(_, v)| v).sorted().dedup().count();
        for (t, v) in vs {
            g.add_node(t, v, NO_PROPS, None)
                .map_err(|err| error!("{:?}", err))
                .ok();
        }

        assert_eq!(g.count_nodes(), expected_len)
    }

    #[quickcheck]
    fn add_node_gets_names(vs: Vec<String>) -> bool {
        global_info_logger();
        let g = Graph::new();

        let expected_len = vs.iter().sorted().dedup().count();
        for (t, name) in vs.iter().enumerate() {
            g.add_node(t as i64, name.clone(), NO_PROPS, None)
                .map_err(|err| info!("{:?}", err))
                .ok();
        }

        assert_eq!(g.count_nodes(), expected_len);

        vs.iter().all(|name| {
            let v = g.node(name.clone()).unwrap();
            v.name() == name.clone()
        })
    }

    #[quickcheck]
    fn add_edge_grows_graph_edge_len(edges: Vec<(i64, u64, u64)>) {
        let g = Graph::new();

        let unique_nodes_count = edges
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
            g.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }

        assert_eq!(g.count_nodes(), unique_nodes_count);
        assert_eq!(g.count_edges(), unique_edge_count);
    }

    #[test]
    fn simle_add_edge() {
        let edges = vec![(1, 1, 2), (2, 2, 3), (3, 3, 4)];

        let g = Graph::new();
        for &(t, src, dst) in edges.iter() {
            g.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }

        assert!(edges.iter().all(|&(_, src, dst)| g.has_edge(src, dst)))
    }

    #[quickcheck]
    fn add_edge_works(edges: Vec<(i64, u64, u64)>) -> bool {
        let g = Graph::new();
        for &(t, src, dst) in edges.iter() {
            g.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }

        edges.iter().all(|&(_, src, dst)| g.has_edge(src, dst))
    }

    #[quickcheck]
    fn get_edge_works(edges: Vec<(i64, u64, u64)>) -> bool {
        let g = Graph::new();
        for &(t, src, dst) in edges.iter() {
            g.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }

        edges
            .iter()
            .all(|&(_, src, dst)| g.edge(src, dst).is_some())
    }

    #[test]
    fn import_from_another_graph() {
        let g = Graph::new();
        let g_a = g.add_node(0, "A", NO_PROPS, None).unwrap();
        let g_b = g
            .add_node(1, "B", vec![("temp".to_string(), Prop::Bool(true))], None)
            .unwrap();

        assert_eq!(g_b.history(), vec![1]);
        let _ = g_b.add_constant_properties(vec![("con".to_string(), Prop::I64(11))]);
        let gg = Graph::new();
        let res = gg.import_node(&g_a, false).unwrap();
        assert_eq!(res.name(), "A");
        assert_eq!(res.history(), vec![0]);
        let res = gg.import_node(&g_b, false).unwrap();
        assert_eq!(res.name(), "B");
        assert_eq!(res.history(), vec![1]);
        assert_eq!(res.properties().get("temp").unwrap(), Prop::Bool(true));
        assert_eq!(
            res.properties().constant().get("con").unwrap(),
            Prop::I64(11)
        );

        let gg = Graph::new();
        gg.add_node(1, "B", NO_PROPS, None).unwrap();
        let res = gg.import_nodes(vec![&g_a, &g_b], false);
        match res {
            Err(GraphError::NodesExistError(ids)) => {
                assert_eq!(
                    ids.into_iter()
                        .map(|id| id.to_string())
                        .collect::<Vec<String>>(),
                    vec!["B"],
                );
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
            Ok(_) => panic!("Expected error but got Ok"),
        }

        assert_eq!(gg.node("A"), None);

        let gg = Graph::new();
        gg.import_nodes(vec![&g_a, &g_b], false).unwrap();
        assert_eq!(gg.nodes().name().collect_vec(), vec!["A", "B"]);

        let e_a_b = g.add_edge(2, "A", "B", NO_PROPS, None).unwrap();
        let res = gg.import_edge(&e_a_b, false).unwrap();
        assert_eq!(
            (res.src().name(), res.dst().name()),
            (e_a_b.src().name(), e_a_b.dst().name())
        );
        let e_a_b_p = g
            .add_edge(
                3,
                "A",
                "B",
                vec![("etemp".to_string(), Prop::Bool(false))],
                None,
            )
            .unwrap();
        let gg = Graph::new();
        let _ = gg.add_node(0, "B", NO_PROPS, None);
        let res = gg.import_edge(&e_a_b_p, false).expect("Failed to add edge");
        assert_eq!(res.properties().as_vec(), e_a_b_p.properties().as_vec());

        let e_c_d = g.add_edge(4, "C", "D", NO_PROPS, None).unwrap();

        let gg = Graph::new();
        gg.import_edges(vec![&e_a_b, &e_c_d], false).unwrap();
        assert_eq!(gg.edges().len(), 2);

        let gg = Graph::new();
        gg.add_edge(1, "C", "D", NO_PROPS, None).unwrap();
        let res = gg.import_edges(vec![&e_a_b, &e_c_d], false);
        match res {
            Err(GraphError::EdgesExistError(duplicates)) => {
                assert_eq!(
                    duplicates
                        .into_iter()
                        .map(|(x, y)| (x.to_string(), y.to_string()))
                        .collect::<Vec<(String, String)>>(),
                    vec![("C".to_string(), "D".to_string())]
                );
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
            Ok(_) => panic!("Expected error but got Ok"),
        }
        assert_eq!(gg.edge("A", "B"), None);
    }

    #[test]
    fn import_node_as() {
        let g = Graph::new();
        let g_a = g.add_node(0, "A", NO_PROPS, None).unwrap();
        let g_b = g
            .add_node(1, "B", vec![("temp".to_string(), Prop::Bool(true))], None)
            .unwrap();
        let _ = g_b.add_constant_properties(vec![("con".to_string(), Prop::I64(11))]);

        let gg = Graph::new();
        let res = gg.import_node_as(&g_a, "X", false).unwrap();
        assert_eq!(res.name(), "X");
        assert_eq!(res.history(), vec![0]);

        let _ = gg.add_node(1, "Y", NO_PROPS, None).unwrap();
        let res = gg.import_node_as(&g_b, "Y", false);
        match res {
            Err(GraphError::NodeExistsError(id)) => {
                assert_eq!(id.to_string(), "Y");
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
            Ok(_) => panic!("Expected error but got Ok"),
        }

        let mut nodes = gg.nodes().name().collect_vec();
        nodes.sort();
        assert_eq!(nodes, vec!["X", "Y"]); // Nodes up until first failure are imported
        let y = gg.node("Y").unwrap();

        assert_eq!(y.name(), "Y");
        assert_eq!(y.history(), vec![1]);
        assert_eq!(y.properties().get("temp"), None);
        assert_eq!(y.properties().constant().get("con"), None);
    }

    #[test]
    fn import_node_as_merge() {
        let g = Graph::new();
        let g_a = g.add_node(0, "A", NO_PROPS, None).unwrap();
        let g_b = g
            .add_node(1, "B", vec![("temp".to_string(), Prop::Bool(true))], None)
            .unwrap();
        let _ = g_b.add_constant_properties(vec![("con".to_string(), Prop::I64(11))]);

        let gg = Graph::new();
        gg.add_node(1, "Y", NO_PROPS, None).unwrap();

        let res = gg.import_node_as(&g_a, "X", false).unwrap();
        assert_eq!(res.name(), "X");
        assert_eq!(res.history(), vec![0]);

        let res = gg.import_node_as(&g_b, "Y", true).unwrap();
        assert_eq!(res.name(), "Y");
        assert_eq!(res.history(), vec![1]);
        assert_eq!(res.properties().get("temp").unwrap(), Prop::Bool(true));
        assert_eq!(
            res.properties().constant().get("con").unwrap(),
            Prop::I64(11)
        );
    }

    #[test]
    fn import_nodes_as() {
        let g = Graph::new();
        let g_a = g.add_node(0, "A", NO_PROPS, None).unwrap();
        let g_b = g
            .add_node(1, "B", vec![("temp".to_string(), Prop::Bool(true))], None)
            .unwrap();
        let _ = g_b.add_constant_properties(vec![("con".to_string(), Prop::I64(11))]);
        let g_c = g.add_node(0, "C", NO_PROPS, None).unwrap();

        let gg = Graph::new();
        gg.add_node(1, "Q", NO_PROPS, None).unwrap();
        gg.add_node(1, "R", NO_PROPS, None).unwrap();
        let res = gg.import_nodes_as(vec![&g_a, &g_b, &g_c], vec!["P", "Q", "R"], false);
        match res {
            Err(GraphError::NodesExistError(ids)) => {
                assert_eq!(
                    ids.into_iter()
                        .map(|id| id.to_string())
                        .collect::<Vec<String>>(),
                    vec!["Q", "R"],
                );
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
            Ok(_) => panic!("Expected error but got Ok"),
        }
        let mut nodes = gg.nodes().name().collect_vec();
        nodes.sort();
        assert_eq!(nodes, vec!["Q", "R"]); // Nodes up until first failure are imported
        let y = gg.node("Q").unwrap();
        assert_eq!(y.name(), "Q");
        assert_eq!(y.history(), vec![1]);
        assert_eq!(y.properties().get("temp"), None);
        assert_eq!(y.properties().constant().get("con"), None);
    }

    #[test]
    fn import_nodes_as_merge() {
        let g = Graph::new();
        let g_a = g.add_node(0, "A", NO_PROPS, None).unwrap();
        let g_b = g
            .add_node(1, "B", vec![("temp".to_string(), Prop::Bool(true))], None)
            .unwrap();
        let _ = g_b.add_constant_properties(vec![("con".to_string(), Prop::I64(11))]);

        let gg = Graph::new();
        gg.add_node(1, "Q", NO_PROPS, None).unwrap();
        gg.import_nodes_as(vec![&g_a, &g_b], vec!["P", "Q"], true)
            .unwrap();
        let mut nodes = gg.nodes().name().collect_vec();
        nodes.sort();
        assert_eq!(nodes, vec!["P", "Q"]);
        let y = gg.node("Q").unwrap();
        assert_eq!(y.name(), "Q");
        assert_eq!(y.history(), vec![1]);
        assert_eq!(y.properties().get("temp").unwrap(), Prop::Bool(true));
        assert_eq!(y.properties().constant().get("con").unwrap(), Prop::I64(11));
    }

    #[test]
    fn import_edge_as() {
        let g = Graph::new();
        g.add_node(0, "A", NO_PROPS, None).unwrap();
        let g_b = g
            .add_node(1, "B", vec![("temp".to_string(), Prop::Bool(true))], None)
            .unwrap();
        g_b.add_constant_properties(vec![("con".to_string(), Prop::I64(11))])
            .unwrap();
        let e_a_b = g
            .add_edge(
                2,
                "A",
                "B",
                vec![("e_temp".to_string(), Prop::Bool(false))],
                None,
            )
            .unwrap();
        let e_b_c = g
            .add_edge(
                2,
                "B",
                "C",
                vec![("e_temp".to_string(), Prop::Bool(false))],
                None,
            )
            .unwrap();

        let gg = Graph::new();
        gg.add_edge(1, "X", "Y", NO_PROPS, None).unwrap();
        gg.import_edge_as(&e_b_c, ("Y", "Z"), false).unwrap();
        let res = gg.import_edge_as(&e_a_b, ("X", "Y"), false);
        match res {
            Err(GraphError::EdgeExistsError(src_id, dst_id)) => {
                assert_eq!(src_id.to_string(), "X");
                assert_eq!(dst_id.to_string(), "Y");
            }
            Err(e) => panic!("Unexpected error: {e:?}"),
            Ok(_) => panic!("Expected error but got Ok"),
        }
        let mut nodes = gg.nodes().name().collect_vec();
        nodes.sort();
        assert_eq!(nodes, vec!["X", "Y", "Z"]);
        let x = gg.node("X").unwrap();
        assert_eq!(x.name(), "X");
        assert_eq!(x.history(), vec![1]);
        let y = gg.node("Y").unwrap();
        assert_eq!(y.name(), "Y");
        assert_eq!(y.history(), vec![1, 2]);
        assert_eq!(y.properties().get("temp"), None);
        assert_eq!(y.properties().constant().get("con"), None);

        let e_src = gg.edge("X", "Y").unwrap().src().name();
        let e_dst = gg.edge("X", "Y").unwrap().dst().name();
        assert_eq!(e_src, "X");
        assert_eq!(e_dst, "Y");

        let props = gg.edge("X", "Y").unwrap().properties().as_vec();
        assert_eq!(props, vec![]);
    }

    #[test]
    fn import_edge_as_merge() {
        let g = Graph::new();
        g.add_node(0, "A", NO_PROPS, None).unwrap();
        let g_b = g
            .add_node(1, "B", vec![("temp".to_string(), Prop::Bool(true))], None)
            .unwrap();
        let _ = g_b.add_constant_properties(vec![("con".to_string(), Prop::I64(11))]);
        let e_a_b = g
            .add_edge(
                2,
                "A",
                "B",
                vec![("e_temp".to_string(), Prop::Bool(false))],
                None,
            )
            .unwrap();

        let gg = Graph::new();
        let _ = gg.add_edge(3, "X", "Y", NO_PROPS, None).unwrap();
        let res = gg.import_edge_as(&e_a_b, ("X", "Y"), true).unwrap();
        assert_eq!(res.src().name(), "X");
        assert_eq!(res.dst().name(), "Y");
        assert_eq!(res.properties().as_vec(), e_a_b.properties().as_vec());
        let mut nodes = gg.nodes().name().collect_vec();
        nodes.sort();
        assert_eq!(nodes, vec!["X", "Y"]);
        let x = gg.node("X").unwrap();
        assert_eq!(x.name(), "X");
        assert_eq!(x.history(), vec![2, 3]);
        let y = gg.node("Y").unwrap();
        assert_eq!(y.name(), "Y");
        assert_eq!(y.history(), vec![2, 3]);
        assert_eq!(y.properties().get("temp"), None);
        assert_eq!(y.properties().constant().get("con"), None);
    }

    #[test]
    fn import_edges_as() {
        let g = Graph::new();
        g.add_node(0, "A", NO_PROPS, None).unwrap();
        let g_b = g
            .add_node(1, "B", vec![("temp".to_string(), Prop::Bool(true))], None)
            .unwrap();
        g_b.add_constant_properties(vec![("con".to_string(), Prop::I64(11))])
            .unwrap();
        g.add_node(0, "C", NO_PROPS, None).unwrap();
        let e_a_b = g
            .add_edge(
                2,
                "A",
                "B",
                vec![("e_temp".to_string(), Prop::Bool(false))],
                None,
            )
            .unwrap();
        let e_b_c = g.add_edge(2, "B", "C", NO_PROPS, None).unwrap();

        let gg = Graph::new();
        gg.add_edge(1, "Y", "Z", NO_PROPS, None).unwrap();
        let res = gg.import_edges_as([&e_a_b, &e_b_c], [("X", "Y"), ("Y", "Z")], false);
        match res {
            Err(GraphError::EdgesExistError(duplicates)) => {
                assert_eq!(
                    duplicates
                        .into_iter()
                        .map(|(x, y)| (x.to_string(), y.to_string()))
                        .collect::<Vec<(String, String)>>(),
                    vec![("Y".to_string(), "Z".to_string())]
                );
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
            Ok(_) => panic!("Expected error but got Ok"),
        }
        let mut nodes = gg.nodes().name().collect_vec();
        nodes.sort();
        assert_eq!(nodes, vec!["Y", "Z"]);
        let y = gg.node("Y").unwrap();
        assert_eq!(y.name(), "Y");
        assert_eq!(y.history(), vec![1]);
        assert_eq!(y.properties().get("temp"), None);
        assert_eq!(y.properties().constant().get("con"), None);
        let x = gg.node("Z").unwrap();
        assert_eq!(x.name(), "Z");
        assert_eq!(x.history(), vec![1]);

        assert!(gg.edge("X", "Y").is_none());

        let e_y_z = gg.edge("Y", "Z").unwrap();
        assert_eq!(
            (e_y_z.src().name().as_str(), e_y_z.dst().name().as_str()),
            ("Y", "Z")
        );

        let props = e_y_z.properties().as_vec();
        assert_eq!(props, vec![]);
    }

    #[test]
    fn import_edges_as_merge() {
        let g = Graph::new();
        g.add_node(0, "A", NO_PROPS, None).unwrap();
        let g_b = g
            .add_node(1, "B", vec![("temp".to_string(), Prop::Bool(true))], None)
            .unwrap();
        let _ = g_b.add_constant_properties(vec![("con".to_string(), Prop::I64(11))]);
        let e_a_b = g
            .add_edge(
                2,
                "A",
                "B",
                vec![("e_temp".to_string(), Prop::Bool(false))],
                None,
            )
            .unwrap();

        let gg = Graph::new();
        gg.add_edge(3, "X", "Y", NO_PROPS, None).unwrap();
        gg.import_edges_as([&e_a_b], [("X", "Y")], true).unwrap();

        let e_x_y = gg.edge("X", "Y").unwrap();
        assert_eq!(
            (e_x_y.src().name().as_str(), e_x_y.dst().name().as_str()),
            ("X", "Y")
        );
        assert_eq!(e_x_y.properties().get("e_temp").unwrap(), Prop::Bool(false));

        let mut nodes = gg.nodes().name().collect_vec();
        nodes.sort();
        assert_eq!(nodes, vec!["X", "Y"]);
        let x = gg.node("X").unwrap();
        assert_eq!(x.name(), "X");
        assert_eq!(x.history(), vec![2, 3]);
        let y = gg.node("Y").unwrap();
        assert_eq!(y.name(), "Y");
        assert_eq!(y.history(), vec![2, 3]);
        assert_eq!(y.properties().get("temp"), None);
        assert_eq!(y.properties().constant().get("con"), None);
    }

    #[test]
    fn props_with_layers() {
        global_info_logger();
        let g = Graph::new();
        g.add_edge(0, "A", "B", NO_PROPS, None).unwrap();
        let ed = g.edge("A", "B").unwrap();
        ed.add_constant_properties(vec![("CCC", Prop::str("RED"))], None)
            .unwrap();
        info!("{:?}", ed.properties().constant().as_map());
        g.add_edge(0, "A", "B", NO_PROPS, Some("LAYERONE")).unwrap();
        ed.add_constant_properties(vec![("CCC", Prop::str("BLUE"))], Some("LAYERONE"))
            .unwrap();
        info!("{:?}", ed.properties().constant().as_map());
    }

    #[test]
    #[cfg(feature = "proto")]
    fn graph_save_to_load_from_file() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = Graph::new();

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }

        let tmp_raphtory_path: TempDir = TempDir::new().unwrap();

        let graph_path = format!("{}/graph.bin", tmp_raphtory_path.path().display());
        g.encode(&graph_path).unwrap();

        // Load from files
        let g2 = Graph::decode(&graph_path).unwrap();

        assert_eq!(g, g2);
    }

    #[test]
    fn has_edge() {
        let g = Graph::new();
        g.add_edge(1, 7, 8, NO_PROPS, None).unwrap();

        assert!(!g.has_edge(8, 7));
        assert!(g.has_edge(7, 8));

        g.add_edge(1, 7, 9, NO_PROPS, None).unwrap();

        assert!(!g.has_edge(9, 7));
        assert!(g.has_edge(7, 9));
    }

    #[test]
    fn has_edge_str() {
        let g = Graph::new();
        g.add_edge(2, "haaroon", "northLondon", NO_PROPS, None)
            .unwrap();
        assert!(g.has_edge("haaroon", "northLondon"));
    }

    #[test]
    fn graph_edge() {
        let graph = Graph::new();
        let es = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];
        for (t, src, dst) in es {
            graph.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let e = graph
                .window(i64::MIN, i64::MAX)
                .layers(Layer::Default)
                .unwrap()
                .edge(1, 3)
                .unwrap();
            assert_eq!(e.src().id().into_u64(), Some(1u64));
            assert_eq!(e.dst().id().into_u64(), Some(3u64));
        });
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

        let graph = Graph::new();

        for (t, src, dst) in &vs {
            graph.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }
        test_storage!(&graph, |graph| {
            let expected = vec![(2, 3, 1), (1, 0, 0), (1, 0, 0)];
            let actual = (1..=3)
                .map(|i| {
                    let v = graph.node(i).unwrap();
                    (
                        v.window(-1, 7).in_degree(),
                        v.window(1, 7).out_degree(),
                        v.window(0, 1).degree(),
                    )
                })
                .collect::<Vec<_>>();

            assert_eq!(actual, expected);
        });
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

        let graph = Graph::new();

        for (t, src, dst) in &vs {
            graph.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }
        test_storage!(&graph, |graph| {
            let expected = vec![(2, 3, 1), (1, 0, 0), (1, 0, 0)];
            let actual = (1..=3)
                .map(|i| {
                    let v = graph.node(i).unwrap();
                    (
                        v.window(-1, 7).in_edges().iter().count(),
                        v.window(1, 7).out_edges().iter().count(),
                        v.window(0, 1).edges().iter().count(),
                    )
                })
                .collect::<Vec<_>>();

            assert_eq!(actual, expected);
        });
    }

    #[test]
    fn test_explode_layers_time() {
        global_info_logger();
        let g = Graph::new();
        g.add_edge(
            1,
            1,
            2,
            vec![("duration".to_string(), Prop::U32(5))],
            Some("a"),
        )
        .map_err(|err| error!("{:?}", err))
        .ok();
        g.add_edge(
            2,
            1,
            2,
            vec![("duration".to_string(), Prop::U32(5))],
            Some("a"),
        )
        .map_err(|err| error!("{:?}", err))
        .ok();
        g.add_edge(
            3,
            1,
            2,
            vec![("duration".to_string(), Prop::U32(5))],
            Some("a"),
        )
        .map_err(|err| error!("{:?}", err))
        .ok();
        g.add_edge(
            4,
            1,
            2,
            vec![("duration".to_string(), Prop::U32(6))],
            Some("b"),
        )
        .map_err(|err| error!("{:?}", err))
        .ok();
        g.add_edge(5, 1, 2, NO_PROPS, Some("c"))
            .map_err(|err| error!("{:?}", err))
            .ok();

        assert_eq!(g.latest_time(), Some(5));

        let earliest_times = g
            .edge(1, 2)
            .unwrap()
            .explode_layers()
            .earliest_time()
            .map(|t| t.unwrap())
            .collect_vec();

        assert_eq!(earliest_times, vec![1, 4, 5]);

        let latest_times = g
            .edge(1, 2)
            .unwrap()
            .explode_layers()
            .latest_time()
            .map(|t| t.unwrap())
            .collect_vec();

        assert_eq!(latest_times, vec![3, 4, 5]);
    }

    #[test]
    fn time_test() {
        global_info_logger();
        let g = Graph::new();

        assert_eq!(g.latest_time(), None);
        assert_eq!(g.earliest_time(), None);

        g.add_node(5, 1, NO_PROPS, None)
            .map_err(|err| error!("{:?}", err))
            .ok();

        assert_eq!(g.latest_time(), Some(5));
        assert_eq!(g.earliest_time(), Some(5));

        let g = Graph::new();

        g.add_edge(10, 1, 2, NO_PROPS, None).unwrap();
        assert_eq!(g.latest_time(), Some(10));
        assert_eq!(g.earliest_time(), Some(10));

        g.add_node(5, 1, NO_PROPS, None)
            .map_err(|err| error!("{:?}", err))
            .ok();
        assert_eq!(g.latest_time(), Some(10));
        assert_eq!(g.earliest_time(), Some(5));

        g.add_edge(20, 3, 4, NO_PROPS, None).unwrap();
        assert_eq!(g.latest_time(), Some(20));
        assert_eq!(g.earliest_time(), Some(5));

        random_attachment(&g, 100, 10, None);
        assert_eq!(g.latest_time(), Some(126));
        assert_eq!(g.earliest_time(), Some(5));
    }

    #[test]
    fn constant_properties() {
        let g = Graph::new();
        g.add_edge(0, 11, 22, NO_PROPS, None).unwrap();
        g.add_edge(
            0,
            11,
            11,
            vec![("temp".to_string(), Prop::Bool(true))],
            None,
        )
        .unwrap();
        g.add_edge(0, 22, 33, NO_PROPS, None).unwrap();
        g.add_edge(0, 33, 11, NO_PROPS, None).unwrap();
        g.add_node(0, 11, vec![("temp".to_string(), Prop::Bool(true))], None)
            .unwrap();
        g.add_edge(0, 44, 55, NO_PROPS, None).unwrap();
        let v11 = g.node(11).unwrap();
        let v22 = g.node(22).unwrap();
        let v33 = g.node(33).unwrap();
        let v44 = g.node(44).unwrap();
        let v55 = g.node(55).unwrap();
        let edge1111 = g.edge(&v11, &v11).unwrap();
        let edge2233 = g.edge(&v22, &v33).unwrap();
        let edge3311 = g.edge(&v33, &v11).unwrap();

        v11.add_constant_properties(vec![("a", Prop::U64(11)), ("b", Prop::I64(11))])
            .unwrap();
        v11.add_constant_properties(vec![("c", Prop::U32(11))])
            .unwrap();

        v44.add_constant_properties(vec![("e", Prop::U8(1))])
            .unwrap();
        v55.add_constant_properties(vec![("f", Prop::U16(1))])
            .unwrap();
        edge1111
            .add_constant_properties(vec![("d", Prop::U64(1111))], None)
            .unwrap();
        edge3311
            .add_constant_properties(vec![("a", Prop::U64(3311))], None)
            .unwrap();

        // cannot add properties to non-existant layer
        assert!(edge1111
            .add_constant_properties([("test", "test")], Some("test"))
            .is_err());

        // cannot change property type
        assert!(v22
            .add_constant_properties(vec![("b", Prop::U64(22))])
            .is_err());

        assert_eq!(
            v11.properties().constant().values().collect::<Vec<_>>(),
            vec![
                Some(Prop::U64(11)),
                Some(Prop::I64(11)),
                Some(Prop::U32(11)),
                None,
                None
            ],
        );
        assert_eq!(
            v11.properties().constant().keys().collect::<Vec<_>>(),
            vec!["a", "b", "c", "e", "f"]
        );
        assert_eq!(v22.properties().constant().keys().count(), 5);
        assert_eq!(v33.properties().constant().keys().count(), 5);
        assert_eq!(v44.properties().constant().keys().count(), 5);
        assert_eq!(v55.properties().constant().keys().count(), 5);
        assert_eq!(
            edge1111.properties().constant().keys().collect::<Vec<_>>(),
            vec!["d", "a"] // all edges get all ids anyhow
        );
        assert_eq!(
            edge3311.properties().constant().keys().collect::<Vec<_>>(),
            vec!["d", "a"]
        );
        assert_eq!(
            edge2233.properties().constant().keys().collect::<Vec<_>>(),
            vec!["d", "a"]
        );

        assert_eq!(v11.properties().constant().get("a"), Some(Prop::U64(11)));
        assert_eq!(v11.properties().constant().get("b"), Some(Prop::I64(11)));
        assert_eq!(v11.properties().constant().get("c"), Some(Prop::U32(11)));
        assert_eq!(v22.properties().constant().get("b"), None);
        assert_eq!(v44.properties().constant().get("e"), Some(Prop::U8(1)));
        assert_eq!(v55.properties().constant().get("f"), Some(Prop::U16(1)));
        assert_eq!(v22.properties().constant().get("a"), None);
        assert_eq!(
            edge1111.properties().constant().get("d"),
            Some(Prop::U64(1111))
        );
        assert_eq!(
            edge3311.properties().constant().get("a"),
            Some(Prop::U64(3311))
        );
        assert_eq!(edge2233.properties().constant().get("a"), None);

        // cannot add properties to non-existant layer
        assert!(edge1111
            .add_constant_properties([("test", "test")], Some("test"))
            .is_err());
        g.add_edge(0, 1, 2, NO_PROPS, Some("test")).unwrap();
        // cannot add properties to layer without updates
        assert!(edge1111
            .add_constant_properties([("test", "test")], Some("test"))
            .is_err());
    }

    #[test]
    fn temporal_node_rows_1_node() {
        let graph = Graph::new();

        graph
            .add_node(0, 1, [("cool".to_string(), Prop::Bool(true))], None)
            .unwrap();

        test_storage!(&graph, |graph| {
            let actual = graph
                .node(1)
                .unwrap()
                .rows()
                .map(|(t, row)| (t, row.into_iter().map(|(_, a)| a).collect::<Vec<_>>()))
                .collect::<Vec<_>>();

            assert_eq!(actual, vec![(0.into(), vec![Prop::Bool(true)])]);
        });

        graph
            .add_node(0, 1, [("coolio".to_string(), Prop::U64(9))], None)
            .unwrap();

        test_storage!(&graph, |graph| {
            let actual = graph
                .node(1)
                .unwrap()
                .rows()
                .map(|(t, row)| (t, row.into_iter().map(|(_, a)| a).collect::<Vec<_>>()))
                .collect::<Vec<_>>();

            assert_eq!(
                actual,
                vec![
                    (TimeIndexEntry::new(0, 0), vec![Prop::Bool(true)]),
                    (TimeIndexEntry::new(0, 1), vec![Prop::U64(9)])
                ]
            );
        });

        graph
            .add_node(
                1,
                1,
                [
                    ("cool".to_string(), Prop::Bool(false)),
                    ("coolio".to_string(), Prop::U64(19)),
                ],
                None,
            )
            .unwrap();

        test_storage!(&graph, |graph| {
            let actual = graph
                .node(1)
                .unwrap()
                .rows()
                .map(|(t, row)| (t, row.into_iter().map(|(_, a)| a).collect::<Vec<_>>()))
                .collect::<Vec<_>>();

            let expected = vec![
                (TimeIndexEntry::new(0, 0), vec![Prop::Bool(true)]),
                (TimeIndexEntry::new(0, 1), vec![Prop::U64(9)]),
                (
                    TimeIndexEntry::new(1, 2),
                    vec![Prop::Bool(false), Prop::U64(19)],
                ),
            ];
            assert_eq!(actual, expected);
        });

        graph.add_node(2, 1, NO_PROPS, None).unwrap();
        // edge additions should not show up in prop rows
        graph.add_edge(3, 1, 1, NO_PROPS, None).unwrap();

        test_storage!(&graph, |graph| {
            let actual = graph
                .node(1)
                .unwrap()
                .rows()
                .map(|(t, row)| (t, row.into_iter().map(|(_, a)| a).collect::<Vec<_>>()))
                .collect::<Vec<_>>();

            let expected = vec![
                (TimeIndexEntry::new(0, 0), vec![Prop::Bool(true)]),
                (TimeIndexEntry::new(0, 1), vec![Prop::U64(9)]),
                (
                    TimeIndexEntry::new(1, 2),
                    vec![Prop::Bool(false), Prop::U64(19)],
                ),
                (TimeIndexEntry::new(2, 3), vec![]),
            ];

            assert_eq!(actual, expected);
        });
    }

    #[test]
    fn temporal_node_rows_nodes() {
        let graph = Graph::new();

        graph
            .add_node(0, 1, [("cool".to_string(), Prop::U64(1))], None)
            .unwrap();
        graph
            .add_node(1, 2, [("cool".to_string(), Prop::U64(2))], None)
            .unwrap();
        graph
            .add_node(2, 3, [("cool".to_string(), Prop::U64(3))], None)
            .unwrap();

        test_storage!(&graph, |graph| {
            for id in 0..3 {
                let actual = graph
                    .core_graph()
                    .nodes()
                    .node(VID(id))
                    .temp_prop_rows()
                    .map(|(t, _, row)| (t, row.into_iter().map(|(_, p)| p).collect::<Vec<_>>()))
                    .collect::<Vec<_>>();

                let expected = vec![(
                    TimeIndexEntry::new(id as i64, id),
                    vec![Prop::U64((id as u64) + 1)],
                )];
                assert_eq!(actual, expected);
            }
        });
    }

    #[test]
    fn temporal_node_rows_window() {
        let graph = Graph::new();
        graph
            .add_node(0, 1, [("cool".to_string(), Prop::U64(1))], None)
            .unwrap();
        graph
            .add_node(1, 1, [("cool".to_string(), Prop::U64(2))], None)
            .unwrap();
        graph
            .add_node(2, 1, [("cool".to_string(), Prop::U64(3))], None)
            .unwrap();

        test_storage!(&graph, |graph| {
            let get_rows = |vid: VID, range: Range<TimeIndexEntry>| {
                graph
                    .core_graph()
                    .nodes()
                    .node(vid)
                    .temp_prop_rows_range(Some(range))
                    .map(|(t, _, row)| (t, row.into_iter().map(|(_, p)| p).collect::<Vec<_>>()))
                    .collect::<Vec<_>>()
            };
            let actual = get_rows(VID(0), TimeIndexEntry::new(2, 0)..TimeIndexEntry::new(3, 0));

            let expected = vec![(TimeIndexEntry::new(2, 2), vec![Prop::U64(3)])];

            assert_eq!(actual, expected);

            let actual = get_rows(VID(0), TimeIndexEntry::new(0, 0)..TimeIndexEntry::new(3, 0));
            let expected = vec![
                (TimeIndexEntry::new(0, 0), vec![Prop::U64(1)]),
                (TimeIndexEntry::new(1, 1), vec![Prop::U64(2)]),
                (TimeIndexEntry::new(2, 2), vec![Prop::U64(3)]),
            ];

            assert_eq!(actual, expected);
        });
    }

    #[test]
    fn temporal_props_node() {
        let graph = Graph::new();

        graph
            .add_node(0, 1, [("cool".to_string(), Prop::Bool(true))], None)
            .unwrap();

        let v = graph.node(1).unwrap();

        let actual = v.properties().get("cool");
        assert_eq!(actual, Some(Prop::Bool(true)));

        // we flip cool from true to false after t 3
        graph
            .add_node(3, 1, [("cool".to_string(), Prop::Bool(false))], None)
            .unwrap();

        test_storage!(&graph, |graph| {
            let wg = graph.window(3, 15);
            let v = wg.node(1).unwrap();

            let actual = v.properties().get("cool");
            assert_eq!(actual, Some(Prop::Bool(false)));

            let hist: Vec<_> = v
                .properties()
                .temporal()
                .get("cool")
                .unwrap()
                .iter()
                .collect();
            assert_eq!(hist, vec![(3, Prop::Bool(false))]);

            let v = graph.node(1).unwrap();

            let hist: Vec<_> = v
                .properties()
                .temporal()
                .get("cool")
                .unwrap()
                .iter()
                .collect();
            assert_eq!(hist, vec![(0, Prop::Bool(true)), (3, Prop::Bool(false))]);
        });
    }

    #[test]
    fn temporal_props_edge() {
        let graph = Graph::new();

        graph
            .add_edge(1, 0, 1, vec![("distance".to_string(), Prop::U32(5))], None)
            .expect("add edge");

        test_storage!(&graph, |graph| {
            let e = graph.edge(0, 1).unwrap();

            let prop = e.properties().get("distance").unwrap();
            assert_eq!(prop, Prop::U32(5));
        });
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

        let graph = Graph::new();

        for (t, src, dst) in &vs {
            graph.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }
        test_storage!(&graph, |graph| {
            let expected = vec![
                (vec![1, 2], vec![1, 2, 3], vec![1]),
                (vec![1], vec![], vec![]),
                (vec![1], vec![], vec![]),
            ];
            let actual = (1..=3)
                .map(|i| {
                    let v = graph.node(i).unwrap();
                    (
                        v.window(-1, 7)
                            .in_neighbours()
                            .id()
                            .filter_map(|id| id.as_u64())
                            .collect::<Vec<_>>(),
                        v.window(1, 7)
                            .out_neighbours()
                            .id()
                            .filter_map(|id| id.as_u64())
                            .collect::<Vec<_>>(),
                        v.window(0, 1)
                            .neighbours()
                            .id()
                            .filter_map(|id| id.as_u64())
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>();

            assert_eq!(actual, expected);
        });
    }

    #[test]
    fn test_time_range_on_empty_graph() {
        let graph = Graph::new();

        test_storage!(&graph, |graph| {
            let rolling = graph.rolling(1, None).unwrap().collect_vec();
            assert!(rolling.is_empty());

            let expanding = graph.expanding(1).unwrap().collect_vec();
            assert!(expanding.is_empty());
        });
    }

    #[test]
    fn test_add_node_with_nums() {
        let graph = Graph::new();

        graph.add_node(1, 831, NO_PROPS, None).unwrap();
        test_storage!(&graph, |graph| {
            assert!(graph.has_node(831));

            assert_eq!(graph.count_nodes(), 1);
        });
    }

    #[test]
    fn test_add_node_with_strings() {
        let graph = Graph::new();

        graph.add_node(0, "haaroon", NO_PROPS, None).unwrap();
        graph.add_node(1, "hamza", NO_PROPS, None).unwrap();
        test_storage!(&graph, |graph| {
            assert!(graph.has_node("haaroon"));
            assert!(graph.has_node("hamza"));

            assert_eq!(graph.count_nodes(), 2);
        });
    }

    #[test]
    fn layers() -> Result<(), GraphError> {
        let graph = Graph::new();
        graph.add_edge(0, 11, 22, NO_PROPS, None)?;
        graph.add_edge(0, 11, 33, NO_PROPS, None)?;
        graph.add_edge(0, 33, 11, NO_PROPS, None)?;
        graph.add_edge(0, 11, 22, NO_PROPS, Some("layer1"))?;
        graph.add_edge(0, 11, 33, NO_PROPS, Some("layer2"))?;
        graph.add_edge(0, 11, 44, NO_PROPS, Some("layer2"))?;

        test_storage!(&graph, |graph| {
            assert!(graph.has_edge(11, 22));
            assert!(graph.default_layer().has_edge(11, 22));
            assert!(!graph.default_layer().has_edge(11, 44));
            assert!(!graph.layers("layer2").unwrap().has_edge(11, 22));
            assert!(graph.layers("layer2").unwrap().has_edge(11, 44));

            assert!(graph.edge(11, 22).is_some());
            assert!(graph.layers(Layer::Default).unwrap().edge(11, 44).is_none());
            assert!(graph.layers("layer2").unwrap().edge(11, 22).is_none());
            assert!(graph.layers("layer2").unwrap().edge(11, 44).is_some());

            assert!(graph
                .exclude_layers("layer2")
                .unwrap()
                .edge(11, 44)
                .is_none());
            assert!(graph
                .exclude_layers("layer2")
                .unwrap()
                .edge(11, 33)
                .is_some());
            assert!(graph
                .exclude_layers("layer2")
                .unwrap()
                .edge(11, 22)
                .is_some());

            let dft_layer = graph.default_layer();
            let layer1 = graph.layers("layer1").expect("layer1");
            let layer2 = graph.layers("layer2").expect("layer2");
            assert!(graph.layers("missing layer").is_err());

            assert_eq!(graph.count_nodes(), 4);
            assert_eq!(graph.count_edges(), 4);
            assert_eq!(dft_layer.count_edges(), 3);
            assert_eq!(layer1.count_edges(), 1);
            assert_eq!(layer2.count_edges(), 2);

            let node = graph.node(11).unwrap();
            let node_dft = dft_layer.node(11).unwrap();
            let node1 = layer1.node(11).unwrap();
            let node2 = layer2.node(11).unwrap();

            assert_eq!(node.degree(), 3);
            assert_eq!(node_dft.degree(), 2);
            assert_eq!(node1.degree(), 1);
            assert_eq!(node2.degree(), 2);

            assert_eq!(node.out_degree(), 3);
            assert_eq!(node_dft.out_degree(), 2);
            assert_eq!(node1.out_degree(), 1);
            assert_eq!(node2.out_degree(), 2);

            assert_eq!(node.in_degree(), 1);
            assert_eq!(node_dft.in_degree(), 1);
            assert_eq!(node1.in_degree(), 0);
            assert_eq!(node2.in_degree(), 0);

            fn to_tuples<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>(
                edges: Edges<'graph, G, GH>,
            ) -> Vec<(u64, u64)> {
                edges
                    .id()
                    .filter_map(|(s, d)| s.to_u64().zip(d.to_u64()))
                    .sorted()
                    .collect_vec()
            }

            assert_eq!(
                to_tuples(node.edges()),
                vec![(11, 22), (11, 33), (11, 44), (33, 11)]
            );
            assert_eq!(
                to_tuples(node_dft.edges()),
                vec![(11, 22), (11, 33), (33, 11)]
            );
            assert_eq!(to_tuples(node1.edges()), vec![(11, 22)]);
            assert_eq!(to_tuples(node2.edges()), vec![(11, 33), (11, 44)]);

            assert_eq!(to_tuples(node.in_edges()), vec![(33, 11)]);
            assert_eq!(to_tuples(node_dft.in_edges()), vec![(33, 11)]);
            assert_eq!(to_tuples(node1.in_edges()), vec![]);
            assert_eq!(to_tuples(node2.in_edges()), vec![]);

            assert_eq!(
                to_tuples(node.out_edges()),
                vec![(11, 22), (11, 33), (11, 44)]
            );
            assert_eq!(to_tuples(node_dft.out_edges()), vec![(11, 22), (11, 33)]);
            assert_eq!(to_tuples(node1.out_edges()), vec![(11, 22)]);
            assert_eq!(to_tuples(node2.out_edges()), vec![(11, 33), (11, 44)]);

            fn to_ids<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>(
                neighbours: PathFromNode<'graph, G, GH>,
            ) -> Vec<u64> {
                neighbours
                    .iter()
                    .filter_map(|n| n.id().as_u64())
                    .sorted()
                    .collect_vec()
            }

            assert_eq!(to_ids(node.neighbours()), vec![22, 33, 44]);
            assert_eq!(to_ids(node_dft.neighbours()), vec![22, 33]);
            assert_eq!(to_ids(node1.neighbours()), vec![22]);
            assert_eq!(to_ids(node2.neighbours()), vec![33, 44]);

            assert_eq!(to_ids(node.out_neighbours()), vec![22, 33, 44]);
            assert_eq!(to_ids(node_dft.out_neighbours()), vec![22, 33]);
            assert_eq!(to_ids(node1.out_neighbours()), vec![22]);
            assert_eq!(to_ids(node2.out_neighbours()), vec![33, 44]);

            assert_eq!(to_ids(node.in_neighbours()), vec![33]);
            assert_eq!(to_ids(node_dft.in_neighbours()), vec![33]);
            assert!(to_ids(node1.in_neighbours()).is_empty());
            assert!(to_ids(node2.in_neighbours()).is_empty());
        });
        Ok(())
    }

    #[test]
    fn test_props() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, [("weight", Prop::I64(1))], None)
            .unwrap();
        g.add_edge(1, 1, 2, [("weight", Prop::I64(2))], None)
            .unwrap();
        g.add_edge(2, 1, 2, NO_PROPS, None).unwrap();

        let exploded = g.edge(1, 2).unwrap().explode();
        let res = exploded
            .properties()
            .map(|p| p.as_vec().len())
            .collect_vec();
        assert_eq!(res, vec![1, 1, 0]);
    }

    #[test]
    fn test_exploded_edge() {
        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, [("weight", Prop::I64(1))], None)
            .unwrap();
        graph
            .add_edge(1, 1, 2, [("weight", Prop::I64(2))], None)
            .unwrap();
        graph
            .add_edge(2, 1, 2, [("weight", Prop::I64(3))], None)
            .unwrap();
        test_storage!(&graph, |graph| {
            let exploded = graph.edge(1, 2).unwrap().explode();

            let res = exploded.properties().map(|p| p.as_vec()).collect_vec();

            let mut expected = Vec::new();
            for i in 1..4 {
                expected.push(vec![("weight".into(), Prop::I64(i))]);
            }

            assert_eq!(res, expected);

            let e = graph
                .node(1)
                .unwrap()
                .edges()
                .explode()
                .properties()
                .map(|p| p.as_vec())
                .collect_vec();
            assert_eq!(e, expected);
        });
    }

    #[test]
    fn test_edge_earliest_latest() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(1, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(2, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(0, 1, 3, NO_PROPS, None).unwrap();
        graph.add_edge(1, 1, 3, NO_PROPS, None).unwrap();
        graph.add_edge(2, 1, 3, NO_PROPS, None).unwrap();

        test_storage!(&graph, |graph| {
            let mut res = graph.edge(1, 2).unwrap().earliest_time().unwrap();
            assert_eq!(res, 0);

            res = graph.edge(1, 2).unwrap().latest_time().unwrap();
            assert_eq!(res, 2);

            res = graph.at(1).edge(1, 2).unwrap().earliest_time().unwrap();
            assert_eq!(res, 1);

            res = graph.before(1).edge(1, 2).unwrap().earliest_time().unwrap();
            assert_eq!(res, 0);

            res = graph.after(1).edge(1, 2).unwrap().earliest_time().unwrap();
            assert_eq!(res, 2);

            res = graph.at(1).edge(1, 2).unwrap().latest_time().unwrap();
            assert_eq!(res, 1);

            res = graph.before(1).edge(1, 2).unwrap().latest_time().unwrap();
            assert_eq!(res, 0);

            res = graph.after(1).edge(1, 2).unwrap().latest_time().unwrap();
            assert_eq!(res, 2);

            let res_list: Vec<i64> = graph
                .node(1)
                .unwrap()
                .edges()
                .earliest_time()
                .flatten()
                .collect();
            assert_eq!(res_list, vec![0, 0]);

            let res_list: Vec<i64> = graph
                .node(1)
                .unwrap()
                .edges()
                .latest_time()
                .flatten()
                .collect();
            assert_eq!(res_list, vec![2, 2]);

            let res_list: Vec<i64> = graph
                .node(1)
                .unwrap()
                .at(1)
                .edges()
                .earliest_time()
                .flatten()
                .collect();
            assert_eq!(res_list, vec![1, 1]);

            let res_list: Vec<i64> = graph
                .node(1)
                .unwrap()
                .before(1)
                .edges()
                .earliest_time()
                .flatten()
                .collect();
            assert_eq!(res_list, vec![0, 0]);

            let res_list: Vec<i64> = graph
                .node(1)
                .unwrap()
                .after(1)
                .edges()
                .earliest_time()
                .flatten()
                .collect();
            assert_eq!(res_list, vec![2, 2]);

            let res_list: Vec<i64> = graph
                .node(1)
                .unwrap()
                .at(1)
                .edges()
                .latest_time()
                .flatten()
                .collect();
            assert_eq!(res_list, vec![1, 1]);

            let res_list: Vec<i64> = graph
                .node(1)
                .unwrap()
                .before(1)
                .edges()
                .latest_time()
                .flatten()
                .collect();
            assert_eq!(res_list, vec![0, 0]);

            let res_list: Vec<i64> = graph
                .node(1)
                .unwrap()
                .after(1)
                .edges()
                .latest_time()
                .flatten()
                .collect();
            assert_eq!(res_list, vec![2, 2]);
        });
    }

    #[test]
    fn node_properties() -> Result<(), GraphError> {
        let g = Graph::new_with_shards(2);

        g.add_node(
            0,
            1,
            [("t", Prop::str("wallet")), ("cost", Prop::F64(99.5))],
            Some("a"),
        )?;

        let n1 = g.node(1).unwrap();
        g.add_node(1, 2, [("t", Prop::str("person"))], None)?;
        g.add_node(
            6,
            3,
            [
                ("list_prop", vec![1.1, 2.2, 3.3].into_prop_list()),
                ("cost_b", Prop::F64(76.0)),
            ],
            Some("b"),
        )?;

        g.add_node(
            7,
            4,
            [
                ("str_prop", Prop::str("hello")),
                ("bool_prop", Prop::Bool(true)),
            ],
            Some("b"),
        )?;

        let node = g.node(1).unwrap();
        node.add_constant_properties([("lol", Prop::str("smile"))])?;

        let node_1_props = n1
            .properties()
            .iter()
            .filter_map(|(k, v)| v.map(move |v| (k.to_string(), v)))
            .collect::<Vec<_>>();
        assert_eq!(
            node_1_props,
            vec![
                ("t".to_string(), Prop::str("wallet")),
                ("cost".to_string(), Prop::F64(99.5)),
                ("lol".to_string(), Prop::str("smile"))
            ]
        );

        Ok(())
    }

    #[test]
    fn test_decimal_properties() {
        let graph = Graph::new();
        let dec_prop = Prop::Decimal(BigDecimal::new(123456234234123123i64.into(), 9));
        graph
            .add_node(0, 1, [("cost".to_string(), dec_prop.clone())], None)
            .unwrap();

        test_storage!(&graph, |graph| {
            let node = graph.node(1).unwrap();
            let prop = node.properties().get("cost").unwrap();
            assert_eq!(prop, dec_prop);
        });
    }

    #[test]
    fn node_history_rows() {
        let graph = Graph::new();
        graph
            .add_node(1, 2, [("cool".to_string(), Prop::U64(1))], None)
            .unwrap();
        graph
            .add_node(0, 1, [("cool".to_string(), 1u64)], None)
            .unwrap();
        graph
            .add_node(
                1,
                1,
                [
                    ("coolio".to_string(), Prop::Bool(true)),
                    ("bla".to_string(), Prop::I64(2)),
                ],
                None,
            )
            .unwrap();
        graph.add_node(2, 1, NO_PROPS, None).unwrap();
        graph
            .add_node(1, 1, [("cool".to_string(), 3u64)], None)
            .unwrap();

        test_storage!(&graph, |graph| {
            let node = graph.node(1).unwrap();

            let actual = node
                .rows()
                .map(|(t, row)| (t, row.into_iter().map(|(_, a)| a).collect::<Vec<_>>()))
                .collect::<Vec<_>>();

            let expected = vec![
                (TimeIndexEntry::new(0, 1), vec![Prop::U64(1)]),
                (
                    TimeIndexEntry::new(1, 2),
                    vec![Prop::Bool(true), Prop::I64(2)],
                ),
                (TimeIndexEntry::new(1, 4), vec![Prop::U64(3)]),
                (TimeIndexEntry::new(2, 3), vec![]),
            ];

            assert_eq!(actual, expected);

            let node = graph.node(2).unwrap();

            let actual = node
                .rows()
                .map(|(t, row)| (t, row.into_iter().map(|(_, a)| a).collect::<Vec<_>>()))
                .collect::<Vec<_>>();

            let expected = vec![(TimeIndexEntry::new(1, 0), vec![Prop::U64(1)])];

            assert_eq!(actual, expected);
        });
    }

    #[test]
    fn check_node_history_str() {
        let graph = Graph::new();

        graph.add_node(4, "Lord Farquaad", NO_PROPS, None).unwrap();
        graph.add_node(6, "Lord Farquaad", NO_PROPS, None).unwrap();
        graph.add_node(7, "Lord Farquaad", NO_PROPS, None).unwrap();
        graph.add_node(8, "Lord Farquaad", NO_PROPS, None).unwrap();

        // FIXME: Node updates without properties or edges are currently not supported in disk_graph (see issue #46)
        test_graph(&graph, |graph| {
            let times_of_farquaad = graph.node("Lord Farquaad").unwrap().history();

            assert_eq!(times_of_farquaad, [4, 6, 7, 8]);

            let view = graph.window(1, 8);

            let windowed_times_of_farquaad = view.node("Lord Farquaad").unwrap().history();
            assert_eq!(windowed_times_of_farquaad, [4, 6, 7]);
        });
    }

    #[test]
    fn check_node_history_num() {
        let graph = Graph::new();

        graph.add_node(1, 1, NO_PROPS, None).unwrap();
        graph.add_node(2, 1, NO_PROPS, None).unwrap();
        graph.add_node(3, 1, NO_PROPS, None).unwrap();
        graph.add_node(4, 1, NO_PROPS, None).unwrap();
        graph.add_node(8, 1, NO_PROPS, None).unwrap();

        // FIXME: Node updates without properties or edges are currently not supported in disk_graph (see issue #46)
        test_graph(&graph, |graph| {
            let times_of_one = graph.node(1).unwrap().history();

            assert_eq!(times_of_one, [1, 2, 3, 4, 8]);

            let view = graph.window(1, 8);

            let windowed_times_of_one = view.node(1).unwrap().history();
            assert_eq!(windowed_times_of_one, [1, 2, 3, 4]);
        });
    }

    #[test]
    fn check_edge_history() {
        let graph = Graph::new();

        graph.add_edge(1, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(2, 1, 3, NO_PROPS, None).unwrap();
        graph.add_edge(3, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(4, 1, 4, NO_PROPS, None).unwrap();
        test_storage!(&graph, |graph| {
            let times_of_onetwo = graph.edge(1, 2).unwrap().history();
            let times_of_four = graph.edge(1, 4).unwrap().window(1, 5).history();
            let view = graph.window(2, 5);
            let windowed_times_of_four = view.edge(1, 4).unwrap().window(2, 4).history();

            assert_eq!(times_of_onetwo, [1, 3]);
            assert_eq!(times_of_four, [4]);
            assert!(windowed_times_of_four.is_empty());
        });
    }

    #[test]
    fn check_edge_history_on_multiple_shards() {
        let graph = Graph::new();

        graph.add_edge(1, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(2, 1, 3, NO_PROPS, None).unwrap();
        graph.add_edge(3, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(4, 1, 4, NO_PROPS, None).unwrap();
        graph.add_edge(5, 1, 4, NO_PROPS, None).unwrap();
        graph.add_edge(6, 1, 4, NO_PROPS, None).unwrap();
        graph.add_edge(7, 1, 4, NO_PROPS, None).unwrap();
        graph.add_edge(8, 1, 4, NO_PROPS, None).unwrap();
        graph.add_edge(9, 1, 4, NO_PROPS, None).unwrap();
        graph.add_edge(10, 1, 4, NO_PROPS, None).unwrap();

        test_storage!(&graph, |graph| {
            let times_of_onetwo = graph.edge(1, 2).unwrap().history();
            let times_of_four = graph.edge(1, 4).unwrap().window(1, 5).history();
            let times_of_outside_window = graph.edge(1, 4).unwrap().window(1, 4).history();
            let times_of_four_higher = graph.edge(1, 4).unwrap().window(6, 11).history();

            let view = graph.window(1, 11);
            let windowed_times_of_four = view.edge(1, 4).unwrap().window(2, 5).history();
            let windowed_times_of_four_higher = view.edge(1, 4).unwrap().window(8, 11).history();

            assert_eq!(times_of_onetwo, [1, 3]);
            assert_eq!(times_of_four, [4]);
            assert_eq!(times_of_four_higher, [6, 7, 8, 9, 10]);
            assert!(times_of_outside_window.is_empty());
            assert_eq!(windowed_times_of_four, [4]);
            assert_eq!(windowed_times_of_four_higher, [8, 9, 10]);
        });
    }

    #[derive(Debug)]
    struct CustomTime<'a>(&'a str, &'a str);

    impl<'a> TryIntoTime for CustomTime<'a> {
        fn try_into_time(self) -> Result<i64, ParseTimeError> {
            let CustomTime(time, fmt) = self;
            let time = NaiveDateTime::parse_from_str(time, fmt)?;
            let time = time.and_utc().timestamp_millis();
            Ok(time)
        }
    }

    #[test]
    fn test_ingesting_timestamps() {
        let earliest_time = "2022-06-06 12:34:00".try_into_time().unwrap();
        let latest_time = "2022-06-07 12:34:00".try_into_time().unwrap();

        let g = Graph::new();
        g.add_node("2022-06-06T12:34:00.000", 0, NO_PROPS, None)
            .unwrap();
        g.add_edge("2022-06-07T12:34:00", 1, 2, NO_PROPS, None)
            .unwrap();
        assert_eq!(g.earliest_time().unwrap(), earliest_time);
        assert_eq!(g.latest_time().unwrap(), latest_time);

        let g = Graph::new();
        let fmt = "%Y-%m-%d %H:%M";

        g.add_node(CustomTime("2022-06-06 12:34", fmt), 0, NO_PROPS, None)
            .unwrap();
        g.add_edge(CustomTime("2022-06-07 12:34", fmt), 1, 2, NO_PROPS, None)
            .unwrap();
        assert_eq!(g.earliest_time().unwrap(), earliest_time);
        assert_eq!(g.latest_time().unwrap(), latest_time);
    }

    #[test]
    fn test_prop_display_str() {
        let mut prop = Prop::Str("hello".into());
        assert_eq!(format!("{}", prop), "hello");

        prop = Prop::I32(42);
        assert_eq!(format!("{}", prop), "42");

        prop = Prop::I64(9223372036854775807);
        assert_eq!(format!("{}", prop), "9223372036854775807");

        prop = Prop::U32(4294967295);
        assert_eq!(format!("{}", prop), "4294967295");

        prop = Prop::U64(18446744073709551615);
        assert_eq!(format!("{}", prop), "18446744073709551615");

        prop = Prop::U8(255);
        assert_eq!(format!("{}", prop), "255");

        prop = Prop::U16(65535);
        assert_eq!(format!("{}", prop), "65535");

        prop = Prop::F32(3.14159);
        assert_eq!(format!("{}", prop), "3.14159");

        prop = Prop::F64(3.141592653589793);
        assert_eq!(format!("{}", prop), "3.141592653589793");

        prop = Prop::Bool(true);
        assert_eq!(format!("{}", prop), "true");
    }

    #[quickcheck]
    fn test_graph_constant_props(u64_props: HashMap<String, u64>) -> bool {
        let g = Graph::new();

        let as_props = u64_props
            .into_iter()
            .map(|(name, value)| (name, Prop::U64(value)))
            .collect::<Vec<_>>();

        g.add_constant_properties(as_props.clone()).unwrap();

        let props_map = as_props.into_iter().collect::<HashMap<_, _>>();

        props_map
            .into_iter()
            .all(|(name, value)| g.properties().constant().get(&name).unwrap() == value)
    }

    #[test]
    fn test_graph_constant_props2() {
        let g = Graph::new();

        let as_props: Vec<(&str, Prop)> = vec![(
            "mylist",
            Prop::List(Arc::from(vec![Prop::I64(1), Prop::I64(2)])),
        )];

        g.add_constant_properties(as_props.clone()).unwrap();

        let props_names = as_props
            .into_iter()
            .map(|(name, _)| name.into())
            .collect::<HashSet<_>>();

        assert_eq!(
            g.properties().constant().keys().collect::<HashSet<_>>(),
            props_names
        );

        let data = vec![
            ("key1", Prop::I64(10)),
            ("key2", Prop::I64(20)),
            ("key3", Prop::I64(30)),
        ];
        let as_props: Vec<(&str, Prop)> = vec![("mylist2", Prop::map(data))];

        g.add_constant_properties(as_props.clone()).unwrap();

        let props_names2: HashSet<ArcStr> = as_props
            .into_iter()
            .map(|(name, _)| name.into())
            .collect::<HashSet<_>>();

        assert_eq!(
            g.properties().constant().keys().collect::<HashSet<_>>(),
            props_names.union(&props_names2).cloned().collect()
        );
    }

    #[quickcheck]
    fn test_graph_constant_props_names(u64_props: HashMap<String, u64>) -> bool {
        let g = Graph::new();

        let as_props = u64_props
            .into_iter()
            .map(|(name, value)| (name.into(), Prop::U64(value)))
            .collect::<Vec<_>>();

        g.add_constant_properties(as_props.clone()).unwrap();

        let props_names = as_props
            .into_iter()
            .map(|(name, _)| name)
            .collect::<HashSet<_>>();

        g.properties().constant().keys().collect::<HashSet<_>>() == props_names
    }

    #[quickcheck]
    fn test_graph_temporal_props(str_props: HashMap<String, String>) -> bool {
        global_info_logger();
        let g = Graph::new();

        let (t0, t1) = (1, 2);

        let (t0_props, t1_props): (Vec<_>, Vec<_>) = str_props
            .iter()
            .enumerate()
            .map(|(i, props)| {
                let (name, value) = props;
                let value = Prop::from(value);
                (name.as_str().into(), value, i % 2)
            })
            .partition(|(_, _, i)| *i == 0);

        let t0_props: HashMap<ArcStr, Prop> = t0_props
            .into_iter()
            .map(|(name, value, _)| (name, value))
            .collect();

        let t1_props: HashMap<ArcStr, Prop> = t1_props
            .into_iter()
            .map(|(name, value, _)| (name, value))
            .collect();

        g.add_properties(t0, t0_props.clone()).unwrap();
        g.add_properties(t1, t1_props.clone()).unwrap();

        let check = t0_props.iter().all(|(name, value)| {
            g.properties().temporal().get(name).unwrap().at(t0) == Some(value.clone())
        }) && t1_props.iter().all(|(name, value)| {
            g.properties().temporal().get(name).unwrap().at(t1) == Some(value.clone())
        });
        if !check {
            error!("failed time-specific comparison for {:?}", str_props);
            return false;
        }
        let check = check
            && g.at(t0)
                .properties()
                .temporal()
                .iter_latest()
                .map(|(k, v)| (k.clone(), v))
                .collect::<HashMap<_, _, _>>()
                == t0_props;
        if !check {
            error!("failed latest value comparison for {:?} at t0", str_props);
            return false;
        }
        let check = check
            && t1_props.iter().all(|(k, ve)| {
                g.at(t1)
                    .properties()
                    .temporal()
                    .get(k)
                    .and_then(|v| v.latest())
                    == Some(ve.clone())
            });
        if !check {
            error!("failed latest value comparison for {:?} at t1", str_props);
            return false;
        }
        check
    }

    #[test]
    fn test_temporral_edge_props_window() {
        let graph = Graph::new();
        graph
            .add_edge(1, 1, 2, vec![("weight".to_string(), Prop::I64(1))], None)
            .unwrap();
        graph
            .add_edge(2, 1, 2, vec![("weight".to_string(), Prop::I64(2))], None)
            .unwrap();
        graph
            .add_edge(3, 1, 2, vec![("weight".to_string(), Prop::I64(3))], None)
            .unwrap();
        test_storage!(&graph, |graph| {
            let e = graph
                .node(1)
                .unwrap()
                .out_edges()
                .into_iter()
                .next()
                .unwrap();
            let res: HashMap<ArcStr, Vec<(i64, Prop)>> = e
                .window(1, 3)
                .properties()
                .temporal()
                .iter()
                .map(|(k, v)| (k.clone(), v.iter().collect()))
                .collect();

            let mut exp = HashMap::new();
            exp.insert(
                ArcStr::from("weight"),
                vec![(1, Prop::I64(1)), (2, Prop::I64(2))],
            );
            assert_eq!(res, exp);
        });
    }

    #[test]
    fn test_node_early_late_times() {
        let graph = Graph::new();
        graph.add_node(1, 1, NO_PROPS, None).unwrap();
        graph.add_node(2, 1, NO_PROPS, None).unwrap();
        graph.add_node(3, 1, NO_PROPS, None).unwrap();

        // FIXME: Node add without properties not showing up (Issue #46)
        test_graph(&graph, |graph| {
            assert_eq!(graph.node(1).unwrap().earliest_time(), Some(1));
            assert_eq!(graph.node(1).unwrap().latest_time(), Some(3));

            assert_eq!(graph.at(2).node(1).unwrap().earliest_time(), Some(2));
            assert_eq!(graph.at(2).node(1).unwrap().latest_time(), Some(2));

            assert_eq!(graph.before(2).node(1).unwrap().earliest_time(), Some(1));
            assert_eq!(graph.before(2).node(1).unwrap().latest_time(), Some(1));

            assert_eq!(graph.after(2).node(1).unwrap().earliest_time(), Some(3));
            assert_eq!(graph.after(2).node(1).unwrap().latest_time(), Some(3));
        })
    }

    #[test]
    fn test_node_ids() {
        let graph = Graph::new();
        graph.add_node(1, 1, NO_PROPS, None).unwrap();
        graph.add_node(1, 2, NO_PROPS, None).unwrap();
        graph.add_node(2, 3, NO_PROPS, None).unwrap();

        // FIXME: Node add without properties not showing up (Issue #46)
        test_graph(&graph, |graph| {
            assert_eq!(
                graph.nodes().id().collect::<Vec<_>>(),
                vec![1u64, 2u64, 3u64]
            );

            let g_at = graph.at(1);
            assert_eq!(g_at.nodes().id().collect::<Vec<_>>(), vec![1u64, 2u64]);
        });
    }

    #[test]
    fn test_edge_layer_name() -> Result<(), GraphError> {
        let graph = Graph::new();
        graph.add_edge(0, 0, 1, NO_PROPS, None)?;
        graph.add_edge(0, 0, 1, NO_PROPS, Some("awesome name"))?;

        test_storage!(&graph, |graph| {
            let what = graph.edges().id().collect_vec();
            assert_eq!(what, vec![(0u64.into(), 1u64.into())]);

            let layer_names = graph.edges().layer_names().flatten().sorted().collect_vec();
            assert_eq!(layer_names, vec!["_default", "awesome name"]);
        });
        Ok(())
    }

    #[test]
    fn test_edge_from_single_layer() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, Some("layer")).unwrap();

        test_storage!(&graph, |graph| {
            assert!(graph.edge(1, 2).is_some());
            assert!(graph.layers("layer").unwrap().edge(1, 2).is_some())
        });
    }

    #[test]
    fn test_edge_layer_intersect_layer() {
        let graph = Graph::new();

        graph
            .add_edge(1, 1, 2, NO_PROPS, Some("layer1"))
            .expect("add edge");
        graph
            .add_edge(1, 1, 3, NO_PROPS, Some("layer3"))
            .expect("add edge");
        graph.add_edge(1, 1, 4, NO_PROPS, None).expect("add edge");

        test_storage!(&graph, |graph| {
            let g_layers = graph.layers(vec!["layer1", "layer3"]).expect("layer");

            assert!(g_layers.layers("layer1").unwrap().edge(1, 2).is_some());
            assert!(g_layers.layers("layer3").unwrap().edge(1, 3).is_some());
            assert!(g_layers.edge(1, 2).is_some());
            assert!(g_layers.edge(1, 3).is_some());

            assert!(g_layers.edge(1, 4).is_none());

            let one = g_layers.node(1).expect("node");
            let ns = one
                .neighbours()
                .iter()
                .filter_map(|v| v.id().as_u64())
                .collect::<Vec<_>>();
            assert_eq!(ns, vec![2, 3]);

            let g_layers2 = g_layers.layers(vec!["layer1"]).expect("layer");

            assert!(g_layers2.layers("layer1").unwrap().edge(1, 2).is_some());
            assert!(g_layers2.edge(1, 2).is_some());

            assert!(g_layers2.edge(1, 3).is_none());

            assert!(g_layers2.edge(1, 4).is_none());

            let one = g_layers2.node(1).expect("node");
            let ns = one
                .neighbours()
                .iter()
                .filter_map(|v| v.id().as_u64())
                .collect::<Vec<_>>();
            assert_eq!(ns, vec![2]);
        });
    }

    #[test]
    fn simple_triangle() {
        let graph = Graph::new();

        let vs = vec![(1, 1, 2), (2, 1, 3), (3, 2, 1), (4, 3, 2)];

        for (t, src, dst) in &vs {
            graph.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let windowed_graph = graph.window(0, 5);
            let one = windowed_graph.node(1).expect("node");
            let ns_win = one
                .neighbours()
                .id()
                .filter_map(|id| id.to_u64())
                .collect::<Vec<_>>();

            let one = graph.node(1).expect("node");
            let ns = one
                .neighbours()
                .id()
                .filter_map(|id| id.to_u64())
                .collect::<Vec<_>>();
            assert_eq!(ns, vec![2, 3]);
            assert_eq!(ns_win, ns);
        });
    }

    #[test]
    fn test_layer_explode() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, Some("layer1")).unwrap();
        graph.add_edge(1, 1, 2, NO_PROPS, Some("layer2")).unwrap();
        graph.add_edge(2, 1, 2, NO_PROPS, Some("layer1")).unwrap();
        graph.add_edge(3, 1, 2, NO_PROPS, None).unwrap();

        test_storage!(&graph, |graph| {
            let e = graph.edge(1, 2).expect("edge");

            let layer_exploded = e
                .explode_layers()
                .iter()
                .filter_map(|e| {
                    e.edge.layer().and_then(|layer| {
                        Some((e.src().id().as_u64()?, e.dst().id().as_u64()?, layer))
                    })
                })
                .collect::<Vec<_>>();

            assert_eq!(layer_exploded, vec![(1, 2, 1), (1, 2, 2), (1, 2, 3)]);
        });
    }

    #[test]
    fn test_layer_explode_window() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, Some("layer1")).unwrap();
        graph.add_edge(1, 1, 2, NO_PROPS, Some("layer2")).unwrap();
        graph.add_edge(2, 1, 2, NO_PROPS, Some("layer1")).unwrap();
        graph.add_edge(3, 1, 2, NO_PROPS, None).unwrap();

        test_storage!(&graph, |graph| {
            let g = graph.window(0, 3);
            let e = g.edge(1, 2).expect("edge");

            let layer_exploded = e
                .explode_layers()
                .iter()
                .filter_map(|e| {
                    e.layer_name()
                        .ok()
                        .map(|layer| (e.src().id(), e.dst().id(), layer))
                })
                .collect::<Vec<_>>();

            assert_eq!(
                layer_exploded,
                vec![
                    (GID::U64(1), GID::U64(2), ArcStr::from("layer1")),
                    (GID::U64(1), GID::U64(2), ArcStr::from("layer2"))
                ]
            );
        });
    }

    #[test]
    fn test_layer_explode_stacking() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, Some("layer1")).unwrap();
        graph.add_edge(1, 1, 2, NO_PROPS, Some("layer2")).unwrap();
        graph.add_edge(2, 1, 2, NO_PROPS, Some("layer1")).unwrap();
        graph.add_edge(3, 1, 2, NO_PROPS, None).unwrap();

        test_storage!(&graph, |graph| {
            let e = graph.edge(1, 2).expect("edge");

            let layer_exploded = e
                .explode_layers()
                .iter()
                .flat_map(|e| {
                    e.explode().into_iter().filter_map(|e| {
                        e.layer_name()
                            .ok()
                            .zip(e.time().ok())
                            .map(|(layer, t)| (t, e.src().id(), e.dst().id(), layer))
                    })
                })
                .collect::<Vec<_>>();

            assert_eq!(
                layer_exploded,
                vec![
                    (0, 1, 2, "layer1"),
                    (2, 1, 2, "layer1"),
                    (1, 1, 2, "layer2"),
                    (3, 1, 2, "_default"),
                ]
                .into_iter()
                .map(|(a, b, c, d)| (a, GID::U64(b), GID::U64(c), ArcStr::from(d)))
                .collect::<Vec<_>>()
            );
        });
    }

    #[test]
    fn test_layer_explode_stacking_window() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, Some("layer1")).unwrap();
        graph.add_edge(1, 1, 2, NO_PROPS, Some("layer2")).unwrap();
        graph.add_edge(2, 1, 2, NO_PROPS, Some("layer1")).unwrap();
        graph.add_edge(3, 1, 2, NO_PROPS, None).unwrap();

        test_storage!(&graph, |graph| {
            let g = graph.window(0, 3);
            let e = g.edge(1, 2).expect("edge");

            let layer_exploded = e
                .explode_layers()
                .iter()
                .flat_map(|e| {
                    e.explode().into_iter().filter_map(|e| {
                        e.layer_name()
                            .ok()
                            .zip(e.time().ok())
                            .map(|(layer, t)| (t, e.src().id(), e.dst().id(), layer))
                    })
                })
                .collect::<Vec<_>>();

            assert_eq!(
                layer_exploded,
                vec![
                    (0, 1, 2, "layer1"),
                    (2, 1, 2, "layer1"),
                    (1, 1, 2, "layer2")
                ]
                .into_iter()
                .map(|(a, b, c, d)| { (a, GID::U64(b), GID::U64(c), ArcStr::from(d)) })
                .collect::<Vec<_>>()
            );
        });
    }

    #[test]
    fn test_multi_layer_degree() {
        let graph = Graph::new();
        graph.add_node(1, 2, NO_PROPS, None).unwrap();
        graph.add_node(1, 3, NO_PROPS, None).unwrap();
        graph.add_node(1, 4, NO_PROPS, None).unwrap();

        graph.add_edge(1, 1, 4, NO_PROPS, None).expect("failed");
        graph
            .add_edge(1, 1, 2, NO_PROPS, "eth".into())
            .expect("failed");
        graph
            .add_edge(1, 1, 3, NO_PROPS, "eth".into())
            .expect("failed");

        graph
            .add_edge(1, 2, 3, NO_PROPS, "eth".into())
            .expect("failed");
        graph.add_edge(1, 4, 3, NO_PROPS, None).expect("failed");

        test_storage!(&graph, |graph| {
            let actual = graph.node(1u64).map(|n| n.out_degree());
            assert_eq!(actual, Some(3));

            let actual = graph.node(3u64).map(|n| n.in_degree());
            assert_eq!(actual, Some(3));

            let actual = graph.node(3u64).map(|n| n.degree());
            assert_eq!(actual, Some(3));
        });
    }

    #[test]
    fn test_multiple_layers_fundamentals() {
        let graph = Graph::new();

        graph
            .add_edge(1, 1, 2, [("tx_sent", 10u64)], "btc".into())
            .expect("failed");
        graph
            .add_edge(1, 1, 2, [("tx_sent", 20u64)], "eth".into())
            .expect("failed");
        graph
            .add_edge(1, 1, 2, [("tx_sent", 70u64)], "tether".into())
            .expect("failed");

        test_storage!(&graph, |graph| {
            let e = graph.edge(1, 2).expect("failed to get edge");
            let sum: u64 = e
                .properties()
                .temporal()
                .get("tx_sent")
                .unwrap()
                .iter()
                .filter_map(|(_, prop)| prop.into_u64())
                .sum();

            assert_eq!(sum, 100);

            let lg = graph
                .layers(vec!["eth", "btc"])
                .expect("failed to layer graph");

            let e = lg.edge(1, 2).expect("failed to get edge");

            let sum_eth_btc: u64 = e
                .properties()
                .temporal()
                .get("tx_sent")
                .unwrap()
                .iter()
                .filter_map(|(_, prop)| prop.into_u64())
                .sum();

            assert_eq!(sum_eth_btc, 30);

            assert_eq!(lg.count_edges(), 1);

            let e = graph.edge(1, 2).expect("failed to get edge");

            let e_btc = e.layers("btc").expect("failed to get btc layer");
            let e_eth = e.layers("eth").expect("failed to get eth layer");

            let edge_btc_sum = e_btc
                .properties()
                .temporal()
                .get("tx_sent")
                .unwrap()
                .iter()
                .filter_map(|(_, prop)| prop.into_u64())
                .sum::<u64>();

            let edge_eth_sum = e_eth
                .properties()
                .temporal()
                .get("tx_sent")
                .unwrap()
                .iter()
                .filter_map(|(_, prop)| prop.into_u64())
                .sum::<u64>();

            assert!(edge_btc_sum < edge_eth_sum);

            let e_eth = e_eth
                .layers(vec!["eth", "btc"])
                .expect("failed to get eth,btc layers");

            let eth_sum = e_eth
                .properties()
                .temporal()
                .get("tx_sent")
                .unwrap()
                .iter()
                .filter_map(|(_, prop)| prop.into_u64())
                .sum::<u64>();

            // layer does not have a way to reset yet!
            assert_eq!(eth_sum, 20);
        });
    }

    #[test]
    fn test_unique_layers() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, Some("layer1")).unwrap();
        graph.add_edge(0, 1, 2, NO_PROPS, Some("layer2")).unwrap();

        test_storage!(&graph, |graph| {
            assert_eq!(
                graph
                    .layers("layer2")
                    .unwrap()
                    .unique_layers()
                    .collect_vec(),
                vec!["layer2"]
            )
        });
    }

    #[quickcheck]
    fn node_from_id_is_consistent(nodes: Vec<u64>) -> bool {
        let g = Graph::new();
        for v in nodes.iter() {
            g.add_node(0, *v, NO_PROPS, None).unwrap();
        }
        g.nodes()
            .name()
            .into_iter_values()
            .map(|name| g.node(name))
            .all(|v| v.is_some())
    }

    #[test]
    fn large_id_is_consistent() {
        global_info_logger();
        let g = Graph::new();
        g.add_node(0, 10000000000000000006, NO_PROPS, None).unwrap();
        info!("names: {:?}", g.nodes().name().collect_vec());
        assert!(g
            .nodes()
            .name()
            .into_iter_values()
            .map(|name| g.node(name))
            .all(|v| v.is_some()))
    }

    #[quickcheck]
    fn exploded_edge_times_is_consistent(edges: Vec<(u64, u64, Vec<i64>)>, offset: i64) -> bool {
        check_exploded_edge_times_is_consistent(edges, offset)
    }

    #[test]
    fn exploded_edge_times_is_consistent_1() {
        let edges = vec![(0, 0, vec![0, 1])];
        assert!(check_exploded_edge_times_is_consistent(edges, 0));
    }

    fn check_exploded_edge_times_is_consistent(
        edges: Vec<(u64, u64, Vec<i64>)>,
        offset: i64,
    ) -> bool {
        global_info_logger();
        let mut correct = true;
        let mut check = |condition: bool, message: String| {
            if !condition {
                error!("Failed: {}", message);
            }
            correct = correct && condition;
        };
        // checks that exploded edges are preserved with correct timestamps
        let mut edges: Vec<(GID, GID, Vec<i64>)> = edges
            .into_iter()
            .filter_map(|(src, dst, mut ts)| {
                ts.sort();
                ts.dedup();
                let ts: Vec<_> = ts.into_iter().filter(|&t| t < i64::MAX).collect();
                (!ts.is_empty()).then_some((GID::U64(src), GID::U64(dst), ts))
            })
            .collect();
        edges.sort();
        edges.dedup_by_key(|(src, dst, _)| src.as_u64().zip(dst.as_u64()));

        let g = Graph::new();
        for (src, dst, times) in edges.iter() {
            for &t in times.iter() {
                if t < i64::MAX {
                    g.add_edge(t, src, dst, NO_PROPS, None).unwrap();
                }
            }
        }

        let mut actual_edges: Vec<(GID, GID, Vec<i64>)> = g
            .edges()
            .iter()
            .map(|e| {
                (
                    e.src().id(),
                    e.dst().id(),
                    e.explode()
                        .iter()
                        .map(|ee| {
                            check(
                                ee.earliest_time() == ee.latest_time(),
                                format!("times mismatched for {:?}", ee),
                            ); // times are the same for exploded edge
                            let t = ee.earliest_time().unwrap();
                            check(
                                ee.at(t).is_active(),
                                format!("exploded edge {:?} inactive at {}", ee, t),
                            );
                            let t_test = t.saturating_add(offset);
                            if t_test != t && t_test < i64::MAX && t_test > i64::MIN {
                                check(
                                    !ee.at(t_test).is_active(),
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

    #[test]
    fn test_one_hop_filter_reset() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, [("layer", 1)], Some("1")).unwrap();
        graph.add_edge(1, 1, 3, [("layer", 1)], Some("1")).unwrap();
        graph.add_edge(1, 2, 3, [("layer", 2)], Some("2")).unwrap();
        graph.add_edge(2, 3, 4, [("layer", 2)], Some("2")).unwrap();
        graph.add_edge(0, 1, 3, [("layer", 2)], Some("2")).unwrap();

        test_storage!(&graph, |graph| {
            let v = graph.node(1).unwrap();

            // filtering resets on neighbours
            let out_out: Vec<_> = v
                .at(0)
                .layers("1")
                .unwrap()
                .out_neighbours()
                .layers("2")
                .unwrap()
                .out_neighbours()
                .id()
                .collect();
            assert_eq!(out_out, [GID::U64(3)]);

            let out_out: Vec<_> = v
                .at(0)
                .layers("1")
                .unwrap()
                .out_neighbours()
                .layers("2")
                .unwrap()
                .out_edges()
                .properties()
                .flat_map(|p| p.get("layer").into_i32())
                .collect();
            assert_eq!(out_out, [2]);

            // filter applies to edges
            let layers: Vec<_> = v
                .layers("1")
                .unwrap()
                .edges()
                .layer_names()
                .flatten()
                .dedup()
                .collect();
            assert_eq!(layers, ["1"]);

            // graph level filter is preserved
            let out_out_2: Vec<_> = graph
                .at(0)
                .node(1)
                .unwrap()
                .layers("1")
                .unwrap()
                .out_neighbours()
                .layers("2")
                .unwrap()
                .out_neighbours()
                .id()
                .collect();
            assert!(out_out_2.is_empty());

            let v = graph.node(1).unwrap();
            let out_out: Vec<_> = v
                .at(0)
                .out_neighbours()
                .after(1)
                .out_neighbours()
                .id()
                .collect();
            assert_eq!(out_out, [GID::U64(4)]);

            let earliest_time = v
                .at(0)
                .out_neighbours()
                .after(1)
                .out_edges()
                .earliest_time()
                .flatten()
                .min();
            assert_eq!(earliest_time, Some(2));

            // dst and src on edge reset the filter
            let degrees: Vec<_> = v
                .at(0)
                .layers("1")
                .unwrap()
                .edges()
                .dst()
                .out_degree()
                .collect();
            assert_eq!(degrees, [1]);
        });
    }

    #[test]
    fn can_apply_algorithm_on_filtered_graph() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, [("layer", 1)], Some("1")).unwrap();
        graph.add_edge(1, 1, 3, [("layer", 1)], Some("1")).unwrap();
        graph.add_edge(1, 2, 3, [("layer", 2)], Some("2")).unwrap();
        graph.add_edge(2, 3, 4, [("layer", 2)], Some("2")).unwrap();
        graph.add_edge(0, 1, 3, [("layer", 2)], Some("2")).unwrap();

        test_storage!(&graph, |graph| {
            let wl = graph.window(0, 3).layers(vec!["1", "2"]).unwrap();
            assert_eq!(weakly_connected_components(&wl, 10, None).groups().len(), 1);
        });
    }

    #[test]
    #[cfg(feature = "proto")]
    fn save_load_serial() {
        let g = Graph::new();
        g.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("abcd11");
        g.encode(&file_path).unwrap();
        let gg = Graph::decode(file_path).unwrap();
        assert_graph_equal(&g, &gg);
    }

    #[test]
    fn test_node_type_changes() {
        let g = Graph::new();
        g.add_node(0, "A", NO_PROPS, Some("typeA")).unwrap();
        g.add_node(1, "A", NO_PROPS, None).unwrap();
        let node_a = g.node("A").unwrap();
        assert_eq!(node_a.node_type().as_str(), Some("typeA"));
        let result = g.add_node(2, "A", NO_PROPS, Some("typeB"));
        assert!(result.is_err());
    }

    #[test]
    fn test_layer_degree() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, NO_PROPS, Some("layer1")).unwrap();
        g.add_edge(1, 1, 2, NO_PROPS, Some("layer2")).unwrap();
        g.add_edge(2, 1, 3, NO_PROPS, Some("layer1")).unwrap();
        g.add_edge(3, 1, 2, NO_PROPS, None).unwrap();

        test_storage!(&g, |g| {
            let n = g.node(1).unwrap();
            let n_layer = n.layers("layer1").unwrap();
            assert_eq!(n_layer.out_degree(), 2);
            assert_eq!(n_layer.in_degree(), 0);
            assert_eq!(n_layer.degree(), 2);
        });
    }

    #[test]
    fn test_layer_name() {
        let graph = Graph::new();

        graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        graph
            .add_edge(0, 0, 2, NO_PROPS, Some("awesome layer"))
            .unwrap();

        test_storage!(&graph, |graph| {
            assert_eq!(graph.edge(0, 1).unwrap().layer_names(), ["_default"]);
            assert_eq!(graph.edge(0, 2).unwrap().layer_names(), ["awesome layer"]);
        });
    }

    #[test]
    fn test_type_filter() {
        let g = PersistentGraph::new();

        g.add_node(1, 1, NO_PROPS, Some("wallet")).unwrap();
        g.add_node(1, 2, NO_PROPS, Some("timer")).unwrap();
        g.add_node(1, 3, NO_PROPS, Some("timer")).unwrap();
        g.add_node(1, 4, NO_PROPS, Some("wallet")).unwrap();

        assert_eq!(
            g.nodes()
                .type_filter(["wallet"])
                .name()
                .into_iter_values()
                .collect_vec(),
            vec!["1", "4"]
        );

        let g = Graph::new();
        g.add_node(1, 1, NO_PROPS, Some("a")).unwrap();
        g.add_node(1, 2, NO_PROPS, Some("b")).unwrap();
        g.add_node(1, 3, NO_PROPS, Some("b")).unwrap();
        g.add_node(1, 4, NO_PROPS, Some("a")).unwrap();
        g.add_node(1, 5, NO_PROPS, Some("c")).unwrap();
        g.add_node(1, 6, NO_PROPS, Some("e")).unwrap();
        g.add_node(1, 7, NO_PROPS, None).unwrap();
        g.add_node(1, 8, NO_PROPS, None).unwrap();
        g.add_node(1, 9, NO_PROPS, None).unwrap();
        g.add_edge(2, 1, 2, NO_PROPS, Some("a")).unwrap();
        g.add_edge(2, 3, 2, NO_PROPS, Some("a")).unwrap();
        g.add_edge(2, 2, 4, NO_PROPS, Some("a")).unwrap();
        g.add_edge(2, 4, 5, NO_PROPS, Some("a")).unwrap();
        g.add_edge(2, 4, 5, NO_PROPS, Some("a")).unwrap();
        g.add_edge(2, 5, 6, NO_PROPS, Some("a")).unwrap();
        g.add_edge(2, 3, 6, NO_PROPS, Some("a")).unwrap();

        assert_eq!(
            g.nodes()
                .type_filter(["a", "b", "c", "e"])
                .name()
                .collect_vec(),
            vec!["1", "2", "3", "4", "5", "6"]
        );

        assert_eq!(
            g.nodes()
                .type_filter(Vec::<String>::new())
                .name()
                .collect_vec(),
            Vec::<String>::new()
        );

        assert_eq!(
            g.nodes().type_filter([""]).name().collect_vec(),
            vec!["7", "8", "9"]
        );

        let w = g.window(1, 4);
        assert_eq!(
            w.nodes()
                .type_filter(["a"])
                .iter()
                .map(|v| v.degree())
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            w.nodes()
                .type_filter(["a"])
                .neighbours()
                .type_filter(["c", "b"])
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec!["2"], vec!["2", "5"]]
        );

        let l = g.layers(["a"]).unwrap();
        assert_eq!(
            l.nodes()
                .type_filter(["a"])
                .iter()
                .map(|v| v.degree())
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            l.nodes()
                .type_filter(["a"])
                .neighbours()
                .type_filter(["c", "b"])
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec!["2"], vec!["2", "5"]]
        );

        let sg = g.subgraph([1, 2, 3, 4, 5, 6]);
        assert_eq!(
            sg.nodes()
                .type_filter(["a"])
                .iter()
                .map(|v| v.degree())
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            sg.nodes()
                .type_filter(["a"])
                .neighbours()
                .type_filter(["c", "b"])
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec!["2"], vec!["2", "5"]]
        );

        assert_eq!(
            g.nodes().iter().map(|v| v.degree()).collect::<Vec<_>>(),
            vec![1, 3, 2, 2, 2, 2, 0, 0, 0]
        );
        assert_eq!(
            g.nodes()
                .type_filter(["a"])
                .iter()
                .map(|v| v.degree())
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            g.nodes()
                .type_filter(["d"])
                .iter()
                .map(|v| v.degree())
                .collect::<Vec<_>>(),
            Vec::<usize>::new()
        );
        assert_eq!(
            g.nodes()
                .type_filter(["a"])
                .par_iter()
                .map(|v| v.degree())
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            g.nodes()
                .type_filter(["d"])
                .par_iter()
                .map(|v| v.degree())
                .collect::<Vec<_>>(),
            Vec::<usize>::new()
        );

        assert_eq!(
            g.nodes()
                .type_filter(["a"])
                .collect()
                .into_iter()
                .map(|n| n.name())
                .collect_vec(),
            vec!["1", "4"]
        );
        assert_eq!(
            g.nodes()
                .type_filter(Vec::<&str>::new())
                .collect()
                .into_iter()
                .map(|n| n.name())
                .collect_vec(),
            Vec::<&str>::new()
        );

        assert_eq!(g.nodes().len(), 9);
        assert_eq!(g.nodes().type_filter(["b"]).len(), 2);
        assert_eq!(g.nodes().type_filter(["d"]).len(), 0);

        assert!(!g.nodes().is_empty());
        assert!(g.nodes().type_filter(["d"]).is_empty());

        assert_eq!(
            g.nodes()
                .type_filter(["a"])
                .name()
                .into_iter_values()
                .collect_vec(),
            vec!["1", "4"]
        );
        assert_eq!(
            g.nodes()
                .type_filter(["a", "c"])
                .name()
                .into_iter_values()
                .collect_vec(),
            vec!["1", "4", "5"]
        );

        assert_eq!(
            g.nodes()
                .type_filter(["a"])
                .neighbours()
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec!["2"], vec!["2", "5"]]
        );
        assert_eq!(
            g.nodes()
                .type_filter(["a", "c"])
                .neighbours()
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec!["2"], vec!["2", "5"], vec!["4", "6"]]
        );
        assert_eq!(
            g.nodes()
                .type_filter(["d"])
                .neighbours()
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            Vec::<Vec<&str>>::new()
        );

        assert_eq!(
            g.nodes()
                .type_filter(["a"])
                .neighbours()
                .type_filter(["c"])
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec![], vec!["5"]]
        );
        assert_eq!(
            g.nodes()
                .type_filter(["a"])
                .neighbours()
                .type_filter(Vec::<&str>::new())
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec![], Vec::<&str>::new()]
        );
        assert_eq!(
            g.nodes()
                .type_filter(["a"])
                .neighbours()
                .type_filter(["c", "b"])
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec!["2"], vec!["2", "5"]]
        );
        assert_eq!(
            g.nodes()
                .type_filter(["a"])
                .neighbours()
                .type_filter(["d"])
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec![], Vec::<&str>::new()]
        );

        assert_eq!(
            g.nodes()
                .type_filter(["a"])
                .neighbours()
                .neighbours()
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec!["1", "3", "4"], vec!["1", "3", "4", "4", "6"]]
        );

        assert_eq!(
            g.nodes()
                .type_filter(["a"])
                .neighbours()
                .type_filter(["c"])
                .neighbours()
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec![], vec!["4", "6"]]
        );

        assert_eq!(
            g.nodes()
                .neighbours()
                .neighbours()
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![
                vec!["1", "3", "4"],
                vec!["2", "2", "6", "2", "5"],
                vec!["1", "3", "4", "3", "5"],
                vec!["1", "3", "4", "4", "6"],
                vec!["2", "5", "3", "5"],
                vec!["2", "6", "4", "6"],
                vec![],
                vec![],
                vec![],
            ]
        );

        assert_eq!(
            g.nodes()
                .type_filter(["a"])
                .neighbours()
                .type_filter(["d"])
                .total_count(),
            0
        );

        assert!(g
            .nodes()
            .type_filter(["a"])
            .neighbours()
            .type_filter(["d"])
            .is_all_empty());

        assert_eq!(
            g.nodes()
                .type_filter(["a"])
                .neighbours()
                .type_filter(["d"])
                .iter()
                .map(|n| { n.name().collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec![], Vec::<&str>::new()]
        );

        assert_eq!(
            g.nodes()
                .type_filter(["a"])
                .neighbours()
                .type_filter(["b"])
                .collect()
                .into_iter()
                .flatten()
                .map(|n| n.name())
                .collect_vec(),
            vec!["2", "2"]
        );

        assert_eq!(
            g.nodes()
                .type_filter(["a"])
                .neighbours()
                .type_filter(["d"])
                .collect()
                .into_iter()
                .flatten()
                .map(|n| n.name())
                .collect_vec(),
            Vec::<&str>::new()
        );

        assert_eq!(
            g.node("2").unwrap().neighbours().name().collect_vec(),
            vec!["1", "3", "4"]
        );

        assert_eq!(
            g.node("2")
                .unwrap()
                .neighbours()
                .type_filter(["b"])
                .name()
                .collect_vec(),
            vec!["3"]
        );

        assert_eq!(
            g.node("2")
                .unwrap()
                .neighbours()
                .type_filter(["d"])
                .name()
                .collect_vec(),
            Vec::<&str>::new()
        );

        assert_eq!(
            g.node("2")
                .unwrap()
                .neighbours()
                .type_filter(["c", "a"])
                .name()
                .collect_vec(),
            vec!["1", "4"]
        );

        assert_eq!(
            g.node("2")
                .unwrap()
                .neighbours()
                .type_filter(["c"])
                .neighbours()
                .name()
                .collect_vec(),
            Vec::<&str>::new()
        );

        assert_eq!(
            g.node("2")
                .unwrap()
                .neighbours()
                .neighbours()
                .name()
                .collect_vec(),
            vec!["2", "2", "6", "2", "5"],
        );

        assert_eq!(
            g.node("2").unwrap().neighbours().type_filter(["d"]).len(),
            0
        );

        assert_eq!(
            g.node("2")
                .unwrap()
                .neighbours()
                .type_filter(["a"])
                .neighbours()
                .len(),
            3
        );

        assert!(g
            .node("2")
            .unwrap()
            .neighbours()
            .type_filter(["d"])
            .is_empty());

        assert!(!g
            .node("2")
            .unwrap()
            .neighbours()
            .type_filter(["a"])
            .neighbours()
            .is_empty());

        assert!(g
            .node("2")
            .unwrap()
            .neighbours()
            .type_filter(["d"])
            .neighbours()
            .is_empty());

        assert!(g
            .node("2")
            .unwrap()
            .neighbours()
            .type_filter(["d"])
            .iter()
            .collect_vec()
            .is_empty(),);

        assert_eq!(
            g.node("2")
                .unwrap()
                .neighbours()
                .type_filter(["b"])
                .collect()
                .into_iter()
                .map(|n| n.name())
                .collect_vec(),
            vec!["3"]
        );

        assert_eq!(
            g.node("2")
                .unwrap()
                .neighbours()
                .type_filter(["d"])
                .collect()
                .into_iter()
                .map(|n| n.name())
                .collect_vec(),
            Vec::<&str>::new()
        );
    }

    #[test]
    fn test_persistent_graph() {
        let g = Graph::new();
        g.add_edge(0, 0, 1, [("added", Prop::I64(0))], None)
            .unwrap();
        assert_eq!(
            g.edges().id().collect::<Vec<_>>(),
            vec![(GID::U64(0), GID::U64(1))]
        );

        let pg = g.persistent_graph();
        pg.delete_edge(10, 0, 1, None).unwrap();
        assert_eq!(
            g.edges().id().collect::<Vec<_>>(),
            vec![(GID::U64(0), GID::U64(1))]
        );
    }

    #[test]
    fn test_unique_property() {
        let g = Graph::new();
        g.add_edge(1, 1, 2, [("status", "open")], None).unwrap();
        g.add_edge(2, 1, 2, [("status", "open")], None).unwrap();
        g.add_edge(3, 1, 2, [("status", "review")], None).unwrap();
        g.add_edge(4, 1, 2, [("status", "open")], None).unwrap();
        g.add_edge(5, 1, 2, [("status", "in-progress")], None)
            .unwrap();
        g.add_edge(10, 1, 2, [("status", "in-progress")], None)
            .unwrap();
        g.add_edge(9, 1, 2, [("state", true)], None).unwrap();
        g.add_edge(10, 1, 2, [("state", false)], None).unwrap();
        g.add_edge(6, 1, 2, NO_PROPS, None).unwrap();

        let mut props = g
            .edge(1, 2)
            .unwrap()
            .properties()
            .temporal()
            .get("status")
            .unwrap()
            .unique()
            .into_iter()
            .map(|x| x.unwrap_str().to_string())
            .collect_vec();
        props.sort();
        assert_eq!(props, vec!["in-progress", "open", "review"]);

        let ordered_dedupe_latest = g
            .edge(1, 2)
            .unwrap()
            .properties()
            .temporal()
            .get("status")
            .unwrap()
            .ordered_dedupe(true)
            .into_iter()
            .map(|(x, y)| (x, y.unwrap_str().to_string()))
            .collect_vec();

        assert_eq!(
            ordered_dedupe_latest,
            vec![
                (2, "open".to_string()),
                (3, "review".to_string()),
                (4, "open".to_string()),
                (10, "in-progress".to_string()),
            ]
        );

        let ordered_dedupe_earliest = g
            .edge(1, 2)
            .unwrap()
            .properties()
            .temporal()
            .get("status")
            .unwrap()
            .ordered_dedupe(false)
            .into_iter()
            .map(|(x, y)| (x, y.unwrap_str().to_string()))
            .collect_vec();

        assert_eq!(
            ordered_dedupe_earliest,
            vec![
                (1, "open".to_string()),
                (3, "review".to_string()),
                (4, "open".to_string()),
                (5, "in-progress".to_string()),
            ]
        );
    }

    #[test]
    fn num_locks_same_as_threads() {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(5)
            .build()
            .unwrap();
        let graph = pool.install(Graph::new);
        assert_eq!(graph.core_graph().unfiltered_num_nodes(), 0);
    }

    #[test]
    fn test_create_node() {
        let g = Graph::new();
        g.create_node(0, 1, [("test", Prop::Bool(true))], None)
            .unwrap();

        let n = g.node(1).unwrap();

        assert_eq!(n.id().as_u64().unwrap(), 1);
        assert_eq!(n.properties().get("test").unwrap(), Prop::Bool(true));

        let result = g.create_node(1, 1, [("test".to_string(), Prop::Bool(true))], None);
        assert!(matches!(result, Err(GraphError::NodeExistsError(id)) if id == GID::U64(1)));
    }

    #[test]
    fn test_materialize_constant_edge_props() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, NO_PROPS, Some("a")).unwrap();
        let e = g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        e.add_constant_properties([("test", "test")], None).unwrap();
        g.add_edge(10, 1, 2, NO_PROPS, Some("a")).unwrap();

        let gw = g.after(1);
        let gmw = gw.materialize().unwrap();

        assert_graph_equal(&gw, &gmw);
    }

    #[test]
    fn materialize_window_prop_test() {
        proptest!(|(graph_f in build_graph_strat(10, 10, true), w in any::<Range<i64>>())| {
            let g = Graph::from(build_graph(&graph_f));
            let gw = g.window(w.start, w.end);
            let gmw = gw.materialize().unwrap();
            assert_graph_equal(&gw, &gmw);
        })
    }

    #[test]
    fn materialize_temporal_properties_one_edge() {
        let g = Graph::new();
        g.add_edge(
            0,
            0,
            0,
            [("3", Prop::I64(1)), ("0", Prop::str("baa"))],
            Some("a"),
        )
        .unwrap();

        let gw = g.window(-9, 3);
        let gmw = gw.materialize().unwrap();

        assert_graph_equal(&gw, &gmw);
    }

    #[test]
    fn materialize_one_node() {
        let g = Graph::new();
        g.add_node(0, 0, NO_PROPS, None).unwrap();

        let n = g.node(0).unwrap();
        let hist = n.history();
        assert!(!hist.is_empty());
        let rows = n.rows().collect::<Vec<_>>();
        assert!(!rows.is_empty());

        let gw = g.window(0, 1);
        let gmw = gw.materialize().unwrap();

        assert_graph_equal(&gw, &gmw);
    }

    #[test]
    fn materialize_some_edges() -> Result<(), GraphError> {
        let edges1_props = EdgeUpdatesFixture {
            props: PropUpdatesFixture {
                t_props: vec![
                    (2433054617899119663, vec![]),
                    (
                        5623371002478468619,
                        vec![("0".to_owned(), Prop::I64(-180204069376666762))],
                    ),
                ],
                c_props: vec![],
            },
            deletions: vec![-3684372592923241629, 3668280323305195349],
        };

        let edges2_props = EdgeUpdatesFixture {
            props: PropUpdatesFixture {
                t_props: vec![
                    (
                        -7888823724540213280,
                        vec![("0".to_owned(), Prop::I64(1339447446033500001))],
                    ),
                    (-3792330935693192039, vec![]),
                    (
                        4049942931077033460,
                        vec![("0".to_owned(), Prop::I64(-544773539725842277))],
                    ),
                    (5085404190610173488, vec![]),
                    (1445770503123270290, vec![]),
                    (-5628624083683143619, vec![]),
                    (-394401628579820652, vec![]),
                    (-2398199704888544233, vec![]),
                ],
                c_props: vec![("0".to_owned(), Prop::I64(-1877019573933389749))],
            },
            deletions: vec![
                3969804007878301015,
                7040207277685112004,
                7380699292468575143,
                3332576590029503186,
                -1107894292705275349,
                6647229517972286485,
                6359226207899406831,
            ],
        };

        let edges: EdgeFixture = [
            ((2, 7, Some("b")), edges1_props),
            ((7, 2, Some("a")), edges2_props),
        ]
        .into_iter()
        .collect();

        let w = -3619743214445905380..90323088878877991;
        let graph_f = GraphFixture {
            nodes: NodeFixture::default(),
            edges,
        };
        for _ in 0..200 {
            let g = Graph::from(build_graph(&graph_f));
            let gw = g.window(w.start, w.end);
            let gmw = gw.materialize()?;
            assert_graph_equal(&gw, &gmw);
        }
        Ok(())
    }

    #[test]
    fn materialize_window_delete_test() {
        let g = Graph::new();
        g.delete_edge(0, 0, 0, Some("a")).unwrap();
        let w = 0..1;
        let gw = g.window(w.start, w.end);
        let gmw = gw.materialize().unwrap();
        assert_graph_equal(&gw, &gmw);
    }

    #[test]
    fn test_multilayer() {
        let g = Graph::new();
        g.add_edge(0, 0, 0, NO_PROPS, None).unwrap();
        g.add_edge(1, 0, 0, NO_PROPS, Some("a")).unwrap();
        let gw = g.window(0, 1);

        let expected = Graph::new();
        expected.add_edge(0, 0, 0, NO_PROPS, None).unwrap();
        expected.resolve_layer(Some("a")).unwrap();
        assert_graph_equal(&gw, &expected);
    }

    #[test]
    fn test_empty_window() {
        let g = Graph::new();
        g.add_edge(0, 0, 0, NO_PROPS, None).unwrap();
        let gw = g.window(-1, 0);

        assert!(g.window(-1, 0).nodes().is_empty());
        assert_eq!(g.window(-1, 0).count_nodes(), 0);
        for layer in gw.unique_layers() {
            let layered = gw.valid_layers(layer);
            assert_eq!(layered.count_nodes(), 0);
        }
    }

    #[test]
    fn add_edge_and_read_props_concurrent() {
        let g = Graph::new();
        for t in 0..1000 {
            join(
                || g.add_edge(t, 1, 2, [("test", true)], None).unwrap(),
                || {
                    // if the edge exists already, it should have the property set
                    g.window(t, t + 1)
                        .edge(1, 2)
                        .map(|e| assert!(e.properties().get("test").is_some()))
                },
            );
        }
    }
}
