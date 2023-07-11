//! Defines the `Graph` struct, which represents a raphtory graph in memory.
//!
//! This is the base class used to create a temporal graph, add vertices and edges,
//! create windows, and query the graph with a variety of algorithms.
//! It is a wrapper around a set of shards, which are the actual graph data structures.
//!
//! # Examples
//!
//! ```rust
//! use raphtory::prelude::*;
//! let graph = Graph::new();
//! graph.add_vertex(0, "Alice", NO_PROPS).unwrap();
//! graph.add_vertex(1, "Bob", NO_PROPS).unwrap();
//! graph.add_edge(2, "Alice", "Bob", NO_PROPS, None).unwrap();
//! graph.num_edges();
//! ```
//!

use crate::db::api::properties::internal::InheritPropertiesOps;
use crate::{
    core::{entities::graph::tgraph::InnerTemporalGraph, utils::errors::GraphError},
    db::api::{
        mutation::internal::{InheritAdditionOps, InheritPropertyAdditionOps},
        view::internal::{Base, DynamicGraph, InheritViewOps, IntoDynamic},
    },
    prelude::*,
};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, Formatter},
    path::Path,
    sync::Arc,
};

const SEG: usize = 16;
pub(crate) type InternalGraph = InnerTemporalGraph<SEG>;

#[repr(transparent)]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Graph(Arc<InternalGraph>);

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

impl<G: GraphViewOps> PartialEq<G> for Graph {
    fn eq(&self, other: &G) -> bool {
        graph_equal(self, other)
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
    /// use raphtory::prelude::Graph;
    /// let g = Graph::new();
    /// ```
    pub fn new() -> Self {
        Self(Arc::new(InternalGraph::default()))
    }

    pub(crate) fn new_from_inner(inner: Arc<InternalGraph>) -> Self {
        Self(inner)
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
    /// ```no_run
    /// use raphtory::prelude::Graph;
    /// let g = Graph::load_from_file("path/to/graph");
    /// ```
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, GraphError> {
        Ok(Self(Arc::new(InternalGraph::load_from_file(path)?)))
    }

    /// Save a graph to a directory
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), GraphError> {
        self.0.save_to_file(path)?;
        Ok(())
    }

    pub fn as_arc(&self) -> Arc<InternalGraph> {
        self.0.clone()
    }
}

impl IntoDynamic for Graph {
    fn into_dynamic(self) -> DynamicGraph {
        DynamicGraph::new(self)
    }
}

#[cfg(test)]
mod db_tests {
    use super::*;
    use crate::db::api::mutation::Properties;
    use crate::{
        core::{
            entities::{edges::edge_ref::EdgeRef, vertices::vertex_ref::VertexRef},
            utils::time::{error::ParseTimeError, TryIntoTime},
            Direction, Prop,
        },
        db::{
            api::view::{
                internal::*, EdgeListOps, EdgeViewOps, GraphViewOps, LayerOps, TimeOps,
                VertexViewOps,
            },
            graph::{edge::EdgeView, path::PathFromVertex},
        },
        graphgen::random_attachment::random_attachment,
        prelude::{AdditionOps, PropertyAdditionOps},
    };
    use chrono::NaiveDateTime;
    use itertools::Itertools;
    use quickcheck::Arbitrary;
    use std::collections::{HashMap, HashSet};
    use tempdir::TempDir;

    #[quickcheck]
    fn add_vertex_grows_graph_len(vs: Vec<(i64, u64)>) {
        let g = Graph::new();

        let expected_len = vs.iter().map(|(_, v)| v).sorted().dedup().count();
        for (t, v) in vs {
            g.add_vertex(t, v, NO_PROPS)
                .map_err(|err| println!("{:?}", err))
                .ok();
        }

        assert_eq!(g.num_vertices(), expected_len)
    }

    #[quickcheck]
    fn add_vertex_gets_names(vs: Vec<String>) -> bool {
        let g = Graph::new();

        let expected_len = vs.iter().sorted().dedup().count();
        for (t, name) in vs.iter().enumerate() {
            g.add_vertex(t as i64, name.clone(), NO_PROPS)
                .map_err(|err| println!("{:?}", err))
                .ok();
        }

        assert_eq!(g.num_vertices(), expected_len);

        vs.iter().all(|name| {
            let v = g.vertex(name.clone()).unwrap();
            v.name() == name.clone()
        })
    }

    #[quickcheck]
    fn add_edge_grows_graph_edge_len(edges: Vec<(i64, u64, u64)>) {
        let g = Graph::new();

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
            g.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }

        assert_eq!(g.num_vertices(), unique_vertices_count);
        assert_eq!(g.num_edges(), unique_edge_count);
    }

    #[quickcheck]
    fn add_edge_works(edges: Vec<(i64, u64, u64)>) -> bool {
        let g = Graph::new();
        for &(t, src, dst) in edges.iter() {
            g.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }

        edges
            .iter()
            .all(|&(_, src, dst)| g.has_edge(src, dst, None))
    }

    #[quickcheck]
    fn get_edge_works(edges: Vec<(i64, u64, u64)>) -> bool {
        let g = Graph::new();
        for &(t, src, dst) in edges.iter() {
            g.add_edge(t, src, dst, NO_PROPS, None).unwrap();
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

        let g = Graph::new();

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }

        let tmp_raphtory_path: TempDir =
            TempDir::new("raphtory").expect("Failed to create tempdir");

        let graph_path = format!("{}/graph.bin", tmp_raphtory_path.path().display());
        g.save_to_file(&graph_path).expect("Failed to save graph");

        // Load from files
        let g2 = Graph::load_from_file(&graph_path).expect("Failed to load graph");

        assert_eq!(g, g2);

        let _ = tmp_raphtory_path.close();
    }

    #[test]
    fn has_edge() {
        let g = Graph::new();
        g.add_edge(1, 7, 8, NO_PROPS, None).unwrap();

        assert!(!g.has_edge(8, 7, None));
        assert!(g.has_edge(7, 8, None));

        g.add_edge(1, 7, 9, NO_PROPS, None).unwrap();

        assert!(!g.has_edge(9, 7, None));
        assert!(g.has_edge(7, 9, None));

        g.add_edge(2, "haaroon", "northLondon", NO_PROPS, None)
            .unwrap();
        assert!(g.has_edge("haaroon", "northLondon", None));
    }

    #[test]
    fn graph_edge() {
        let g = Graph::new();
        let es = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];
        for (t, src, dst) in es {
            g.add_edge(t, src, dst, NO_PROPS, None).unwrap()
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

        let g = Graph::new();

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
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
        let g = Graph::new();

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
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

        let g = Graph::new();

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
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
        let g = Graph::new();

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
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
        let g = Graph::new();

        assert_eq!(g.latest_time(), None);
        assert_eq!(g.earliest_time(), None);

        g.add_vertex(5, 1, NO_PROPS)
            .map_err(|err| println!("{:?}", err))
            .ok();

        assert_eq!(g.latest_time(), Some(5));
        assert_eq!(g.earliest_time(), Some(5));

        let g = Graph::new();

        g.add_edge(10, 1, 2, NO_PROPS, None).unwrap();
        assert_eq!(g.latest_time(), Some(10));
        assert_eq!(g.earliest_time(), Some(10));

        g.add_vertex(5, 1, NO_PROPS)
            .map_err(|err| println!("{:?}", err))
            .ok();
        assert_eq!(g.latest_time(), Some(10));
        assert_eq!(g.earliest_time(), Some(5));

        g.add_edge(20, 3, 4, NO_PROPS, None).unwrap();
        assert_eq!(g.latest_time(), Some(20));
        assert_eq!(g.earliest_time(), Some(5));

        random_attachment(&g, 100, 10);
        assert_eq!(g.latest_time(), Some(126));
        assert_eq!(g.earliest_time(), Some(5));
    }

    #[test]
    fn static_properties() {
        let g = Graph::new(); // big enough so all edges are very likely remote
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
        g.add_vertex(0, 11, vec![("temp".to_string(), Prop::Bool(true))])
            .unwrap();
        let v11 = g.vertex_ref(11).unwrap();
        let v22 = g.vertex_ref(22).unwrap();
        let v33 = g.vertex_ref(33).unwrap();
        let edge1111 = g.edge_ref(v11.into(), v11.into(), 0).unwrap();
        let edge2233 = g.edge_ref(v22.into(), v33.into(), 0).unwrap();
        let edge3311 = g.edge_ref(v33.into(), v11.into(), 0).unwrap();

        g.add_vertex_properties(
            11,
            vec![
                ("a".to_string(), Prop::U64(11)),
                ("b".to_string(), Prop::I64(11)),
            ],
        )
        .unwrap();
        g.add_vertex_properties(11, vec![("c".to_string(), Prop::U32(11))])
            .unwrap();
        g.add_vertex_properties(22, vec![("b".to_string(), Prop::U64(22))])
            .unwrap();
        g.add_edge_properties(11, 11, vec![("d".to_string(), Prop::U64(1111))], None)
            .unwrap();
        g.add_edge_properties(33, 11, vec![("a".to_string(), Prop::U64(3311))], None)
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
    fn temporal_props_vertex() {
        let g = Graph::new();

        g.add_vertex(0, 1, [("cool".to_string(), Prop::Bool(true))])
            .unwrap();

        let v = g.vertex(1).unwrap();

        let actual = v.properties().get("cool").and_then(|v| v.value());
        assert_eq!(actual, Some(Prop::Bool(true)));

        // we flip cool from true to false after t 3
        let _ = g
            .add_vertex(3, 1, [("cool".to_string(), Prop::Bool(false))])
            .unwrap();

        let wg = g.window(3, 15);
        let v = wg.vertex(1).unwrap();

        let actual = v.properties().get("cool").and_then(|v| v.value());
        assert_eq!(actual, Some(Prop::Bool(false)));

        let hist: Vec<_> = v.properties().get("cool").unwrap().iter().collect();
        assert_eq!(hist, vec![(3, Prop::Bool(false))]);

        let v = g.vertex(1).unwrap();

        let hist: Vec<_> = v.properties().get("cool").unwrap().iter().collect();
        assert_eq!(hist, vec![(0, Prop::Bool(true)), (3, Prop::Bool(false))]);
    }

    #[test]
    fn temporal_props_edge() {
        let g = Graph::new();

        g.add_edge(1, 0, 1, vec![("distance".to_string(), Prop::U32(5))], None)
            .expect("add edge");

        let e = g.edge(0, 1, None).unwrap();

        let prop = e.properties().get("distance").unwrap().value().unwrap();
        assert_eq!(prop, Prop::U32(5));
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

        let g = Graph::new();

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }

        let local_1 = VertexRef::new_local(0.into());
        let local_2 = VertexRef::new_local(1.into());
        let local_3 = VertexRef::new_local(2.into());

        let expected = [
            (
                vec![local_1, local_2],
                vec![local_1, local_2, local_3],
                vec![local_1],
            ),
            (vec![local_1], vec![], vec![]),
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
        let g = Graph::new();

        let rolling = g.rolling(1, None).unwrap().collect_vec();
        assert!(rolling.is_empty());

        let expanding = g.expanding(1).unwrap().collect_vec();
        assert!(expanding.is_empty());
    }

    #[test]
    fn test_add_vertex_with_strings() {
        let g = Graph::new();

        g.add_vertex(0, "haaroon", NO_PROPS).unwrap();
        g.add_vertex(1, "hamza", NO_PROPS).unwrap();
        g.add_vertex(1, 831, NO_PROPS).unwrap();

        assert!(g.has_vertex(831));
        assert!(g.has_vertex("haaroon"));
        assert!(g.has_vertex("hamza"));

        assert_eq!(g.num_vertices(), 3);
    }

    #[test]
    fn layers() {
        let g = Graph::new();
        g.add_edge(0, 11, 22, NO_PROPS, None).unwrap();
        g.add_edge(0, 11, 33, NO_PROPS, None).unwrap();
        g.add_edge(0, 33, 11, NO_PROPS, None).unwrap();
        g.add_edge(0, 11, 22, NO_PROPS, Some("layer1")).unwrap();
        g.add_edge(0, 11, 33, NO_PROPS, Some("layer2")).unwrap();
        g.add_edge(0, 11, 44, NO_PROPS, Some("layer2")).unwrap();

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

        assert_eq!(g.num_vertices(), 4);
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
        let g = Graph::new();
        g.add_edge(0, 1, 2, vec![("weight".to_string(), Prop::I64(1))], None)
            .unwrap();
        g.add_edge(1, 1, 2, vec![("weight".to_string(), Prop::I64(2))], None)
            .unwrap();
        g.add_edge(2, 1, 2, vec![("weight".to_string(), Prop::I64(3))], None)
            .unwrap();

        let exploded = g.edge(1, 2, None).unwrap().explode();

        let res = exploded
            .map(|e| e.properties().collect_properties())
            .collect_vec();

        let mut expected = Vec::new();
        for i in 1..4 {
            let mut map = Vec::new();
            map.push(("weight".to_string(), Prop::I64(i)));
            expected.push(map);
        }

        assert_eq!(res, expected);

        let e = g
            .vertex(1)
            .unwrap()
            .edges()
            .explode()
            .map(|e| e.properties().collect_properties())
            .collect_vec();
        assert_eq!(e, expected);
    }

    #[test]
    fn test_edge_earliest_latest() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.add_edge(1, 1, 2, NO_PROPS, None).unwrap();
        g.add_edge(2, 1, 2, NO_PROPS, None).unwrap();
        g.add_edge(0, 1, 3, NO_PROPS, None).unwrap();
        g.add_edge(1, 1, 3, NO_PROPS, None).unwrap();
        g.add_edge(2, 1, 3, NO_PROPS, None).unwrap();

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
        let g = Graph::new();

        g.add_vertex(1, 1, NO_PROPS).unwrap();
        g.add_vertex(2, 1, NO_PROPS).unwrap();
        g.add_vertex(3, 1, NO_PROPS).unwrap();
        g.add_vertex(4, 1, NO_PROPS).unwrap();
        g.add_vertex(8, 1, NO_PROPS).unwrap();

        g.add_vertex(4, "Lord Farquaad", NO_PROPS).unwrap();
        g.add_vertex(6, "Lord Farquaad", NO_PROPS).unwrap();
        g.add_vertex(7, "Lord Farquaad", NO_PROPS).unwrap();
        g.add_vertex(8, "Lord Farquaad", NO_PROPS).unwrap();

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
        let g = Graph::new();

        g.add_edge(1, 1, 2, NO_PROPS, None).unwrap();
        g.add_edge(2, 1, 3, NO_PROPS, None).unwrap();
        g.add_edge(3, 1, 2, NO_PROPS, None).unwrap();
        g.add_edge(4, 1, 4, NO_PROPS, None).unwrap();

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
        let g = Graph::new();

        g.add_edge(1, 1, 2, NO_PROPS, None).unwrap();
        g.add_edge(2, 1, 3, NO_PROPS, None).unwrap();
        g.add_edge(3, 1, 2, NO_PROPS, None).unwrap();
        g.add_edge(4, 1, 4, NO_PROPS, None).unwrap();
        g.add_edge(5, 1, 4, NO_PROPS, None).unwrap();
        g.add_edge(6, 1, 4, NO_PROPS, None).unwrap();
        g.add_edge(7, 1, 4, NO_PROPS, None).unwrap();
        g.add_edge(8, 1, 4, NO_PROPS, None).unwrap();
        g.add_edge(9, 1, 4, NO_PROPS, None).unwrap();
        g.add_edge(10, 1, 4, NO_PROPS, None).unwrap();

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
        let g = Graph::new();

        g.add_vertex(1, 1, NO_PROPS).unwrap();
        g.add_vertex(2, 1, NO_PROPS).unwrap();
        g.add_vertex(3, 1, NO_PROPS).unwrap();
        g.add_vertex(4, 1, NO_PROPS).unwrap();
        g.add_vertex(5, 2, NO_PROPS).unwrap();
        g.add_vertex(6, 2, NO_PROPS).unwrap();
        g.add_vertex(7, 2, NO_PROPS).unwrap();
        g.add_vertex(8, 1, NO_PROPS).unwrap();
        g.add_vertex(9, 2, NO_PROPS).unwrap();
        g.add_vertex(10, 2, NO_PROPS).unwrap();

        g.add_vertex(4, "Lord Farquaad", NO_PROPS).unwrap();
        g.add_vertex(6, "Lord Farquaad", NO_PROPS).unwrap();
        g.add_vertex(7, "Lord Farquaad", NO_PROPS).unwrap();
        g.add_vertex(8, "Lord Farquaad", NO_PROPS).unwrap();

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

    #[derive(Debug)]
    struct CustomTime<'a>(&'a str, &'a str);

    impl<'a> TryIntoTime for CustomTime<'a> {
        fn try_into_time(self) -> Result<i64, ParseTimeError> {
            let CustomTime(time, fmt) = self;
            let time = NaiveDateTime::parse_from_str(time, fmt)?;
            let time = time.timestamp_millis();
            Ok(time)
        }
    }

    #[test]
    fn test_ingesting_timestamps() {
        let earliest_time = "2022-06-06 12:34:00".try_into_time().unwrap();
        let latest_time = "2022-06-07 12:34:00".try_into_time().unwrap();

        let g = Graph::new();
        g.add_vertex("2022-06-06T12:34:00.000", 0, NO_PROPS).unwrap();
        g.add_edge("2022-06-07T12:34:00", 1, 2, NO_PROPS, None)
            .unwrap();
        assert_eq!(g.earliest_time().unwrap(), earliest_time);
        assert_eq!(g.latest_time().unwrap(), latest_time);

        let g = Graph::new();
        let fmt = "%Y-%m-%d %H:%M";

        g.add_vertex(CustomTime("2022-06-06 12:34", fmt), 0, NO_PROPS)
            .unwrap();
        g.add_edge(CustomTime("2022-06-07 12:34", fmt), 1, 2, NO_PROPS, None)
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

    #[quickcheck]
    fn test_graph_static_props(u64_props: Vec<(String, u64)>) -> bool {
        let g = Graph::new();

        let as_props = u64_props
            .into_iter()
            .map(|(name, value)| (name, Prop::U64(value)))
            .collect::<Vec<_>>();

        g.add_static_properties(as_props.clone()).unwrap();

        let props_map = as_props.into_iter().collect::<HashMap<_, _>>();

        props_map
            .into_iter()
            .all(|(name, value)| g.static_properties().get(&name).unwrap() == value)
    }

    #[quickcheck]
    fn test_graph_static_props_names(u64_props: Vec<(String, u64)>) -> bool {
        let g = Graph::new();

        let as_props = u64_props
            .into_iter()
            .map(|(name, value)| (name, Prop::U64(value)))
            .collect::<Vec<_>>();

        g.add_static_properties(as_props.clone()).unwrap();

        let props_names = as_props
            .into_iter()
            .map(|(name, _)| name)
            .collect::<HashSet<_>>();

        g.static_prop_names().into_iter().collect::<HashSet<_>>() == props_names
    }

    #[quickcheck]
    fn test_graph_temporal_props(str_props: HashMap<String, String>) -> bool {
        let g = Graph::new();

        let (t0, t1) = (1, 2);

        let (t0_props, t1_props): (Vec<_>, Vec<_>) = str_props
            .into_iter()
            .enumerate()
            .map(|(i, props)| {
                let (name, value) = props;
                let value = Prop::Str(value);
                (name, value, i % 2)
            })
            .partition(|(_, _, i)| *i == 0);

        let t0_props: Vec<(String, Prop)> = t0_props
            .into_iter()
            .map(|(name, value, _)| (name, value))
            .collect();

        let t1_props: Vec<(String, Prop)> = t1_props
            .into_iter()
            .map(|(name, value, _)| (name, value))
            .collect();

        g.add_properties(t0, t0_props.clone()).unwrap();
        g.add_properties(t1, t1_props.clone()).unwrap();

        t0_props
            .into_iter()
            .all(|(name, value)| g.temporal_prop_vec(&name).contains(&(t0, value)))
            && t1_props
                .into_iter()
                .all(|(name, value)| g.temporal_prop_vec(&name).contains(&(t1, value)))
    }

    #[test]
    fn test_temporral_edge_props_window() {
        let g = Graph::new();
        g.add_edge(1, 1, 2, vec![("weight".to_string(), Prop::I64(1))], None)
            .unwrap();
        g.add_edge(2, 1, 2, vec![("weight".to_string(), Prop::I64(2))], None)
            .unwrap();
        g.add_edge(3, 1, 2, vec![("weight".to_string(), Prop::I64(3))], None)
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
        let g = Graph::new();
        g.add_vertex(1, 1, NO_PROPS).unwrap();
        g.add_vertex(2, 1, NO_PROPS).unwrap();
        g.add_vertex(3, 1, NO_PROPS).unwrap();

        assert_eq!(g.vertex(1).unwrap().earliest_time(), Some(1));
        assert_eq!(g.vertex(1).unwrap().latest_time(), Some(3));

        assert_eq!(g.at(2).vertex(1).unwrap().earliest_time(), Some(1));
        assert_eq!(g.at(2).vertex(1).unwrap().latest_time(), Some(2));
    }

    #[test]
    fn test_vertex_ids() {
        let g = Graph::new();
        g.add_vertex(1, 1, NO_PROPS).unwrap();
        g.add_vertex(1, 2, NO_PROPS).unwrap();
        g.add_vertex(2, 3, NO_PROPS).unwrap();

        assert_eq!(g.vertices().id().collect::<Vec<u64>>(), vec![1, 2, 3]);

        let g_at = g.at(1);
        assert_eq!(g_at.vertices().id().collect::<Vec<u64>>(), vec![1, 2]);
    }

    #[test]
    fn test_edge_layer_name() -> Result<(), GraphError> {
        let g = Graph::new();
        g.add_edge(0, 0, 1, NO_PROPS, None)?;
        g.add_edge(0, 0, 1, NO_PROPS, Some("awesome name"))?;

        let layer_names = g.edges().map(|e| e.layer_name()).sorted().collect_vec();
        assert_eq!(layer_names, vec!["awesome name", "default layer"]);
        Ok(())
    }

    #[test]
    fn test_edge_from_single_layer() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, NO_PROPS, Some("layer")).unwrap();

        assert!(g.edge(1, 2, None).is_none());
        assert!(g.layer("layer").unwrap().edge(1, 2, None).is_some())
    }

    #[test]
    fn test_unique_layers() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, NO_PROPS, Some("layer1")).unwrap();
        g.add_edge(0, 1, 2, NO_PROPS, Some("layer2")).unwrap();
        assert_eq!(
            g.layer("layer2").unwrap().get_unique_layers(),
            vec!["layer2"]
        )
    }

    #[quickcheck]
    fn vertex_from_id_is_consistent(vertices: Vec<u64>) -> bool {
        let g = Graph::new();
        for v in vertices.iter() {
            g.add_vertex(0, *v, NO_PROPS).unwrap();
        }
        g.vertices()
            .name()
            .map(|name| g.vertex(name))
            .all(|v| v.is_some())
    }

    #[quickcheck]
    fn exploded_edge_times_is_consistent(edges: Vec<(u64, u64, Vec<i64>)>, offset: i64) -> bool {
        exploded_edge_times_is_consistent_t(edges, offset)
    }

    #[test]
    fn exploded_edge_times_is_consistent_1() {
        let edges = vec![(0, 0, vec![0, 1])];
        assert!(exploded_edge_times_is_consistent_t(edges, 0));
    }

    fn exploded_edge_times_is_consistent_t(edges: Vec<(u64, u64, Vec<i64>)>, offset: i64) -> bool {
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

        let g = Graph::new();
        for (src, dst, times) in edges.iter() {
            for t in times.iter() {
                g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
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
