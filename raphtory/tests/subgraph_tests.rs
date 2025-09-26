use ahash::HashSet;
use itertools::Itertools;
use proptest::{proptest, sample::subsequence};
use raphtory::{
    algorithms::{components::weakly_connected_components, motifs::triangle_count::triangle_count},
    db::graph::{graph::assert_graph_equal, views::deletion_graph::PersistentGraph},
    prelude::*,
};
use raphtory_storage::mutation::addition_ops::InternalAdditionOps;
use std::collections::BTreeSet;

use crate::test_utils::{build_graph, build_graph_strat};

mod test_utils;

#[test]
fn test_materialize_no_edges() {
    let graph = Graph::new();

    graph.add_node(1, 1, NO_PROPS, None).unwrap();
    graph.add_node(2, 2, NO_PROPS, None).unwrap();

    test_storage!(&graph, |graph| {
        let sg = graph.subgraph([1, 2, 1]); // <- duplicated nodes should have no effect

        let actual = sg.materialize().unwrap().into_events().unwrap();
        assert_graph_equal(&actual, &sg);
    });
}

#[test]
fn test_remove_degree1_triangle_count() {
    let graph = Graph::new();
    let edges = vec![
        (1, 2, 1),
        (1, 3, 2),
        (1, 4, 3),
        (3, 1, 4),
        (3, 4, 5),
        (3, 5, 6),
        (4, 5, 7),
        (5, 6, 8),
        (5, 8, 9),
        (7, 5, 10),
        (8, 5, 11),
        (1, 9, 12),
        (9, 1, 13),
        (6, 3, 14),
        (4, 8, 15),
        (8, 3, 16),
        (5, 10, 17),
        (10, 5, 18),
        (10, 8, 19),
        (1, 11, 20),
        (11, 1, 21),
        (9, 11, 22),
        (11, 9, 23),
    ];
    for (src, dst, ts) in edges {
        graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
    }
    test_storage!(&graph, |graph| {
        let subgraph = graph.subgraph(graph.nodes().into_iter().filter(|v| v.degree() > 1));
        let ts = triangle_count(&subgraph, None);
        let tg = triangle_count(graph, None);
        assert_eq!(ts, tg)
    });
}

#[test]
fn layer_materialize() {
    let graph = Graph::new();
    graph.add_edge(0, 1, 2, NO_PROPS, Some("1")).unwrap();
    graph.add_edge(0, 3, 4, NO_PROPS, Some("2")).unwrap();

    test_storage!(&graph, |graph| {
        let sg = graph.subgraph([1, 2]);
        let sgm = sg.materialize().unwrap();
        assert_eq!(
            sg.unique_layers().collect_vec(),
            sgm.unique_layers().collect_vec()
        );
    });
}

#[test]
fn test_cc() {
    let graph = Graph::new();
    graph.add_node(0, 0, NO_PROPS, None).unwrap();
    graph.add_node(0, 3, NO_PROPS, None).unwrap();
    graph.add_node(1, 2, NO_PROPS, None).unwrap();
    graph.add_node(1, 4, NO_PROPS, None).unwrap();
    graph.add_edge(0, 0, 1, NO_PROPS, Some("1")).unwrap();
    graph.add_edge(1, 3, 4, NO_PROPS, Some("1")).unwrap();
    let sg = graph.subgraph([0, 1, 3, 4]);
    let cc = weakly_connected_components(&sg);
    let groups = cc.groups();
    let group_sets = groups
        .iter()
        .map(|(_, g)| g.id().into_iter_values().collect::<BTreeSet<_>>())
        .collect::<HashSet<_>>();
    assert_eq!(
        group_sets,
        HashSet::from_iter([
            BTreeSet::from([GID::U64(0), GID::U64(1)]),
            BTreeSet::from([GID::U64(3), GID::U64(4)])
        ])
    );
}

#[test]
fn test_layer_edges() {
    let graph = Graph::new();
    graph.add_edge(0, 0, 1, NO_PROPS, Some("1")).unwrap();
    graph.add_edge(1, 0, 1, NO_PROPS, Some("2")).unwrap();

    assert_eq!(
        graph.subgraph([0, 1]).edges().id().collect_vec(),
        [(GID::U64(0), GID::U64(1))]
    );
    assert_eq!(
        graph
            .subgraph([0, 1])
            .valid_layers("1")
            .edges()
            .id()
            .collect_vec(),
        [(GID::U64(0), GID::U64(1))]
    );
}

pub mod test_filters_node_subgraph {
    use raphtory::{
        db::{
            api::view::StaticGraphViewOps,
            graph::{
                assertions::{GraphTransformer, TestGraphVariants},
                views::{node_subgraph::NodeSubgraph, window_graph::WindowedGraph},
            },
        },
        prelude::{GraphViewOps, NodeViewOps, TimeOps},
    };
    use std::ops::Range;

    pub struct NodeSubgraphTransformer(Option<Vec<String>>);

    impl GraphTransformer for NodeSubgraphTransformer {
        type Return<G: StaticGraphViewOps> = NodeSubgraph<G>;
        fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Self::Return<G> {
            let node_names: Vec<String> = self
                .0
                .clone()
                .unwrap_or_else(|| graph.nodes().name().collect::<Vec<String>>());
            graph.subgraph(node_names)
        }
    }

    struct WindowedNodeSubgraphTransformer(Option<Vec<String>>, Range<i64>);

    impl GraphTransformer for WindowedNodeSubgraphTransformer {
        type Return<G: StaticGraphViewOps> = NodeSubgraph<WindowedGraph<G>>;
        fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Self::Return<G> {
            let graph = graph.window(self.1.start, self.1.end);
            let node_names: Vec<String> = self
                .0
                .clone()
                .unwrap_or_else(|| graph.nodes().name().collect::<Vec<String>>());
            graph.subgraph(node_names)
        }
    }

    mod test_nodes_filters_node_subgraph {
        use raphtory::{
            db::{
                api::view::StaticGraphViewOps,
                graph::{
                    assertions::{
                        assert_filter_nodes_results, assert_search_nodes_results,
                        TestGraphVariants, TestVariants,
                    },
                    views::filter::model::PropertyFilterOps,
                },
            },
            prelude::{AdditionOps, PropertyFilter},
        };
        use raphtory_api::core::entities::properties::prop::Prop;

        use crate::test_filters_node_subgraph::{
            NodeSubgraphTransformer, WindowedNodeSubgraphTransformer,
        };

        fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
            let nodes = vec![
                (6, "N1", vec![("p1", Prop::U64(2u64))]),
                (7, "N1", vec![("p1", Prop::U64(1u64))]),
                (6, "N2", vec![("p1", Prop::U64(1u64))]),
                (7, "N2", vec![("p1", Prop::U64(2u64))]),
                (8, "N3", vec![("p1", Prop::U64(1u64))]),
                (9, "N4", vec![("p1", Prop::U64(1u64))]),
                (5, "N5", vec![("p1", Prop::U64(1u64))]),
                (6, "N5", vec![("p1", Prop::U64(2u64))]),
                (5, "N6", vec![("p1", Prop::U64(1u64))]),
                (6, "N6", vec![("p1", Prop::U64(1u64))]),
                (3, "N7", vec![("p1", Prop::U64(1u64))]),
                (5, "N7", vec![("p1", Prop::U64(1u64))]),
                (3, "N8", vec![("p1", Prop::U64(1u64))]),
                (4, "N8", vec![("p1", Prop::U64(2u64))]),
            ];

            for (id, name, props) in &nodes {
                graph.add_node(*id, name, props.clone(), None).unwrap();
            }

            graph
        }

        #[test]
        fn test_search_nodes_subgraph() {
            let filter = PropertyFilter::property("p1").eq(1u64);
            let expected_results = ["N1", "N3", "N4", "N6", "N7"];
            assert_filter_nodes_results(
                init_graph,
                NodeSubgraphTransformer(None),
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
            assert_search_nodes_results(
                init_graph,
                NodeSubgraphTransformer(None),
                filter,
                &expected_results,
                TestVariants::All,
            );

            let node_names: Option<Vec<String>> =
                Some(vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()]);
            let filter = PropertyFilter::property("p1").le(1u64);
            let expected_results = vec!["N3", "N4"];
            assert_filter_nodes_results(
                init_graph,
                NodeSubgraphTransformer(node_names.clone()),
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
            assert_search_nodes_results(
                init_graph,
                NodeSubgraphTransformer(node_names),
                filter,
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_search_nodes_subgraph_w() {
            // TODO: Enable event_disk_graph for filter_nodes once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
            let filter = PropertyFilter::property("p1").eq(1u64);
            let expected_results = vec!["N1", "N3", "N6"];
            assert_filter_nodes_results(
                init_graph,
                WindowedNodeSubgraphTransformer(None, 6..9),
                filter.clone(),
                &expected_results,
                vec![TestGraphVariants::Graph],
            );
            assert_search_nodes_results(
                init_graph,
                WindowedNodeSubgraphTransformer(None, 6..9),
                filter,
                &expected_results,
                vec![TestGraphVariants::Graph],
            );

            let node_names: Option<Vec<String>> = Some(vec!["N3".into()]);
            let filter = PropertyFilter::property("p1").gt(0u64);
            let expected_results = vec!["N3"];
            assert_filter_nodes_results(
                init_graph,
                WindowedNodeSubgraphTransformer(node_names.clone(), 6..9),
                filter.clone(),
                &expected_results,
                vec![TestGraphVariants::Graph],
            );
            assert_search_nodes_results(
                init_graph,
                WindowedNodeSubgraphTransformer(node_names, 6..9),
                filter,
                &expected_results,
                vec![TestGraphVariants::Graph],
            );
        }

        #[test]
        fn test_search_nodes_pg_w() {
            let filter = PropertyFilter::property("p1").eq(1u64);
            let expected_results = vec!["N1", "N3", "N6", "N7"];
            assert_filter_nodes_results(
                init_graph,
                WindowedNodeSubgraphTransformer(None, 6..9),
                filter.clone(),
                &expected_results,
                TestVariants::PersistentOnly,
            );
            assert_search_nodes_results(
                init_graph,
                WindowedNodeSubgraphTransformer(None, 6..9),
                filter,
                &expected_results,
                TestVariants::PersistentOnly,
            );

            let node_names: Option<Vec<String>> =
                Some(vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()]);
            let filter = PropertyFilter::property("p1").ge(1u64);
            let expected_results = vec!["N2", "N3", "N5"];
            assert_filter_nodes_results(
                init_graph,
                WindowedNodeSubgraphTransformer(node_names.clone(), 6..9),
                filter.clone(),
                &expected_results,
                TestVariants::PersistentOnly,
            );
            assert_search_nodes_results(
                init_graph,
                WindowedNodeSubgraphTransformer(node_names, 6..9),
                filter,
                &expected_results,
                TestVariants::PersistentOnly,
            );
        }
    }

    mod test_edges_filters_node_subgraph {
        use raphtory::{
            db::{
                api::view::StaticGraphViewOps,
                graph::{
                    assertions::{
                        assert_filter_edges_results, assert_search_edges_results, TestVariants,
                    },
                    views::filter::model::PropertyFilterOps,
                },
            },
            prelude::{AdditionOps, PropertyFilter},
        };
        use raphtory_api::core::entities::properties::prop::Prop;

        use crate::test_filters_node_subgraph::{
            NodeSubgraphTransformer, WindowedNodeSubgraphTransformer,
        };

        fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
            let edges = vec![
                (6, "N1", "N2", vec![("p1", Prop::U64(2u64))]),
                (7, "N1", "N2", vec![("p1", Prop::U64(1u64))]),
                (6, "N2", "N3", vec![("p1", Prop::U64(1u64))]),
                (7, "N2", "N3", vec![("p1", Prop::U64(2u64))]),
                (8, "N3", "N4", vec![("p1", Prop::U64(1u64))]),
                (9, "N4", "N5", vec![("p1", Prop::U64(1u64))]),
                (5, "N5", "N6", vec![("p1", Prop::U64(1u64))]),
                (6, "N5", "N6", vec![("p1", Prop::U64(2u64))]),
                (5, "N6", "N7", vec![("p1", Prop::U64(1u64))]),
                (6, "N6", "N7", vec![("p1", Prop::U64(1u64))]),
                (3, "N7", "N8", vec![("p1", Prop::U64(1u64))]),
                (5, "N7", "N8", vec![("p1", Prop::U64(1u64))]),
                (3, "N8", "N1", vec![("p1", Prop::U64(1u64))]),
                (4, "N8", "N1", vec![("p1", Prop::U64(2u64))]),
            ];

            for (id, src, tgt, props) in &edges {
                graph.add_edge(*id, src, tgt, props.clone(), None).unwrap();
            }

            graph
        }

        #[test]
        fn test_edges_filters() {
            // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph.
            let filter = PropertyFilter::property("p1").eq(1u64);
            let expected_results = vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
            assert_filter_edges_results(
                init_graph,
                NodeSubgraphTransformer(None),
                filter.clone(),
                &expected_results,
                TestVariants::EventOnly,
            );
            assert_search_edges_results(
                init_graph,
                NodeSubgraphTransformer(None),
                filter,
                &expected_results,
                TestVariants::All,
            );

            let node_names: Option<Vec<String>> =
                Some(vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()]);
            let filter = PropertyFilter::property("p1").le(1u64);
            let expected_results = vec!["N3->N4", "N4->N5"];
            assert_filter_edges_results(
                init_graph,
                NodeSubgraphTransformer(node_names.clone()),
                filter.clone(),
                &expected_results,
                TestVariants::EventOnly,
            );
            assert_search_edges_results(
                init_graph,
                NodeSubgraphTransformer(node_names),
                filter,
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_edges_filters_w() {
            let filter = PropertyFilter::property("p1").eq(1u64);
            let expected_results = vec!["N1->N2", "N3->N4", "N6->N7"];
            assert_filter_edges_results(
                init_graph,
                WindowedNodeSubgraphTransformer(None, 6..9),
                filter.clone(),
                &expected_results,
                TestVariants::EventOnly,
            );
            assert_search_edges_results(
                init_graph,
                WindowedNodeSubgraphTransformer(None, 6..9),
                filter,
                &expected_results,
                TestVariants::EventOnly,
            );

            let node_names: Option<Vec<String>> =
                Some(vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()]);
            let filter = PropertyFilter::property("p1").ge(1u64);
            let expected_results = vec!["N2->N3", "N3->N4"];
            assert_filter_edges_results(
                init_graph,
                WindowedNodeSubgraphTransformer(node_names.clone(), 6..9),
                filter.clone(),
                &expected_results,
                TestVariants::EventOnly,
            );
            assert_search_edges_results(
                init_graph,
                WindowedNodeSubgraphTransformer(node_names, 6..9),
                filter,
                &expected_results,
                TestVariants::EventOnly,
            );
        }

        #[test]
        fn test_edges_filters_pg_w() {
            // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph.
            let filter = PropertyFilter::property("p1").eq(1u64);
            let expected_results = vec!["N1->N2", "N3->N4", "N6->N7", "N7->N8"];
            assert_filter_edges_results(
                init_graph,
                WindowedNodeSubgraphTransformer(None, 6..9),
                filter.clone(),
                &expected_results,
                vec![],
            );
            assert_search_edges_results(
                init_graph,
                WindowedNodeSubgraphTransformer(None, 6..9),
                filter,
                &expected_results,
                TestVariants::PersistentOnly,
            );

            let node_names: Option<Vec<String>> = Some(vec![
                "N2".into(),
                "N3".into(),
                "N4".into(),
                "N5".into(),
                "N6".into(),
            ]);
            let filter = PropertyFilter::property("p1").lt(2u64);
            let expected_results = vec!["N3->N4"];
            assert_filter_edges_results(
                init_graph,
                WindowedNodeSubgraphTransformer(node_names.clone(), 6..9),
                filter.clone(),
                &expected_results,
                vec![],
            );
            assert_search_edges_results(
                init_graph,
                WindowedNodeSubgraphTransformer(node_names, 6..9),
                filter,
                &expected_results,
                TestVariants::PersistentOnly,
            );
        }
    }
}

#[test]
fn nodes_without_updates_are_filtered() {
    let g = Graph::new();
    g.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
    let expected = Graph::new();
    expected.resolve_layer(None).unwrap();
    let subgraph = g.subgraph([0]);
    assert_graph_equal(&subgraph, &expected);
}

#[test]
fn materialize_proptest() {
    proptest!(|(graph in build_graph_strat(10, 10, false), nodes in subsequence((0..10).collect::<Vec<_>>(), 0..10))| {
        let graph = Graph::from(build_graph(&graph));
        let subgraph = graph.subgraph(nodes);
        assert_graph_equal(&subgraph, &subgraph.materialize().unwrap());
    })
}

#[test]
fn materialize_persistent_proptest() {
    proptest!(|(graph in build_graph_strat(10, 10, true), nodes in subsequence((0..10).collect::<Vec<_>>(), 0..10))| {
        let graph = PersistentGraph::from(build_graph(&graph));
        let subgraph = graph.subgraph(nodes);
        assert_graph_equal(&subgraph, &subgraph.materialize().unwrap());
    })
}

#[test]
fn test_subgraph_only_deletion() {
    let g = PersistentGraph::new();
    g.delete_edge(0, 0, 1, None).unwrap();
    let sg = g.subgraph([0]);
    let expected = PersistentGraph::new();
    expected.resolve_layer(None).unwrap();
    assert_graph_equal(&sg, &expected);
}
