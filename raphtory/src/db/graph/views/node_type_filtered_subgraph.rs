use crate::{
    core::entities::LayerIds,
    db::api::{
        properties::internal::InheritPropertiesOps,
        storage::graph::nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
        view::internal::{
            Base, Immutable, InheritCoreOps, InheritEdgeFilterOps, InheritEdgeHistoryFilter,
            InheritLayerOps, InheritListOps, InheritMaterialize, InheritNodeHistoryFilter,
            InheritTimeSemantics, InternalNodeFilterOps, Static,
        },
    },
    prelude::GraphViewOps,
};
use std::sync::Arc;

use crate::db::api::view::internal::InheritStorageOps;

#[derive(Clone, Debug)]
pub struct TypeFilteredSubgraph<G> {
    pub(crate) graph: G,
    pub(crate) node_types: Arc<[usize]>,
}

impl<G> Static for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> Base for TypeFilteredSubgraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> TypeFilteredSubgraph<G> {
    pub fn new(graph: G, node_types: Vec<usize>) -> Self {
        let node_types = node_types.into();
        Self { graph, node_types }
    }
}

impl<'graph, G: GraphViewOps<'graph>> Immutable for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreOps for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeFilterOps for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritListOps for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InternalNodeFilterOps for TypeFilteredSubgraph<G> {
    #[inline]
    fn internal_nodes_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn edge_and_node_filter_independent(&self) -> bool {
        false
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.node_types.contains(&node.node_type_id())
            && self.graph.internal_filter_node(node, layer_ids)
    }
}

#[cfg(all(test, feature = "search"))]
mod search_nodes_node_type_filtered_subgraph_tests {
    use crate::{
        core::Prop,
        db::{
            api::view::{SearchableGraphOps, StaticGraphViewOps},
            graph::views::{
                deletion_graph::PersistentGraph,
                property_filter::{FilterExpr, PropertyFilterOps},
            },
        },
        prelude::{AdditionOps, Graph, NodeViewOps, PropertyFilter, TimeOps},
    };
    use std::ops::Range;

    fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
        let nodes = vec![
            (6, "N1", vec![("p1", Prop::U64(2u64))], Some("air_nomad")),
            (7, "N1", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
            (6, "N2", vec![("p1", Prop::U64(1u64))], Some("water_tribe")),
            (7, "N2", vec![("p1", Prop::U64(2u64))], Some("water_tribe")),
            (8, "N3", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
            (9, "N4", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
            (5, "N5", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
            (6, "N5", vec![("p1", Prop::U64(2u64))], Some("air_nomad")),
            (5, "N6", vec![("p1", Prop::U64(1u64))], Some("fire_nation")),
            (6, "N6", vec![("p1", Prop::U64(1u64))], Some("fire_nation")),
            (3, "N7", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
            (5, "N7", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
            (3, "N8", vec![("p1", Prop::U64(1u64))], Some("fire_nation")),
            (4, "N8", vec![("p1", Prop::U64(2u64))], Some("fire_nation")),
        ];

        // Add nodes to the graph
        for (id, name, props, layer) in &nodes {
            graph.add_node(*id, name, props.clone(), *layer).unwrap();
        }

        graph
    }

    fn search_nodes_by_composite_filter<G: StaticGraphViewOps + AdditionOps>(
        graph: &G,
        node_types: Vec<String>,
        filter: FilterExpr,
    ) -> Vec<String> {
        graph.create_index().unwrap();
        let sgm = graph.subgraph_node_types(node_types);
        let mut results = sgm
            .search_nodes(filter, 10, 0)
            .expect("Failed to search for nodes")
            .into_iter()
            .map(|v| v.name())
            .collect::<Vec<_>>();
        results.sort();
        results
    }

    fn search_nodes_by_composite_filter_w<G: StaticGraphViewOps + AdditionOps>(
        graph: &G,
        w: Range<i64>,
        node_types: Vec<String>,
        filter: FilterExpr,
    ) -> Vec<String> {
        graph.create_index().unwrap();
        let sgm = graph.subgraph_node_types(node_types);
        let mut results = sgm
            .window(w.start, w.end)
            .search_nodes(filter, 10, 0)
            .expect("Failed to search for nodes")
            .into_iter()
            .map(|v| v.name())
            .collect::<Vec<_>>();
        results.sort();
        results
    }

    fn get_all_node_types<G: StaticGraphViewOps + AdditionOps>(graph: &G) -> Vec<String> {
        graph
            .nodes()
            .node_type()
            .into_iter()
            .flat_map(|(_, node_type)| node_type)
            .map(|s| s.to_string())
            .collect()
    }

    #[test]
    fn test_search_nodes_type_filtered_subgraph() {
        let graph = Graph::new();
        let graph = init_graph(graph);

        let node_types = get_all_node_types(&graph);
        let filter = PropertyFilter::property("p1").eq(1u64);
        let results = search_nodes_by_composite_filter(&graph, node_types, filter);
        assert_eq!(results, vec!["N1", "N3", "N4", "N6", "N7"]);

        let node_types = vec!["air_nomad".into(), "water_tribe".into()];
        let filter = PropertyFilter::property("p1").eq(1u64);
        let results = search_nodes_by_composite_filter(&graph, node_types, filter);
        assert_eq!(results, vec!["N1", "N3", "N4", "N7"]);
    }

    #[test]
    fn test_search_nodes_type_filtered_subgraph_w() {
        let graph = Graph::new();
        let graph = init_graph(graph);

        let node_types = get_all_node_types(&graph);
        let filter = PropertyFilter::property("p1").eq(1u64);
        let results = search_nodes_by_composite_filter_w(&graph, 6..9, node_types, filter);
        assert_eq!(results, vec!["N1", "N3", "N6"]);

        let node_types = vec!["air_nomad".into(), "water_tribe".into()];
        let filter = PropertyFilter::property("p1").eq(1u64);
        let results = search_nodes_by_composite_filter_w(&graph, 6..9, node_types, filter);
        assert_eq!(results, vec!["N1", "N3"]);
    }

    #[test]
    fn test_search_nodes_persistent_type_filtered_subgraph() {
        let graph = PersistentGraph::new();
        let graph = init_graph(graph);

        let node_types = get_all_node_types(&graph);
        let filter = PropertyFilter::property("p1").eq(1u64);
        let results = search_nodes_by_composite_filter(&graph, node_types, filter);
        assert_eq!(results, vec!["N1", "N3", "N4", "N6", "N7"]);

        let node_types = vec!["air_nomad".into(), "water_tribe".into()];
        let filter = PropertyFilter::property("p1").eq(1u64);
        let results = search_nodes_by_composite_filter(&graph, node_types, filter);
        assert_eq!(results, vec!["N1", "N3", "N4", "N7"]);
    }

    #[test]
    fn test_search_nodes_persistent_type_filtered_subgraph_w() {
        let graph = PersistentGraph::new();
        let graph = init_graph(graph);

        let node_types = get_all_node_types(&graph);
        let filter = PropertyFilter::property("p1").eq(1u64);
        let results = search_nodes_by_composite_filter_w(&graph, 6..9, node_types, filter);
        assert_eq!(results, vec!["N1", "N3", "N6", "N7"]);

        let node_types = vec!["air_nomad".into(), "water_tribe".into()];
        let filter = PropertyFilter::property("p1").eq(1u64);
        let results = search_nodes_by_composite_filter_w(&graph, 6..9, node_types, filter);
        assert_eq!(results, vec!["N1", "N3", "N7"]);
    }
}

#[cfg(all(test, feature = "search"))]
mod search_edges_node_type_filtered_subgraph_tests {
    use crate::{
        core::Prop,
        db::{
            api::view::{SearchableGraphOps, StaticGraphViewOps},
            graph::views::{
                deletion_graph::PersistentGraph,
                property_filter::{FilterExpr, PropertyFilterOps},
            },
        },
        prelude::{
            AdditionOps, EdgeViewOps, Graph, NodeViewOps, PropertyFilter, TimeOps, NO_PROPS,
        },
    };
    use std::ops::Range;

    fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
        let edges = vec![
            (6, "N1", "N2", vec![("p1", Prop::U64(2u64))], None),
            (7, "N1", "N2", vec![("p1", Prop::U64(1u64))], None),
            (6, "N2", "N3", vec![("p1", Prop::U64(1u64))], None),
            (7, "N2", "N3", vec![("p1", Prop::U64(2u64))], None),
            (8, "N3", "N4", vec![("p1", Prop::U64(1u64))], None),
            (9, "N4", "N5", vec![("p1", Prop::U64(1u64))], None),
            (5, "N5", "N6", vec![("p1", Prop::U64(1u64))], None),
            (6, "N5", "N6", vec![("p1", Prop::U64(2u64))], None),
            (5, "N6", "N7", vec![("p1", Prop::U64(1u64))], None),
            (6, "N6", "N7", vec![("p1", Prop::U64(1u64))], None),
            (3, "N7", "N8", vec![("p1", Prop::U64(1u64))], None),
            (5, "N7", "N8", vec![("p1", Prop::U64(1u64))], None),
            (3, "N8", "N1", vec![("p1", Prop::U64(1u64))], None),
            (4, "N8", "N1", vec![("p1", Prop::U64(2u64))], None),
        ];

        for (id, src, dst, props, layer) in &edges {
            graph
                .add_edge(*id, src, dst, props.clone(), *layer)
                .unwrap();
        }

        let nodes = vec![
            (6, "N1", NO_PROPS, Some("air_nomad")),
            (6, "N2", NO_PROPS, Some("water_tribe")),
            (8, "N3", NO_PROPS, Some("air_nomad")),
            (9, "N4", NO_PROPS, Some("air_nomad")),
            (5, "N5", NO_PROPS, Some("air_nomad")),
            (5, "N6", NO_PROPS, Some("fire_nation")),
            (3, "N7", NO_PROPS, Some("air_nomad")),
            (4, "N8", NO_PROPS, Some("fire_nation")),
        ];

        for (id, name, props, layer) in &nodes {
            graph.add_node(*id, name, props.clone(), *layer).unwrap();
        }

        graph
    }

    fn search_edges_by_composite_filter<G: StaticGraphViewOps + AdditionOps>(
        graph: &G,
        node_types: Vec<String>,
        filter: FilterExpr,
    ) -> Vec<String> {
        graph.create_index().unwrap();
        let sgm = graph.subgraph_node_types(node_types);
        let mut results = sgm
            .search_edges(filter, 10, 0)
            .expect("Failed to search for nodes")
            .into_iter()
            .map(|v| format!("{}->{}", v.src().name(), v.dst().name()))
            .collect::<Vec<_>>();
        results.sort();
        results
    }

    fn search_edges_by_composite_filter_w<G: StaticGraphViewOps + AdditionOps>(
        graph: &G,
        w: Range<i64>,
        node_types: Vec<String>,
        filter: FilterExpr,
    ) -> Vec<String> {
        graph.create_index().unwrap();
        let sgm = graph.subgraph_node_types(node_types);
        let mut results = sgm
            .window(w.start, w.end)
            .search_edges(filter, 10, 0)
            .expect("Failed to search for nodes")
            .into_iter()
            .map(|v| format!("{}->{}", v.src().name(), v.dst().name()))
            .collect::<Vec<_>>();
        results.sort();
        results
    }

    fn get_all_node_types<G: StaticGraphViewOps + AdditionOps>(graph: &G) -> Vec<String> {
        graph
            .nodes()
            .node_type()
            .into_iter()
            .flat_map(|(_, node_type)| node_type)
            .map(|s| s.to_string())
            .collect()
    }

    #[test]
    fn test_search_edges_type_filtered_subgraph() {
        let graph = Graph::new();
        let graph = init_graph(graph);

        let node_types = get_all_node_types(&graph);
        let filter = PropertyFilter::property("p1").eq(1u64);
        let results = search_edges_by_composite_filter(&graph, node_types, filter);
        assert_eq!(
            results,
            vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"]
        );

        let node_types = vec!["air_nomad".into(), "water_tribe".into()];
        let filter = PropertyFilter::property("p1").eq(1u64);
        let results = search_edges_by_composite_filter(&graph, node_types, filter);
        assert_eq!(results, vec!["N1->N2", "N3->N4", "N4->N5"]);
    }

    #[test]
    fn test_search_edges_type_filtered_subgraph_w() {
        let graph = Graph::new();
        let graph = init_graph(graph);

        let node_types = get_all_node_types(&graph);
        let filter = PropertyFilter::property("p1").eq(1u64);
        let results = search_edges_by_composite_filter_w(&graph, 6..9, node_types, filter);
        assert_eq!(results, vec!["N1->N2", "N3->N4", "N6->N7"]);

        let node_types = vec!["air_nomad".into(), "water_tribe".into()];
        let filter = PropertyFilter::property("p1").eq(1u64);
        let results = search_edges_by_composite_filter_w(&graph, 6..9, node_types, filter);
        assert_eq!(results, vec!["N1->N2", "N3->N4"]);
    }

    #[test]
    fn test_search_edges_persistent_type_filtered_subgraph() {
        let graph = PersistentGraph::new();
        let graph = init_graph(graph);

        let node_types = get_all_node_types(&graph);
        let filter = PropertyFilter::property("p1").eq(1u64);
        let results = search_edges_by_composite_filter(&graph, node_types, filter);
        assert_eq!(
            results,
            vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"]
        );

        let node_types = vec!["air_nomad".into(), "water_tribe".into()];
        let filter = PropertyFilter::property("p1").eq(1u64);
        let results = search_edges_by_composite_filter(&graph, node_types, filter);
        assert_eq!(results, vec!["N1->N2", "N3->N4", "N4->N5"]);
    }

    #[test]
    fn test_search_edges_persistent_type_filtered_subgraph_w() {
        let graph = PersistentGraph::new();
        let graph = init_graph(graph);

        let node_types = get_all_node_types(&graph);
        let filter = PropertyFilter::property("p1").eq(1u64);
        let results = search_edges_by_composite_filter_w(&graph, 6..9, node_types, filter);
        assert_eq!(results, vec!["N1->N2", "N3->N4", "N6->N7", "N7->N8"]);

        let node_types = vec!["air_nomad".into(), "water_tribe".into()];
        let filter = PropertyFilter::property("p1").eq(1u64);
        let results = search_edges_by_composite_filter_w(&graph, 6..9, node_types, filter);
        assert_eq!(results, vec!["N1->N2", "N3->N4"]);
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        db::graph::{graph::assert_graph_equal, views::property_filter::PropertyRef},
        prelude::*,
        test_utils::{build_graph, build_graph_strat},
    };
    use proptest::{arbitrary::any, proptest};
    use std::ops::Range;

    #[test]
    fn test_type_filtered_subgraph() {
        let graph = Graph::new();
        let edges = vec![
            (1, "A", "B", vec![("p1", 1u64)], None),
            (2, "B", "C", vec![("p1", 2u64)], None),
            (3, "C", "D", vec![("p1", 3u64)], None),
            (4, "D", "E", vec![("p1", 4u64)], None),
        ];

        for (id, src, dst, props, layer) in &edges {
            graph
                .add_edge(*id, src, dst, props.clone(), *layer)
                .unwrap();
        }

        let nodes = vec![
            (1, "A", vec![("p1", 1u64)], Some("water_tribe")),
            (2, "B", vec![("p1", 2u64)], Some("water_tribe")),
            (3, "C", vec![("p1", 1u64)], Some("fire_nation")),
            (4, "D", vec![("p1", 1u64)], Some("air_nomads")),
        ];

        for (id, name, props, layer) in &nodes {
            graph.add_node(*id, name, props.clone(), *layer).unwrap();
        }

        let type_filtered_subgraph = graph
            .subgraph_node_types(vec!["fire_nation", "air_nomads"])
            .window(1, 5);

        assert_eq!(type_filtered_subgraph.nodes(), vec!["C", "D"]);

        assert_eq!(
            type_filtered_subgraph
                .filter_nodes(PropertyFilter::eq(PropertyRef::Property("p1".into()), 1u64))
                .unwrap()
                .nodes(),
            vec!["C", "D"]
        );

        assert!(type_filtered_subgraph
            .filter_edges(PropertyFilter::eq(PropertyRef::Property("p1".into()), 1u64))
            .unwrap()
            .edges()
            .is_empty())
    }

    #[test]
    fn materialize_prop_test() {
        proptest!(|(graph_f in build_graph_strat(10, 10, true))| {
            let g = Graph::from(build_graph(&graph_f));
            let gm = g.materialize().unwrap();
            assert_graph_equal(&g, &gm);
        })
    }

    #[test]
    fn materialize_valid_window_prop_test() {
        proptest!(|(graph_f in build_graph_strat(10, 10, true), w in any::<Range<i64>>())| {
            let g = Graph::from(build_graph(&graph_f));
            let gvw = g.valid().unwrap().window(w.start, w.end);
            let gmw = gvw.materialize().unwrap();
            assert_graph_equal(&gvw, &gmw);
        })
    }

    #[test]
    fn materialize_window_valid_prop_test() {
        proptest!(|(graph_f in build_graph_strat(10, 10, true), w in any::<Range<i64>>())| {
            let g = Graph::from(build_graph(&graph_f));
            let gvw = g.window(w.start, w.end).valid().unwrap();
            let gmw = gvw.materialize().unwrap();
            assert_graph_equal(&gvw, &gmw);
        })
    }
}
