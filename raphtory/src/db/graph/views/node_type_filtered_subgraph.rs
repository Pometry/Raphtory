use crate::{
    core::entities::LayerIds,
    db::api::{
        properties::internal::InheritPropertiesOps,
        storage::graph::nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
        view::internal::{
            Base, Immutable, InheritCoreOps, InheritEdgeFilterOps, InheritEdgeHistoryFilter,
            InheritIndexSearch, InheritLayerOps, InheritListOps, InheritMaterialize,
            InheritNodeHistoryFilter, InheritTimeSemantics, NodeFilterOps, Static,
        },
    },
    prelude::GraphViewOps,
};
use std::sync::Arc;

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
impl<'graph, G: GraphViewOps<'graph>> InheritIndexSearch for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeFilterOps for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritListOps for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> NodeFilterOps for TypeFilteredSubgraph<G> {
    #[inline]
    fn nodes_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn node_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn edge_filter_includes_node_filter(&self) -> bool {
        false
    }

    #[inline]
    fn filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.node_types.contains(&node.node_type_id()) && self.graph.filter_node(node, layer_ids)
    }
}

// TODO: We don't need to materialize the subgraph. Refer: https://github.com/Pometry/Raphtory/issues/1948
#[cfg(all(test, feature = "search"))]
mod search_nodes_node_type_filtered_subgraph_tests {
    use crate::{
        core::Prop,
        db::{
            api::view::{SearchableGraphOps, StaticGraphViewOps},
            graph::views::{
                deletion_graph::PersistentGraph,
                property_filter::{
                    CompositeNodeFilter, FilterExpr, PropertyFilterOps, PropertyRef,
                },
            },
        },
        prelude::{AdditionOps, Graph, GraphViewOps, NodeViewOps, PropertyFilter, TimeOps},
    };
    use std::ops::Range;

    fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
        graph
            .add_node(6, "N1", [("p1", Prop::U64(2u64))], Some("air_nomad"))
            .unwrap();
        graph
            .add_node(7, "N1", [("p1", Prop::U64(1u64))], Some("air_nomad"))
            .unwrap();

        graph
            .add_node(6, "N2", [("p1", Prop::U64(1u64))], Some("water_tribe"))
            .unwrap();
        graph
            .add_node(7, "N2", [("p1", Prop::U64(2u64))], Some("water_tribe"))
            .unwrap();

        graph
            .add_node(8, "N3", [("p1", Prop::U64(1u64))], Some("air_nomad"))
            .unwrap();

        graph
            .add_node(9, "N4", [("p1", Prop::U64(1u64))], Some("air_nomad"))
            .unwrap();

        graph
            .add_node(5, "N5", [("p1", Prop::U64(1u64))], Some("air_nomad"))
            .unwrap();
        graph
            .add_node(6, "N5", [("p1", Prop::U64(2u64))], Some("air_nomad"))
            .unwrap();

        graph
            .add_node(5, "N6", [("p1", Prop::U64(1u64))], Some("fire_nation"))
            .unwrap();
        graph
            .add_node(6, "N6", [("p1", Prop::U64(1u64))], Some("fire_nation"))
            .unwrap();

        graph
            .add_node(3, "N7", [("p1", Prop::U64(1u64))], Some("air_nomad"))
            .unwrap();
        graph
            .add_node(5, "N7", [("p1", Prop::U64(1u64))], Some("air_nomad"))
            .unwrap();

        graph
            .add_node(3, "N8", [("p1", Prop::U64(1u64))], Some("fire_nation"))
            .unwrap();
        graph
            .add_node(4, "N8", [("p1", Prop::U64(2u64))], Some("fire_nation"))
            .unwrap();

        graph
    }

    fn search_nodes_by_composite_filter<G: StaticGraphViewOps + AdditionOps>(
        graph: &G,
        node_types: Vec<String>,
        filter: FilterExpr,
    ) -> Vec<String> {
        let sgm = graph.subgraph_node_types(node_types).materialize().unwrap();
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
        let sgm = graph.subgraph_node_types(node_types).materialize().unwrap();
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

// TODO: We don't need to materialize the subgraph. Refer: https://github.com/Pometry/Raphtory/issues/1948
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
            AdditionOps, EdgeViewOps, Graph, GraphViewOps, NodeViewOps, PropertyFilter, TimeOps,
            NO_PROPS,
        },
    };
    use std::ops::Range;

    fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
        graph
            .add_edge(6, "N1", "N2", [("p1", Prop::U64(2u64))], None)
            .unwrap();
        graph
            .add_edge(7, "N1", "N2", [("p1", Prop::U64(1u64))], None)
            .unwrap();

        graph
            .add_edge(6, "N2", "N3", [("p1", Prop::U64(1u64))], None)
            .unwrap();
        graph
            .add_edge(7, "N2", "N3", [("p1", Prop::U64(2u64))], None)
            .unwrap();

        graph
            .add_edge(8, "N3", "N4", [("p1", Prop::U64(1u64))], None)
            .unwrap();

        graph
            .add_edge(9, "N4", "N5", [("p1", Prop::U64(1u64))], None)
            .unwrap();

        graph
            .add_edge(5, "N5", "N6", [("p1", Prop::U64(1u64))], None)
            .unwrap();
        graph
            .add_edge(6, "N5", "N6", [("p1", Prop::U64(2u64))], None)
            .unwrap();

        graph
            .add_edge(5, "N6", "N7", [("p1", Prop::U64(1u64))], None)
            .unwrap();
        graph
            .add_edge(6, "N6", "N7", [("p1", Prop::U64(1u64))], None)
            .unwrap();

        graph
            .add_edge(3, "N7", "N8", [("p1", Prop::U64(1u64))], None)
            .unwrap();
        graph
            .add_edge(5, "N7", "N8", [("p1", Prop::U64(1u64))], None)
            .unwrap();

        graph
            .add_edge(3, "N8", "N1", [("p1", Prop::U64(1u64))], None)
            .unwrap();
        graph
            .add_edge(4, "N8", "N1", [("p1", Prop::U64(2u64))], None)
            .unwrap();

        graph
            .add_node(6, "N1", NO_PROPS, Some("air_nomad"))
            .unwrap();
        graph
            .add_node(6, "N2", NO_PROPS, Some("water_tribe"))
            .unwrap();
        graph
            .add_node(8, "N3", NO_PROPS, Some("air_nomad"))
            .unwrap();
        graph
            .add_node(9, "N4", NO_PROPS, Some("air_nomad"))
            .unwrap();
        graph
            .add_node(5, "N5", NO_PROPS, Some("air_nomad"))
            .unwrap();
        graph
            .add_node(5, "N6", NO_PROPS, Some("fire_nation"))
            .unwrap();
        graph
            .add_node(3, "N7", NO_PROPS, Some("air_nomad"))
            .unwrap();
        graph
            .add_node(4, "N8", NO_PROPS, Some("fire_nation"))
            .unwrap();

        graph
    }

    fn search_edges_by_composite_filter<G: StaticGraphViewOps + AdditionOps>(
        graph: &G,
        node_types: Vec<String>,
        filter: FilterExpr,
    ) -> Vec<String> {
        let sgm = graph.subgraph_node_types(node_types).materialize().unwrap();
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
        let sgm = graph.subgraph_node_types(node_types).materialize().unwrap();
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
    use crate::{db::graph::views::property_filter::PropertyRef, prelude::*};

    #[test]
    fn test_type_filtered_subgraph() {
        let graph = Graph::new();
        graph.add_edge(1, "A", "B", [("p1", 1u64)], None).unwrap();
        graph.add_edge(2, "B", "C", [("p1", 2u64)], None).unwrap();
        graph.add_edge(3, "C", "D", [("p1", 3u64)], None).unwrap();
        graph.add_edge(4, "D", "E", [("p1", 4u64)], None).unwrap();

        graph
            .add_node(1, "A", [("p1", 1u64)], Some("water_tribe"))
            .unwrap();
        graph
            .add_node(2, "B", [("p1", 2u64)], Some("water_tribe"))
            .unwrap();
        graph
            .add_node(3, "C", [("p1", 1u64)], Some("fire_nation"))
            .unwrap();
        graph
            .add_node(4, "D", [("p1", 1u64)], Some("air_nomads"))
            .unwrap();

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
}
