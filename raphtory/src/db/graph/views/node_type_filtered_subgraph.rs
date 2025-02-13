use crate::{
    core::entities::LayerIds,
    db::api::{
        properties::internal::InheritPropertiesOps,
        storage::graph::nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
        view::internal::{
            Base, Immutable, InheritCoreOps, InheritEdgeFilterOps, InheritIndexSearch,
            InheritLayerOps, InheritListOps, InheritMaterialize, InheritNodeHistoryFilter,
            InheritTimeSemantics, NodeFilterOps, Static,
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

impl<'graph, G: GraphViewOps<'graph>> NodeFilterOps for TypeFilteredSubgraph<G> {
    fn nodes_filtered(&self) -> bool {
        true
    }

    fn node_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.node_types.contains(&node.node_type_id()) && self.graph.filter_node(node, layer_ids)
    }
}

#[cfg(test)]
mod search_node_type_filtered_subgraph_tests {
    use crate::{
        core::Prop,
        db::{
            api::view::{SearchableGraphOps, StaticGraphViewOps},
            graph::views::{deletion_graph::PersistentGraph, property_filter::CompositeNodeFilter},
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
        filter: &CompositeNodeFilter,
    ) -> Vec<String> {
        let sgm = graph.subgraph_node_types(node_types).materialize().unwrap();
        let mut results = sgm
            .search_nodes(&filter, 10, 0)
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
        filter: &CompositeNodeFilter,
    ) -> Vec<String> {
        let sgm = graph.subgraph_node_types(node_types).materialize().unwrap();
        let mut results = sgm
            .window(w.start, w.end)
            .search_nodes(&filter, 10, 0)
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
        let filter = CompositeNodeFilter::Property(PropertyFilter::eq("p1", 1u64));
        let results = search_nodes_by_composite_filter(&graph, node_types, &filter);
        assert_eq!(
            results,
            vec!["N1", "N2", "N3", "N4", "N5", "N6", "N7", "N8"]
        );

        let node_types = vec!["air_nomad".into(), "water_tribe".into()];
        let filter = CompositeNodeFilter::Property(PropertyFilter::eq("p1", 1u64));
        let results = search_nodes_by_composite_filter(&graph, node_types, &filter);
        assert_eq!(results, vec!["N1", "N2", "N3", "N4", "N5", "N7"]);
    }

    #[test]
    fn test_search_nodes_windowed_type_filtered_subgraph() {
        let graph = Graph::new();
        let graph = init_graph(graph);

        let node_types = get_all_node_types(&graph);
        let filter = CompositeNodeFilter::Property(PropertyFilter::eq("p1", 1u64));
        let results = search_nodes_by_composite_filter_w(&graph, 6..9, node_types, &filter);
        assert_eq!(results, vec!["N1", "N2", "N3", "N6"]);

        let node_types = vec!["air_nomad".into(), "water_tribe".into()];
        let filter = CompositeNodeFilter::Property(PropertyFilter::eq("p1", 1u64));
        let results = search_nodes_by_composite_filter_w(&graph, 6..9, node_types, &filter);
        assert_eq!(results, vec!["N1", "N2", "N3"]);
    }

    #[test]
    fn test_search_nodes_persistent_type_filtered_subgraph() {
        let graph = PersistentGraph::new();
        let graph = init_graph(graph);

        let node_types = get_all_node_types(&graph);
        let filter = CompositeNodeFilter::Property(PropertyFilter::eq("p1", 1u64));
        let results = search_nodes_by_composite_filter(&graph, node_types, &filter);
        assert_eq!(
            results,
            vec!["N1", "N2", "N3", "N4", "N5", "N6", "N7", "N8"]
        );

        let node_types = vec!["air_nomad".into(), "water_tribe".into()];
        let filter = CompositeNodeFilter::Property(PropertyFilter::eq("p1", 1u64));
        let results = search_nodes_by_composite_filter(&graph, node_types, &filter);
        assert_eq!(results, vec!["N1", "N2", "N3", "N4", "N5", "N7"]);
    }

    #[test]
    fn test_search_nodes_windowed_persistent_type_filtered_subgraph() {
        let graph = PersistentGraph::new();
        let graph = init_graph(graph);

        let node_types = get_all_node_types(&graph);
        let filter = CompositeNodeFilter::Property(PropertyFilter::eq("p1", 1u64));
        let results = search_nodes_by_composite_filter_w(&graph, 6..9, node_types, &filter);
        assert_eq!(results, vec!["N1", "N2", "N3", "N5", "N6", "N7", "N8"]);

        let node_types = vec!["air_nomad".into(), "water_tribe".into()];
        let filter = CompositeNodeFilter::Property(PropertyFilter::eq("p1", 1u64));
        let results = search_nodes_by_composite_filter_w(&graph, 6..9, node_types, &filter);
        assert_eq!(results, vec!["N1", "N2", "N3", "N5", "N7"]);
    }
}
