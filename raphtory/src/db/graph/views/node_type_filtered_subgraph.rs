use crate::{
    core::entities::LayerIds,
    db::api::{
        properties::internal::InheritPropertiesOps,
        storage::graph::nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
        view::internal::{
            Base, Immutable, InheritCoreOps, InheritEdgeFilterOps, InheritLayerOps, InheritListOps,
            InheritMaterialize, InheritTimeSemantics, NodeFilterOps, Static,
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

impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeFilterOps for TypeFilteredSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritListOps for TypeFilteredSubgraph<G> {}

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

#[cfg(test)]
mod tests {
    use crate::prelude::*;

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
                .filter_nodes(PropertyFilter::eq("p1", 1u64))
                .unwrap()
                .nodes(),
            vec!["C", "D"]
        );

        assert!(type_filtered_subgraph
            .filter_edges(PropertyFilter::eq("p1", 1u64))
            .unwrap()
            .edges()
            .is_empty())
    }
}
