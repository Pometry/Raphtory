use crate::{
    core::entities::{nodes::node_store::NodeStore, LayerIds},
    db::api::{
        properties::internal::InheritPropertiesOps,
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
    fn nodes_filtered(&self) -> bool {
        true
    }

    fn node_list_trusted(&self) -> bool {
        false
    }

    fn filter_node(&self, node: &NodeStore, layer_ids: &LayerIds) -> bool {
        self.node_types.contains(&node.node_type) && self.graph.filter_node(node, layer_ids)
    }
}
