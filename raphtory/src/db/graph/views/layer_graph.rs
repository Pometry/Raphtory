use crate::{
    core::entities::LayerIds,
    db::api::{
        properties::internal::InheritPropertiesOps,
        view::internal::{
            GraphView, Immutable, InheritEdgeFilterOps, InheritEdgeHistoryFilter,
            InheritEdgeLayerFilterOps, InheritExplodedEdgeFilterOps, InheritListOps,
            InheritMaterialize, InheritNodeFilterOps, InheritNodeHistoryFilter, InheritStorageOps,
            InheritTimeSemantics, InternalLayerOps, InternalNodeFilterOps, Static,
        },
    },
};
use raphtory_api::inherit::Base;
use raphtory_storage::{core_ops::InheritCoreGraphOps, graph::nodes::node_ref::NodeStorageRef};
use std::fmt::{Debug, Formatter};

#[derive(Clone)]
pub struct LayeredGraph<G> {
    /// The underlying `Graph` object.
    pub graph: G,
    /// The layer this graphs points to.
    pub layers: LayerIds,
}

impl<G: GraphView> Immutable for LayeredGraph<G> {}

impl<G> Static for LayeredGraph<G> {}

impl<G: GraphView + Debug> Debug for LayeredGraph<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LayeredGraph")
            .field("graph", &self.graph as &dyn Debug)
            .field("layers", &self.layers)
            .finish()
    }
}

impl<G: GraphView> Base for LayeredGraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G: GraphView> InheritTimeSemantics for LayeredGraph<G> {}

impl<G: GraphView> InheritListOps for LayeredGraph<G> {}

impl<G: GraphView> InheritCoreGraphOps for LayeredGraph<G> {}

impl<G: GraphView> InheritMaterialize for LayeredGraph<G> {}

impl<G: GraphView> InheritPropertiesOps for LayeredGraph<G> {}

impl<G: GraphView> InheritStorageOps for LayeredGraph<G> {}

impl<G: GraphView> InheritNodeHistoryFilter for LayeredGraph<G> {}

impl<G: GraphView> InheritEdgeHistoryFilter for LayeredGraph<G> {}
impl<G: GraphView> InternalNodeFilterOps for LayeredGraph<G> {
    fn internal_nodes_filtered(&self) -> bool {
        self.graph.internal_nodes_filtered()
    }

    fn internal_node_list_trusted(&self) -> bool {
        // after applying a layer, previously filtered lists can no longer be trusted
        self.graph.internal_node_list_trusted() && self.graph.node_list().unfiltered()
    }

    fn edge_filter_includes_node_filter(&self) -> bool {
        self.graph.edge_filter_includes_node_filter()
    }

    fn edge_layer_filter_includes_node_filter(&self) -> bool {
        self.graph.edge_layer_filter_includes_node_filter()
    }

    fn exploded_edge_filter_includes_node_filter(&self) -> bool {
        self.graph.exploded_edge_filter_includes_node_filter()
    }

    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_node(node, layer_ids)
    }
}

impl<G: GraphView> LayeredGraph<G> {
    pub fn new(graph: G, layers: LayerIds) -> Self {
        Self { graph, layers }
    }
}

impl<G: GraphView> InternalLayerOps for LayeredGraph<G> {
    fn layer_ids(&self) -> &LayerIds {
        &self.layers
    }
}

impl<G: GraphView> InheritEdgeLayerFilterOps for LayeredGraph<G> {}

impl<G: GraphView> InheritEdgeFilterOps for LayeredGraph<G> {}

impl<G: GraphView> InheritExplodedEdgeFilterOps for LayeredGraph<G> {}
