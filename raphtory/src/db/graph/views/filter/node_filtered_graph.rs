use crate::{
    db::api::{
        properties::internal::{
            InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps, InheritPropertiesOps,
        },
        state::ops::NodeFilterOp,
        view::internal::{
            EdgeList, GraphView, Immutable, InheritAllEdgeFilterOps, InheritEdgeHistoryFilter,
            InheritLayerOps, InheritMaterialize, InheritNodeHistoryFilter, InheritStorageOps,
            InheritTimeSemantics, InternalNodeFilterOps, ListOps, NodeList, Static,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::{core::entities::LayerIds, inherit::Base};
use raphtory_storage::{core_ops::InheritCoreGraphOps, graph::nodes::node_ref::NodeStorageRef};
use storage::api::nodes::NodeRefOps;

#[derive(Debug, Clone)]
pub struct NodeFilteredGraph<G, F> {
    graph: G,
    filter: F,
}

impl<G, F> NodeFilteredGraph<G, F> {
    pub fn new(graph: G, filter: F) -> Self {
        Self { graph, filter }
    }
}

impl<G, F> Base for NodeFilteredGraph<G, F> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G, F> Static for NodeFilteredGraph<G, F> {}
impl<G, F> Immutable for NodeFilteredGraph<G, F> {}

impl<'graph, G: GraphViewOps<'graph>, F: NodeFilterOp> InheritCoreGraphOps
    for NodeFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: NodeFilterOp> InheritStorageOps
    for NodeFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: NodeFilterOp> InheritLayerOps for NodeFilteredGraph<G, F> {}
impl<'graph, G: GraphViewOps<'graph>, F: NodeFilterOp> InheritMaterialize
    for NodeFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: NodeFilterOp> InheritAllEdgeFilterOps
    for NodeFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: NodeFilterOp> InheritPropertiesOps
    for NodeFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: NodeFilterOp> InheritNodePropertySchemaOps
    for NodeFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: NodeFilterOp> InheritEdgePropertySchemaOps
    for NodeFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: NodeFilterOp> InheritTimeSemantics
    for NodeFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: NodeFilterOp> InheritNodeHistoryFilter
    for NodeFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: NodeFilterOp> InheritEdgeHistoryFilter
    for NodeFilteredGraph<G, F>
{
}

impl<'graph, G: GraphViewOps<'graph>, F: NodeFilterOp> InternalNodeFilterOps
    for NodeFilteredGraph<G, F>
{
    fn internal_nodes_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_node(node, layer_ids)
            && self.filter.apply(self.graph.core_graph(), node.vid())
    }

    fn internal_node_list_trusted(&self) -> bool {
        self.graph.internal_node_list_trusted()
            && self.filter.const_value_in_domain().is_some_and(|v| v)
    }
}

impl<G: GraphView, F: NodeFilterOp> ListOps for NodeFilteredGraph<G, F> {
    fn node_list(&self) -> NodeList {
        self.filter
            .domain(self.graph.core_graph())
            .intersection(&self.graph.node_list())
    }

    fn edge_list(&self) -> EdgeList {
        self.graph.edge_list()
    }
}
