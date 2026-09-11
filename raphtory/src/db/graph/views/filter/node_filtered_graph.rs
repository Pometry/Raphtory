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
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
};

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
            && self
                .filter
                .const_value_in_domain(self.graph.core_graph())
                .is_some_and(|v| v)
    }
}

impl<G: GraphView, F: NodeFilterOp> ListOps for NodeFilteredGraph<G, F> {
    /// The nodes this view can contain.
    ///
    /// An exactness claim on the result means "every key here satisfies every
    /// filter of this view", which `list_trusted` relies on to skip
    /// re-checking. Two ways to lose it:
    ///
    /// - this view's own filter is not reflected in the list, because no index
    ///   could serve it and its domain came back as everything; or
    /// - the inner view is not trusted for its own filters, so a claim built
    ///   on top of its list does not account for them.
    fn node_list(&self) -> NodeList {
        let inner = self.graph.node_list();
        let combined = match self.filter.domain(self.graph.core_graph()) {
            NodeList::All if self.filter.is_filtered() => inner.clone().into_inexact(),
            domain => domain.intersection(&inner),
        };
        if self.graph.internal_node_list_trusted() {
            combined
        } else {
            combined.into_inexact()
        }
    }

    fn edge_list(&self) -> EdgeList {
        self.graph.edge_list()
    }
}
