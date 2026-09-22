use crate::{
    core::entities::LayerIds,
    db::{
        api::{
            properties::internal::{
                InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps, InheritPropertiesOps,
            },
            view::internal::{
                Immutable, InheritEdgeHistoryFilter, InheritEdgeLayerFilterOps,
                InheritExplodedEdgeFilterOps, InheritLayerOps, InheritListOps, InheritMaterialize,
                InheritNodeFilterOps, InheritNodeHistoryFilter, InheritStorageOps,
                InheritTimeSemantics, InternalEdgeFilterOps, Static,
            },
        },
        graph::views::filter::model::edge_expr::EdgeOp,
    },
    prelude::GraphViewOps,
};
use raphtory_api::{core::entities::edges::edge_ref::EdgeRef, inherit::Base};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps, graph::edges::edge_storage_ops::EdgeStorageOps,
};
use storage::EdgeEntryRef;

/// Edge-filtered graph: hides edges that fail the predicate `filter`.
///
/// Parallel to `NodeFilteredGraph` but for edges: `internal_filter_edge` evaluates
/// `filter.apply(storage, edge_ref)` in O(1) after a single compile step.
#[derive(Clone)]
pub struct EdgeExprFilteredGraph<G, F> {
    pub(crate) graph: G,
    pub(crate) filter: F,
}

impl<G, F> EdgeExprFilteredGraph<G, F> {
    pub fn new(graph: G, filter: F) -> Self {
        Self { graph, filter }
    }
}

impl<G, F> Base for EdgeExprFilteredGraph<G, F> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G, F> Static for EdgeExprFilteredGraph<G, F> {}
impl<G, F> Immutable for EdgeExprFilteredGraph<G, F> {}

impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritCoreGraphOps
    for EdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritStorageOps
    for EdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritLayerOps
    for EdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritListOps
    for EdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritMaterialize
    for EdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritNodeFilterOps
    for EdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritPropertiesOps
    for EdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritNodePropertySchemaOps
    for EdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritEdgePropertySchemaOps
    for EdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritTimeSemantics
    for EdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritNodeHistoryFilter
    for EdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritEdgeHistoryFilter
    for EdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritExplodedEdgeFilterOps
    for EdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritEdgeLayerFilterOps
    for EdgeExprFilteredGraph<G, F>
{
}

impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InternalEdgeFilterOps
    for EdgeExprFilteredGraph<G, F>
{
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeEntryRef, layer_ids: &LayerIds) -> bool {
        if !self.graph.internal_filter_edge(edge, layer_ids) {
            return false;
        }
        let edge_ref: EdgeRef = edge.out_ref();
        self.filter.apply(self.graph.core_graph(), edge_ref)
    }
}
