use crate::{
    core::entities::LayerIds,
    db::{
        api::{
            properties::internal::{
                InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps, InheritPropertiesOps,
            },
            view::internal::{
                Immutable, InheritEdgeFilterOps, InheritEdgeHistoryFilter,
                InheritEdgeLayerFilterOps, InheritLayerOps, InheritListOps, InheritMaterialize,
                InheritNodeFilterOps, InheritNodeHistoryFilter, InheritStorageOps,
                InheritTimeSemantics, InternalExplodedEdgeFilterOps, Static,
            },
        },
        graph::views::filter::model::edge_expr::EdgeOp,
    },
    prelude::GraphViewOps,
};
use either::Either;
use raphtory_api::{
    core::{entities::ELID, storage::timeindex::EventTime},
    inherit::Base,
};
use raphtory_storage::core_ops::{CoreGraphOps, InheritCoreGraphOps};

/// Exploded-edge-filtered graph: hides the exploded instances that fail the
/// predicate `filter`, which is asked about each instance through `apply_exploded`.
#[derive(Clone)]
pub struct ExplodedEdgeExprFilteredGraph<G, F> {
    pub(crate) graph: G,
    pub(crate) filter: F,
}

impl<G, F> ExplodedEdgeExprFilteredGraph<G, F> {
    pub fn new(graph: G, filter: F) -> Self {
        Self { graph, filter }
    }
}

impl<G, F> Base for ExplodedEdgeExprFilteredGraph<G, F> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G, F> Static for ExplodedEdgeExprFilteredGraph<G, F> {}
impl<G, F> Immutable for ExplodedEdgeExprFilteredGraph<G, F> {}

impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritCoreGraphOps
    for ExplodedEdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritStorageOps
    for ExplodedEdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritLayerOps
    for ExplodedEdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritListOps
    for ExplodedEdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritMaterialize
    for ExplodedEdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritNodeFilterOps
    for ExplodedEdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritPropertiesOps
    for ExplodedEdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritNodePropertySchemaOps
    for ExplodedEdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritEdgePropertySchemaOps
    for ExplodedEdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritTimeSemantics
    for ExplodedEdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritNodeHistoryFilter
    for ExplodedEdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritEdgeHistoryFilter
    for ExplodedEdgeExprFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone>
    InternalExplodedEdgeFilterOps for ExplodedEdgeExprFilteredGraph<G, F>
{
    fn internal_exploded_edge_filtered(&self) -> bool {
        true
    }

    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_exploded_edge(&self, eid: ELID, t: EventTime, layer_ids: &LayerIds) -> bool {
        if !self.graph.internal_filter_exploded_edge(eid, t, layer_ids) {
            return false;
        }
        // Deletions carry no properties, so they always pass through: filtering
        // them out would silently extend the previous addition's interval on a
        // persistent graph.
        if eid.is_deletion() {
            return true;
        }
        let edge = self.core_edge(Either::Left(eid.eid()));
        self.filter
            .apply_exploded(self.graph.core_graph(), edge.as_ref(), eid.layer(), t)
    }
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritEdgeLayerFilterOps
    for ExplodedEdgeExprFilteredGraph<G, F>
{
}

impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InheritEdgeFilterOps
    for ExplodedEdgeExprFilteredGraph<G, F>
{
}
