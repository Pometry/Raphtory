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
    core::{
        entities::{edges::edge_ref::EdgeRef, ELID},
        storage::timeindex::EventTime,
    },
    inherit::Base,
};
use raphtory_storage::{
    core_ops::{CoreGraphOps, InheritCoreGraphOps},
    graph::edges::edge_storage_ops::EdgeStorageOps,
};

/// Edge-filtered graph: hides edges that fail the predicate `filter`.
///
/// Parallel to `NodeFilteredGraph` but for edges: `internal_filter_edge` evaluates
/// `filter.apply(storage, edge_ref)` in O(1) after a single compile step.
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
        let edge_ref: EdgeRef = self.core_edge(Either::Left(eid.eid())).out_ref();
        self.filter.apply(
            self.graph.core_graph(),
            edge_ref.at_layer(eid.layer()).at(t),
        )
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
