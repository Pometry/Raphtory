use crate::{
    core::entities::LayerIds,
    db::{
        api::{
            properties::internal::{
                InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps, InheritPropertiesOps,
            },
            view::internal::{
                Immutable, InheritEdgeHistoryFilter, InheritLayerOps, InheritListOps,
                InheritMaterialize, InheritNodeFilterOps, InheritNodeHistoryFilter,
                InheritStorageOps, InheritTimeSemantics, InternalEdgeFilterOps,
                InternalEdgeLayerFilterOps, InternalExplodedEdgeFilterOps, Static,
            },
        },
        graph::views::filter::model::edge_expr::EdgeOp,
    },
    prelude::GraphViewOps,
};
use either::Either;
use raphtory_api::{
    core::{
        entities::{LayerId, ELID},
        storage::timeindex::EventTime,
    },
    inherit::Base,
};
use raphtory_storage::core_ops::{CoreGraphOps, InheritCoreGraphOps};
use storage::EdgeEntryRef;

/// Edge-filtered graph: hides edges that fail the predicate `filter`.
///
/// Parallel to `NodeFilteredGraph` but for edges: `internal_filter_edge` hands the
/// storage entry it is given straight to `filter.apply`.
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
/// An op that tells exploded instances apart (`filters_exploded`) is asked about each
/// one; a plain op decides per edge and the instances of a kept edge all pass.
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone>
    InternalExplodedEdgeFilterOps for EdgeExprFilteredGraph<G, F>
{
    fn internal_exploded_edge_filtered(&self) -> bool {
        self.filter.filters_exploded() || self.graph.internal_exploded_edge_filtered()
    }

    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        !self.filter.filters_exploded() && self.graph.internal_exploded_filter_edge_list_trusted()
    }

    fn internal_filter_exploded_edge(&self, eid: ELID, t: EventTime, layer_ids: &LayerIds) -> bool {
        if !self.graph.internal_filter_exploded_edge(eid, t, layer_ids) {
            return false;
        }
        // Deletions carry no properties, so they always pass through: filtering
        // them out would silently extend the previous addition's interval on a
        // persistent graph.
        if !self.filter.filters_exploded() || eid.is_deletion() {
            return true;
        }
        let edge = self.core_edge(Either::Left(eid.eid()));
        self.filter
            .apply_exploded(self.graph.core_graph(), edge.as_ref(), eid.layer(), t)
    }

    fn node_filter_includes_exploded_edge_filter(&self) -> bool {
        !self.filter.filters_exploded() && self.graph.node_filter_includes_exploded_edge_filter()
    }
}
impl<'graph, G: GraphViewOps<'graph>, F: EdgeOp<Output = bool> + Clone> InternalEdgeLayerFilterOps
    for EdgeExprFilteredGraph<G, F>
{
    fn internal_edge_layer_filtered(&self) -> bool {
        self.filter.filters_exploded() || self.graph.internal_edge_layer_filtered()
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        !self.filter.filters_exploded() && self.graph.internal_layer_filter_edge_list_trusted()
    }

    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: LayerId) -> bool {
        if !self.graph.internal_filter_edge_layer(edge, layer) {
            return false;
        }
        !self.filter.filters_exploded()
            || self
                .filter
                .apply_layer(self.graph.core_graph(), edge, layer)
    }

    fn node_filter_includes_edge_layer_filter(&self) -> bool {
        !self.filter.filters_exploded() && self.graph.node_filter_includes_edge_layer_filter()
    }
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
        self.filter.apply(self.graph.core_graph(), edge)
    }
}
