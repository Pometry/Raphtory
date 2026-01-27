use crate::{
    core::entities::LayerIds,
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            view::internal::{
                Immutable, InheritEdgeFilterOps, InheritEdgeHistoryFilter,
                InheritEdgeLayerFilterOps, InheritLayerOps, InheritListOps, InheritMaterialize,
                InheritNodeFilterOps, InheritNodeHistoryFilter, InheritStorageOps,
                InheritTimeSemantics, InternalExplodedEdgeFilterOps, Static,
            },
        },
        graph::views::filter::model::{
            exploded_edge_filter::ExplodedEdgeFilter, property_filter::PropertyFilter,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::{entities::ELID, storage::timeindex::EventTime},
    inherit::Base,
};
use raphtory_storage::core_ops::InheritCoreGraphOps;

#[derive(Debug, Clone)]
pub struct ExplodedEdgePropertyFilteredGraph<G> {
    graph: G,
    prop_id: usize,
    filter: PropertyFilter<ExplodedEdgeFilter>,
}

impl<G> Static for ExplodedEdgePropertyFilteredGraph<G> {}
impl<G> Immutable for ExplodedEdgePropertyFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> ExplodedEdgePropertyFilteredGraph<G> {
    pub(crate) fn new(
        graph: G,
        prop_id: usize,
        filter: PropertyFilter<ExplodedEdgeFilter>,
    ) -> Self {
        Self {
            graph,
            prop_id,
            filter,
        }
    }
}

impl<G> Base for ExplodedEdgePropertyFilteredGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for ExplodedEdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter
    for ExplodedEdgePropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter
    for ExplodedEdgePropertyFilteredGraph<G>
{
}

impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for ExplodedEdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for ExplodedEdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for ExplodedEdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for ExplodedEdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps
    for ExplodedEdgePropertyFilteredGraph<G>
{
}

impl<'graph, G: GraphViewOps<'graph>> InheritNodeFilterOps
    for ExplodedEdgePropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics
    for ExplodedEdgePropertyFilteredGraph<G>
{
}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeFilterOps
    for ExplodedEdgePropertyFilteredGraph<G>
{
}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeLayerFilterOps
    for ExplodedEdgePropertyFilteredGraph<G>
{
}

impl<'graph, G: GraphViewOps<'graph>> InternalExplodedEdgeFilterOps
    for ExplodedEdgePropertyFilteredGraph<G>
{
    fn internal_exploded_edge_filtered(&self) -> bool {
        true
    }

    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_exploded_edge(&self, eid: ELID, t: EventTime, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_exploded_edge(eid, t, layer_ids) && {
            if eid.is_deletion() {
                true
            } else {
                self.filter.matches_exploded_edge(
                    &self.graph,
                    self.prop_id,
                    eid.edge,
                    t,
                    eid.layer(),
                )
            }
        }
    }
}
