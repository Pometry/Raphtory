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
        graph::views::filter::internal::CreateExplodedEdgeFilter,
    },
    errors::GraphError,
    prelude::{GraphViewOps, LayerOps, PropertyFilter},
};
use raphtory_api::{
    core::{
        entities::{EID, ELID},
        storage::timeindex::TimeIndexEntry,
    },
    inherit::Base,
};
use raphtory_storage::core_ops::InheritCoreGraphOps;

#[derive(Debug, Clone)]
pub struct ExplodedEdgePropertyFilteredGraph<G> {
    graph: G,
    prop_id: Option<usize>,
    filter: PropertyFilter,
}

impl<G> Static for ExplodedEdgePropertyFilteredGraph<G> {}
impl<G> Immutable for ExplodedEdgePropertyFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> ExplodedEdgePropertyFilteredGraph<G> {
    pub(crate) fn new(graph: G, prop_id: Option<usize>, filter: PropertyFilter) -> Self {
        Self {
            graph,
            prop_id,
            filter,
        }
    }

    fn filter(&self, e: EID, t: TimeIndexEntry, layer: usize) -> bool {
        self.filter
            .matches_exploded_edge(&self.graph, self.prop_id, e, t, layer)
    }
}

impl CreateExplodedEdgeFilter for PropertyFilter {
    type ExplodedEdgeFiltered<'graph, G: GraphViewOps<'graph>> =
        ExplodedEdgePropertyFilteredGraph<G>;

    fn create_exploded_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::ExplodedEdgeFiltered<'graph, G>, GraphError> {
        let prop_id = self.resolve_prop_id(graph.edge_meta(), graph.num_layers() > 1)?;
        Ok(ExplodedEdgePropertyFilteredGraph::new(
            graph.clone(),
            prop_id,
            self,
        ))
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

    fn internal_filter_exploded_edge(
        &self,
        eid: ELID,
        t: TimeIndexEntry,
        layer_ids: &LayerIds,
    ) -> bool {
        self.graph.internal_filter_exploded_edge(eid, t, layer_ids) && {
            if eid.is_deletion() {
                true
            } else {
                self.filter(eid.edge, t, eid.layer())
            }
        }
    }
}
