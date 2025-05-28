use crate::{
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            view::internal::{
                EdgeFilterOps, EdgeHistoryFilter, Immutable, InheritLayerOps, InheritListOps,
                InheritMaterialize, InheritNodeFilterOps, InheritNodeHistoryFilter,
                InheritStorageOps, InheritTimeSemantics, Static,
            },
        },
        graph::views::filter::{internal::InternalEdgeFilterOps, model::OrFilter},
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::{
        entities::{LayerIds, EID, ELID},
        storage::timeindex::TimeIndexEntry,
    },
    inherit::Base,
};
use raphtory_storage::{core_ops::InheritCoreGraphOps, graph::edges::edge_ref::EdgeStorageRef};
use std::ops::Range;

#[derive(Debug, Clone)]
pub struct EdgeOrFilteredGraph<G, L, R> {
    graph: G,
    left: L,
    right: R,
}

impl<L: InternalEdgeFilterOps, R: InternalEdgeFilterOps> InternalEdgeFilterOps for OrFilter<L, R> {
    type EdgeFiltered<'graph, G: GraphViewOps<'graph>>
        = EdgeOrFilteredGraph<G, L::EdgeFiltered<'graph, G>, R::EdgeFiltered<'graph, G>>
    where
        Self: 'graph;

    fn create_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFiltered<'graph, G>, GraphError> {
        let left = self.left.create_edge_filter(graph.clone())?;
        let right = self.right.create_edge_filter(graph.clone())?;
        Ok(EdgeOrFilteredGraph { graph, left, right })
    }
}

impl<G, L, R> Base for EdgeOrFilteredGraph<G, L, R> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G, L, R> Static for EdgeOrFilteredGraph<G, L, R> {}
impl<G, L, R> Immutable for EdgeOrFilteredGraph<G, L, R> {}

impl<'graph, G: GraphViewOps<'graph>, L, R> InheritCoreGraphOps for EdgeOrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritStorageOps for EdgeOrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritLayerOps for EdgeOrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritListOps for EdgeOrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritMaterialize for EdgeOrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritNodeFilterOps for EdgeOrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritPropertiesOps for EdgeOrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritTimeSemantics for EdgeOrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritNodeHistoryFilter
    for EdgeOrFilteredGraph<G, L, R>
{
}

impl<G, L, R> EdgeHistoryFilter for EdgeOrFilteredGraph<G, L, R>
where
    L: EdgeHistoryFilter,
    R: EdgeHistoryFilter,
{
    fn is_edge_prop_update_available(
        &self,
        layer_id: usize,
        prop_id: usize,
        edge_id: EID,
        time: TimeIndexEntry,
    ) -> bool {
        self.left
            .is_edge_prop_update_available(layer_id, prop_id, edge_id, time)
            || self
                .right
                .is_edge_prop_update_available(layer_id, prop_id, edge_id, time)
    }

    fn is_edge_prop_update_available_window(
        &self,
        layer_id: usize,
        prop_id: usize,
        edge_id: EID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        self.left
            .is_edge_prop_update_available_window(layer_id, prop_id, edge_id, time, w.clone())
            || self
                .right
                .is_edge_prop_update_available_window(layer_id, prop_id, edge_id, time, w)
    }

    fn is_edge_prop_update_latest(
        &self,
        layer_ids: &LayerIds,
        layer_id: usize,
        prop_id: usize,
        edge_id: EID,
        time: TimeIndexEntry,
    ) -> bool {
        self.left
            .is_edge_prop_update_latest(layer_ids, layer_id, prop_id, edge_id, time)
            || self
                .right
                .is_edge_prop_update_latest(layer_ids, layer_id, prop_id, edge_id, time)
    }

    fn is_edge_prop_update_latest_window(
        &self,
        layer_ids: &LayerIds,
        layer_id: usize,
        prop_id: usize,
        edge_id: EID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        self.left.is_edge_prop_update_latest_window(
            layer_ids,
            layer_id,
            prop_id,
            edge_id,
            time,
            w.clone(),
        ) || self
            .right
            .is_edge_prop_update_latest_window(layer_ids, layer_id, prop_id, edge_id, time, w)
    }
}

impl<G, L: EdgeFilterOps, R: EdgeFilterOps> EdgeFilterOps for EdgeOrFilteredGraph<G, L, R> {
    #[inline]
    fn edges_filtered(&self) -> bool {
        self.left.edges_filtered() && self.right.edges_filtered()
    }

    fn edge_history_filtered(&self) -> bool {
        self.left.edge_history_filtered() && self.right.edge_history_filtered()
    }

    #[inline]
    fn edge_list_trusted(&self) -> bool {
        self.left.edge_list_trusted() && self.right.edge_list_trusted()
    }

    fn filter_edge_history(&self, eid: ELID, t: TimeIndexEntry, layer_ids: &LayerIds) -> bool {
        self.left.filter_edge_history(eid, t, layer_ids)
            || self.right.filter_edge_history(eid, t, layer_ids)
    }

    #[inline]
    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        self.left.filter_edge(edge.clone(), layer_ids)
            || self.right.filter_edge(edge.clone(), layer_ids)
    }
}
