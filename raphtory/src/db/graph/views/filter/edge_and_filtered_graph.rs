use crate::{
    core::utils::errors::GraphError,
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            storage::graph::edges::edge_ref::EdgeStorageRef,
            view::{
                internal::{
                    EdgeFilterOps, EdgeHistoryFilter, EdgeList, Immutable, InheritCoreOps,
                    InheritMaterialize, InheritNodeFilterOps, InheritNodeHistoryFilter,
                    InheritStorageOps, InheritTimeSemantics, InternalLayerOps, ListOps, NodeList,
                    Static,
                },
                Base,
            },
        },
        graph::views::filter::{internal::InternalEdgeFilterOps, model::AndFilter},
    },
    prelude::{GraphViewOps, Layer},
};
use raphtory_api::core::{
    entities::{LayerIds, EID},
    storage::timeindex::TimeIndexEntry,
};
use std::ops::Range;

#[derive(Debug, Clone)]
pub struct EdgeAndFilteredGraph<G, L, R> {
    graph: G,
    left: L,
    right: R,
    layer_ids: LayerIds,
}

impl<L: InternalEdgeFilterOps, R: InternalEdgeFilterOps> InternalEdgeFilterOps for AndFilter<L, R> {
    type EdgeFiltered<'graph, G: GraphViewOps<'graph>>
        = EdgeAndFilteredGraph<G, L::EdgeFiltered<'graph, G>, R::EdgeFiltered<'graph, G>>
    where
        Self: 'graph;

    fn create_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFiltered<'graph, G>, GraphError> {
        let left = self.left.create_edge_filter(graph.clone())?;
        let right = self.right.create_edge_filter(graph.clone())?;
        let layer_ids = left.layer_ids().intersect(right.layer_ids());
        Ok(EdgeAndFilteredGraph {
            graph,
            left,
            right,
            layer_ids,
        })
    }
}

impl<G, L, R> Base for EdgeAndFilteredGraph<G, L, R> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G, L, R> Static for EdgeAndFilteredGraph<G, L, R> {}
impl<G, L, R> Immutable for EdgeAndFilteredGraph<G, L, R> {}

impl<'graph, G: GraphViewOps<'graph>, L, R> InheritCoreOps for EdgeAndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritStorageOps for EdgeAndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritMaterialize for EdgeAndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritNodeFilterOps for EdgeAndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritPropertiesOps for EdgeAndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritTimeSemantics for EdgeAndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritNodeHistoryFilter
    for EdgeAndFilteredGraph<G, L, R>
{
}

impl<G, L, R> InternalLayerOps for EdgeAndFilteredGraph<G, L, R>
where
    G: InternalLayerOps,
{
    fn layer_ids(&self) -> &LayerIds {
        &self.layer_ids
    }

    fn layer_ids_from_names(&self, key: Layer) -> Result<LayerIds, GraphError> {
        Ok(self
            .layer_ids
            .intersect(&self.graph.layer_ids_from_names(key)?))
    }

    fn valid_layer_ids_from_names(&self, key: Layer) -> LayerIds {
        self.layer_ids
            .intersect(&self.graph.valid_layer_ids_from_names(key))
    }
}

impl<G, L, R> ListOps for EdgeAndFilteredGraph<G, L, R>
where
    L: ListOps,
    R: ListOps,
{
    fn node_list(&self) -> NodeList {
        let left = self.left.node_list();
        let right = self.right.node_list();
        left.intersection(&right)
    }

    fn edge_list(&self) -> EdgeList {
        let left = self.left.edge_list();
        let right = self.right.edge_list();
        left.intersection(&right)
    }
}

impl<G, L, R> EdgeHistoryFilter for EdgeAndFilteredGraph<G, L, R>
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
            && self
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
            && self
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
            && self
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
        ) && self
            .right
            .is_edge_prop_update_latest_window(layer_ids, layer_id, prop_id, edge_id, time, w)
    }
}

impl<G, L: EdgeFilterOps, R: EdgeFilterOps> EdgeFilterOps for EdgeAndFilteredGraph<G, L, R> {
    #[inline]
    fn edges_filtered(&self) -> bool {
        self.left.edges_filtered() || self.right.edges_filtered()
    }

    #[inline]
    fn edge_list_trusted(&self) -> bool {
        self.left.edge_list_trusted() && self.right.edge_list_trusted()
    }

    #[inline]
    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        self.left.filter_edge(edge.clone(), layer_ids)
            && self.right.filter_edge(edge.clone(), layer_ids)
    }
}
