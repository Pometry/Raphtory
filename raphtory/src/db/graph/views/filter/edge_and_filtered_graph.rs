use crate::{
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            view::internal::{
                EdgeHistoryFilter, EdgeList, Immutable, InheritMaterialize, InheritNodeFilterOps,
                InheritNodeHistoryFilter, InheritStorageOps, InheritTimeSemantics,
                InternalEdgeFilterOps, InternalEdgeLayerFilterOps, InternalExplodedEdgeFilterOps,
                InternalLayerOps, ListOps, NodeList, Static,
            },
        },
        graph::views::filter::{
            internal::{CreateEdgeFilter, CreateExplodedEdgeFilter},
            model::AndFilter,
        },
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
pub struct EdgeAndFilteredGraph<G, L, R> {
    graph: G,
    left: L,
    right: R,
    layer_ids: LayerIds,
}

impl<L: CreateEdgeFilter, R: CreateEdgeFilter> CreateEdgeFilter for AndFilter<L, R> {
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

impl<L: CreateExplodedEdgeFilter, R: CreateExplodedEdgeFilter> CreateExplodedEdgeFilter
    for AndFilter<L, R>
{
    type ExplodedEdgeFiltered<'graph, G: GraphViewOps<'graph>>
        = EdgeAndFilteredGraph<
        G,
        L::ExplodedEdgeFiltered<'graph, G>,
        R::ExplodedEdgeFiltered<'graph, G>,
    >
    where
        Self: 'graph;

    fn create_exploded_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::ExplodedEdgeFiltered<'graph, G>, GraphError> {
        let left = self.left.create_exploded_edge_filter(graph.clone())?;
        let right = self.right.create_exploded_edge_filter(graph.clone())?;
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

impl<G, L, R> InheritCoreGraphOps for EdgeAndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritStorageOps for EdgeAndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritMaterialize for EdgeAndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritNodeFilterOps for EdgeAndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritPropertiesOps for EdgeAndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritTimeSemantics for EdgeAndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritNodeHistoryFilter
    for EdgeAndFilteredGraph<G, L, R>
{
}

impl<G, L: Send + Sync, R: Send + Sync> InternalLayerOps for EdgeAndFilteredGraph<G, L, R>
where
    G: InternalLayerOps,
{
    fn layer_ids(&self) -> &LayerIds {
        &self.layer_ids
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

impl<G, L: InternalEdgeLayerFilterOps, R: InternalEdgeLayerFilterOps> InternalEdgeLayerFilterOps
    for EdgeAndFilteredGraph<G, L, R>
{
    fn internal_edge_layer_filtered(&self) -> bool {
        self.left.internal_edge_layer_filtered() || self.right.internal_edge_layer_filtered()
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        self.left.internal_layer_filter_edge_list_trusted()
            && self.right.internal_layer_filter_edge_list_trusted()
    }

    fn internal_filter_edge_layer(&self, edge: EdgeStorageRef, layer: usize) -> bool {
        self.left.internal_filter_edge_layer(edge, layer)
            && self.right.internal_filter_edge_layer(edge, layer)
    }
}

impl<G, L: InternalExplodedEdgeFilterOps, R: InternalExplodedEdgeFilterOps>
    InternalExplodedEdgeFilterOps for EdgeAndFilteredGraph<G, L, R>
{
    fn internal_exploded_edge_filtered(&self) -> bool {
        self.left.internal_exploded_edge_filtered() || self.right.internal_exploded_edge_filtered()
    }

    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        self.left.internal_exploded_filter_edge_list_trusted()
            && self.right.internal_exploded_filter_edge_list_trusted()
    }

    fn internal_filter_exploded_edge(
        &self,
        eid: ELID,
        t: TimeIndexEntry,
        layer_ids: &LayerIds,
    ) -> bool {
        self.left.internal_filter_exploded_edge(eid, t, layer_ids)
            && self.right.internal_filter_exploded_edge(eid, t, layer_ids)
    }
}

impl<G, L: InternalEdgeFilterOps, R: InternalEdgeFilterOps> InternalEdgeFilterOps
    for EdgeAndFilteredGraph<G, L, R>
{
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        self.left.internal_edge_filtered() || self.right.internal_edge_filtered()
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        self.left.internal_edge_list_trusted() && self.right.internal_edge_list_trusted()
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        self.left.internal_filter_edge(edge, layer_ids)
            && self.right.internal_filter_edge(edge, layer_ids)
    }
}
