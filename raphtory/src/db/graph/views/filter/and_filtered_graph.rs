use crate::{
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            state::ops::{filter::AndOp, NodeFilterOp},
            view::internal::{
                EdgeList, GraphView, Immutable, InheritMaterialize, InheritStorageOps,
                InheritTimeSemantics, InternalEdgeFilterOps, InternalEdgeLayerFilterOps,
                InternalExplodedEdgeFilterOps, InternalLayerOps, InternalNodeFilterOps, ListOps,
                NodeList, Static,
            },
        },
        graph::views::filter::{internal::CreateFilter, model::AndFilter},
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::{
        entities::{LayerIds, ELID},
        storage::timeindex::TimeIndexEntry,
    },
    inherit::Base,
};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::{edges::edge_ref::EdgeStorageRef, nodes::node_ref::NodeStorageRef},
};

#[derive(Debug, Clone)]
pub struct AndFilteredGraph<G, L, R> {
    graph: G,
    left: L,
    right: R,
    layer_ids: LayerIds,
}

impl<L: CreateFilter, R: CreateFilter> CreateFilter for AndFilter<L, R> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>>
        = AndFilteredGraph<G, L::EntityFiltered<'graph, G>, R::EntityFiltered<'graph, G>>
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph>
        = AndOp<L::NodeFilter<'graph, G>, R::NodeFilter<'graph, G>>
    where
        Self: 'graph;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let left = self.left.create_filter(graph.clone())?;
        let right = self.right.create_filter(graph.clone())?;
        let layer_ids = left.layer_ids().intersect(right.layer_ids());
        Ok(AndFilteredGraph {
            graph,
            left,
            right,
            layer_ids,
        })
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        let left = self.left.create_node_filter(graph.clone())?;
        let right = self.right.create_node_filter(graph.clone())?;
        Ok(left.and(right))
    }
}

impl<G, L, R> Base for AndFilteredGraph<G, L, R> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G, L, R> Static for AndFilteredGraph<G, L, R> {}
impl<G, L, R> Immutable for AndFilteredGraph<G, L, R> {}

impl<G, L, R> InheritCoreGraphOps for AndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritStorageOps for AndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritMaterialize for AndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritPropertiesOps for AndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritTimeSemantics for AndFilteredGraph<G, L, R> {}

impl<G, L: Send + Sync, R: Send + Sync> InternalLayerOps for AndFilteredGraph<G, L, R>
where
    G: InternalLayerOps,
{
    fn layer_ids(&self) -> &LayerIds {
        &self.layer_ids
    }
}

impl<G, L, R> ListOps for AndFilteredGraph<G, L, R>
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

impl<G, L: InternalNodeFilterOps, R: InternalNodeFilterOps> InternalNodeFilterOps
    for AndFilteredGraph<G, L, R>
{
    #[inline]
    fn internal_nodes_filtered(&self) -> bool {
        self.left.internal_nodes_filtered() || self.right.internal_nodes_filtered()
    }

    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        self.left.internal_node_list_trusted() && self.right.internal_node_list_trusted()
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.left.internal_filter_node(node, layer_ids)
            && self.right.internal_filter_node(node, layer_ids)
    }
}

impl<G, L: InternalEdgeLayerFilterOps, R: InternalEdgeLayerFilterOps> InternalEdgeLayerFilterOps
    for AndFilteredGraph<G, L, R>
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
    InternalExplodedEdgeFilterOps for AndFilteredGraph<G, L, R>
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
    for AndFilteredGraph<G, L, R>
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
