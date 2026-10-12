use crate::{
    db::{
        api::{
            properties::internal::{
                InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps, InheritPropertiesOps,
            },
            view::internal::{
                GraphView, Immutable, InheritLayerOps, InheritListOps, InheritMaterialize,
                InheritStorageOps, InheritTimeSemantics, InternalEdgeFilterOps,
                InternalEdgeLayerFilterOps, InternalExplodedEdgeFilterOps, InternalNodeFilterOps,
                Static,
            },
        },
        graph::views::filter::leg::{
            keeps_edge, keeps_edge_layer, keeps_exploded_edge, keeps_node, restricts_edge_layers,
            restricts_edges, restricts_exploded_edges, restricts_nodes, restricts_view,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::{
        entities::{LayerId, LayerIds, ELID},
        storage::timeindex::EventTime,
    },
    inherit::Base,
};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::{edges::edge_ref::EdgeEntryRef, nodes::node_ref::NodeStorageRef},
};

#[derive(Debug, Clone)]
pub struct OrFilteredGraph<G, L, R> {
    pub(crate) graph: G,
    pub(crate) left: L,
    pub(crate) right: R,
}

impl<G, L, R> OrFilteredGraph<G, L, R> {
    pub fn new(graph: G, left: L, right: R) -> Self {
        Self { graph, left, right }
    }
}

impl<G, L, R> Base for OrFilteredGraph<G, L, R> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G, L, R> Static for OrFilteredGraph<G, L, R> {}
impl<G, L, R> Immutable for OrFilteredGraph<G, L, R> {}

impl<'graph, G: GraphViewOps<'graph>, L, R> InheritCoreGraphOps for OrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritStorageOps for OrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritLayerOps for OrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritListOps for OrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritMaterialize for OrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritPropertiesOps for OrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritNodePropertySchemaOps
    for OrFilteredGraph<G, L, R>
{
}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritEdgePropertySchemaOps
    for OrFilteredGraph<G, L, R>
{
}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritTimeSemantics for OrFilteredGraph<G, L, R> {}

impl<G, L: GraphView, R: GraphView> InternalNodeFilterOps for OrFilteredGraph<G, L, R> {
    #[inline]
    fn internal_nodes_filtered(&self) -> bool {
        restricts_nodes(&self.left) && restricts_nodes(&self.right)
    }

    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        self.left.internal_node_list_trusted()
            && self.right.internal_node_list_trusted()
            && !restricts_view(&self.left)
            && !restricts_view(&self.right)
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, _layer_ids: &LayerIds) -> bool {
        keeps_node(&self.left, node) || keeps_node(&self.right, node)
    }
}

/// A layer or an update of an edge belongs to the union through the legs that
/// keep the edge, so either leg restricting layers or updates can drop one
/// that the other leg's edge would otherwise let through.
impl<G, L: GraphView, R: GraphView> InternalEdgeLayerFilterOps for OrFilteredGraph<G, L, R> {
    fn internal_edge_layer_filtered(&self) -> bool {
        restricts_edge_layers(&self.left) || restricts_edge_layers(&self.right)
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        self.left.internal_layer_filter_edge_list_trusted()
            && self.right.internal_layer_filter_edge_list_trusted()
            && !restricts_view(&self.left)
            && !restricts_view(&self.right)
    }

    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: LayerId) -> bool {
        keeps_edge_layer(&self.left, edge, layer) || keeps_edge_layer(&self.right, edge, layer)
    }
}

impl<G, L: GraphView, R: GraphView> InternalExplodedEdgeFilterOps for OrFilteredGraph<G, L, R> {
    fn internal_exploded_edge_filtered(&self) -> bool {
        restricts_exploded_edges(&self.left) || restricts_exploded_edges(&self.right)
    }

    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        self.left.internal_exploded_filter_edge_list_trusted()
            && self.right.internal_exploded_filter_edge_list_trusted()
            && !restricts_view(&self.left)
            && !restricts_view(&self.right)
    }

    fn internal_filter_exploded_edge(
        &self,
        eid: ELID,
        t: EventTime,
        _layer_ids: &LayerIds,
    ) -> bool {
        keeps_exploded_edge(&self.left, eid, t) || keeps_exploded_edge(&self.right, eid, t)
    }
}

impl<G, L: GraphView, R: GraphView> InternalEdgeFilterOps for OrFilteredGraph<G, L, R> {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        restricts_edges(&self.left) && restricts_edges(&self.right)
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        self.left.internal_edge_list_trusted()
            && self.right.internal_edge_list_trusted()
            && !restricts_view(&self.left)
            && !restricts_view(&self.right)
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeEntryRef, _layer_ids: &LayerIds) -> bool {
        keeps_edge(&self.left, edge) || keeps_edge(&self.right, edge)
    }
}
