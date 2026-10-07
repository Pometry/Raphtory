use crate::{
    db::api::{
        properties::internal::{
            InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps, InheritPropertiesOps,
        },
        view::internal::{
            Immutable, InheritLayerOps, InheritListOps, InheritMaterialize, InheritStorageOps,
            InheritTimeSemantics, InternalEdgeFilterOps, InternalEdgeLayerFilterOps,
            InternalExplodedEdgeFilterOps, InternalNodeFilterOps, Static,
        },
    },
    prelude::GraphViewOps,
};
use either::Either;
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

impl<G, L: InternalNodeFilterOps, R: InternalNodeFilterOps> InternalNodeFilterOps
    for OrFilteredGraph<G, L, R>
{
    #[inline]
    fn internal_nodes_filtered(&self) -> bool {
        self.left.internal_nodes_filtered() && self.right.internal_nodes_filtered()
    }

    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        self.left.internal_node_list_trusted() && self.right.internal_node_list_trusted()
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.left.internal_filter_node(node, layer_ids)
            || self.right.internal_filter_node(node, layer_ids)
    }
}

/// Whether `side` keeps `edge` at all: a side that does not filter edges keeps
/// every edge. A layer or an update of an edge belongs to the union through the
/// sides that keep the edge, so a side that drops the edge cannot let its layers
/// or updates through by not looking at them.
fn side_keeps_edge<S: InternalEdgeFilterOps>(
    side: &S,
    edge: EdgeEntryRef,
    layer_ids: &LayerIds,
) -> bool {
    !side.internal_edge_filtered() || side.internal_filter_edge(edge, layer_ids)
}

impl<'graph, G: GraphViewOps<'graph>, L, R> InternalEdgeLayerFilterOps for OrFilteredGraph<G, L, R>
where
    L: InternalEdgeFilterOps + InternalEdgeLayerFilterOps,
    R: InternalEdgeFilterOps + InternalEdgeLayerFilterOps,
{
    fn internal_edge_layer_filtered(&self) -> bool {
        self.left.internal_edge_layer_filtered() || self.right.internal_edge_layer_filtered()
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        self.left.internal_layer_filter_edge_list_trusted()
            && self.right.internal_layer_filter_edge_list_trusted()
    }

    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: LayerId) -> bool {
        let layer_ids = self.graph.layer_ids();
        layer_through(&self.left, edge, layer, layer_ids)
            || layer_through(&self.right, edge, layer, layer_ids)
    }
}

impl<'graph, G: GraphViewOps<'graph>, L, R> InternalExplodedEdgeFilterOps
    for OrFilteredGraph<G, L, R>
where
    L: InternalEdgeFilterOps + InternalExplodedEdgeFilterOps,
    R: InternalEdgeFilterOps + InternalExplodedEdgeFilterOps,
{
    fn internal_exploded_edge_filtered(&self) -> bool {
        self.left.internal_exploded_edge_filtered() || self.right.internal_exploded_edge_filtered()
    }

    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        self.left.internal_exploded_filter_edge_list_trusted()
            && self.right.internal_exploded_filter_edge_list_trusted()
    }

    fn internal_filter_exploded_edge(&self, eid: ELID, t: EventTime, layer_ids: &LayerIds) -> bool {
        let edge = self.graph.core_edge(Either::Left(eid.eid()));
        exploded_through(&self.left, edge.as_ref(), eid, t, layer_ids)
            || exploded_through(&self.right, edge.as_ref(), eid, t, layer_ids)
    }
}

/// Whether `side` lets `layer` of `edge` into the union: it keeps the edge and
/// either does not filter layers or keeps this one.
fn layer_through<S: InternalEdgeFilterOps + InternalEdgeLayerFilterOps>(
    side: &S,
    edge: EdgeEntryRef,
    layer: LayerId,
    layer_ids: &LayerIds,
) -> bool {
    side_keeps_edge(side, edge, layer_ids)
        && (!side.internal_edge_layer_filtered() || side.internal_filter_edge_layer(edge, layer))
}

/// Whether `side` lets the update of `edge` at `t` into the union: it keeps the
/// edge and either does not filter updates or keeps this one.
fn exploded_through<S: InternalEdgeFilterOps + InternalExplodedEdgeFilterOps>(
    side: &S,
    edge: EdgeEntryRef,
    eid: ELID,
    t: EventTime,
    layer_ids: &LayerIds,
) -> bool {
    side_keeps_edge(side, edge, layer_ids)
        && (!side.internal_exploded_edge_filtered()
            || side.internal_filter_exploded_edge(eid, t, layer_ids))
}

impl<G, L: InternalEdgeFilterOps, R: InternalEdgeFilterOps> InternalEdgeFilterOps
    for OrFilteredGraph<G, L, R>
{
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        self.left.internal_edge_filtered() && self.right.internal_edge_filtered()
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        self.left.internal_edge_list_trusted() && self.right.internal_edge_list_trusted()
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeEntryRef, layer_ids: &LayerIds) -> bool {
        self.left.internal_filter_edge(edge, layer_ids)
            || self.right.internal_filter_edge(edge, layer_ids)
    }
}
