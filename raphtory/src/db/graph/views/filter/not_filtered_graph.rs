use crate::{
    db::{
        api::{
            properties::internal::{
                InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps, InheritPropertiesOps,
            },
            view::internal::{
                GraphView, Immutable, InheritEdgeHistoryFilter, InheritLayerOps, InheritListOps,
                InheritMaterialize, InheritNodeHistoryFilter, InheritStorageOps,
                InheritTimeSemantics, InternalEdgeFilterOps, InternalEdgeLayerFilterOps,
                InternalExplodedEdgeFilterOps, InternalNodeFilterOps, Static,
            },
        },
        graph::views::filter::leg::{
            keeps_edge, keeps_edge_layer, keeps_exploded_edge, keeps_node, restricts_edges,
            restricts_nodes,
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

/// The graph keeping what a predicate leg drops; a view leg is refused before this graph is built.
#[derive(Debug, Clone)]
pub struct NotFilteredGraph<G, T> {
    pub(crate) graph: G,
    pub(crate) filter: T,
}

impl<G, T> NotFilteredGraph<G, T> {
    pub fn new(graph: G, filter: T) -> Self {
        Self { graph, filter }
    }
}

impl<G, T> Base for NotFilteredGraph<G, T> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G, T> Static for NotFilteredGraph<G, T> {}
impl<G, T> Immutable for NotFilteredGraph<G, T> {}

impl<'graph, G: GraphViewOps<'graph>, T> InheritCoreGraphOps for NotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritStorageOps for NotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritLayerOps for NotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritListOps for NotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritMaterialize for NotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritPropertiesOps for NotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritNodePropertySchemaOps for NotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritEdgePropertySchemaOps for NotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritTimeSemantics for NotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritNodeHistoryFilter for NotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritEdgeHistoryFilter for NotFilteredGraph<G, T> {}

/// The complement of a leg: a node or an edge stays when the graph keeps it and
/// the leg drops it, so a node or an edge that exists in a window or layer leg
/// goes with all its layers and updates. Only a leg's predicate on layers or on
/// updates is complemented a layer or an update at a time.
impl<G: GraphView, T: GraphView> InternalNodeFilterOps for NotFilteredGraph<G, T> {
    fn internal_nodes_filtered(&self) -> bool {
        self.graph.internal_nodes_filtered() || restricts_nodes(&self.filter)
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_node(node, layer_ids)
            && !(restricts_nodes(&self.filter) && keeps_node(&self.filter, node))
    }
}

impl<'graph, G: GraphViewOps<'graph>, T: GraphView> InternalEdgeLayerFilterOps
    for NotFilteredGraph<G, T>
{
    fn internal_edge_layer_filtered(&self) -> bool {
        self.graph.internal_edge_layer_filtered() || self.filter.internal_edge_layer_filtered()
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: LayerId) -> bool {
        self.graph.internal_filter_edge_layer(edge, layer)
            && !(self.filter.internal_edge_layer_filtered()
                && keeps_edge_layer(&self.filter, edge, layer))
    }
}

impl<'graph, G: GraphViewOps<'graph>, T: GraphView> InternalExplodedEdgeFilterOps
    for NotFilteredGraph<G, T>
{
    fn internal_exploded_edge_filtered(&self) -> bool {
        self.graph.internal_exploded_edge_filtered()
            || self.filter.internal_exploded_edge_filtered()
    }

    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_exploded_edge(&self, eid: ELID, t: EventTime, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_exploded_edge(eid, t, layer_ids)
            && !(self.filter.internal_exploded_edge_filtered()
                && keeps_exploded_edge(&self.filter, eid, t))
    }
}

impl<'graph, G: GraphViewOps<'graph>, T: GraphView> InternalEdgeFilterOps
    for NotFilteredGraph<G, T>
{
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        self.graph.internal_edge_filtered() || restricts_edges(&self.filter)
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeEntryRef, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_edge(edge, layer_ids)
            && !(restricts_edges(&self.filter) && keeps_edge(&self.filter, edge))
    }
}
