use crate::{
    db::{
        api::{
            properties::internal::{
                InheritEdgePropertySchemaOps, InheritMetadataPropertiesOps,
                InheritNodePropertySchemaOps, InternalTemporalPropertiesOps,
                InternalTemporalPropertyViewOps,
            },
            view::{
                internal::{
                    DynGraphArc, EdgeList, GraphTimeSemanticsOps, Immutable, InheritMaterialize,
                    InheritStorageOps, InternalEdgeFilterOps, InternalEdgeLayerFilterOps,
                    InternalExplodedEdgeFilterOps, InternalLayerOps, InternalNodeFilterOps,
                    ListOps, NodeList, Static, TimeSemantics,
                },
                BoxedLIter,
            },
        },
        graph::views::{
            filter::operand::{
                keeps_edge, keeps_edge_layer, keeps_exploded_edge, keeps_node, lists_trusted,
                restricts_edge_layers, restricts_edges, restricts_exploded_edges, restricts_nodes,
            },
            window_graph::WindowBounds,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::{
        entities::{
            properties::prop::{Prop, PropType},
            LayerId, LayerIds, ELID,
        },
        storage::{arc_str::ArcStr, timeindex::EventTime},
    },
    inherit::Base,
};
use raphtory_storage::{
    core_ops::{CoreGraphOps, InheritCoreGraphOps},
    graph::{edges::edge_ref::EdgeEntryRef, nodes::node_ref::NodeStorageRef},
};
use std::{ops::Range, sync::Arc};

/// The graph keeping what every operand keeps. A level is filtered when any
/// operand restricts it.
///
/// The `and` resolves its layers and its window once, when built: the layers
/// every operand keeps, and the overlap of the operands' windows. The graph is
/// seen through that window; a window with no bounds is the graph itself.
#[derive(Clone)]
pub struct AndFilteredGraph<'graph, G> {
    pub(crate) graph: G,
    pub(crate) operands: Arc<[DynGraphArc<'graph>]>,
    pub(crate) layer_ids: LayerIds,
    /// The optional window bounds: the overlap of the operands' windows.
    pub(crate) bounds: WindowBounds,
}

impl<'graph, G: GraphViewOps<'graph>> AndFilteredGraph<'graph, G> {
    /// `operands` is never empty: an `and` of nothing is refused before this
    /// graph is built. The fallbacks below only keep the folds total.
    pub fn new(graph: G, operands: Arc<[DynGraphArc<'graph>]>) -> Self {
        let layer_ids = operands
            .iter()
            .map(|operand| operand.layer_ids().clone())
            .reduce(|layer_ids, operand| layer_ids.intersect(&operand))
            .unwrap_or_else(|| graph.layer_ids().clone());
        let start = operands
            .iter()
            .filter_map(|operand| operand.view_start())
            .max();
        let end = operands
            .iter()
            .filter_map(|operand| operand.view_end())
            .min();
        // Windows that do not overlap leave an empty window, not a reversed one.
        let end = end.map(|end| start.map_or(end, |start| end.max(start)));
        Self {
            graph,
            operands,
            layer_ids,
            bounds: WindowBounds::new(start, end),
        }
    }
}

impl<G> Base for AndFilteredGraph<'_, G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G> Static for AndFilteredGraph<'_, G> {}
impl<G> Immutable for AndFilteredGraph<'_, G> {}

impl<G> InheritCoreGraphOps for AndFilteredGraph<'_, G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for AndFilteredGraph<'graph, G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for AndFilteredGraph<'graph, G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMetadataPropertiesOps for AndFilteredGraph<'graph, G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodePropertySchemaOps for AndFilteredGraph<'graph, G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgePropertySchemaOps for AndFilteredGraph<'graph, G> {}

impl<'graph, G: GraphViewOps<'graph>> InternalTemporalPropertyViewOps
    for AndFilteredGraph<'graph, G>
{
    fn dtype(&self, id: usize) -> PropType {
        self.graph.dtype(id)
    }

    fn temporal_value(&self, id: usize) -> Option<Prop> {
        self.bounds.temporal_value(&self.graph, id)
    }

    fn temporal_iter(&self, id: usize) -> BoxedLIter<'_, (EventTime, Prop)> {
        self.bounds.temporal_iter(&self.graph, id)
    }

    fn temporal_iter_rev(&self, id: usize) -> BoxedLIter<'_, (EventTime, Prop)> {
        self.bounds.temporal_iter_rev(&self.graph, id)
    }

    fn temporal_value_at(&self, id: usize, t: EventTime) -> Option<Prop> {
        self.bounds.temporal_value_at(&self.graph, id, t)
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalTemporalPropertiesOps
    for AndFilteredGraph<'graph, G>
{
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.bounds.get_temporal_prop_id(&self.graph, name)
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.graph.get_temporal_prop_name(id)
    }

    fn temporal_prop_ids(&self) -> BoxedLIter<'_, usize> {
        self.bounds.temporal_prop_ids(&self.graph)
    }
}

impl<'graph, G: GraphViewOps<'graph>> GraphTimeSemanticsOps for AndFilteredGraph<'graph, G> {
    fn node_time_semantics(&self) -> TimeSemantics {
        self.bounds.node_time_semantics(&self.graph)
    }

    fn edge_time_semantics(&self) -> TimeSemantics {
        self.bounds.edge_time_semantics(&self.graph)
    }

    fn window_filtered(&self) -> bool {
        self.bounds.window_is_bounding(&self.graph)
    }

    fn view_start(&self) -> Option<EventTime> {
        self.bounds.start
    }

    fn view_end(&self) -> Option<EventTime> {
        self.bounds.end
    }

    fn earliest_time_global(&self) -> Option<i64> {
        self.bounds.earliest_time_global(&self.graph)
    }

    fn latest_time_global(&self) -> Option<i64> {
        self.bounds.latest_time_global(&self.graph)
    }

    fn earliest_time_window(&self, start: EventTime, end: EventTime) -> Option<i64> {
        self.graph.earliest_time_window(start, end)
    }

    fn latest_time_window(&self, start: EventTime, end: EventTime) -> Option<i64> {
        self.graph.latest_time_window(start, end)
    }

    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        self.bounds.has_temporal_prop(&self.graph, prop_id)
    }

    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLIter<'_, (EventTime, Prop)> {
        self.bounds.temporal_prop_iter(&self.graph, prop_id)
    }

    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<EventTime>) -> bool {
        self.graph.has_temporal_prop_window(prop_id, w)
    }

    fn temporal_prop_iter_window(
        &self,
        prop_id: usize,
        start: EventTime,
        end: EventTime,
    ) -> BoxedLIter<'_, (EventTime, Prop)> {
        self.graph.temporal_prop_iter_window(prop_id, start, end)
    }

    fn temporal_prop_iter_window_rev(
        &self,
        prop_id: usize,
        start: EventTime,
        end: EventTime,
    ) -> BoxedLIter<'_, (EventTime, Prop)> {
        self.graph
            .temporal_prop_iter_window_rev(prop_id, start, end)
    }

    fn temporal_prop_last_at(&self, prop_id: usize, t: EventTime) -> Option<(EventTime, Prop)> {
        self.bounds.temporal_prop_last_at(&self.graph, prop_id, t)
    }

    fn temporal_prop_last_at_window(
        &self,
        prop_id: usize,
        t: EventTime,
        w: Range<EventTime>,
    ) -> Option<(EventTime, Prop)> {
        self.graph.temporal_prop_last_at_window(prop_id, t, w)
    }
}

impl<G: InternalLayerOps> InternalLayerOps for AndFilteredGraph<'_, G> {
    fn layer_ids(&self) -> &LayerIds {
        &self.layer_ids
    }
}

impl<G: CoreGraphOps + ListOps> ListOps for AndFilteredGraph<'_, G> {
    fn node_list(&self) -> NodeList {
        self.operands
            .iter()
            .map(|operand| operand.node_list())
            .reduce(|nodes, operand| nodes.intersection(&operand, self.graph.core_graph()))
            .unwrap_or_else(|| self.graph.node_list())
    }

    fn edge_list(&self) -> EdgeList {
        self.operands
            .iter()
            .map(|operand| operand.edge_list())
            .reduce(|edges, operand| edges.intersection(&operand, self.graph.core_graph()))
            .unwrap_or_else(|| self.graph.edge_list())
    }
}

impl<G> InternalNodeFilterOps for AndFilteredGraph<'_, G> {
    #[inline]
    fn internal_nodes_filtered(&self) -> bool {
        self.operands.iter().any(restricts_nodes)
    }

    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        lists_trusted(&self.operands, |operand| {
            operand.internal_node_list_trusted()
        })
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, _layer_ids: &LayerIds) -> bool {
        self.operands
            .iter()
            .all(|operand| keeps_node(operand, node))
    }
}

impl<G> InternalEdgeLayerFilterOps for AndFilteredGraph<'_, G> {
    fn internal_edge_layer_filtered(&self) -> bool {
        self.operands.iter().any(restricts_edge_layers)
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        lists_trusted(&self.operands, |operand| {
            operand.internal_layer_filter_edge_list_trusted()
        })
    }

    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: LayerId) -> bool {
        self.operands
            .iter()
            .all(|operand| keeps_edge_layer(operand, edge, layer))
    }
}

impl<G> InternalExplodedEdgeFilterOps for AndFilteredGraph<'_, G> {
    fn internal_exploded_edge_filtered(&self) -> bool {
        self.operands.iter().any(restricts_exploded_edges)
    }

    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        lists_trusted(&self.operands, |operand| {
            operand.internal_exploded_filter_edge_list_trusted()
        })
    }

    fn internal_filter_exploded_edge(
        &self,
        eid: ELID,
        t: EventTime,
        _layer_ids: &LayerIds,
    ) -> bool {
        self.operands
            .iter()
            .all(|operand| keeps_exploded_edge(operand, eid, t))
    }
}

impl<G> InternalEdgeFilterOps for AndFilteredGraph<'_, G> {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        self.operands.iter().any(restricts_edges)
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        lists_trusted(&self.operands, |operand| {
            operand.internal_edge_list_trusted()
        })
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeEntryRef, _layer_ids: &LayerIds) -> bool {
        self.operands
            .iter()
            .all(|operand| keeps_edge(operand, edge))
    }
}
