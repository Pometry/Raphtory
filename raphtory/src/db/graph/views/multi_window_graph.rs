//! A view restricted to several disjoint time ranges at once: the union of the
//! windowed views over them. A node or an edge is in it when it is in any of
//! the windows, and its history is the per-range histories in range order,
//! which stays sorted and duplicate-free because the ranges are sorted and
//! disjoint. The graph-level answers come from `WindowBounds`, which every
//! view bounded in time shares.

use crate::{
    core::entities::LayerIds,
    db::{
        api::{
            properties::internal::{
                InheritEdgePropertySchemaOps, InheritMetadataPropertiesOps,
                InheritNodePropertySchemaOps, InternalTemporalPropertiesOps,
                InternalTemporalPropertyViewOps,
            },
            state::Index,
            view::{
                internal::{
                    EdgeList, GraphTimeSemanticsOps, GraphView, Immutable, InheritLayerOps,
                    InheritMaterialize, InheritStorageOps, InternalEdgeFilterOps,
                    InternalEdgeLayerFilterOps, InternalExplodedEdgeFilterOps,
                    InternalNodeFilterOps, ListOps, NodeList, Static, TimeRanges, TimeSemantics,
                },
                BoxedLIter,
            },
        },
        graph::{graph::graph_equal, views::window_graph::WindowBounds},
    },
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::{
        entities::{
            properties::prop::{Prop, PropType},
            LayerId, ELID,
        },
        storage::{arc_str::ArcStr, timeindex::EventTime},
    },
    inherit::Base,
};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::{edges::edge_ref::EdgeEntryRef, nodes::node_ref::NodeStorageRef},
};
use std::{
    fmt::{Debug, Formatter},
    ops::Range,
};

/// A view restricted to a set of disjoint time ranges. Produced when views
/// combine under `or` or `not`, whose results need not be single windows; with
/// one range it is a window, and with none it is empty.
#[derive(Clone)]
pub struct MultiWindowedGraph<G> {
    /// The underlying `Graph` object.
    pub graph: G,
    /// The ranges, already intersected with whatever `graph` is restricted to,
    /// so no call resolves them again.
    bounds: WindowBounds,
}

impl<G: GraphView> MultiWindowedGraph<G> {
    /// A view of `graph` restricted to `ranges`. Any windowing the graph
    /// already carries is folded in here, once.
    pub fn new(graph: G, ranges: TimeRanges) -> Self {
        let ranges = ranges.intersect(&graph.view_ranges());
        MultiWindowedGraph {
            graph,
            bounds: WindowBounds::from_ranges(ranges),
        }
    }

    /// No range means no time at all, so the view is empty.
    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.bounds.window_is_empty()
    }

    /// Whether the ranges remove any event of the graph.
    #[inline(always)]
    fn is_bounding(&self) -> bool {
        self.bounds.window_is_bounding(&self.graph)
    }
}

impl<G> Static for MultiWindowedGraph<G> {}

impl<G: Debug> Debug for MultiWindowedGraph<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MultiWindowedGraph(bounds={:?}, graph={:?})",
            self.bounds, self.graph,
        )
    }
}

impl<'graph1, 'graph2, G1: GraphViewOps<'graph1>, G2: GraphViewOps<'graph2>> PartialEq<G2>
    for MultiWindowedGraph<G1>
{
    fn eq(&self, other: &G2) -> bool {
        graph_equal(self, other)
    }
}

impl<'graph, G: GraphViewOps<'graph>> Base for MultiWindowedGraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> Immutable for MultiWindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for MultiWindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for MultiWindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for MultiWindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritMetadataPropertiesOps for MultiWindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritNodePropertySchemaOps for MultiWindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgePropertySchemaOps for MultiWindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for MultiWindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> ListOps for MultiWindowedGraph<G> {
    fn node_list(&self) -> NodeList {
        if self.is_empty() {
            NodeList::List {
                elems: Index::default(),
            }
        } else {
            self.graph.node_list()
        }
    }

    fn edge_list(&self) -> EdgeList {
        if self.is_empty() {
            EdgeList::List {
                elems: Index::default(),
            }
        } else {
            self.graph.edge_list()
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalNodeFilterOps for MultiWindowedGraph<G> {
    #[inline]
    fn internal_nodes_filtered(&self) -> bool {
        self.is_empty() || self.graph.internal_nodes_filtered()
    }

    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        self.is_empty() || (self.graph.internal_node_list_trusted() && !self.is_bounding())
    }

    #[inline]
    fn edge_filter_includes_node_filter(&self) -> bool {
        self.is_empty() || self.graph.edge_filter_includes_node_filter()
    }

    #[inline]
    fn edge_layer_filter_includes_node_filter(&self) -> bool {
        self.is_empty() || self.graph.edge_layer_filter_includes_node_filter()
    }

    #[inline]
    fn exploded_edge_filter_includes_node_filter(&self) -> bool {
        self.is_empty() || self.graph.exploded_edge_filter_includes_node_filter()
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        !self.is_empty() && self.graph.internal_filter_node(node, layer_ids)
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalTemporalPropertyViewOps for MultiWindowedGraph<G> {
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

impl<'graph, G: GraphViewOps<'graph>> InternalTemporalPropertiesOps for MultiWindowedGraph<G> {
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

impl<'graph, G: GraphViewOps<'graph>> GraphTimeSemanticsOps for MultiWindowedGraph<G> {
    fn node_time_semantics(&self) -> TimeSemantics {
        self.bounds.node_time_semantics(&self.graph)
    }

    fn edge_time_semantics(&self) -> TimeSemantics {
        self.bounds.edge_time_semantics(&self.graph)
    }

    #[inline]
    fn window_filtered(&self) -> bool {
        self.is_bounding()
    }

    fn view_start(&self) -> Option<EventTime> {
        self.bounds.start()
    }

    fn view_end(&self) -> Option<EventTime> {
        self.bounds.end()
    }

    fn earliest_time_global(&self) -> Option<i64> {
        self.bounds.earliest_time_global(&self.graph)
    }

    fn latest_time_global(&self) -> Option<i64> {
        self.bounds.latest_time_global(&self.graph)
    }

    fn earliest_time_window(&self, start: EventTime, end: EventTime) -> Option<i64> {
        self.bounds.earliest_time_window(&self.graph, start, end)
    }

    fn latest_time_window(&self, start: EventTime, end: EventTime) -> Option<i64> {
        self.bounds.latest_time_window(&self.graph, start, end)
    }

    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        self.bounds.has_temporal_prop(&self.graph, prop_id)
    }

    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLIter<'_, (EventTime, Prop)> {
        self.bounds.temporal_prop_iter(&self.graph, prop_id)
    }

    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<EventTime>) -> bool {
        self.bounds
            .has_temporal_prop_window(&self.graph, prop_id, w)
    }

    fn temporal_prop_iter_window(
        &self,
        prop_id: usize,
        start: EventTime,
        end: EventTime,
    ) -> BoxedLIter<'_, (EventTime, Prop)> {
        self.bounds
            .temporal_prop_iter_window(&self.graph, prop_id, start, end)
    }

    fn temporal_prop_iter_window_rev(
        &self,
        prop_id: usize,
        start: EventTime,
        end: EventTime,
    ) -> BoxedLIter<'_, (EventTime, Prop)> {
        self.bounds
            .temporal_prop_iter_window_rev(&self.graph, prop_id, start, end)
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
        self.bounds
            .temporal_prop_last_at_window(&self.graph, prop_id, t, w)
    }
}

// actual filtering is handled upstream for efficiency and to avoid double-checking nested windows
// here we just define the optimisation flags
impl<G: GraphView> InternalEdgeFilterOps for MultiWindowedGraph<G> {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        self.is_empty() || self.graph.internal_edge_filtered()
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        self.is_empty() || (!self.is_bounding() && self.graph.internal_edge_list_trusted())
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeEntryRef, layer_ids: &LayerIds) -> bool {
        !self.is_empty() && self.graph.internal_filter_edge(edge, layer_ids)
    }

    #[inline]
    fn node_filter_includes_edge_filter(&self) -> bool {
        self.is_empty() || self.graph.node_filter_includes_edge_filter()
    }
}

impl<G: GraphView> InternalEdgeLayerFilterOps for MultiWindowedGraph<G> {
    #[inline]
    fn internal_edge_layer_filtered(&self) -> bool {
        self.graph.internal_edge_layer_filtered()
    }

    #[inline]
    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        self.is_empty()
            || (!self.is_bounding() && self.graph.internal_layer_filter_edge_list_trusted())
    }

    #[inline]
    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: LayerId) -> bool {
        self.graph.internal_filter_edge_layer(edge, layer)
    }

    #[inline]
    fn node_filter_includes_edge_layer_filter(&self) -> bool {
        self.is_empty() || self.graph.node_filter_includes_edge_layer_filter()
    }
}

impl<G: GraphView> InternalExplodedEdgeFilterOps for MultiWindowedGraph<G> {
    #[inline]
    fn internal_exploded_edge_filtered(&self) -> bool {
        self.graph.internal_exploded_edge_filtered()
    }

    #[inline]
    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        self.is_empty()
            || (!self.is_bounding() && self.graph.internal_exploded_filter_edge_list_trusted())
    }

    #[inline]
    fn internal_filter_exploded_edge(&self, eid: ELID, t: EventTime, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_exploded_edge(eid, t, layer_ids)
    }

    #[inline]
    fn node_filter_includes_exploded_edge_filter(&self) -> bool {
        self.is_empty() || self.graph.node_filter_includes_exploded_edge_filter()
    }
}
