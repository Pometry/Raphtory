//! A view restricted to several disjoint time ranges at once.
//!
//! Every method here is the union of what the corresponding
//! [`WindowedGraph`](super::window_graph::WindowedGraph) method would return
//! for each of the ranges: an existence check is an `any`, an earliest time a
//! `min`, a latest time a `max`, and a history the per-range histories
//! concatenated in range order — which stays sorted and duplicate-free because
//! the ranges are sorted and disjoint.

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
                BoxedLIter, IntoDynBoxed,
            },
        },
        graph::graph::graph_equal,
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
    iter,
    ops::Range,
};

/// A view restricted to several disjoint time ranges at once — the union of
/// the windowed views over them. Produced when views are combined with `|` or
/// `~`, whose results are not single windows; a single range uses
/// `WindowedGraph`.
#[derive(Clone)]
pub struct MultiWindowedGraph<G> {
    /// The underlying `Graph` object.
    pub graph: G,
    /// Sorted, disjoint, at least two of them (one range is a `WindowedGraph`).
    pub windows: TimeRanges,
}

impl<G> MultiWindowedGraph<G> {
    /// Create a new multi-windowed graph over the given ranges.
    pub fn new(graph: G, windows: TimeRanges) -> Self {
        MultiWindowedGraph { graph, windows }
    }

    /// No range means no time at all, so the view is empty.
    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.windows.is_empty()
    }

    /// Anything short of every time removes events.
    #[inline(always)]
    fn is_bounding(&self) -> bool {
        !self.windows.is_all()
    }

    /// The end of the last range: the exclusive upper bound of the view.
    #[inline(always)]
    fn hull_end(&self) -> EventTime {
        self.windows.end().unwrap_or(EventTime::MIN)
    }
}

impl<G> Static for MultiWindowedGraph<G> {}

impl<G: Debug> Debug for MultiWindowedGraph<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MultiWindowedGraph(windows={:?}, graph={:?})",
            self.windows, self.graph,
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
        self.graph
            .graph_props_meta()
            .temporal_prop_mapper()
            .get_dtype(id)
            .unwrap()
    }

    /// The value as of the end of the view, which for a set of ranges is the
    /// end of the last one.
    fn temporal_value(&self, id: usize) -> Option<Prop> {
        self.graph.temporal_value_at(id, self.hull_end())
    }

    /// Union: the per-range histories concatenated in range order.
    fn temporal_iter(&self, id: usize) -> BoxedLIter<'_, (EventTime, Prop)> {
        if self.is_empty() {
            return iter::empty().into_dyn_boxed();
        }
        self.windows
            .iter()
            .flat_map(move |w| self.graph.temporal_prop_iter_window(id, w.start, w.end))
            .into_dyn_boxed()
    }

    /// Union, reversed: the ranges in reverse order, each reversed.
    fn temporal_iter_rev(&self, id: usize) -> BoxedLIter<'_, (EventTime, Prop)> {
        if self.is_empty() {
            return iter::empty().into_dyn_boxed();
        }
        self.windows
            .iter()
            .rev()
            .flat_map(move |w| self.graph.temporal_prop_iter_window_rev(id, w.start, w.end))
            .into_dyn_boxed()
    }

    /// The last update at or before `t` in any range: the latest range that
    /// has one wins, so search the ranges from the back.
    fn temporal_value_at(&self, id: usize, t: EventTime) -> Option<Prop> {
        self.windows
            .as_slice()
            .iter()
            .rev()
            .find_map(|w| self.graph.temporal_prop_last_at_window(id, t, w.clone()))
            .map(|(_, p)| p)
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalTemporalPropertiesOps for MultiWindowedGraph<G> {
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.graph
            .get_temporal_prop_id(name)
            .filter(|id| self.has_temporal_prop(*id))
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.graph.get_temporal_prop_name(id)
    }

    fn temporal_prop_ids(&self) -> BoxedLIter<'_, usize> {
        Box::new(
            self.graph
                .temporal_prop_ids()
                .filter(|id| self.has_temporal_prop(*id)),
        )
    }
}

impl<'graph, G: GraphViewOps<'graph>> GraphTimeSemanticsOps for MultiWindowedGraph<G> {
    fn node_time_semantics(&self) -> TimeSemantics {
        self.graph
            .node_time_semantics()
            .restrict(self.windows.clone())
    }

    fn edge_time_semantics(&self) -> TimeSemantics {
        self.graph
            .edge_time_semantics()
            .restrict(self.windows.clone())
    }

    #[inline]
    fn window_filtered(&self) -> bool {
        self.is_bounding()
    }

    fn view_start(&self) -> Option<EventTime> {
        self.windows.start()
    }

    fn view_end(&self) -> Option<EventTime> {
        self.windows.end()
    }

    /// Union: the earliest of the per-range earliest times. The ranges are ordered and
    /// disjoint, so the first one holding an event holds the earliest of them all.
    #[inline]
    fn earliest_time_global(&self) -> Option<i64> {
        self.windows
            .iter()
            .find_map(|w| self.graph.earliest_time_window(w.start, w.end))
    }

    /// Union: the latest of the per-range latest times. The ranges are ordered and
    /// disjoint, so the last one holding an event holds the latest of them all.
    #[inline]
    fn latest_time_global(&self) -> Option<i64> {
        self.windows
            .iter()
            .rev()
            .find_map(|w| self.graph.latest_time_window(w.start, w.end))
    }

    /// Union over the ranges clipped to the caller's window: the earliest of
    /// the per-range earliest times.
    #[inline]
    fn earliest_time_window(&self, start: EventTime, end: EventTime) -> Option<i64> {
        self.windows
            .clipped_to(&(start..end))
            .iter()
            .filter_map(|w| self.graph.earliest_time_window(w.start, w.end))
            .min()
    }

    /// Union over the ranges clipped to the caller's window: the latest of the
    /// per-range latest times.
    #[inline]
    fn latest_time_window(&self, start: EventTime, end: EventTime) -> Option<i64> {
        self.windows
            .clipped_to(&(start..end))
            .iter()
            .filter_map(|w| self.graph.latest_time_window(w.start, w.end))
            .max()
    }

    /// Union: the property exists if it has an update in any range.
    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        self.windows
            .iter()
            .any(|w| self.graph.has_temporal_prop_window(prop_id, w.clone()))
    }

    /// Union: the per-range histories concatenated in range order.
    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLIter<'_, (EventTime, Prop)> {
        if self.is_empty() {
            return iter::empty().into_dyn_boxed();
        }
        self.windows
            .iter()
            .flat_map(move |w| {
                self.graph
                    .temporal_prop_iter_window(prop_id, w.start, w.end)
            })
            .into_dyn_boxed()
    }

    /// Union over the ranges clipped to the caller's window: an update in any
    /// of them is enough.
    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<EventTime>) -> bool {
        self.windows
            .clipped_to(&w)
            .iter()
            .any(|w| self.graph.has_temporal_prop_window(prop_id, w.clone()))
    }

    /// Union over the ranges clipped to the caller's window, concatenated in
    /// range order.
    fn temporal_prop_iter_window(
        &self,
        prop_id: usize,
        start: EventTime,
        end: EventTime,
    ) -> BoxedLIter<'_, (EventTime, Prop)> {
        let windows = self.windows.clipped_to(&(start..end)).as_slice().to_vec();
        windows
            .into_iter()
            .flat_map(move |w| {
                self.graph
                    .temporal_prop_iter_window(prop_id, w.start, w.end)
            })
            .into_dyn_boxed()
    }

    /// The same, reversed: the clipped ranges in reverse order, each reversed.
    fn temporal_prop_iter_window_rev(
        &self,
        prop_id: usize,
        start: EventTime,
        end: EventTime,
    ) -> BoxedLIter<'_, (EventTime, Prop)> {
        let windows = self.windows.clipped_to(&(start..end)).as_slice().to_vec();
        windows
            .into_iter()
            .rev()
            .flat_map(move |w| {
                self.graph
                    .temporal_prop_iter_window_rev(prop_id, w.start, w.end)
            })
            .into_dyn_boxed()
    }

    /// The last update at or before `t` in any range: the latest range that
    /// has one wins, so search the ranges from the back.
    fn temporal_prop_last_at(&self, prop_id: usize, t: EventTime) -> Option<(EventTime, Prop)> {
        self.windows.as_slice().iter().rev().find_map(|w| {
            self.graph
                .temporal_prop_last_at_window(prop_id, t, w.clone())
        })
    }

    /// The same, over the ranges clipped to the caller's window.
    fn temporal_prop_last_at_window(
        &self,
        prop_id: usize,
        t: EventTime,
        w: Range<EventTime>,
    ) -> Option<(EventTime, Prop)> {
        self.windows
            .clipped_to(&w)
            .as_slice()
            .iter()
            .rev()
            .find_map(|w| {
                self.graph
                    .temporal_prop_last_at_window(prop_id, t, w.clone())
            })
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

#[cfg(test)]
mod tests {
    //! A multi-window view is the union of the windowed views over its ranges.
    //! Every expectation below is computed from the single windows, never from
    //! the view under test.
    //!
    //! ```text
    //! time:  0    1    2    3    4    5    6    7    8    9   10
    //! a→b         ●                             ●               events 1 and 7
    //! c→d                        ●                              event 4
    //! g→h                             ●                         event 5: in the GAP
    //! e→f                                  ●                    event 6
    //!
    //! window(0,5)  [==================)
    //! window(6,10)                          [==================)
    //! ```

    use super::*;
    use crate::{
        db::{
            api::view::{
                internal::{time_semantics::TimeRanges, TimeSemantics},
                StaticGraphViewOps,
            },
            graph::{
                edge::EdgeView,
                views::{deletion_graph::PersistentGraph, window_graph::WindowedGraph},
            },
        },
        prelude::*,
    };
    use raphtory_api::core::storage::timeindex::{AsTime, EventTime};
    use std::collections::BTreeSet;

    fn build<G: StaticGraphViewOps + AdditionOps>(g: &G) {
        g.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        g.add_edge(7, "a", "b", NO_PROPS, None).unwrap();
        g.add_edge(4, "c", "d", NO_PROPS, None).unwrap();
        g.add_edge(5, "g", "h", NO_PROPS, None).unwrap();
        g.add_edge(6, "e", "f", NO_PROPS, None).unwrap();
    }

    fn edge_ids<'a, G: GraphViewOps<'a>>(g: &G) -> BTreeSet<(String, String)> {
        g.edges()
            .iter()
            .map(|e| (e.src().name(), e.dst().name()))
            .collect()
    }

    fn node_names<'a, G: GraphViewOps<'a>>(g: &G) -> BTreeSet<String> {
        g.nodes().iter().map(|n| n.name()).collect()
    }

    fn history<'a, G: GraphViewOps<'a>>(g: &G, src: &str, dst: &str) -> Vec<i64> {
        g.edge(src, dst)
            .map(|e| e.history().iter().map(|t| t.t()).collect())
            .unwrap_or_default()
    }

    fn check_union<G: StaticGraphViewOps + AdditionOps>(g: G) {
        build(&g);
        let w1 = g.window(0, 5);
        let w2 = g.window(6, 10);
        let multi = MultiWindowedGraph::new(
            g.clone(),
            TimeRanges::new(vec![EventTime::range(0..5), EventTime::range(6..10)]),
        );

        // Membership is the union. Whether the gap edge g->h (event at t=5) is
        // in it follows from the windows alone: on an event graph it is in
        // neither; on a persistent graph it is alive at t=6 and so in the second.
        let want_edges: BTreeSet<_> = edge_ids(&w1).union(&edge_ids(&w2)).cloned().collect();
        assert_eq!(edge_ids(&multi), want_edges);
        let gap = ("g".to_string(), "h".to_string());
        assert_eq!(
            edge_ids(&multi).contains(&gap),
            edge_ids(&w1).contains(&gap) || edge_ids(&w2).contains(&gap)
        );
        let want_nodes: BTreeSet<_> = node_names(&w1).union(&node_names(&w2)).cloned().collect();
        assert_eq!(node_names(&multi), want_nodes);

        // History concatenates per range, in order.
        let mut want_hist = history(&w1, "a", "b");
        want_hist.extend(history(&w2, "a", "b"));
        assert_eq!(history(&multi, "a", "b"), want_hist);

        // Bounds are the extremes over the ranges.
        assert_eq!(
            multi.earliest_time(),
            w1.earliest_time()
                .into_iter()
                .chain(w2.earliest_time())
                .min()
        );
        assert_eq!(
            multi.latest_time(),
            w1.latest_time().into_iter().chain(w2.latest_time()).max()
        );

        // Exploded count is the sum over ranges.
        fn exploded<'a, G: GraphViewOps<'a>>(e: Option<EdgeView<G>>) -> usize {
            e.map(|e| e.explode().iter().count()).unwrap_or(0)
        }
        assert_eq!(
            exploded(multi.edge("a", "b")),
            exploded(w1.edge("a", "b")) + exploded(w2.edge("a", "b"))
        );
    }

    #[test]
    fn multi_window_is_the_union_of_its_windows_on_an_event_graph() {
        check_union(Graph::new());
    }

    #[test]
    fn multi_window_is_the_union_of_its_windows_on_a_persistent_graph() {
        check_union(PersistentGraph::new());
    }

    #[test]
    fn persistent_edge_alive_across_the_gap_is_present_in_both_ranges() {
        // On a persistent graph an edge added before the first range and never
        // deleted is alive at the start of each range, so each contributes the
        // boundary event the single window would report.
        let g = PersistentGraph::new();
        g.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        let w1 = g.window(3, 5);
        let w2 = g.window(6, 10);
        let multi = MultiWindowedGraph::new(
            g.clone(),
            TimeRanges::new(vec![EventTime::range(3..5), EventTime::range(6..10)]),
        );
        let single = |w: &WindowedGraph<PersistentGraph>| {
            w.edge("a", "b")
                .map(|e| e.explode().iter().count())
                .unwrap_or(0)
        };
        assert_eq!(single(&w1), 1);
        assert_eq!(single(&w2), 1);
        assert_eq!(
            multi
                .edge("a", "b")
                .map(|e| e.explode().iter().count())
                .unwrap_or(0),
            single(&w1) + single(&w2)
        );
    }

    #[test]
    fn restricting_a_window_by_a_range_set_intersects() {
        // window(0,8) restricted to {[0,5), [6,10)} is {[0,5), [6,8)}.
        let g = Graph::new();
        build(&g);
        let sem = g.window(0, 8).edge_time_semantics();
        match sem.restrict(TimeRanges::new(vec![
            EventTime::range(0..5),
            EventTime::range(6..10),
        ])) {
            TimeSemantics::MultiWindow(m) => {
                assert_eq!(
                    m.windows(),
                    &TimeRanges::new(vec![EventTime::range(0..5), EventTime::range(6..8)])
                );
            }
            other => panic!("expected MultiWindow, got {other:?}"),
        }
        // Restricting to ranges that leave a single interval collapses to Window.
        let sem = g.window(0, 8).edge_time_semantics();
        assert!(matches!(
            sem.restrict(TimeRanges::new(vec![EventTime::range(3..20)])),
            TimeSemantics::Window(_)
        ));
    }
}
