//! A windowed view is a subset of a graph between a specific time window.
//! For example, lets say you wanted to run an algorithm each month over a graph, graph window
//! would allow you to split the graph into 30 day chunks to do so.
//!
//! This module also defines the `GraphWindow` trait, which represents a window of time over
//! which a graph can be queried.
//!
//! GraphWindowSet implements the `Iterator` trait, producing `WindowedGraph` views
//! for each perspective within it.
//!
//! # Types
//!
//! * `GraphWindowSet` - A struct that allows iterating over a Graph broken down into multiple
//! windowed views. It contains a `Graph` and an iterator of `Perspective`.
//!
//! * `WindowedGraph` - A struct that represents a windowed view of a `Graph`.
//! It contains a `Graph`, a start time (`start`) and an end time (`end`).
//!
//! # Traits
//!
//! * `GraphViewInternalOps` - A trait that provides operations to a `WindowedGraph`
//! used internally by the `GraphWindowSet`.
//!
//! # Examples
//!
//! ```rust
//!
//! use raphtory::prelude::*;
//! use raphtory::db::api::view::*;
//!
//! let graph = Graph::new();
//! graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
//! graph.add_edge(1, 1, 3, NO_PROPS, None).unwrap();
//! graph.add_edge(2, 2, 3, NO_PROPS, None).unwrap();
//!
//!  let wg = graph.window(0, 1);
//!  assert_eq!(wg.edge(1, 2).unwrap().src().id(), GID::U64(1));
//! ```

use crate::{
    core::entities::LayerIds,
    db::{
        api::{
            properties::internal::{
                InheritConstantPropertiesOps, TemporalPropertiesOps, TemporalPropertyViewOps,
            },
            state::Index,
            view::{
                internal::{
                    EdgeFilterOps, EdgeHistoryFilter, EdgeList, EdgeTimeSemanticsOps,
                    GraphTimeSemanticsOps, Immutable, InheritLayerOps, InheritMaterialize,
                    InheritStorageOps, InternalNodeFilterOps, ListOps, NodeHistoryFilter, NodeList,
                    Static, TimeSemantics,
                },
                BoxableGraphView, BoxedLIter, IntoDynBoxed,
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
            EID, ELID, VID,
        },
        storage::{arc_str::ArcStr, timeindex::TimeIndexEntry},
    },
    inherit::Base,
    iter::IntoDynDBoxed,
};
use raphtory_storage::{
    core_ops::{CoreGraphOps, InheritCoreGraphOps},
    graph::{edges::edge_ref::EdgeStorageRef, nodes::node_ref::NodeStorageRef},
};
use std::{
    fmt::{Debug, Formatter},
    iter,
    ops::Range,
};

/// A struct that represents a windowed view of a `Graph`.
#[derive(Copy, Clone)]
pub struct WindowedGraph<G> {
    /// The underlying `Graph` object.
    pub graph: G,
    /// The inclusive start time of the window.
    pub start: Option<i64>,
    /// The exclusive end time of the window.
    pub end: Option<i64>,
}

impl<G> Static for WindowedGraph<G> {}

impl<G: Debug> Debug for WindowedGraph<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WindowedGraph(start={:?}, end={:?}, graph={:?})",
            self.start, self.end, self.graph,
        )
    }
}

impl<'graph1, 'graph2, G1: GraphViewOps<'graph1>, G2: GraphViewOps<'graph2>> PartialEq<G2>
    for WindowedGraph<G1>
{
    fn eq(&self, other: &G2) -> bool {
        graph_equal(self, other)
    }
}

impl<'graph, G: GraphViewOps<'graph>> Base for WindowedGraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G: BoxableGraphView + Clone> WindowedGraph<G> {
    #[inline(always)]
    fn window_bound(&self) -> Range<i64> {
        self.start_bound()..self.end_bound()
    }

    fn start_bound(&self) -> i64 {
        self.start.unwrap_or(i64::MIN)
    }

    #[inline(always)]
    fn end_bound(&self) -> i64 {
        self.end.unwrap_or(i64::MAX)
    }

    #[inline(always)]
    fn window_is_empty(&self) -> bool {
        self.start_bound() >= self.end_bound()
    }

    #[inline]
    fn start_is_bounding(&self) -> bool {
        match self.start {
            None => false,
            Some(start) => match self.graph.core_graph().earliest_time() {
                None => false,
                Some(graph_earliest) => start >= graph_earliest, // deletions have exclusive window, thus >= here!
            },
        }
    }

    #[inline]
    fn end_is_bounding(&self) -> bool {
        match self.end {
            None => false,
            Some(end) => match self.core_graph().latest_time() {
                None => false,
                Some(graph_latest) => end <= graph_latest,
            },
        }
    }
    #[inline]
    fn window_is_bounding(&self) -> bool {
        self.start_is_bounding() || self.end_is_bounding()
    }
}

impl<'graph, G: GraphViewOps<'graph>> Immutable for WindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for WindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for WindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> NodeHistoryFilter for WindowedGraph<G> {
    fn is_node_prop_update_available(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
    ) -> bool {
        self.graph
            .is_node_prop_update_available_window(prop_id, node_id, time, self.window_bound())
    }

    fn is_node_prop_update_available_window(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        self.graph
            .is_node_prop_update_available_window(prop_id, node_id, time, w)
    }

    fn is_node_prop_update_latest(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
    ) -> bool {
        self.graph
            .is_node_prop_update_latest_window(prop_id, node_id, time, self.window_bound())
    }

    fn is_node_prop_update_latest_window(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        self.graph
            .is_node_prop_update_latest_window(prop_id, node_id, time, w)
    }
}

impl<'graph, G: GraphViewOps<'graph>> EdgeHistoryFilter for WindowedGraph<G> {
    fn is_edge_prop_update_available(
        &self,
        layer_id: usize,
        prop_id: usize,
        edge_id: EID,
        time: TimeIndexEntry,
    ) -> bool {
        self.graph.is_edge_prop_update_available_window(
            layer_id,
            prop_id,
            edge_id,
            time,
            self.window_bound(),
        )
    }

    fn is_edge_prop_update_available_window(
        &self,
        layer_id: usize,
        prop_id: usize,
        edge_id: EID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        self.graph
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
        self.graph.is_edge_prop_update_latest_window(
            layer_ids,
            layer_id,
            prop_id,
            edge_id,
            time,
            self.window_bound(),
        )
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
        self.graph
            .is_edge_prop_update_latest_window(layer_ids, layer_id, prop_id, edge_id, time, w)
    }
}

impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for WindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritConstantPropertiesOps for WindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for WindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> ListOps for WindowedGraph<G> {
    fn node_list(&self) -> NodeList {
        if self.window_is_empty() {
            NodeList::List {
                elems: Index::default(),
            }
        } else {
            self.graph.node_list()
        }
    }

    fn edge_list(&self) -> EdgeList {
        if self.window_is_empty() {
            EdgeList::List {
                elems: Index::default(),
            }
        } else {
            self.graph.edge_list()
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalNodeFilterOps for WindowedGraph<G> {
    #[inline]
    fn internal_nodes_filtered(&self) -> bool {
        self.window_is_empty() || self.graph.internal_nodes_filtered() || self.window_is_bounding()
    }

    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        self.window_is_empty()
            || (self.graph.internal_node_list_trusted() && !self.window_is_bounding())
    }

    #[inline]
    fn edge_and_node_filter_independent(&self) -> bool {
        self.window_is_empty() || self.graph.edge_and_node_filter_independent()
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        !self.window_is_empty() && self.graph.internal_filter_node(node, layer_ids)
    }
}

impl<'graph, G: GraphViewOps<'graph>> TemporalPropertyViewOps for WindowedGraph<G> {
    fn dtype(&self, id: usize) -> PropType {
        self.graph
            .graph_meta()
            .temporal_prop_meta()
            .get_dtype(id)
            .unwrap()
    }

    fn temporal_value(&self, id: usize) -> Option<Prop> {
        self.graph.temporal_value_at(id, self.end_bound())
    }

    fn temporal_iter(&self, id: usize) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        if self.window_is_empty() {
            return iter::empty().into_dyn_boxed();
        }
        self.graph
            .temporal_prop_iter_window(id, self.start_bound(), self.end_bound())
            .into_dyn_boxed()
    }

    fn temporal_iter_rev(&self, id: usize) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        self.graph
            .temporal_prop_iter_window_rev(id, self.start_bound(), self.end_bound())
            .into_dyn_boxed()
    }

    fn temporal_value_at(&self, id: usize, t: i64) -> Option<Prop> {
        self.graph
            .temporal_prop_last_at_window(
                id,
                TimeIndexEntry::end(t),
                self.start_bound()..self.end_bound(),
            )
            .map(|(_, p)| p)
    }
}

impl<'graph, G: GraphViewOps<'graph>> TemporalPropertiesOps for WindowedGraph<G> {
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.graph
            .get_temporal_prop_id(name)
            .filter(|id| self.has_temporal_prop(*id))
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.graph.get_temporal_prop_name(id)
    }

    fn temporal_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        Box::new(
            self.graph
                .temporal_prop_ids()
                .filter(|id| self.has_temporal_prop(*id)),
        )
    }
}

impl<'graph, G: GraphViewOps<'graph>> GraphTimeSemanticsOps for WindowedGraph<G> {
    fn node_time_semantics(&self) -> TimeSemantics {
        self.graph
            .node_time_semantics()
            .window(self.start_bound()..self.end_bound())
    }

    fn edge_time_semantics(&self) -> TimeSemantics {
        self.graph
            .edge_time_semantics()
            .window(self.start_bound()..self.end_bound())
    }
    fn view_start(&self) -> Option<i64> {
        self.start
    }

    fn view_end(&self) -> Option<i64> {
        self.end
    }

    #[inline]
    fn earliest_time_global(&self) -> Option<i64> {
        if self.window_is_empty() {
            return None;
        }
        self.graph
            .earliest_time_window(self.start_bound(), self.end_bound())
    }

    #[inline]
    fn latest_time_global(&self) -> Option<i64> {
        if self.window_is_empty() {
            return None;
        }
        self.graph
            .latest_time_window(self.start_bound(), self.end_bound())
    }

    #[inline]
    fn earliest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.graph.earliest_time_window(start, end)
    }

    #[inline]
    fn latest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.graph.latest_time_window(start, end)
    }

    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        if self.window_is_empty() {
            return false;
        }
        self.graph
            .has_temporal_prop_window(prop_id, self.start_bound()..self.end_bound())
    }

    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        if self.window_is_empty() {
            return iter::empty().into_dyn_dboxed();
        }
        self.graph
            .temporal_prop_iter_window(prop_id, self.start_bound(), self.end_bound())
    }

    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<i64>) -> bool {
        self.graph.has_temporal_prop_window(prop_id, w.start..w.end)
    }

    fn temporal_prop_iter_window(
        &self,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        self.graph.temporal_prop_iter_window(prop_id, start, end)
    }

    fn temporal_prop_iter_window_rev(
        &self,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        self.graph
            .temporal_prop_iter_window_rev(prop_id, start, end)
    }

    fn temporal_prop_last_at(
        &self,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<(TimeIndexEntry, Prop)> {
        self.graph
            .temporal_prop_last_at_window(prop_id, t, self.start_bound()..self.end_bound())
    }

    fn temporal_prop_last_at_window(
        &self,
        prop_id: usize,
        t: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<(TimeIndexEntry, Prop)> {
        self.graph.temporal_prop_last_at_window(prop_id, t, w)
    }
}

impl<'graph, G: GraphViewOps<'graph>> EdgeFilterOps for WindowedGraph<G> {
    #[inline]
    fn edges_filtered(&self) -> bool {
        self.window_is_empty() || self.graph.edges_filtered() || self.window_is_bounding()
    }

    #[inline]
    fn edge_history_filtered(&self) -> bool {
        self.graph.edge_history_filtered()
    }
    #[inline]
    fn edge_list_trusted(&self) -> bool {
        self.window_is_empty() || (!self.window_is_bounding() && self.graph.edge_list_trusted())
    }

    #[inline]
    fn filter_edge_history(&self, eid: ELID, t: TimeIndexEntry, layer_ids: &LayerIds) -> bool {
        self.graph.filter_edge_history(eid, t, layer_ids)
    }

    #[inline]
    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        !self.window_is_empty()
            && self.graph.filter_edge(edge, layer_ids)
            && (!self.window_is_bounding()
                || self.graph.edge_time_semantics().include_edge_window(
                    edge,
                    &self.graph,
                    layer_ids,
                    self.window_bound(),
                ))
    }
}

/// A windowed graph is a graph that only allows access to nodes and edges within a time window.
///
/// This struct is used to represent a graph with a time window. It is constructed
/// by providing a `Graph` object and a time range that defines the window.
///
/// # Examples
///
/// ```rust
/// use raphtory::db::api::view::*;
/// use raphtory::prelude::*;
///
/// let graph = Graph::new();
/// graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
/// graph.add_edge(1, 2, 3, NO_PROPS, None).unwrap();
/// let windowed_graph = graph.window(0, 1);
/// ```
impl<'graph, G: GraphViewOps<'graph>> WindowedGraph<G> {
    /// Create a new windowed graph
    ///
    /// # Arguments
    ///
    /// - `graph` - The graph to create the windowed graph from
    /// - `start` - The inclusive start time of the window.
    /// - `end` - The exclusive end time of the window.
    ///
    /// Returns:
    ///
    /// A new windowed graph
    pub(crate) fn new(graph: G, start: Option<i64>, end: Option<i64>) -> Self {
        WindowedGraph { graph, start, end }
    }
}

#[cfg(test)]
mod views_test {
    use super::*;
    use crate::{
        algorithms::centrality::degree_centrality::degree_centrality,
        db::graph::graph::assert_graph_equal, prelude::*, test_storage, test_utils::test_graph,
    };
    use itertools::Itertools;
    use proptest::prelude::*;
    use rand::{prelude::*, Rng};
    use raphtory_api::core::{entities::GID, utils::logging::global_info_logger};
    use rayon::prelude::*;
    #[cfg(feature = "storage")]
    use tempfile::TempDir;
    use tracing::{error, info};

    #[test]
    fn test_non_restricted_window() {
        let g = Graph::new();
        g.add_edge(0, 0, 1, NO_PROPS, None).unwrap();

        for n in g.window(0, 1).nodes() {
            assert!(g.has_node(n));
        }

        assert_graph_equal(&g.window(0, 1), &g)
    }

    #[test]
    fn windowed_graph_nodes_degree() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let graph = Graph::new();

        for (t, src, dst) in &vs {
            graph.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }
        test_storage!(&graph, |graph| {
            let wg = graph.window(-1, 1);

            let actual = wg
                .nodes()
                .iter()
                .map(|v| (v.id(), v.degree()))
                .collect::<Vec<_>>();

            let expected = vec![(GID::U64(1), 2), (GID::U64(2), 1)];

            assert_eq!(actual, expected);
        });
    }

    #[test]
    fn windowed_graph_edge() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let graph = Graph::new();

        for (t, src, dst) in vs {
            graph.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }
        test_storage!(&graph, |graph| {
            let wg = graph.window(i64::MIN, i64::MAX);
            assert_eq!(wg.edge(1, 3).unwrap().src().id(), GID::U64(1));
            assert_eq!(wg.edge(1, 3).unwrap().dst().id(), GID::U64(3));
        });
    }

    #[test]
    fn windowed_graph_node_edges() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let graph = Graph::new();

        for (t, src, dst) in &vs {
            graph.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }
        test_storage!(&graph, |graph| {
            let wg = graph.window(-1, 1);

            assert_eq!(wg.node(1).unwrap().id(), GID::U64(1));
        });
    }

    #[test]
    fn graph_has_node_check_fail() {
        let vs: Vec<(i64, u64)> = vec![
            (1, 0),
            (-100, 262),
            // (327226439, 108748364996394682),
            (1, 9135428456135679950),
            // (0, 1),
            // (2, 2),
        ];
        let graph = Graph::new();

        for (t, v) in &vs {
            graph.add_node(*t, *v, NO_PROPS, None).unwrap();
        }

        // FIXME: Issue #46: arrow_test(&graph, test)
        test_graph(&graph, |graph| {
            let wg = graph.window(1, 2);
            assert!(!wg.has_node(262))
        });
    }

    #[test]
    fn windowed_graph_has_node() {
        proptest!(|(mut vs: Vec<(i64, u64)>)| {
            global_info_logger();
            prop_assume!(!vs.is_empty());

            vs.sort_by_key(|v| v.1); // Sorted by node
            vs.dedup_by_key(|v| v.1); // Have each node only once to avoid headaches
            vs.sort_by_key(|v| v.0); // Sorted by time

            let rand_start_index = thread_rng().gen_range(0..vs.len());
            let rand_end_index = thread_rng().gen_range(rand_start_index..vs.len());

            let g = Graph::new();

            for (t, v) in &vs {
                g.add_node(*t, *v, NO_PROPS, None)
                    .map_err(|err| error!("{:?}", err))
                    .ok();
            }

            let start = vs.get(rand_start_index).expect("start index in range").0;
            let end = vs.get(rand_end_index).expect("end index in range").0;

            let wg = g.window(start, end);

            let rand_test_index: usize = thread_rng().gen_range(0..vs.len());

            let (i, v) = vs.get(rand_test_index).expect("test index in range");
            if (start..end).contains(i) {
                prop_assert!(wg.has_node(*v), "Node {:?} was not in window {:?}", (i, v), start..end);
            } else {
                prop_assert!(!wg.has_node(*v), "Node {:?} was in window {:?}", (i, v), start..end);
            }
        });
    }

    // FIXME: Issue #46
    // #[quickcheck]
    // fn windowed_disk_graph_has_node(mut vs: Vec<(i64, u64)>) -> TestResult {
    //     global_info_logger();
    //      if vs.is_empty() {
    //         return TestResult::discard();
    //     }
    //
    //     vs.sort_by_key(|v| v.1); // Sorted by node
    //     vs.dedup_by_key(|v| v.1); // Have each node only once to avoid headaches
    //     vs.sort_by_key(|v| v.0); // Sorted by time
    //
    //     let rand_start_index = thread_rng().gen_range(0..vs.len());
    //     let rand_end_index = thread_rng().gen_range(rand_start_index..vs.len());
    //
    //     let g = Graph::new();
    //     for (t, v) in &vs {
    //         g.add_node(*t, *v, NO_PROPS, None)
    //             .map_err(|err| error!("{:?}", err))
    //             .ok();
    //     }
    //     let test_dir = TempDir::new().unwrap();
    #[cfg(feature = "storage")]
    //     let g = g.persist_as_disk_graph(test_dir.path()).unwrap();
    //
    //     let start = vs.get(rand_start_index).expect("start index in range").0;
    //     let end = vs.get(rand_end_index).expect("end index in range").0;
    //
    //     let wg = g.window(start, end);
    //
    //     let rand_test_index: usize = thread_rng().gen_range(0..vs.len());
    //
    //     let (i, v) = vs.get(rand_test_index).expect("test index in range");
    //     if (start..end).contains(i) {
    //         if wg.has_node(*v) {
    //             TestResult::passed()
    //         } else {
    //             TestResult::error(format!(
    //                 "Node {:?} was not in window {:?}",
    //                 (i, v),
    //                 start..end
    //             ))
    //         }
    //     } else if !wg.has_node(*v) {
    //         TestResult::passed()
    //     } else {
    //         TestResult::error(format!("Node {:?} was in window {:?}", (i, v), start..end))
    //     }
    // }
    #[test]
    fn windowed_graph_has_edge() {
        proptest!(|(mut edges: Vec<(i64, (u64, u64))>)| {
            prop_assume!(!edges.is_empty());

            edges.sort_by_key(|e| e.1); // Sorted by edge
            edges.dedup_by_key(|e| e.1); // Have each edge only once to avoid headaches
            edges.sort_by_key(|e| e.0); // Sorted by time

            let rand_start_index = thread_rng().gen_range(0..edges.len());
            let rand_end_index = thread_rng().gen_range(rand_start_index..edges.len());

            let g = Graph::new();

            for (t, e) in &edges {
                g.add_edge(*t, e.0, e.1, NO_PROPS, None).unwrap();
            }

            let start = edges.get(rand_start_index).expect("start index in range").0;
            let end = edges.get(rand_end_index).expect("end index in range").0;

            let wg = g.window(start, end);

            let rand_test_index: usize = thread_rng().gen_range(0..edges.len());

            let (i, e) = edges.get(rand_test_index).expect("test index in range");
            if (start..end).contains(i) {
                prop_assert!(wg.has_edge(e.0, e.1), "Edge {:?} was not in window {:?}", (i, e), start..end);
            } else {
                prop_assert!(!wg.has_edge(e.0, e.1), "Edge {:?} was in window {:?}", (i, e), start..end);
            }
        });
    }

    #[cfg(feature = "storage")]
    #[test]
    fn windowed_disk_graph_has_edge() {
        proptest!(|(mut edges: Vec<(i64, (u64, u64))>)| {
            prop_assume!(!edges.is_empty());

            edges.sort_by_key(|e| e.1); // Sorted by edge
            edges.dedup_by_key(|e| e.1); // Have each edge only once to avoid headaches
            edges.sort_by_key(|e| e.0); // Sorted by time

            let rand_start_index = thread_rng().gen_range(0..edges.len());
            let rand_end_index = thread_rng().gen_range(rand_start_index..edges.len());

            let g = Graph::new();

            for (t, e) in &edges {
                g.add_edge(*t, e.0, e.1, NO_PROPS, None).unwrap();
            }

            let test_dir = TempDir::new().unwrap();
            let disk_graph = g.persist_as_disk_graph(test_dir.path()).unwrap();

            let start = edges.get(rand_start_index).expect("start index in range").0;
            let end = edges.get(rand_end_index).expect("end index in range").0;

            let wg = disk_graph.window(start, end);

            let rand_test_index: usize = thread_rng().gen_range(0..edges.len());

            let (i, e) = edges.get(rand_test_index).expect("test index in range");
            if (start..end).contains(i) {
                prop_assert!(wg.has_edge(e.0, e.1), "Edge {:?} was not in window {:?}", (i, e), start..end);
            } else {
                prop_assert!(!wg.has_edge(e.0, e.1), "Edge {:?} was in window {:?}", (i, e), start..end);
            }
        });
    }

    #[test]
    fn windowed_graph_edge_count() {
        proptest!(|(mut edges: Vec<(i64, (u64, u64))>, window: Range<i64>)| {
            global_info_logger();
            prop_assume!(window.end >= window.start);

            edges.sort_by_key(|e| e.1); // Sorted by edge
            edges.dedup_by_key(|e| e.1); // Have each edge only once to avoid headaches

            let true_edge_count = edges.iter().filter(|e| window.contains(&e.0)).count();

            let g = Graph::new();

            for (t, e) in &edges {
                g.add_edge(*t, e.0, e.1, [("test".to_owned(), Prop::Bool(true))], None)
                    .unwrap();
            }

            let wg = g.window(window.start, window.end);
            if wg.count_edges() != true_edge_count {
                info!(
                    "failed, g.num_edges() = {}, true count = {}",
                    wg.count_edges(),
                    true_edge_count
                );
                info!("g.edges() = {:?}", wg.edges().iter().collect_vec());
            }
            prop_assert_eq!(wg.count_edges(), true_edge_count);
        });
    }

    #[test]
    fn trivial_window_has_all_edges() {
        proptest!(|(edges: Vec<(i64, u64, u64)>)| {
            let g = Graph::new();
            edges
                .into_par_iter()
                .filter(|e| e.0 < i64::MAX)
                .for_each(|(t, src, dst)| {
                    g.add_edge(t, src, dst, [("test".to_owned(), Prop::Bool(true))], None)
                        .unwrap();
                });
            let w = g.window(i64::MIN, i64::MAX);
            prop_assert!(g.edges()
                .iter()
                .all(|e| w.has_edge(e.src().id(), e.dst().id())));
        });
    }

    #[test]
    fn large_node_in_window() {
        proptest!(|(dsts: Vec<u64>)| {
            let dsts: Vec<u64> = dsts.into_iter().unique().collect();
            let n = dsts.len();
            let g = Graph::new();

            for dst in dsts {
                let t = 1;
                g.add_edge(t, 0, dst, NO_PROPS, None).unwrap();
            }
            let w = g.window(i64::MIN, i64::MAX);
            prop_assert_eq!(w.count_edges(), n);
        });
    }

    #[test]
    fn windowed_graph_node_ids() {
        let vs = vec![(1, 1, 2), (3, 3, 4), (5, 5, 6), (7, 7, 1)];

        let args = [(i64::MIN, 8), (i64::MIN, 2), (i64::MIN, 4), (3, 6)];

        let expected = vec![
            vec![1, 2, 3, 4, 5, 6, 7],
            vec![1, 2],
            vec![1, 2, 3, 4],
            vec![3, 4, 5, 6],
        ];

        let graph = Graph::new();

        for (t, src, dst) in &vs {
            graph.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let res: Vec<_> = (0..=3)
                .map(|i| {
                    let wg = graph.window(args[i].0, args[i].1);
                    let mut e = wg
                        .nodes()
                        .id()
                        .iter_values()
                        .filter_map(|id| id.to_u64())
                        .collect::<Vec<_>>();
                    e.sort();
                    e
                })
                .collect_vec();

            assert_eq!(res, expected);
        });

        let graph = Graph::new();
        for (src, dst, t) in &vs {
            graph.add_edge(*src, *dst, *t, NO_PROPS, None).unwrap();
        }
        test_storage!(&graph, |graph| {
            let res: Vec<_> = (0..=3)
                .map(|i| {
                    let wg = graph.window(args[i].0, args[i].1);
                    let mut e = wg
                        .nodes()
                        .id()
                        .iter_values()
                        .filter_map(|id| id.to_u64())
                        .collect::<Vec<_>>();
                    e.sort();
                    e
                })
                .collect_vec();
            assert_eq!(res, expected);
        });
    }

    #[test]
    fn windowed_graph_nodes() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let graph = Graph::new();

        graph
            .add_node(
                0,
                1,
                [("type", "wallet".into_prop()), ("cost", 99.5.into_prop())],
                None,
            )
            .unwrap();

        graph
            .add_node(
                -1,
                2,
                [("type", "wallet".into_prop()), ("cost", 10.0.into_prop())],
                None,
            )
            .unwrap();

        graph
            .add_node(
                6,
                3,
                [("type", "wallet".into_prop()), ("cost", 76.2.into_prop())],
                None,
            )
            .unwrap();

        for (t, src, dst) in &vs {
            graph
                .add_edge(*t, *src, *dst, [("eprop", "commons")], None)
                .unwrap();
        }
        test_storage!(&graph, |graph| {
            let wg = graph.window(-2, 0);

            let actual = wg
                .nodes()
                .id()
                .iter_values()
                .filter_map(|id| id.to_u64())
                .collect::<Vec<_>>();

            let expected = vec![1, 2];

            assert_eq!(actual, expected);
        });
    }

    #[test]
    fn test_reference() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();

        test_storage!(&graph, |graph| {
            let mut w = WindowedGraph::new(&graph, Some(0), Some(1));
            assert_eq!(w, graph);
            w = WindowedGraph::new(&graph, Some(1), Some(2));
            assert_eq!(w, Graph::new());
        });
    }

    #[test]
    fn test_algorithm_on_windowed_graph() {
        global_info_logger();
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        test_storage!(&graph, |graph| {
            let w = graph.window(0, 1);
            let _ = degree_centrality(&w);
        });
    }

    #[test]
    fn test_view_resetting() {
        let graph = Graph::new();
        for t in 0..10 {
            let t1 = t * 3;
            let t2 = t * 3 + 1;
            let t3 = t * 3 + 2;
            graph.add_edge(t1, 1, 2, NO_PROPS, None).unwrap();
            graph.add_edge(t2, 2, 3, NO_PROPS, None).unwrap();
            graph.add_edge(t3, 3, 1, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            assert_graph_equal(&graph.before(9).after(2), &graph.window(3, 9));
            let res = graph
                .window(3, 9)
                .nodes()
                .before(6)
                .edges()
                .window(1, 9)
                .earliest_time()
                .map(|it| it.collect_vec())
                .collect_vec();
            assert_eq!(
                res,
                [[Some(3), Some(5)], [Some(3), Some(4)], [Some(5), Some(4)]]
            );
        });
    }

    #[test]
    fn test_entity_history() {
        let graph = Graph::new();
        graph.add_node(0, 0, NO_PROPS, None).unwrap();
        graph.add_node(1, 0, NO_PROPS, None).unwrap();
        graph.add_node(2, 0, NO_PROPS, None).unwrap();
        graph.add_node(3, 0, NO_PROPS, None).unwrap();
        graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(1, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(2, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(3, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(4, 1, 3, NO_PROPS, None).unwrap();
        graph.add_edge(5, 1, 3, NO_PROPS, None).unwrap();
        graph.add_edge(6, 1, 3, NO_PROPS, None).unwrap();
        graph.add_edge(7, 1, 3, NO_PROPS, None).unwrap();

        // FIXME: Issue #46
        test_graph(&graph, |graph| {
            let e = graph.edge(1, 2).unwrap();
            let v = graph.node(0).unwrap();
            let full_history_1 = vec![0i64, 1, 2, 3];

            let full_history_2 = vec![4i64, 5, 6, 7];

            let windowed_history = vec![0i64, 1];

            assert_eq!(v.history(), full_history_1);

            assert_eq!(v.window(0, 2).history(), windowed_history);
            assert_eq!(e.history(), full_history_1);
            assert_eq!(e.window(0, 2).history(), windowed_history);

            assert_eq!(
                graph.edges().history().collect_vec(),
                [full_history_1.clone(), full_history_2.clone()]
            );
            assert_eq!(
                graph
                    .nodes()
                    .in_edges()
                    .history()
                    .map(|it| it.collect_vec())
                    .collect_vec(),
                [vec![], vec![], vec![full_history_1], vec![full_history_2],]
            );

            assert_eq!(
                graph
                    .nodes()
                    .earliest_time()
                    .iter_values()
                    .flatten()
                    .collect_vec(),
                [0, 0, 0, 4,]
            );

            assert_eq!(
                graph
                    .nodes()
                    .latest_time()
                    .iter_values()
                    .flatten()
                    .collect_vec(),
                [3, 7, 3, 7]
            );

            assert_eq!(
                graph
                    .nodes()
                    .neighbours()
                    .latest_time()
                    .map(|it| it.flatten().collect_vec())
                    .collect_vec(),
                [vec![], vec![3, 7], vec![7], vec![7],]
            );

            assert_eq!(
                graph
                    .nodes()
                    .neighbours()
                    .earliest_time()
                    .map(|it| it.flatten().collect_vec())
                    .collect_vec(),
                [vec![], vec![0, 4], vec![0], vec![0],]
            );
        });
    }

    mod test_filters_window_graph {
        use crate::{
            db::{
                api::view::StaticGraphViewOps,
                graph::{assertions::GraphTransformer, views::window_graph::WindowedGraph},
            },
            prelude::TimeOps,
        };
        use std::ops::Range;

        struct WindowGraphTransformer(Range<i64>);

        impl GraphTransformer for WindowGraphTransformer {
            type Return<G: StaticGraphViewOps> = WindowedGraph<G>;
            fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Self::Return<G> {
                graph.window(self.0.start, self.0.end)
            }
        }

        mod test_nodes_filters_window_graph {
            use crate::{
                db::{
                    api::view::StaticGraphViewOps,
                    graph::{
                        assertions::{
                            assert_filter_nodes_results, assert_search_nodes_results,
                            TestGraphVariants, TestVariants,
                        },
                        views::filter::model::{
                            ComposableFilter, NodeFilter, NodeFilterBuilderOps, PropertyFilterOps,
                        },
                    },
                },
                prelude::{AdditionOps, PropertyAdditionOps, PropertyFilter},
            };
            use raphtory_api::core::{entities::properties::prop::Prop, storage::arc_str::ArcStr};
            use raphtory_storage::mutation::{
                addition_ops::InternalAdditionOps,
                property_addition_ops::InternalPropertyAdditionOps,
            };
            use std::sync::Arc;

            use crate::{
                db::graph::views::window_graph::views_test::test_filters_window_graph::WindowGraphTransformer,
                prelude::GraphViewOps,
            };

            fn init_graph<G: StaticGraphViewOps + AdditionOps + PropertyAdditionOps>(
                graph: G,
            ) -> G {
                let nodes = vec![
                    (
                        6,
                        "N1",
                        vec![
                            ("p1", Prop::U64(2u64)),
                            ("k1", Prop::I64(2i64)),
                            ("k2", Prop::Str(ArcStr::from("Paper_Airplane"))),
                            ("k3", Prop::Bool(true)),
                            ("k4", Prop::F64(6.0f64)),
                        ],
                        Some("air_nomad"),
                    ),
                    (
                        7,
                        "N1",
                        vec![
                            ("p1", Prop::U64(1u64)),
                            ("k1", Prop::I64(5i64)),
                            ("k3", Prop::Bool(false)),
                        ],
                        Some("air_nomad"),
                    ),
                    (
                        6,
                        "N2",
                        vec![("p1", Prop::U64(1u64)), ("k4", Prop::F64(6.0f64))],
                        Some("water_tribe"),
                    ),
                    (
                        7,
                        "N2",
                        vec![
                            ("p1", Prop::U64(2u64)),
                            ("k1", Prop::I64(2i64)),
                            ("k2", Prop::Str(ArcStr::from("Paper_Ship"))),
                            ("k3", Prop::Bool(true)),
                            ("k4", Prop::F64(10.0f64)),
                        ],
                        Some("water_tribe"),
                    ),
                    (8, "N3", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
                    (9, "N4", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
                    (
                        5,
                        "N5",
                        vec![
                            ("p1", Prop::U64(1u64)),
                            ("k1", Prop::I64(2i64)),
                            ("k2", Prop::Str(ArcStr::from("Paper_Airplane"))),
                            ("k3", Prop::Bool(true)),
                            ("k4", Prop::F64(6.0f64)),
                        ],
                        Some("air_nomad"),
                    ),
                    (
                        6,
                        "N5",
                        vec![
                            ("p1", Prop::U64(2u64)),
                            ("k2", Prop::Str(ArcStr::from("Pometry"))),
                            ("k4", Prop::F64(1.0f64)),
                        ],
                        Some("air_nomad"),
                    ),
                    (5, "N6", vec![("p1", Prop::U64(1u64))], Some("fire_nation")),
                    (
                        6,
                        "N6",
                        vec![("p1", Prop::U64(1u64)), ("k4", Prop::F64(1.0f64))],
                        Some("fire_nation"),
                    ),
                    (
                        3,
                        "N7",
                        vec![
                            ("p1", Prop::U64(1u64)),
                            ("k1", Prop::I64(2i64)),
                            ("k2", Prop::Str(ArcStr::from("Paper_Ship"))),
                            ("k3", Prop::Bool(true)),
                            ("k4", Prop::F64(10.0f64)),
                        ],
                        Some("air_nomad"),
                    ),
                    (5, "N7", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
                    (3, "N8", vec![("p1", Prop::U64(1u64))], Some("fire_nation")),
                    (
                        4,
                        "N8",
                        vec![
                            ("p1", Prop::U64(2u64)),
                            ("k1", Prop::I64(2i64)),
                            ("k2", Prop::Str(ArcStr::from("Sand_Clown"))),
                            ("k3", Prop::Bool(true)),
                            ("k4", Prop::F64(10.0f64)),
                        ],
                        Some("fire_nation"),
                    ),
                    (2, "N9", vec![("p1", Prop::U64(2u64))], None),
                    (2, "N10", vec![("q1", Prop::U64(0u64))], None),
                    (2, "N10", vec![("p1", Prop::U64(3u64))], None),
                    (2, "N11", vec![("p1", Prop::U64(3u64))], None),
                    (2, "N11", vec![("q1", Prop::U64(0u64))], None),
                    (2, "N12", vec![("q1", Prop::U64(0u64))], None),
                    (
                        3,
                        "N12",
                        vec![
                            ("p1", Prop::U64(3u64)),
                            ("k1", Prop::I64(2i64)),
                            ("k2", Prop::Str(ArcStr::from("Sand_Clown"))),
                            ("k3", Prop::Bool(true)),
                            ("k4", Prop::F64(10.0f64)),
                        ],
                        None,
                    ),
                    (2, "N13", vec![("q1", Prop::U64(0u64))], None),
                    (3, "N13", vec![("p1", Prop::U64(3u64))], None),
                    (2, "N14", vec![("q1", Prop::U64(0u64))], None),
                    (2, "N15", vec![], None),
                ];

                // Add nodes to the graph
                for (id, name, props, layer) in &nodes {
                    graph.add_node(*id, name, props.clone(), *layer).unwrap();
                }

                // Constant property assignments
                let constant_properties = vec![
                    (
                        "N1",
                        vec![
                            ("p1", Prop::U64(1u64)),
                            ("k1", Prop::I64(3i64)),
                            ("k2", Prop::Str(ArcStr::from("Paper_Airplane"))),
                            ("k3", Prop::Bool(true)),
                            ("k4", Prop::F64(6.0f64)),
                        ],
                    ),
                    ("N4", vec![("p1", Prop::U64(2u64))]),
                    ("N9", vec![("p1", Prop::U64(1u64))]),
                    ("N10", vec![("p1", Prop::U64(1u64))]),
                    ("N11", vec![("p1", Prop::U64(1u64))]),
                    ("N12", vec![("p1", Prop::U64(1u64))]),
                    (
                        "N13",
                        vec![
                            ("p1", Prop::U64(1u64)),
                            ("k1", Prop::I64(2i64)),
                            ("k2", Prop::Str(ArcStr::from("Sand_Clown"))),
                            ("k3", Prop::Bool(true)),
                            ("k4", Prop::F64(10.0f64)),
                        ],
                    ),
                    ("N14", vec![("p1", Prop::U64(1u64))]),
                    ("N15", vec![("p1", Prop::U64(1u64))]),
                ];

                // Apply constant properties
                for (node, props) in constant_properties {
                    graph
                        .node(node)
                        .unwrap()
                        .add_constant_properties(props)
                        .unwrap();
                }

                graph
            }

            fn init_graph2<
                G: StaticGraphViewOps
                    + AdditionOps
                    + InternalAdditionOps
                    + InternalPropertyAdditionOps
                    + PropertyAdditionOps,
            >(
                graph: G,
            ) -> G {
                let nodes = vec![(
                    2,
                    "N14",
                    vec![
                        ("q1", Prop::U64(0u64)),
                        (
                            "x",
                            Prop::List(Arc::from(vec![Prop::U64(1), Prop::U64(6), Prop::U64(9)])),
                        ),
                    ],
                    None,
                )];

                // Add nodes to the graph
                for (id, name, props, layer) in &nodes {
                    graph.add_node(*id, name, props.clone(), *layer).unwrap();
                }

                graph
            }

            #[test]
            fn test_nodes_filters_for_node_name_eq() {
                // TODO: Enable event_disk_graph once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
                let filter = NodeFilter::name().eq("N2");
                let expected_results = vec!["N2"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );
            }

            #[test]
            fn test_nodes_filters_pg_for_node_name_eq() {
                let filter = NodeFilter::name().eq("N2");
                let expected_results = vec!["N2"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }

            #[test]
            fn test_nodes_filters_for_node_name_ne() {
                // TODO: Enable event_disk_graph once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
                let filter = NodeFilter::name().ne("N2");
                let expected_results = vec!["N1", "N3", "N5", "N6"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
            }

            #[test]
            fn test_nodes_filters_pg_for_node_name_ne() {
                let filter = NodeFilter::name().ne("N2");
                let expected_results = vec![
                    "N1", "N10", "N11", "N12", "N13", "N14", "N15", "N3", "N5", "N6", "N7", "N8",
                    "N9",
                ];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }

            #[test]
            fn test_nodes_filters_for_node_name_in() {
                // TODO: Enable event_disk_graph once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
                let filter = NodeFilter::name().is_in(vec!["N2".into()]);
                let expected_results = vec!["N2"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = NodeFilter::name().is_in(vec!["N2".into(), "N5".into()]);
                let expected_results = vec!["N2", "N5"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );
            }

            #[test]
            fn test_nodes_filters_pg_for_node_name_in() {
                let filter = NodeFilter::name().is_in(vec!["N2".into()]);
                let expected_results = vec!["N2"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = NodeFilter::name().is_in(vec!["N2".into(), "N5".into()]);
                let expected_results = vec!["N2", "N5"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }

            #[test]
            fn test_nodes_filters_for_node_name_not_in() {
                // TODO: Enable event_disk_graph once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
                let filter = NodeFilter::name().is_not_in(vec!["N5".into()]);
                let expected_results = vec!["N1", "N2", "N3", "N6"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
            }

            #[test]
            fn test_nodes_filters_pg_for_node_name_not_in() {
                let filter = NodeFilter::name().is_not_in(vec!["N5".into()]);
                let expected_results = vec![
                    "N1", "N10", "N11", "N12", "N13", "N14", "N15", "N2", "N3", "N6", "N7", "N8",
                    "N9",
                ];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }

            #[test]
            fn test_nodes_filters_for_node_type_eq() {
                // TODO: Enable event_disk_graph once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
                let filter = NodeFilter::node_type().eq("fire_nation");
                let expected_results = vec!["N6"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
            }

            #[test]
            fn test_nodes_filters_pg_for_node_type_eq() {
                let filter = NodeFilter::node_type().eq("fire_nation");
                let expected_results = vec!["N6", "N8"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }

            #[test]
            fn test_nodes_filters_for_node_type_ne() {
                // TODO: Enable event_disk_graph once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
                let filter = NodeFilter::node_type().ne("fire_nation");
                let expected_results = vec!["N1", "N2", "N3", "N5"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
            }

            #[test]
            fn test_nodes_filters_pg_for_node_type_ne() {
                let filter = NodeFilter::node_type().ne("fire_nation");
                let expected_results = vec![
                    "N1", "N10", "N11", "N12", "N13", "N14", "N15", "N2", "N3", "N5", "N7", "N9",
                ];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }

            #[test]
            fn test_nodes_filters_for_node_type_in() {
                // TODO: Enable event_disk_graph once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
                let filter = NodeFilter::node_type().is_in(vec!["fire_nation".into()]);
                let expected_results = vec!["N6"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter =
                    NodeFilter::node_type().is_in(vec!["fire_nation".into(), "air_nomad".into()]);
                let expected_results = vec!["N1", "N3", "N5", "N6"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
            }

            #[test]
            fn test_nodes_filters_pg_for_node_type_in() {
                let filter = NodeFilter::node_type().is_in(vec!["fire_nation".into()]);
                let expected_results = vec!["N6", "N8"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter =
                    NodeFilter::node_type().is_in(vec!["fire_nation".into(), "air_nomad".into()]);
                let expected_results = vec!["N1", "N3", "N5", "N6", "N7", "N8"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }

            #[test]
            fn test_nodes_filters_for_node_type_not_in() {
                // TODO: Enable event_disk_graph once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
                let filter = NodeFilter::node_type().is_not_in(vec!["fire_nation".into()]);
                let expected_results = vec!["N1", "N2", "N3", "N5"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
            }

            #[test]
            fn test_nodes_filters_pg_for_node_type_not_in() {
                let filter = NodeFilter::node_type().is_not_in(vec!["fire_nation".into()]);
                let expected_results = vec![
                    "N1", "N10", "N11", "N12", "N13", "N14", "N15", "N2", "N3", "N5", "N7", "N9",
                ];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }

            #[test]
            fn test_nodes_filters_for_property_eq() {
                // TODO: Enable event_disk_graph once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N6"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("k1").eq(2i64);
                let expected_results = vec!["N2"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("k2").eq("Paper_Airplane");
                let expected_results = vec!["N1"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k3").eq(true);
                let expected_results = vec!["N2"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k4").eq(6.0f64);
                let expected_results = vec!["N1"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("x").eq(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(6),
                    Prop::U64(9),
                ])));
                let expected_results = vec!["N14"];
                // TODO: List(U64) not supported as disk_graph property
                // assert_filter_nodes_results_w!(
                //     init_graph2,
                //     filter,
                //     1..9,
                //     expected_results,
                //     variants = [graph]
                // );
                assert_filter_nodes_results(
                    init_graph2,
                    WindowGraphTransformer(1..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                // TODO: Search APIs don't support list yet
                // assert_search_nodes_results(
                //     init_graph,
                //     WindowGraphTransformer(6..9),
                //     filter,
                //     &expected_results,
                //     TestVariants::EventOnly,
                // );
            }

            #[test]
            fn test_nodes_filters_pg_for_property_eq() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N14", "N15", "N3", "N6", "N7"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k1").eq(2i64);
                let expected_results = vec!["N12", "N13", "N2", "N5", "N7", "N8"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k2").eq("Paper_Airplane");
                let expected_results = vec!["N1"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                // TODO: Const properties not supported for disk_graph.
                let filter = PropertyFilter::property("k3").eq(true);
                let expected_results = vec!["N12", "N13", "N2", "N5", "N7", "N8"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );

                // TODO: Const properties not supported for disk_graph.
                let filter = PropertyFilter::property("k3").eq(true);
                let expected_results = vec!["N12", "N2", "N5", "N7", "N8"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentDiskGraph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::PersistentDiskGraph],
                );

                let filter = PropertyFilter::property("k4").eq(6.0f64);
                let expected_results = vec!["N1"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("x").eq(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(6),
                    Prop::U64(9),
                ])));
                let expected_results = vec!["N14"];
                // TODO: List(U64) not supported as disk_graph property
                // assert_filter_nodes_results_pg_w!(
                //     init_graph2,
                //     filter,
                //     1..9,
                //     expected_results,
                //     variants = [persistent_graph]
                // );
                assert_filter_nodes_results(
                    init_graph2,
                    WindowGraphTransformer(1..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );
                // TODO: Search APIs don't support list yet
                // assert_search_nodes_results(
                //     init_graph,
                //     WindowGraphTransformer(6..9),
                //     filter,
                //     &expected_results,
                //     vec![TestGraphVariants::PersistentGraph],
                // );
            }

            #[test]
            fn test_nodes_filters_for_property_ne() {
                // TODO: Enable event_disk_graph once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
                let filter = PropertyFilter::property("p1").ne(1u64);
                let expected_results = vec!["N2", "N5"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("k1").ne(2i64);
                let expected_results = vec!["N1"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("k2").ne("Paper_Airplane");
                let expected_results = vec!["N2", "N5"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("k3").ne(true);
                let expected_results = vec!["N1"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("k4").ne(6.0f64);
                let expected_results = vec!["N2", "N5", "N6"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("x").ne(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(6),
                    Prop::U64(9),
                ])));
                let expected_results = Vec::<&str>::new();
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                // TODO: Search APIs don't support list yet
                // assert_search_nodes_results(
                //     init_graph,
                //     WindowGraphTransformer(6..9),
                //     filter,
                //     &expected_results,
                //     TestVariants::EventOnly,
                // );
            }

            #[test]
            fn test_nodes_filters_pg_for_property_ne() {
                let filter = PropertyFilter::property("p1").ne(1u64);
                let expected_results = vec!["N10", "N11", "N12", "N13", "N2", "N5", "N8", "N9"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k1").ne(2i64);
                let expected_results = vec!["N1"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k2").ne("Paper_Airplane");
                let expected_results = vec!["N12", "N13", "N2", "N5", "N7", "N8"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k3").ne(true);
                let expected_results = vec!["N1"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k4").ne(6.0f64);
                let expected_results = vec!["N12", "N13", "N2", "N5", "N6", "N7", "N8"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("x").ne(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(6),
                    Prop::U64(9),
                ])));
                let expected_results = Vec::<&str>::new();
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(1..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );
                // TODO: Search APIs don't support list yet
                // assert_search_nodes_results(
                //     init_graph,
                //     WindowGraphTransformer(1..9),
                //     filter,
                //     &expected_results,
                //     vec![TestGraphVariants::PersistentGraph],
                // );
            }

            #[test]
            fn test_nodes_filters_for_property_lt() {
                // TODO: Enable event_disk_graph once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
                let filter = PropertyFilter::property("p1").lt(3u64);
                let expected_results = vec!["N1", "N2", "N3", "N5", "N6"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("k1").lt(3i64);
                let expected_results = vec!["N2"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("k4").lt(10.0f64);
                let expected_results = vec!["N1", "N5", "N6"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("x").lt(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(7),
                    Prop::U64(0),
                ])));
                let expected_results = vec!["N14"];
                // TODO: List(U64) not supported as disk_graph property
                // assert_filter_nodes_results_w!(
                //     init_graph2,
                //     filter,
                //     1..9,
                //     expected_results,
                //     variants = [graph]
                // );
                assert_filter_nodes_results(
                    init_graph2,
                    WindowGraphTransformer(1..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                // TODO: Search APIs don't support list yet
                // assert_search_nodes_results(
                //     init_graph,
                //     WindowGraphTransformer(6..9),
                //     filter,
                //     &expected_results,
                //     TestVariants::EventOnly,
                // );
            }

            #[test]
            fn test_nodes_filters_pg_for_property_lt() {
                let filter = PropertyFilter::property("p1").lt(3u64);
                let expected_results =
                    vec!["N1", "N14", "N15", "N2", "N3", "N5", "N6", "N7", "N8", "N9"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k1").lt(3i64);
                let expected_results = vec!["N12", "N13", "N2", "N5", "N7", "N8"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k4").lt(10.0f64);
                let expected_results = vec!["N1", "N5", "N6"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("x").lt(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(7),
                    Prop::U64(0),
                ])));
                let expected_results = vec!["N14"];
                // TODO: List(U64) not supported as disk_graph property
                // assert_filter_nodes_results_pg_w!(
                //     init_graph2,
                //     filter,
                //     1..9,
                //     expected_results,
                //     variants = [persistent_graph]
                // );
                assert_filter_nodes_results(
                    init_graph2,
                    WindowGraphTransformer(1..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );
                // TODO: Search APIs don't support list yet
                // assert_search_nodes_results(
                //     init_graph,
                //     WindowGraphTransformer(1..9),
                //     filter,
                //     &expected_results,
                //     vec![TestGraphVariants::PersistentGraph],
                // );
            }

            #[test]
            fn test_nodes_filters_for_property_le() {
                // TODO: Enable event_disk_graph once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
                let filter = PropertyFilter::property("p1").le(1u64);
                let expected_results = vec!["N1", "N3", "N6"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("k1").le(2i64);
                let expected_results = vec!["N2"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("k4").le(6.0f64);
                let expected_results = vec!["N1", "N5", "N6"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("x").le(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(7),
                    Prop::U64(0),
                ])));
                let expected_results = vec!["N14"];
                // TODO: List(U64) not supported as disk_graph property
                // assert_filter_nodes_results_w!(
                //     init_graph2,
                //     filter,
                //     1..9,
                //     expected_results,
                //     variants = [graph]
                // );
                assert_filter_nodes_results(
                    init_graph2,
                    WindowGraphTransformer(1..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                // TODO: Search APIs don't support list yet
                // assert_search_nodes_results(
                //     init_graph,
                //     WindowGraphTransformer(6..9),
                //     filter,
                //     &expected_results,
                //     TestVariants::EventOnly,
                // );
            }

            #[test]
            fn test_nodes_filters_pg_for_property_le() {
                let filter = PropertyFilter::property("p1").le(1u64);
                let expected_results = vec!["N1", "N14", "N15", "N3", "N6", "N7"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k1").le(2i64);
                let expected_results = vec!["N12", "N13", "N2", "N5", "N7", "N8"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k4").le(6.0f64);
                let expected_results = vec!["N1", "N5", "N6"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("x").le(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(2),
                    Prop::U64(3),
                ])));
                let expected_results = Vec::<&str>::new();
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(1..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                // TODO: Search APIs don't support list yet
                // assert_search_nodes_results(
                //     init_graph,
                //     WindowGraphTransformer(1..9),
                //     filter,
                //     &expected_results,
                //     TestVariants::PersistentOnly,
                // );
            }

            #[test]
            fn test_nodes_filters_for_property_gt() {
                // TODO: Enable event_disk_graph once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
                let filter = PropertyFilter::property("p1").gt(1u64);
                let expected_results = vec!["N2", "N5"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("k1").gt(2i64);
                let expected_results = vec!["N1"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("k4").gt(6.0f64);
                let expected_results = vec!["N2"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("x").gt(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(6),
                    Prop::U64(9),
                ])));
                let expected_results = Vec::<&str>::new();
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                // TODO: Search APIs don't support list yet
                // assert_search_nodes_results(
                //     init_graph,
                //     WindowGraphTransformer(6..9),
                //     filter,
                //     &expected_results,
                //     TestVariants::EventOnly,
                // );
            }

            #[test]
            fn test_nodes_filters_pg_for_property_gt() {
                let filter = PropertyFilter::property("p1").gt(1u64);
                let expected_results = vec!["N10", "N11", "N12", "N13", "N2", "N5", "N8", "N9"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k1").gt(2i64);
                let expected_results = vec!["N1"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k4").gt(6.0f64);
                let expected_results = vec!["N12", "N13", "N2", "N7", "N8"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("x").gt(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(2),
                    Prop::U64(3),
                ])));
                let expected_results = vec!["N14"];
                // TODO: List(U64) not supported as disk_graph property
                // assert_filter_nodes_results_pg_w!(
                //     init_graph2,
                //     filter,
                //     1..9,
                //     expected_results,
                //     variants = [persistent_graph]
                // );
                assert_filter_nodes_results(
                    init_graph2,
                    WindowGraphTransformer(1..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );
                // TODO: Search APIs don't support list yet
                // assert_search_nodes_results(
                //     init_graph,
                //     WindowGraphTransformer(1..9),
                //     filter,
                //     &expected_results,
                //     TestVariants::PersistentOnly,
                // );
            }

            #[test]
            fn test_nodes_filters_for_property_ge() {
                // TODO: Enable event_disk_graph once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
                let filter = PropertyFilter::property("p1").ge(1u64);
                let expected_results = vec!["N1", "N2", "N3", "N5", "N6"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("k1").ge(2i64);
                let expected_results = vec!["N1", "N2"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("k4").ge(6.0f64);
                let expected_results = vec!["N1", "N2"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("x").ge(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(6),
                    Prop::U64(9),
                ])));
                let expected_results = vec!["N14"];
                // TODO: List(U64) not supported as disk_graph property
                // assert_filter_nodes_results_w!(
                //     init_graph2,
                //     filter,
                //     1..9,
                //     expected_results,
                //     variants = [graph]
                // );
                assert_filter_nodes_results(
                    init_graph2,
                    WindowGraphTransformer(1..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                // TODO: Search APIs don't support list yet
                // assert_search_nodes_results(
                //     init_graph,
                //     WindowGraphTransformer(6..9),
                //     filter,
                //     &expected_results,
                //     TestVariants::EventOnly,
                // );
            }

            #[test]
            fn test_nodes_filters_pg_for_property_ge() {
                let filter = PropertyFilter::property("p1").ge(1u64);
                let expected_results = vec![
                    "N1", "N10", "N11", "N12", "N13", "N14", "N15", "N2", "N3", "N5", "N6", "N7",
                    "N8", "N9",
                ];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k1").ge(2i64);
                let expected_results = vec!["N1", "N12", "N13", "N2", "N5", "N7", "N8"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k4").ge(6.0f64);
                let expected_results = vec!["N1", "N12", "N13", "N2", "N7", "N8"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("x").ge(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(2),
                    Prop::U64(3),
                ])));
                let expected_results = vec!["N14"];
                // TODO: List(U64) not supported as disk_graph property
                // assert_filter_nodes_results_pg_w!(
                //     init_graph2,
                //     filter,
                //     1..9,
                //     expected_results,
                //     variants = [persistent_graph]
                // );
                assert_filter_nodes_results(
                    init_graph2,
                    WindowGraphTransformer(1..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );
                // TODO: Search APIs don't support list yet
                // assert_search_nodes_results(
                //     init_graph,
                //     WindowGraphTransformer(1..9),
                //     filter,
                //     &expected_results,
                //     TestVariants::PersistentOnly,
                // );
            }

            #[test]
            fn test_nodes_filters_for_property_in() {
                // TODO: Enable event_disk_graph once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
                let filter = PropertyFilter::property("p1").is_in(vec![2u64.into()]);
                let expected_results = vec!["N2", "N5"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("k1").is_in(vec![2i64.into()]);
                let expected_results = vec!["N2"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("k2").is_in(vec!["Paper_Airplane".into()]);
                let expected_results = vec!["N1"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("k3").is_in(vec![true.into()]);
                let expected_results = vec!["N2"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("k4").is_in(vec![6.0f64.into()]);
                let expected_results = vec!["N1"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
            }

            #[test]
            fn test_nodes_filters_pg_for_property_in() {
                let filter = PropertyFilter::property("p1").is_in(vec![2u64.into()]);
                let expected_results = vec!["N2", "N5", "N8", "N9"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k1").is_in(vec![2i64.into()]);
                let expected_results = vec!["N12", "N13", "N2", "N5", "N7", "N8"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k2").is_in(vec!["Paper_Airplane".into()]);
                let expected_results = vec!["N1"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                // TODO: Const properties not supported for disk_graph.
                let filter = PropertyFilter::property("k3").is_in(vec![true.into()]);
                let expected_results = vec!["N12", "N13", "N2", "N5", "N7", "N8"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );

                // TODO: Const properties not supported for disk_graph.
                let filter = PropertyFilter::property("k3").is_in(vec![true.into()]);
                let expected_results = vec!["N12", "N2", "N5", "N7", "N8"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentDiskGraph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::PersistentDiskGraph],
                );

                let filter = PropertyFilter::property("k4").is_in(vec![6.0f64.into()]);
                let expected_results = vec!["N1"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }

            #[test]
            fn test_nodes_filters_for_property_not_in() {
                // TODO: Enable event_disk_graph once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
                let filter = PropertyFilter::property("p1").is_not_in(vec![1u64.into()]);
                let expected_results = vec!["N2", "N5"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("k1").is_not_in(vec![2i64.into()]);
                let expected_results = vec!["N1"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter =
                    PropertyFilter::property("k2").is_not_in(vec!["Paper_Airplane".into()]);
                let expected_results = vec!["N2", "N5"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("k3").is_not_in(vec![true.into()]);
                let expected_results = vec!["N1"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("k4").is_not_in(vec![6.0f64.into()]);
                let expected_results = vec!["N2", "N5", "N6"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
            }

            #[test]
            fn test_nodes_filters_pg_for_property_not_in() {
                let filter = PropertyFilter::property("p1").is_not_in(vec![1u64.into()]);
                let expected_results = vec!["N10", "N11", "N12", "N13", "N2", "N5", "N8", "N9"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k1").is_not_in(vec![2i64.into()]);
                let expected_results = vec!["N1"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter =
                    PropertyFilter::property("k2").is_not_in(vec!["Paper_Airplane".into()]);
                let expected_results = vec!["N12", "N13", "N2", "N5", "N7", "N8"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k3").is_not_in(vec![true.into()]);
                let expected_results = vec!["N1"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k4").is_not_in(vec![6.0f64.into()]);
                let expected_results = vec!["N12", "N13", "N2", "N5", "N6", "N7", "N8"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }

            #[test]
            fn test_nodes_filters_for_property_is_some() {
                // TODO: Enable event_disk_graph once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
                let filter = PropertyFilter::property("p1").is_some();
                let expected_results = vec!["N1", "N2", "N3", "N5", "N6"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let expected_results = Vec::<&str>::new();
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(1..2),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(1..2),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(10..12),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(10..12),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
            }

            #[test]
            fn test_nodes_filters_pg_for_property_is_some() {
                let filter = PropertyFilter::property("p1").is_some();
                let expected_results = vec![
                    "N1", "N10", "N11", "N12", "N13", "N14", "N15", "N2", "N3", "N5", "N6", "N7",
                    "N8", "N9",
                ];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let expected_results = Vec::<&str>::new();
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(1..2),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(1..2),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let expected_results = vec![
                    "N1", "N10", "N11", "N12", "N13", "N14", "N15", "N2", "N3", "N4", "N5", "N6",
                    "N7", "N8", "N9",
                ];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(10..12),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(10..12),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }

            #[test]
            fn test_nodes_filters_for_props_added_at_different_times() {
                let filter = PropertyFilter::property("q1")
                    .eq(0u64)
                    .and(PropertyFilter::property("p1").eq(3u64));
                let expected_results = vec!["N10", "N11", "N12", "N13"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(1..4),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(1..4),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );
            }

            #[test]
            fn test_nodes_filters_pg_for_props_added_at_different_times() {
                let filter = PropertyFilter::property("q1")
                    .eq(0u64)
                    .and(PropertyFilter::property("p1").eq(3u64));
                let expected_results = vec!["N10", "N11", "N12", "N13"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }

            #[test]
            fn test_nodes_filters_fuzzy_search() {
                // TODO: Enable event_disk_graph once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
                let filter = PropertyFilter::property("k2").fuzzy_search("Paper_Airpla", 2, false);
                let expected_results = vec!["N1"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
            }

            #[test]
            fn test_nodes_filters_pg_fuzzy_search() {
                let filter = PropertyFilter::property("k2").fuzzy_search("Paper_Air", 5, false);
                let expected_results = vec!["N1", "N2", "N7"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }

            #[test]
            fn test_nodes_filters_fuzzy_search_prefix_match() {
                // TODO: Enable event_disk_graph once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
                let filter = PropertyFilter::property("k2").fuzzy_search("Pa", 2, true);
                let expected_results = vec!["N1", "N2"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );

                let filter = PropertyFilter::property("k2").fuzzy_search("Pa", 2, false);
                let expected_results = Vec::<&str>::new();
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
            }

            #[test]
            fn test_nodes_filters_pg_fuzzy_search_prefix_match() {
                let filter = PropertyFilter::property("k2").fuzzy_search("Pa", 2, true);
                let expected_results = vec!["N1", "N2", "N7"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k2").fuzzy_search("Pa", 2, false);
                let expected_results = Vec::<&str>::new();
                assert_filter_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }
        }

        mod test_edges_filters_window_graph {
            use crate::{
                db::{
                    api::view::StaticGraphViewOps,
                    graph::{
                        assertions::{
                            assert_filter_edges_results, assert_search_edges_results,
                            TestGraphVariants, TestVariants,
                        },
                        views::{
                            filter::model::{
                                ComposableFilter, EdgeFilter, EdgeFilterOps, PropertyFilterOps,
                            },
                            window_graph::views_test::test_filters_window_graph::WindowGraphTransformer,
                        },
                    },
                },
                prelude::{AdditionOps, GraphViewOps, PropertyAdditionOps, PropertyFilter},
            };
            use raphtory_api::core::{entities::properties::prop::Prop, storage::arc_str::ArcStr};
            use std::sync::Arc;

            fn init_graph<G: StaticGraphViewOps + AdditionOps + PropertyAdditionOps>(
                graph: G,
            ) -> G {
                let edges = vec![
                    (
                        6,
                        "N1",
                        "N2",
                        vec![
                            ("p1", Prop::U64(2u64)),
                            ("k1", Prop::I64(2i64)),
                            ("k2", Prop::Str(ArcStr::from("Paper_Airplane"))),
                            ("k3", Prop::Bool(true)),
                            ("k4", Prop::F64(6.0f64)),
                        ],
                        Some("air_nomad"),
                    ),
                    (
                        7,
                        "N1",
                        "N2",
                        vec![
                            ("p1", Prop::U64(1u64)),
                            ("k1", Prop::I64(5i64)),
                            ("k3", Prop::Bool(false)),
                        ],
                        Some("air_nomad"),
                    ),
                    (
                        6,
                        "N2",
                        "N3",
                        vec![("p1", Prop::U64(1u64)), ("k4", Prop::F64(6.0f64))],
                        Some("water_tribe"),
                    ),
                    (
                        7,
                        "N2",
                        "N3",
                        vec![
                            ("p1", Prop::U64(2u64)),
                            ("k1", Prop::I64(2i64)),
                            ("k2", Prop::Str(ArcStr::from("Paper_Ship"))),
                            ("k3", Prop::Bool(true)),
                            ("k4", Prop::F64(10.0f64)),
                        ],
                        Some("water_tribe"),
                    ),
                    (
                        8,
                        "N3",
                        "N4",
                        vec![("p1", Prop::U64(1u64))],
                        Some("air_nomad"),
                    ),
                    (
                        9,
                        "N4",
                        "N5",
                        vec![("p1", Prop::U64(1u64))],
                        Some("air_nomad"),
                    ),
                    (
                        5,
                        "N5",
                        "N6",
                        vec![
                            ("p1", Prop::U64(1u64)),
                            ("k1", Prop::I64(2i64)),
                            ("k2", Prop::Str(ArcStr::from("Paper_Airplane"))),
                            ("k3", Prop::Bool(true)),
                            ("k4", Prop::F64(6.0f64)),
                        ],
                        Some("air_nomad"),
                    ),
                    (
                        6,
                        "N5",
                        "N6",
                        vec![
                            ("p1", Prop::U64(2u64)),
                            ("k2", Prop::Str(ArcStr::from("Pometry"))),
                            ("k4", Prop::F64(1.0f64)),
                        ],
                        Some("air_nomad"),
                    ),
                    (
                        5,
                        "N6",
                        "N7",
                        vec![("p1", Prop::U64(1u64))],
                        Some("fire_nation"),
                    ),
                    (
                        6,
                        "N6",
                        "N7",
                        vec![("p1", Prop::U64(1u64)), ("k4", Prop::F64(1.0f64))],
                        Some("fire_nation"),
                    ),
                    (
                        3,
                        "N7",
                        "N8",
                        vec![
                            ("p1", Prop::U64(1u64)),
                            ("k1", Prop::I64(2i64)),
                            ("k2", Prop::Str(ArcStr::from("Paper_Ship"))),
                            ("k3", Prop::Bool(true)),
                            ("k4", Prop::F64(10.0f64)),
                        ],
                        Some("air_nomad"),
                    ),
                    (
                        5,
                        "N7",
                        "N8",
                        vec![("p1", Prop::U64(1u64))],
                        Some("air_nomad"),
                    ),
                    (
                        3,
                        "N8",
                        "N9",
                        vec![("p1", Prop::U64(1u64))],
                        Some("fire_nation"),
                    ),
                    (
                        4,
                        "N8",
                        "N9",
                        vec![
                            ("p1", Prop::U64(2u64)),
                            ("k1", Prop::I64(2i64)),
                            ("k2", Prop::Str(ArcStr::from("Sand_Clown"))),
                            ("k3", Prop::Bool(true)),
                            ("k4", Prop::F64(10.0f64)),
                        ],
                        Some("fire_nation"),
                    ),
                    (2, "N9", "N10", vec![("p1", Prop::U64(2u64))], None),
                    (2, "N10", "N11", vec![("q1", Prop::U64(0u64))], None),
                    (2, "N10", "N11", vec![("p1", Prop::U64(3u64))], None),
                    (2, "N11", "N12", vec![("p1", Prop::U64(3u64))], None),
                    (2, "N11", "N12", vec![("q1", Prop::U64(0u64))], None),
                    (2, "N12", "N13", vec![("q1", Prop::U64(0u64))], None),
                    (
                        3,
                        "N12",
                        "N13",
                        vec![
                            ("p1", Prop::U64(3u64)),
                            ("k1", Prop::I64(2i64)),
                            ("k2", Prop::Str(ArcStr::from("Sand_Clown"))),
                            ("k3", Prop::Bool(true)),
                            ("k4", Prop::F64(10.0f64)),
                        ],
                        None,
                    ),
                    (2, "N13", "N14", vec![("q1", Prop::U64(0u64))], None),
                    (3, "N13", "N14", vec![("p1", Prop::U64(3u64))], None),
                    (2, "N14", "N15", vec![("q1", Prop::U64(0u64))], None),
                    (2, "N15", "N1", vec![], None),
                ];

                for (id, src, dst, props, layer) in &edges {
                    graph
                        .add_edge(*id, src, dst, props.clone(), *layer)
                        .unwrap();
                }

                // Constant property assignments
                let constant_properties = vec![
                    (
                        "N1",
                        "N2",
                        vec![
                            ("p1", Prop::U64(1u64)),
                            ("k1", Prop::I64(3i64)),
                            ("k2", Prop::Str(ArcStr::from("Paper_Airplane"))),
                            ("k3", Prop::Bool(true)),
                            ("k4", Prop::F64(6.0f64)),
                        ],
                        Some("air_nomad"),
                    ),
                    ("N4", "N5", vec![("p1", Prop::U64(2u64))], Some("air_nomad")),
                    ("N9", "N10", vec![("p1", Prop::U64(1u64))], None),
                    ("N10", "N11", vec![("p1", Prop::U64(1u64))], None),
                    ("N11", "N12", vec![("p1", Prop::U64(1u64))], None),
                    ("N12", "N13", vec![("p1", Prop::U64(1u64))], None),
                    (
                        "N13",
                        "N14",
                        vec![
                            ("p1", Prop::U64(1u64)),
                            ("k1", Prop::I64(2i64)),
                            ("k2", Prop::Str(ArcStr::from("Sand_Clown"))),
                            ("k3", Prop::Bool(true)),
                            ("k4", Prop::F64(10.0f64)),
                        ],
                        None,
                    ),
                    ("N14", "N15", vec![("p1", Prop::U64(1u64))], None),
                    ("N15", "N1", vec![("p1", Prop::U64(1u64))], None),
                ];

                for (src, dst, props, layer) in constant_properties {
                    graph
                        .edge(src, dst)
                        .unwrap()
                        .add_constant_properties(props, layer)
                        .unwrap();
                }

                graph
            }

            fn init_graph2<G: StaticGraphViewOps + AdditionOps + PropertyAdditionOps>(
                graph: G,
            ) -> G {
                let edges = vec![(
                    2,
                    "N14",
                    "N15",
                    vec![
                        ("q1", Prop::U64(0u64)),
                        (
                            "x",
                            Prop::List(Arc::from(vec![Prop::U64(1), Prop::U64(6), Prop::U64(9)])),
                        ),
                    ],
                    None,
                )];

                for (id, src, dst, props, layer) in &edges {
                    graph
                        .add_edge(*id, src, dst, props.clone(), *layer)
                        .unwrap();
                }

                graph
            }

            #[test]
            fn test_edges_filters_for_src_eq() {
                let filter = EdgeFilter::src().name().eq("N2");
                let expected_results = vec!["N2->N3"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );
            }

            #[test]
            fn test_edges_filters_pg_for_src_eq() {
                let filter = EdgeFilter::src().name().eq("N2");
                let expected_results = vec!["N2->N3"];
                // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }

            #[test]
            fn test_edges_filters_for_src_ne() {
                let filter = EdgeFilter::src().name().ne("N2");
                let expected_results = vec!["N1->N2", "N3->N4", "N5->N6", "N6->N7"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );
            }

            #[test]
            fn test_edges_filters_pg_for_src_ne() {
                let filter = EdgeFilter::src().name().ne("N2");
                let expected_results = vec![
                    "N1->N2", "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N14->N15",
                    "N15->N1", "N3->N4", "N5->N6", "N6->N7", "N7->N8", "N8->N9", "N9->N10",
                ];
                // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );
            }

            #[test]
            fn test_edges_filters_for_dst_in() {
                let filter = EdgeFilter::dst().name().is_in(vec!["N2".into()]);
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = EdgeFilter::dst()
                    .name()
                    .is_in(vec!["N2".into(), "N5".into()]);
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );
            }

            #[test]
            fn test_edges_filters_pg_for_dst_in() {
                // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
                let filter = EdgeFilter::dst().name().is_in(vec!["N2".into()]);
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = EdgeFilter::dst()
                    .name()
                    .is_in(vec!["N2".into(), "N5".into()]);
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }

            #[test]
            fn test_edges_filters_for_dst_not_in() {
                let filter = EdgeFilter::dst().name().is_not_in(vec!["N5".into()]);
                let expected_results = vec!["N1->N2", "N2->N3", "N3->N4", "N5->N6", "N6->N7"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );
            }

            #[test]
            fn test_edges_filters_pg_for_dst_not_in() {
                let filter = EdgeFilter::dst().name().is_not_in(vec!["N5".into()]);
                let expected_results = vec![
                    "N1->N2", "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N14->N15",
                    "N15->N1", "N2->N3", "N3->N4", "N5->N6", "N6->N7", "N7->N8", "N8->N9",
                    "N9->N10",
                ];
                // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }

            #[test]
            fn test_edges_filters_for_property_eq() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N6->N7"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k1").eq(2i64);
                let expected_results = vec!["N2->N3"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k2").eq("Paper_Airplane");
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k3").eq(true);
                let expected_results = vec!["N2->N3"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k4").eq(6.0f64);
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("x").eq(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(6),
                    Prop::U64(9),
                ])));
                let expected_results = vec!["N14->N15"];
                // TODO: List(U64) not supported as disk_graph property
                // assert_filter_edges_results_w!(
                //     init_graph2,
                //     filter,
                //     1..9,
                //     expected_results,
                //     variants = [graph]
                // );
                assert_filter_edges_results(
                    init_graph2,
                    WindowGraphTransformer(1..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                // TODO: Search APIs don't support list yet
                // assert_search_edges_results(
                //     init_graph2,
                //     WindowGraphTransformer(6..9),
                //     filter,
                //     &expected_results,
                //     TestVariants::PersistentOnly,
                // );
            }

            #[test]
            fn test_edges_filters_pg_for_property_eq() {
                // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
                // TODO: Const properties not supported for disk_graph.
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec![
                    "N1->N2", "N14->N15", "N15->N1", "N3->N4", "N6->N7", "N7->N8",
                ];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );
                let expected_results = vec!["N1->N2", "N3->N4", "N6->N7", "N7->N8"];
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::PersistentDiskGraph],
                );

                let filter = PropertyFilter::property("k1").eq(2i64);
                let expected_results = vec![
                    "N12->N13", "N13->N14", "N2->N3", "N5->N6", "N7->N8", "N8->N9",
                ];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );
                let expected_results = vec!["N12->N13", "N2->N3", "N5->N6", "N7->N8", "N8->N9"];
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentDiskGraph],
                );

                let filter = PropertyFilter::property("k2").eq("Paper_Airplane");
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k3").eq(true);
                let expected_results = vec![
                    "N12->N13", "N13->N14", "N2->N3", "N5->N6", "N7->N8", "N8->N9",
                ];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );
                let expected_results = vec!["N12->N13", "N2->N3", "N5->N6", "N7->N8", "N8->N9"];
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentDiskGraph],
                );

                let filter = PropertyFilter::property("k4").eq(6.0f64);
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("x").eq(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(6),
                    Prop::U64(9),
                ])));
                let expected_results = vec!["N14->N15"];
                // TODO: List(U64) not supported as disk_graph property
                // assert_filter_edges_results_pg_w!(
                //     init_graph2,
                //     filter,
                //     1..9,
                //     expected_results,
                //     variants = []
                // );
                assert_filter_edges_results(
                    init_graph2,
                    WindowGraphTransformer(1..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                // TODO: Search APIs don't support list yet
                // assert_search_edges_results(
                //     init_graph2,
                //     WindowGraphTransformer(1..9),
                //     filter.clone(),
                //     &expected_results,
                //     vec![TestGraphVariants::PersistentGraph],
                // );
            }

            #[test]
            fn test_edges_filters_for_property_ne() {
                let filter = PropertyFilter::property("p1").ne(1u64);
                let expected_results = vec!["N2->N3", "N5->N6"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k1").ne(2i64);
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k2").ne("Paper_Airplane");
                let expected_results = vec!["N2->N3", "N5->N6"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k3").ne(true);
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k4").ne(6.0f64);
                let expected_results = vec!["N2->N3", "N5->N6", "N6->N7"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("x").ne(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(6),
                    Prop::U64(9),
                ])));
                let expected_results = Vec::<&str>::new();
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(1..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                // TODO: Search APIs don't support list yet
                // assert_search_edges_results(
                //     init_graph2,
                //     WindowGraphTransformer(1..9),
                //     filter.clone(),
                //     &expected_results,
                //     TestVariants::EventOnly,
                // );
            }

            #[test]
            fn test_edges_filters_pg_for_property_ne() {
                // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
                // TODO: Const properties not supported for disk_graph.
                let filter = PropertyFilter::property("p1").ne(1u64);
                let expected_results = vec![
                    "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N2->N3", "N5->N6", "N8->N9",
                    "N9->N10",
                ];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k1").ne(2i64);
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k2").ne("Paper_Airplane");
                let expected_results = vec![
                    "N12->N13", "N13->N14", "N2->N3", "N5->N6", "N7->N8", "N8->N9",
                ];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );
                let expected_results = vec!["N12->N13", "N2->N3", "N5->N6", "N7->N8", "N8->N9"];
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentDiskGraph],
                );

                let filter = PropertyFilter::property("k3").ne(true);
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k4").ne(6.0f64);
                let expected_results = vec![
                    "N12->N13", "N13->N14", "N2->N3", "N5->N6", "N6->N7", "N7->N8", "N8->N9",
                ];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );
                let expected_results =
                    vec!["N12->N13", "N2->N3", "N5->N6", "N6->N7", "N7->N8", "N8->N9"];
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentDiskGraph],
                );

                let filter = PropertyFilter::property("x").ne(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(6),
                    Prop::U64(9),
                ])));
                let expected_results = Vec::<&str>::new();
                assert_filter_edges_results(
                    init_graph2,
                    WindowGraphTransformer(1..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                // TODO: Search APIs don't support list yet
                // assert_search_edges_results(
                //     init_graph2,
                //     WindowGraphTransformer(1..9),
                //     filter.clone(),
                //     &expected_results,
                //     TestVariants::PersistentOnly,
                // );
            }

            #[test]
            fn test_edges_filters_for_property_lt() {
                let filter = PropertyFilter::property("p1").lt(3u64);
                let expected_results = vec!["N1->N2", "N2->N3", "N3->N4", "N5->N6", "N6->N7"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k1").lt(3i64);
                let expected_results = vec!["N2->N3"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k4").lt(10.0f64);
                let expected_results = vec!["N1->N2", "N5->N6", "N6->N7"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("x").lt(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(7),
                    Prop::U64(0),
                ])));
                let expected_results = vec!["N14->N15"];
                // TODO: List(U64) not supported as disk_graph property
                // assert_filter_edges_results_w!(
                //     init_graph2,
                //     filter,
                //     1..9,
                //     expected_results,
                //     variants = [graph]
                // );
                assert_filter_edges_results(
                    init_graph2,
                    WindowGraphTransformer(1..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                // TODO: Search APIs don't support list yet
                // assert_search_edges_results(
                //     init_graph2,
                //     WindowGraphTransformer(1..9),
                //     filter,
                //     &expected_results,
                //     TestVariants::EventOnly,
                // );
            }

            #[test]
            fn test_edges_filters_pg_for_property_lt() {
                // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
                // TODO: Const properties not supported for disk_graph.
                let filter = PropertyFilter::property("p1").lt(3u64);
                let expected_results = vec![
                    "N1->N2", "N14->N15", "N15->N1", "N2->N3", "N3->N4", "N5->N6", "N6->N7",
                    "N7->N8", "N8->N9", "N9->N10",
                ];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );

                let expected_results = vec![
                    "N1->N2", "N2->N3", "N3->N4", "N5->N6", "N6->N7", "N7->N8", "N8->N9", "N9->N10",
                ];
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentDiskGraph],
                );

                let filter = PropertyFilter::property("k1").lt(3i64);
                let expected_results = vec![
                    "N12->N13", "N13->N14", "N2->N3", "N5->N6", "N7->N8", "N8->N9",
                ];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );

                let expected_results = vec!["N12->N13", "N2->N3", "N5->N6", "N7->N8", "N8->N9"];
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentDiskGraph],
                );

                let filter = PropertyFilter::property("k4").lt(10.0f64);
                let expected_results = vec!["N1->N2", "N5->N6", "N6->N7"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("x").lt(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(7),
                    Prop::U64(0),
                ])));
                let expected_results = vec!["N14->N15"];
                assert_filter_edges_results(
                    init_graph2,
                    WindowGraphTransformer(1..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                // TODO: Search APIs don't support list yet
                // assert_search_edges_results(
                //     init_graph2,
                //     WindowGraphTransformer(1..9),
                //     filter,
                //     &expected_results,
                //     TestVariants::PersistentOnly,
                // );
            }

            #[test]
            fn test_edges_filters_for_property_le() {
                let filter = PropertyFilter::property("p1").le(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N6->N7"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k1").le(2i64);
                let expected_results = vec!["N2->N3"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k4").le(6.0f64);
                let expected_results = vec!["N1->N2", "N5->N6", "N6->N7"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("x").le(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(7),
                    Prop::U64(0),
                ])));
                let expected_results = vec!["N14->N15"];
                // TODO: List(U64) not supported as disk_graph property
                // assert_filter_edges_results_w!(
                //     init_graph2,
                //     filter,
                //     1..9,
                //     expected_results,
                //     variants = [graph]
                // );
                assert_filter_edges_results(
                    init_graph2,
                    WindowGraphTransformer(1..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                // TODO: Search APIs don't support list yet
                // assert_search_edges_results(
                //     init_graph,
                //     WindowGraphTransformer(6..9),
                //     filter,
                //     &expected_results,
                //     TestVariants::EventOnly,
                // );
            }

            #[test]
            fn test_edges_filters_pg_for_property_le() {
                // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
                // TODO: Const properties not supported for disk_graph.
                let filter = PropertyFilter::property("p1").le(1u64);
                let expected_results = vec![
                    "N1->N2", "N14->N15", "N15->N1", "N3->N4", "N6->N7", "N7->N8",
                ];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );
                let expected_results = vec!["N1->N2", "N3->N4", "N6->N7", "N7->N8"];
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::PersistentDiskGraph],
                );

                let filter = PropertyFilter::property("k1").le(2i64);
                let expected_results = vec![
                    "N12->N13", "N13->N14", "N2->N3", "N5->N6", "N7->N8", "N8->N9",
                ];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );

                let expected_results = vec!["N12->N13", "N2->N3", "N5->N6", "N7->N8", "N8->N9"];
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentDiskGraph],
                );

                let filter = PropertyFilter::property("k4").le(6.0f64);
                let expected_results = vec!["N1->N2", "N5->N6", "N6->N7"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("x").le(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(2),
                    Prop::U64(3),
                ])));
                let expected_results = Vec::<&str>::new();
                assert_filter_edges_results(
                    init_graph2,
                    WindowGraphTransformer(1..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                // TODO: Search APIs don't support list yet
                // assert_search_edges_results(
                //     init_graph2,
                //     WindowGraphTransformer(1..9),
                //     filter,
                //     &expected_results,
                //     TestVariants::EventOnly,
                // );
            }

            #[test]
            fn test_edges_filters_for_property_gt() {
                let filter = PropertyFilter::property("p1").gt(1u64);
                let expected_results = vec!["N2->N3", "N5->N6"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k1").gt(2i64);
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k4").gt(6.0f64);
                let expected_results = vec!["N2->N3"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("x").gt(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(6),
                    Prop::U64(9),
                ])));
                let expected_results = Vec::<&str>::new();
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(1..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                // TODO: Search APIs don't support list yet
                // assert_search_edges_results(
                //     init_graph,
                //     WindowGraphTransformer(1..9),
                //     filter,
                //     &expected_results,
                //     TestVariants::EventOnly,
                // );
            }

            #[test]
            fn test_edges_filters_pg_for_property_gt() {
                // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
                // TODO: Const properties not supported for disk_graph.
                let filter = PropertyFilter::property("p1").gt(1u64);
                let expected_results = vec![
                    "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N2->N3", "N5->N6", "N8->N9",
                    "N9->N10",
                ];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k1").gt(2i64);
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k4").gt(6.0f64);
                let expected_results = vec!["N12->N13", "N13->N14", "N2->N3", "N7->N8", "N8->N9"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );

                let expected_results = vec!["N12->N13", "N2->N3", "N7->N8", "N8->N9"];
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::PersistentDiskGraph],
                );

                let filter = PropertyFilter::property("x").gt(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(2),
                    Prop::U64(3),
                ])));
                let expected_results = vec!["N14->N15"];
                assert_filter_edges_results(
                    init_graph2,
                    WindowGraphTransformer(1..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                // TODO: Search APIs don't support list yet
                // assert_search_edges_results(
                //     init_graph2,
                //     WindowGraphTransformer(1..9),
                //     filter,
                //     &expected_results,
                //     TestVariants::PersistentOnly,
                // );
            }

            #[test]
            fn test_edges_filters_for_property_ge() {
                let filter = PropertyFilter::property("p1").ge(1u64);
                let expected_results = vec!["N1->N2", "N2->N3", "N3->N4", "N5->N6", "N6->N7"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k1").ge(2i64);
                let expected_results = vec!["N1->N2", "N2->N3"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k4").ge(6.0f64);
                let expected_results = vec!["N1->N2", "N2->N3"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("x").ge(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(6),
                    Prop::U64(9),
                ])));
                let expected_results = vec!["N14->N15"];
                // TODO: List(U64) not supported as disk_graph property
                // assert_filter_edges_results_w!(
                //     init_graph2,
                //     filter,
                //     1..9,
                //     expected_results,
                //     variants = [graph]
                // );
                assert_filter_edges_results(
                    init_graph2,
                    WindowGraphTransformer(1..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                // TODO: Search APIs don't support list yet
                // assert_search_edges_results(
                //     init_graph2,
                //     WindowGraphTransformer(1.9),
                //     filter,
                //     &expected_results,
                //     TestVariants::EventOnly,
                // );
            }

            #[test]
            fn test_edges_filters_pg_for_property_ge() {
                // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
                // TODO: Const properties not supported for disk_graph.
                let filter = PropertyFilter::property("p1").ge(1u64);
                let expected_results = vec![
                    "N1->N2", "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N14->N15",
                    "N15->N1", "N2->N3", "N3->N4", "N5->N6", "N6->N7", "N7->N8", "N8->N9",
                    "N9->N10",
                ];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );

                let expected_results = vec![
                    "N1->N2", "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N2->N3", "N3->N4",
                    "N5->N6", "N6->N7", "N7->N8", "N8->N9", "N9->N10",
                ];
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::PersistentDiskGraph],
                );

                let filter = PropertyFilter::property("k1").ge(2i64);
                let expected_results = vec![
                    "N1->N2", "N12->N13", "N13->N14", "N2->N3", "N5->N6", "N7->N8", "N8->N9",
                ];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );

                let expected_results =
                    vec!["N1->N2", "N12->N13", "N2->N3", "N5->N6", "N7->N8", "N8->N9"];
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::PersistentDiskGraph],
                );

                let filter = PropertyFilter::property("k4").ge(6.0f64);
                let expected_results = vec![
                    "N1->N2", "N12->N13", "N13->N14", "N2->N3", "N7->N8", "N8->N9",
                ];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );

                let expected_results = vec!["N1->N2", "N12->N13", "N2->N3", "N7->N8", "N8->N9"];
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::PersistentDiskGraph],
                );

                let filter = PropertyFilter::property("x").ge(Prop::List(Arc::new(vec![
                    Prop::U64(1),
                    Prop::U64(2),
                    Prop::U64(3),
                ])));
                let expected_results = vec!["N14->N15"];
                assert_filter_edges_results(
                    init_graph2,
                    WindowGraphTransformer(1..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                // TODO: Search APIs don't support list yet
                // assert_search_edges_results(
                //     init_graph2,
                //     WindowGraphTransformer(1..9),
                //     filter,
                //     &expected_results,
                //     TestVariants::PersistentOnly,
                // );
            }

            #[test]
            fn test_edges_filters_for_property_in() {
                let filter = PropertyFilter::property("p1").is_in(vec![2u64.into()]);
                let expected_results = vec!["N2->N3", "N5->N6"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k1").is_in(vec![2i64.into()]);
                let expected_results = vec!["N2->N3"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k2").is_in(vec!["Paper_Airplane".into()]);
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k3").is_in(vec![true.into()]);
                let expected_results = vec!["N2->N3"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k4").is_in(vec![6.0f64.into()]);
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );
            }

            #[test]
            fn test_edges_filters_pg_for_property_in() {
                // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
                // TODO: Const properties not supported for disk_graph.
                let filter = PropertyFilter::property("p1").is_in(vec![2u64.into()]);
                let expected_results = vec!["N2->N3", "N5->N6", "N8->N9", "N9->N10"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k1").is_in(vec![2i64.into()]);
                let expected_results = vec![
                    "N12->N13", "N13->N14", "N2->N3", "N5->N6", "N7->N8", "N8->N9",
                ];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );

                let expected_results = vec!["N12->N13", "N2->N3", "N5->N6", "N7->N8", "N8->N9"];
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::PersistentDiskGraph],
                );

                let filter = PropertyFilter::property("k2").is_in(vec!["Paper_Airplane".into()]);
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k3").is_in(vec![true.into()]);
                let expected_results = vec![
                    "N12->N13", "N13->N14", "N2->N3", "N5->N6", "N7->N8", "N8->N9",
                ];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );

                let expected_results = vec!["N12->N13", "N2->N3", "N5->N6", "N7->N8", "N8->N9"];
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::PersistentDiskGraph],
                );

                let filter = PropertyFilter::property("k4").is_in(vec![6.0f64.into()]);
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }

            #[test]
            fn test_edges_filters_for_property_not_in() {
                let filter = PropertyFilter::property("p1").is_not_in(vec![1u64.into()]);
                let expected_results = vec!["N2->N3", "N5->N6"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k1").is_not_in(vec![2i64.into()]);
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter =
                    PropertyFilter::property("k2").is_not_in(vec!["Paper_Airplane".into()]);
                let expected_results = vec!["N2->N3", "N5->N6"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k3").is_not_in(vec![true.into()]);
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k4").is_not_in(vec![6.0f64.into()]);
                let expected_results = vec!["N2->N3", "N5->N6", "N6->N7"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );
            }

            #[test]
            fn test_edges_filters_pg_for_property_not_in() {
                // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
                // TODO: Const properties not supported for disk_graph.
                let filter = PropertyFilter::property("p1").is_not_in(vec![1u64.into()]);
                let expected_results = vec![
                    "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N2->N3", "N5->N6", "N8->N9",
                    "N9->N10",
                ];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k1").is_not_in(vec![2i64.into()]);
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter =
                    PropertyFilter::property("k2").is_not_in(vec!["Paper_Airplane".into()]);
                let expected_results = vec![
                    "N12->N13", "N13->N14", "N2->N3", "N5->N6", "N7->N8", "N8->N9",
                ];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );

                let expected_results = vec!["N12->N13", "N2->N3", "N5->N6", "N7->N8", "N8->N9"];
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::PersistentDiskGraph],
                );

                let filter = PropertyFilter::property("k3").is_not_in(vec![true.into()]);
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k4").is_not_in(vec![6.0f64.into()]);
                let expected_results = vec![
                    "N12->N13", "N13->N14", "N2->N3", "N5->N6", "N6->N7", "N7->N8", "N8->N9",
                ];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );

                let expected_results =
                    vec!["N12->N13", "N2->N3", "N5->N6", "N6->N7", "N7->N8", "N8->N9"];
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::PersistentDiskGraph],
                );
            }

            #[test]
            fn test_edges_filters_for_property_is_some() {
                let filter = PropertyFilter::property("p1").is_some();
                let expected_results = vec!["N1->N2", "N2->N3", "N3->N4", "N5->N6", "N6->N7"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );
            }

            #[test]
            fn test_edges_filters_pg_for_property_is_some() {
                // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
                // TODO: Const properties not supported for disk_graph.
                let filter = PropertyFilter::property("p1").is_some();
                let expected_results = vec![
                    "N1->N2", "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N14->N15",
                    "N15->N1", "N2->N3", "N3->N4", "N5->N6", "N6->N7", "N7->N8", "N8->N9",
                    "N9->N10",
                ];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::PersistentGraph],
                );
                let expected_results = vec![
                    "N1->N2", "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N2->N3", "N3->N4",
                    "N5->N6", "N6->N7", "N7->N8", "N8->N9", "N9->N10",
                ];
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    vec![TestGraphVariants::PersistentDiskGraph],
                );
            }

            #[test]
            fn test_edges_filters_for_src_dst() {
                let filter = EdgeFilter::src()
                    .name()
                    .eq("N1")
                    .and(EdgeFilter::dst().name().eq("N2"));
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );
            }

            #[test]
            fn test_edges_filters_pg_for_src_dst() {
                // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
                let filter = EdgeFilter::src()
                    .name()
                    .eq("N1")
                    .and(EdgeFilter::dst().name().eq("N2"));
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }

            #[test]
            fn test_edges_filters_fuzzy_search() {
                let filter = PropertyFilter::property("k2").fuzzy_search("Paper_Airpla", 2, false);
                let expected_results = vec!["N1->N2"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );
            }

            #[test]
            #[ignore]
            fn test_edges_filters_pg_fuzzy_search() {
                // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
                let filter = PropertyFilter::property("k2").fuzzy_search("Paper_", 2, false);
                let expected_results = vec!["N1->N2", "N2->N3", "N7->N8"];
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }

            #[test]
            fn test_edges_filters_fuzzy_search_prefix_match() {
                let filter = PropertyFilter::property("k2").fuzzy_search("Pa", 2, true);
                let expected_results = vec!["N1->N2", "N2->N3"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let filter = PropertyFilter::property("k2").fuzzy_search("Pa", 2, true);
                let expected_results = vec!["N1->N2", "N2->N3"];
                assert_filter_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );
            }

            #[test]
            #[ignore]
            fn test_edges_filters_pg_fuzzy_search_prefix_match() {
                // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
                let filter = PropertyFilter::property("k2").fuzzy_search("Pa", 2, true);
                let expected_results = vec![
                    "N1->N2", "N12->N13", "N13->N14", "N2->N3", "N5->N6", "N7->N8", "N8->N9",
                ];
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let filter = PropertyFilter::property("k2").fuzzy_search("Pa", 2, false);
                let expected_results = Vec::<&str>::new();
                assert_search_edges_results(
                    init_graph,
                    WindowGraphTransformer(6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }
        }
    }
}
