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
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
        storage::timeindex::AsTime,
        Prop, PropType,
    },
    db::{
        api::{
            properties::internal::{
                InheritStaticPropertiesOps, TemporalPropertiesOps, TemporalPropertyViewOps,
            },
            storage::graph::{edges::edge_ref::EdgeStorageRef, nodes::node_ref::NodeStorageRef},
            view::{
                internal::{
                    Base, EdgeFilterOps, Immutable, InheritCoreOps, InheritLayerOps,
                    InheritListOps, InheritMaterialize, NodeFilterOps, Static, TimeSemantics,
                },
                BoxedIter, BoxedLIter,
            },
        },
        graph::graph::graph_equal,
    },
    prelude::GraphViewOps,
};
use chrono::{DateTime, Utc};
use raphtory_api::core::storage::{arc_str::ArcStr, timeindex::TimeIndexEntry};
use std::{
    fmt::{Debug, Formatter},
    ops::Range,
};

/// A struct that represents a windowed view of a `Graph`.
#[derive(Clone)]
pub struct WindowedGraph<G> {
    /// The underlying `Graph` object.
    pub graph: G,
    /// The inclusive start time of the window.
    pub start: Option<i64>,
    /// The exclusive end time of the window.
    pub end: Option<i64>,
}

impl<G> Static for WindowedGraph<G> {}

impl<'graph, G: Debug + 'graph> Debug for WindowedGraph<G> {
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

impl<G> WindowedGraph<G> {
    fn start_bound(&self) -> i64 {
        self.start.unwrap_or(i64::MIN)
    }

    fn end_bound(&self) -> i64 {
        self.end.unwrap_or(i64::MAX)
    }
}

impl<'graph, G: GraphViewOps<'graph>> Immutable for WindowedGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritCoreOps for WindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for WindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritStaticPropertiesOps for WindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for WindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritListOps for WindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> NodeFilterOps for WindowedGraph<G> {
    #[inline]
    fn node_list_trusted(&self) -> bool {
        self.graph.node_list_trusted() && !self.nodes_filtered()
    }

    #[inline]
    fn nodes_filtered(&self) -> bool {
        self.graph.nodes_filtered()
            || self.start_bound() > self.graph.earliest_time().unwrap_or(i64::MAX)
            || self.end_bound() <= self.graph.latest_time().unwrap_or(i64::MIN)
    }

    #[inline]
    fn filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.filter_node(node, layer_ids)
            && self
                .graph
                .include_node_window(node, self.start_bound()..self.end_bound(), layer_ids)
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
    fn temporal_history(&self, id: usize) -> Vec<i64> {
        self.temporal_prop_vec(id)
            .into_iter()
            .map(|(t, _)| t)
            .collect()
    }

    fn temporal_history_date_time(&self, id: usize) -> Option<Vec<DateTime<Utc>>> {
        self.temporal_prop_vec(id)
            .into_iter()
            .map(|(t, _)| t.dt())
            .collect()
    }

    fn temporal_values(&self, id: usize) -> Vec<Prop> {
        self.temporal_prop_vec(id)
            .into_iter()
            .map(|(_, v)| v)
            .collect()
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

impl<'graph, G: GraphViewOps<'graph>> TimeSemantics for WindowedGraph<G> {
    fn node_earliest_time(&self, v: VID) -> Option<i64> {
        self.graph
            .node_earliest_time_window(v, self.start_bound(), self.end_bound())
    }

    fn node_latest_time(&self, v: VID) -> Option<i64> {
        self.graph
            .node_latest_time_window(v, self.start_bound(), self.end_bound())
    }

    fn view_start(&self) -> Option<i64> {
        self.start
    }

    fn view_end(&self) -> Option<i64> {
        self.end
    }

    #[inline]
    fn earliest_time_global(&self) -> Option<i64> {
        self.graph
            .earliest_time_window(self.start_bound(), self.end_bound())
    }

    #[inline]
    fn latest_time_global(&self) -> Option<i64> {
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

    #[inline]
    fn node_earliest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        self.graph.node_earliest_time_window(v, start, end)
    }

    #[inline]
    fn node_latest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        self.graph.node_latest_time_window(v, start, end)
    }

    #[inline]
    fn include_node_window(
        &self,
        node: NodeStorageRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool {
        self.graph.include_node_window(node, w, layer_ids)
    }

    #[inline]
    fn include_edge_window(
        &self,
        edge: EdgeStorageRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool {
        self.graph.include_edge_window(edge, w, layer_ids)
    }

    fn node_history(&self, v: VID) -> Vec<i64> {
        self.graph
            .node_history_window(v, self.start_bound()..self.end_bound())
    }

    fn node_history_window(&self, v: VID, w: Range<i64>) -> Vec<i64> {
        self.graph.node_history_window(v, w.start..w.end)
    }

    fn edge_history<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        self.graph
            .edge_history_window(e, layer_ids, self.start_bound()..self.end_bound())
    }

    fn edge_history_window<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: &'a LayerIds,
        w: Range<i64>,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        self.graph.edge_history_window(e, layer_ids, w)
    }

    fn edge_exploded_count(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> usize {
        self.graph
            .edge_exploded_count_window(edge, layer_ids, self.start_bound()..self.end_bound())
    }

    fn edge_exploded_count_window(
        &self,
        edge: EdgeStorageRef,
        layer_ids: &LayerIds,
        w: Range<i64>,
    ) -> usize {
        self.graph.edge_exploded_count_window(edge, layer_ids, w)
    }

    fn edge_exploded(&self, e: EdgeRef, layer_ids: &LayerIds) -> BoxedIter<EdgeRef> {
        self.graph
            .edge_window_exploded(e, self.start_bound()..self.end_bound(), layer_ids)
    }

    fn edge_layers(&self, e: EdgeRef, layer_ids: &LayerIds) -> BoxedIter<EdgeRef> {
        self.graph
            .edge_window_layers(e, self.start_bound()..self.end_bound(), layer_ids)
    }

    fn edge_window_exploded(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> BoxedIter<EdgeRef> {
        self.graph
            .edge_window_exploded(e, w.start..w.end, layer_ids)
    }

    fn edge_window_layers(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> BoxedIter<EdgeRef> {
        self.graph.edge_window_layers(e, w.start..w.end, layer_ids)
    }

    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        self.graph
            .edge_earliest_time_window(e, self.start_bound()..self.end_bound(), layer_ids)
    }

    fn edge_earliest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Option<i64> {
        self.graph
            .edge_earliest_time_window(e, w.start..w.end, layer_ids)
    }

    fn edge_latest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        self.graph
            .edge_latest_time_window(e, self.start_bound()..self.end_bound(), layer_ids)
    }

    fn edge_latest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Option<i64> {
        self.graph
            .edge_latest_time_window(e, w.start..w.end, layer_ids)
    }

    fn edge_deletion_history<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        self.graph
            .edge_deletion_history_window(e, self.start_bound()..self.end_bound(), layer_ids)
    }

    fn edge_deletion_history_window<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        self.graph
            .edge_deletion_history_window(e, w.start..w.end, layer_ids)
    }

    fn edge_is_valid(&self, e: EdgeRef, layer_ids: &LayerIds) -> bool {
        self.graph
            .edge_is_valid_at_end(e, layer_ids, self.end_bound())
    }

    fn edge_is_valid_at_end(&self, e: EdgeRef, layer_ids: &LayerIds, t: i64) -> bool {
        // Note, window nesting is already handled, weird behaviour outside window should not matter
        self.graph.edge_is_valid_at_end(e, layer_ids, t)
    }

    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        self.graph
            .has_temporal_prop_window(prop_id, self.start_bound()..self.end_bound())
    }

    fn temporal_prop_vec(&self, prop_id: usize) -> Vec<(i64, Prop)> {
        self.graph
            .temporal_prop_vec_window(prop_id, self.start_bound(), self.end_bound())
    }

    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<i64>) -> bool {
        self.graph.has_temporal_prop_window(prop_id, w.start..w.end)
    }

    fn temporal_prop_vec_window(&self, prop_id: usize, start: i64, end: i64) -> Vec<(i64, Prop)> {
        self.graph.temporal_prop_vec_window(prop_id, start, end)
    }

    fn has_temporal_node_prop(&self, v: VID, prop_id: usize) -> bool {
        self.graph
            .has_temporal_node_prop_window(v, prop_id, self.start_bound()..self.end_bound())
    }

    fn temporal_node_prop_hist(
        &self,
        v: VID,
        prop_id: usize,
    ) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        self.graph
            .temporal_node_prop_hist_window(v, prop_id, self.start_bound(), self.end_bound())
    }

    fn has_temporal_node_prop_window(&self, v: VID, prop_id: usize, w: Range<i64>) -> bool {
        self.graph
            .has_temporal_node_prop_window(v, prop_id, w.start..w.end)
    }

    fn temporal_node_prop_hist_window(
        &self,
        v: VID,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        self.graph
            .temporal_node_prop_hist_window(v, prop_id, start, end)
    }

    fn has_temporal_edge_prop_window(
        &self,
        e: EdgeRef,
        prop_id: usize,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool {
        self.graph
            .has_temporal_edge_prop_window(e, prop_id, w.start..w.end, layer_ids)
    }

    fn temporal_edge_prop_hist_window<'a>(
        &'a self,
        e: EdgeRef,
        prop_id: usize,
        start: i64,
        end: i64,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, (TimeIndexEntry, Prop)> {
        self.graph
            .temporal_edge_prop_hist_window(e, prop_id, start, end, layer_ids)
    }

    fn temporal_edge_prop_at(
        &self,
        e: EdgeRef,
        id: usize,
        t: TimeIndexEntry,
        layer_ids: &LayerIds,
    ) -> Option<Prop> {
        self.graph.temporal_edge_prop_at(e, id, t, layer_ids)
    }

    fn has_temporal_edge_prop(&self, e: EdgeRef, prop_id: usize, layer_ids: &LayerIds) -> bool {
        self.graph.has_temporal_edge_prop_window(
            e,
            prop_id,
            self.start_bound()..self.end_bound(),
            layer_ids,
        )
    }

    fn temporal_edge_prop_hist<'a>(
        &'a self,
        e: EdgeRef,
        prop_id: usize,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, (TimeIndexEntry, Prop)> {
        self.graph.temporal_edge_prop_hist_window(
            e,
            prop_id,
            self.start_bound(),
            self.end_bound(),
            layer_ids,
        )
    }
}

impl<'graph, G: GraphViewOps<'graph>> EdgeFilterOps for WindowedGraph<G> {
    #[inline]
    fn edges_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn edge_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn edge_filter_includes_node_filter(&self) -> bool {
        self.graph.edge_filter_includes_node_filter()
    }

    #[inline]
    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.filter_edge(edge, layer_ids)
            && self
                .graph
                .include_edge_window(edge, self.start_bound()..self.end_bound(), layer_ids)
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
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;
    use rand::prelude::*;
    use raphtory_api::core::{entities::GID, utils::logging::global_info_logger};
    use rayon::prelude::*;
    #[cfg(feature = "storage")]
    use tempfile::TempDir;
    use tracing::{error, info};

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
        global_info_logger();
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
            graph
                .add_node(*t, *v, NO_PROPS, None)
                .map_err(|err| error!("{:?}", err))
                .ok();
        }

        // FIXME: Issue #46: arrow_test(&graph, test)
        test_graph(&graph, |graph| {
            let wg = graph.window(1, 2);
            assert!(!wg.has_node(262))
        });
    }

    #[quickcheck]
    fn windowed_graph_has_node(mut vs: Vec<(i64, u64)>) -> TestResult {
        global_info_logger();
        if vs.is_empty() {
            return TestResult::discard();
        }

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
            if wg.has_node(*v) {
                TestResult::passed()
            } else {
                TestResult::error(format!(
                    "Node {:?} was not in window {:?}",
                    (i, v),
                    start..end
                ))
            }
        } else if !wg.has_node(*v) {
            TestResult::passed()
        } else {
            TestResult::error(format!("Node {:?} was in window {:?}", (i, v), start..end))
        }
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
    #[quickcheck]
    fn windowed_graph_has_edge(mut edges: Vec<(i64, (u64, u64))>) -> TestResult {
        if edges.is_empty() {
            return TestResult::discard();
        }

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
            if wg.has_edge(e.0, e.1) {
                TestResult::passed()
            } else {
                TestResult::error(format!(
                    "Edge {:?} was not in window {:?}",
                    (i, e),
                    start..end
                ))
            }
        } else if !wg.has_edge(e.0, e.1) {
            TestResult::passed()
        } else {
            TestResult::error(format!("Edge {:?} was in window {:?}", (i, e), start..end))
        }
    }

    #[cfg(feature = "storage")]
    #[quickcheck]
    fn windowed_disk_graph_has_edge(mut edges: Vec<(i64, (u64, u64))>) -> TestResult {
        if edges.is_empty() {
            return TestResult::discard();
        }

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
        let g = g
            .persist_as_disk_graph(test_dir.path())
            .unwrap()
            .into_graph();

        let start = edges.get(rand_start_index).expect("start index in range").0;
        let end = edges.get(rand_end_index).expect("end index in range").0;

        let wg = g.window(start, end);

        let rand_test_index: usize = thread_rng().gen_range(0..edges.len());

        let (i, e) = edges.get(rand_test_index).expect("test index in range");
        if (start..end).contains(i) {
            if wg.has_edge(e.0, e.1) {
                TestResult::passed()
            } else {
                TestResult::error(format!(
                    "Edge {:?} was not in window {:?}",
                    (i, e),
                    start..end
                ))
            }
        } else if !wg.has_edge(e.0, e.1) {
            TestResult::passed()
        } else {
            TestResult::error(format!("Edge {:?} was in window {:?}", (i, e), start..end))
        }
    }

    #[quickcheck]
    fn windowed_graph_edge_count(
        mut edges: Vec<(i64, (u64, u64))>,
        window: Range<i64>,
    ) -> TestResult {
        global_info_logger();
        if window.end < window.start {
            return TestResult::discard();
        }
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
        TestResult::from_bool(wg.count_edges() == true_edge_count)
    }

    #[quickcheck]
    fn trivial_window_has_all_edges(edges: Vec<(i64, u64, u64)>) -> bool {
        let g = Graph::new();
        edges
            .into_par_iter()
            .filter(|e| e.0 < i64::MAX)
            .for_each(|(t, src, dst)| {
                g.add_edge(t, src, dst, [("test".to_owned(), Prop::Bool(true))], None)
                    .unwrap();
            });
        let w = g.window(i64::MIN, i64::MAX);
        g.edges()
            .iter()
            .all(|e| w.has_edge(e.src().id(), e.dst().id()))
    }

    #[quickcheck]
    fn large_node_in_window(dsts: Vec<u64>) -> bool {
        let dsts: Vec<u64> = dsts.into_iter().unique().collect();
        let n = dsts.len();
        let g = Graph::new();

        for dst in dsts {
            let t = 1;
            g.add_edge(t, 0, dst, NO_PROPS, None).unwrap();
        }
        let w = g.window(i64::MIN, i64::MAX);
        w.count_edges() == n
    }

    #[test]
    fn windowed_graph_node_ids() {
        let vs = vec![(1, 1, 2), (3, 3, 4), (5, 5, 6), (7, 7, 1)];

        let args = vec![(i64::MIN, 8), (i64::MIN, 2), (i64::MIN, 4), (3, 6)];

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
                        .values()
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
                        .values()
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
                .values()
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

            let res = degree_centrality(&w, None);
            info!("{:?}", res)
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
                    .values()
                    .flatten()
                    .collect_vec(),
                [0, 0, 0, 4,]
            );

            assert_eq!(
                graph.nodes().latest_time().values().flatten().collect_vec(),
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
}
