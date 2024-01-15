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
//!  assert_eq!(wg.edge(1, 2).unwrap().src().id(), 1);
//! ```

use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, nodes::node_ref::NodeRef, LayerIds, EID, VID},
        storage::timeindex::AsTime,
        ArcStr, Direction, Prop,
    },
    db::{
        api::{
            properties::internal::{
                InheritStaticPropertiesOps, TemporalPropertiesOps, TemporalPropertyViewOps,
            },
            view::{
                internal::{
                    Base, EdgeFilter, EdgeFilterOps, EdgeWindowFilter, GraphOps, Immutable,
                    InheritCoreOps, InheritLayerOps, InheritMaterialize, Static, TimeSemantics,
                },
                BoxedIter, BoxedLIter,
            },
        },
        graph::graph::graph_equal,
    },
    prelude::GraphViewOps,
};
use chrono::{DateTime, Utc};
use std::{
    fmt::{Debug, Formatter},
    ops::Range,
    sync::Arc,
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
    filter: EdgeFilter,
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

impl<'graph, G: GraphViewOps<'graph>> TemporalPropertyViewOps for WindowedGraph<G> {
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
        v: VID,
        w: Range<i64>,
        layer_ids: &LayerIds,
        edge_filter: Option<&EdgeFilter>,
    ) -> bool {
        self.graph
            .include_node_window(v, w.start..w.end, layer_ids, edge_filter)
    }

    #[inline]
    fn include_edge_window(&self) -> &EdgeWindowFilter {
        self.graph.include_edge_window()
    }

    fn node_history(&self, v: VID) -> Vec<i64> {
        self.graph
            .node_history_window(v, self.start_bound()..self.end_bound())
    }

    fn node_history_window(&self, v: VID, w: Range<i64>) -> Vec<i64> {
        self.graph.node_history_window(v, w.start..w.end)
    }

    fn edge_history(&self, e: EdgeRef, layer_ids: LayerIds) -> Vec<i64> {
        self.graph
            .edge_history_window(e, layer_ids, self.start_bound()..self.end_bound())
    }

    fn edge_history_window(&self, e: EdgeRef, layer_ids: LayerIds, w: Range<i64>) -> Vec<i64> {
        self.graph.edge_history_window(e, layer_ids, w.start..w.end)
    }

    fn edge_exploded(&self, e: EdgeRef, layer_ids: LayerIds) -> BoxedIter<EdgeRef> {
        self.graph
            .edge_window_exploded(e, self.start_bound()..self.end_bound(), layer_ids)
    }

    fn edge_layers(&self, e: EdgeRef, layer_ids: LayerIds) -> BoxedIter<EdgeRef> {
        self.graph
            .edge_window_layers(e, self.start_bound()..self.end_bound(), layer_ids)
    }

    fn edge_window_exploded(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> BoxedIter<EdgeRef> {
        self.graph
            .edge_window_exploded(e, w.start..w.end, layer_ids)
    }

    fn edge_window_layers(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> BoxedIter<EdgeRef> {
        self.graph.edge_window_layers(e, w.start..w.end, layer_ids)
    }

    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: LayerIds) -> Option<i64> {
        self.graph
            .edge_earliest_time_window(e, self.start_bound()..self.end_bound(), layer_ids)
    }

    fn edge_earliest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Option<i64> {
        self.graph
            .edge_earliest_time_window(e, w.start..w.end, layer_ids)
    }

    fn edge_latest_time(&self, e: EdgeRef, layer_ids: LayerIds) -> Option<i64> {
        self.graph
            .edge_latest_time_window(e, self.start_bound()..self.end_bound(), layer_ids)
    }

    fn edge_latest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Option<i64> {
        self.graph
            .edge_latest_time_window(e, w.start..w.end, layer_ids)
    }

    fn edge_deletion_history(&self, e: EdgeRef, layer_ids: LayerIds) -> Vec<i64> {
        self.graph
            .edge_deletion_history_window(e, self.start_bound()..self.end_bound(), layer_ids)
    }

    fn edge_deletion_history_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Vec<i64> {
        self.graph
            .edge_deletion_history_window(e, w.start..w.end, layer_ids)
    }

    fn edge_is_valid(&self, e: EdgeRef, layer_ids: LayerIds) -> bool {
        self.graph
            .edge_is_valid_at_end(e, layer_ids, self.end_bound())
    }

    fn edge_is_valid_at_end(&self, e: EdgeRef, layer_ids: LayerIds, t: i64) -> bool {
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

    fn temporal_node_prop_vec(&self, v: VID, prop_id: usize) -> Vec<(i64, Prop)> {
        self.graph
            .temporal_node_prop_vec_window(v, prop_id, self.start_bound(), self.end_bound())
    }

    fn has_temporal_node_prop_window(&self, v: VID, prop_id: usize, w: Range<i64>) -> bool {
        self.graph
            .has_temporal_node_prop_window(v, prop_id, w.start..w.end)
    }

    fn temporal_node_prop_vec_window(
        &self,
        v: VID,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph
            .temporal_node_prop_vec_window(v, prop_id, start, end)
    }

    fn has_temporal_edge_prop_window(
        &self,
        e: EdgeRef,
        prop_id: usize,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> bool {
        self.graph
            .has_temporal_edge_prop_window(e, prop_id, w.start..w.end, layer_ids)
    }

    fn temporal_edge_prop_vec_window(
        &self,
        e: EdgeRef,
        prop_id: usize,
        start: i64,
        end: i64,
        layer_ids: LayerIds,
    ) -> Vec<(i64, Prop)> {
        self.graph
            .temporal_edge_prop_vec_window(e, prop_id, start, end, layer_ids)
    }

    fn has_temporal_edge_prop(&self, e: EdgeRef, prop_id: usize, layer_ids: LayerIds) -> bool {
        self.graph.has_temporal_edge_prop_window(
            e,
            prop_id,
            self.start_bound()..self.end_bound(),
            layer_ids,
        )
    }

    fn temporal_edge_prop_vec(
        &self,
        e: EdgeRef,
        prop_id: usize,
        layer_ids: LayerIds,
    ) -> Vec<(i64, Prop)> {
        self.graph.temporal_edge_prop_vec_window(
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
    fn edge_filter(&self) -> Option<&EdgeFilter> {
        Some(&self.filter)
    }
}

impl<'graph, G: GraphViewOps<'graph>> GraphOps<'graph> for WindowedGraph<G> {
    /// Get an iterator over the references of all nodes as references
    ///
    /// Returns:
    ///
    /// An iterator over the references of all nodes
    #[inline]
    fn node_refs(&self, layers: LayerIds, filter: Option<&EdgeFilter>) -> BoxedLIter<'graph, VID> {
        let g = self.clone();
        let filter_cloned = filter.cloned();
        Box::new(
            self.graph
                .node_refs(layers.clone(), filter)
                .filter(move |v| {
                    g.include_node_window(
                        *v,
                        g.start_bound()..g.end_bound(),
                        &layers,
                        filter_cloned.as_ref(),
                    )
                }),
        )
    }

    /// Get an iterator of all edges as references
    ///
    /// Returns:
    ///
    /// An iterator over all edges as references
    #[inline]
    fn edge_refs(
        &self,
        layer: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> BoxedLIter<'graph, EdgeRef> {
        self.graph.edge_refs(layer, filter)
    }

    #[inline]
    fn node_edges(
        &self,
        v: VID,
        d: Direction,
        layer: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> BoxedLIter<'graph, EdgeRef> {
        self.graph.node_edges(v, d, layer, filter)
    }

    /// Get the neighbours of a node as references in a given direction
    ///
    /// # Arguments
    ///
    /// - `v` - The node to get the neighbours for
    /// - `d` - The direction of the edges
    ///
    /// Returns:
    ///
    /// An iterator over all neighbours in that node direction as references
    #[inline]
    fn neighbours(
        &self,
        v: VID,
        d: Direction,
        layer: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> BoxedLIter<'graph, VID> {
        self.graph.neighbours(v, d, layer, filter)
    }

    #[inline]
    fn internal_node_ref(
        &self,
        v: NodeRef,
        layers: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Option<VID> {
        self.graph.internal_node_ref(v, layers, filter).filter(|v| {
            self.include_node_window(*v, self.start_bound()..self.end_bound(), layers, filter)
        })
    }

    #[inline]
    fn find_edge_id(
        &self,
        e_id: EID,
        layer_ids: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Option<EdgeRef> {
        self.graph.find_edge_id(e_id, layer_ids, filter)
    }

    /// Returns the number of nodes in the windowed view.
    #[inline]
    fn nodes_len(&self, layer_ids: LayerIds, filter: Option<&EdgeFilter>) -> usize {
        self.node_refs(layer_ids, filter).count()
    }

    /// Returns the number of edges in the windowed view.
    #[inline]
    fn edges_len(&self, layer: LayerIds, filter: Option<&EdgeFilter>) -> usize {
        // filter takes care of checking the window
        self.graph.edges_len(layer, filter)
    }

    #[inline]
    fn temporal_edges_len(&self, layers: LayerIds, filter: Option<&EdgeFilter>) -> usize {
        self.graph
            .edge_refs(layers.clone(), filter)
            .flat_map(|eref| self.edge_exploded(eref, layers.clone()))
            .count()
    }

    /// Check if there is an edge from src to dst in the window.
    ///
    /// # Arguments
    ///
    /// - `src` - The source node.
    /// - `dst` - The destination node.
    ///
    /// Returns:
    ///
    /// A result containing `true` if there is an edge from src to dst in the window, `false` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if either `src` or `dst` is not a valid node.
    #[inline]
    fn has_edge_ref(
        &self,
        src: VID,
        dst: VID,
        layer: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> bool {
        // filter takes care of checking the window
        self.graph.has_edge_ref(src, dst, layer, filter)
    }

    /// Check if a node v exists in the window.
    ///
    /// # Arguments
    ///
    /// - `v` - The node to check.
    ///
    /// Returns:
    ///
    /// A result containing `true` if the node exists in the window, `false` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if `v` is not a valid node.
    #[inline]
    fn has_node_ref(&self, v: NodeRef, layers: &LayerIds, filter: Option<&EdgeFilter>) -> bool {
        self.internal_node_ref(v, layers, filter).is_some()
    }

    /// Returns the number of edges from a node in the window.
    ///
    /// # Arguments
    ///
    /// - `v` - The node to check.
    /// - `d` - The direction of the edges to count.
    ///
    /// Returns:
    ///
    /// A result containing the number of edges from the node in the window.
    ///
    /// # Errors
    ///
    /// Returns an error if `v` is not a valid node.
    #[inline]
    fn degree(&self, v: VID, d: Direction, layer: &LayerIds, filter: Option<&EdgeFilter>) -> usize {
        self.graph.degree(v, d, layer, filter)
    }

    /// Get the reference of the node with ID v if it exists
    ///
    /// # Arguments
    ///
    /// - `v` - The ID of the node to get
    ///
    /// Returns:
    ///
    /// A result of an option containing the node reference if it exists, `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if `v` is not a valid node.
    #[inline]
    fn node_ref(&self, v: u64, layers: &LayerIds, filter: Option<&EdgeFilter>) -> Option<VID> {
        self.internal_node_ref(v.into(), layers, filter)
    }

    /// Get an iterator over the references of an edges as a reference
    ///
    /// # Arguments
    ///
    /// - `src` - The source node of the edge
    /// - `dst` - The destination node of the edge
    ///
    /// Returns:
    ///
    /// A result of an option containing the edge reference if it exists, `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if `src` or `dst` are not valid nodes.
    #[inline]
    fn edge_ref(
        &self,
        src: VID,
        dst: VID,
        layer: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Option<EdgeRef> {
        self.graph.edge_ref(src, dst, layer, filter)
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
        let start_bound = start.unwrap_or(i64::MIN);
        let end_bound = end.unwrap_or(i64::MAX);
        let base_filter = graph.edge_filter_window().cloned();
        let base_window_filter = graph.include_edge_window().clone();
        let filter: EdgeFilter = match base_filter {
            Some(f) => Arc::new(move |e, layers| {
                f(e, layers) && base_window_filter(e, layers, start_bound..end_bound)
            }),
            None => {
                Arc::new(move |e, layers| base_window_filter(e, layers, start_bound..end_bound))
            }
        };

        WindowedGraph {
            graph,
            start,
            end,
            filter,
        }
    }
}

#[cfg(test)]
mod views_test {

    use super::*;
    use crate::{
        algorithms::centrality::degree_centrality::degree_centrality,
        db::graph::graph::assert_graph_equal, prelude::*,
    };
    use itertools::Itertools;
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;
    use rand::prelude::*;
    use rayon::prelude::*;

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

        let g = Graph::new();

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }

        let wg = g.window(-1, 1);

        let actual = wg
            .nodes()
            .iter()
            .map(|v| (v.id(), v.degree()))
            .collect::<Vec<_>>();

        let expected = vec![(1, 2), (2, 1)];

        assert_eq!(actual, expected);
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

        let g = Graph::new();

        for (t, src, dst) in vs {
            g.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }

        let wg = g.window(i64::MIN, i64::MAX);
        assert_eq!(wg.edge(1, 3).unwrap().src().id(), 1);
        assert_eq!(wg.edge(1, 3).unwrap().dst().id(), 3);
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

        let g = Graph::new();

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }

        let wg = g.window(-1, 1);

        assert_eq!(wg.node(1).unwrap().id(), 1);
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
        let g = Graph::new();

        for (t, v) in &vs {
            g.add_node(*t, *v, NO_PROPS)
                .map_err(|err| println!("{:?}", err))
                .ok();
        }

        let wg = g.window(1, 2);
        assert!(!wg.has_node(262))
    }

    #[quickcheck]
    fn windowed_graph_has_node(mut vs: Vec<(i64, u64)>) -> TestResult {
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
            g.add_node(*t, *v, NO_PROPS)
                .map_err(|err| println!("{:?}", err))
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

    #[quickcheck]
    fn windowed_graph_edge_count(
        mut edges: Vec<(i64, (u64, u64))>,
        window: Range<i64>,
    ) -> TestResult {
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
            println!(
                "failed, g.num_edges() = {}, true count = {}",
                wg.count_edges(),
                true_edge_count
            );
            println!("g.edges() = {:?}", wg.edges().iter().collect_vec());
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

        let g = Graph::new();

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }

        let res: Vec<_> = (0..=3)
            .map(|i| {
                let wg = g.window(args[i].0, args[i].1);
                let mut e = wg.nodes().id().collect::<Vec<_>>();
                e.sort();
                e
            })
            .collect_vec();

        assert_eq!(res, expected);

        let g = Graph::new();
        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, NO_PROPS, None).unwrap();
        }
        let res: Vec<_> = (0..=3)
            .map(|i| {
                let wg = g.window(args[i].0, args[i].1);
                let mut e = wg.nodes().id().collect::<Vec<_>>();
                e.sort();
                e
            })
            .collect_vec();
        assert_eq!(res, expected);
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

        let g = Graph::new();

        g.add_node(
            0,
            1,
            [("type", "wallet".into_prop()), ("cost", 99.5.into_prop())],
        )
        .map_err(|err| println!("{:?}", err))
        .ok();

        g.add_node(
            -1,
            2,
            [("type", "wallet".into_prop()), ("cost", 10.0.into_prop())],
        )
        .map_err(|err| println!("{:?}", err))
        .ok();

        g.add_node(
            6,
            3,
            [("type", "wallet".into_prop()), ("cost", 76.2.into_prop())],
        )
        .map_err(|err| println!("{:?}", err))
        .ok();

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, [("eprop", "commons")], None)
                .unwrap();
        }

        let wg = g.window(-2, 0);

        let actual = wg.nodes().id().collect::<Vec<_>>();

        let expected = vec![1, 2];

        assert_eq!(actual, expected);

        // Check results from multiple graphs with different number of shards
        let g = Graph::new();

        g.add_node(
            0,
            1,
            [("type", "wallet".into_prop()), ("cost", 99.5.into_prop())],
        )
        .map_err(|err| println!("{:?}", err))
        .ok();

        g.add_node(
            -1,
            2,
            [("type", "wallet".into_prop()), ("cost", 10.0.into_prop())],
        )
        .map_err(|err| println!("{:?}", err))
        .ok();

        g.add_node(
            6,
            3,
            [("type", "wallet".into_prop()), ("cost", 76.2.into_prop())],
        )
        .map_err(|err| println!("{:?}", err))
        .ok();

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }

        let expected = wg.nodes().id().collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_reference() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        let mut w = WindowedGraph::new(&g, Some(0), Some(1));
        assert_eq!(w, g);
        w = WindowedGraph::new(&g, Some(1), Some(2));

        assert_eq!(w, Graph::new());
    }

    #[test]
    fn test_algorithm_on_windowed_graph() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        let w = WindowedGraph::new(g, Some(0), Some(1));

        let res = degree_centrality(&w, None);
        println!("{:?}", res)
    }

    #[test]
    fn test_view_resetting() {
        let g = Graph::new();
        for t in 0..10 {
            let t1 = t * 3;
            let t2 = t * 3 + 1;
            let t3 = t * 3 + 2;
            g.add_edge(t1, 1, 2, NO_PROPS, None).unwrap();
            g.add_edge(t2, 2, 3, NO_PROPS, None).unwrap();
            g.add_edge(t3, 3, 1, NO_PROPS, None).unwrap();
        }
        assert_graph_equal(&g.before(9).after(2), &g.window(3, 9));
        let res = g
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
    }

    #[test]
    fn test_entity_history() {
        let g = Graph::new();
        g.add_node(0, 1, NO_PROPS).unwrap();
        g.add_node(1, 1, NO_PROPS).unwrap();
        g.add_node(2, 1, NO_PROPS).unwrap();
        let v = g.add_node(3, 1, NO_PROPS).unwrap();
        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.add_edge(1, 1, 2, NO_PROPS, None).unwrap();
        g.add_edge(2, 1, 2, NO_PROPS, None).unwrap();
        let e = g.add_edge(3, 1, 2, NO_PROPS, None).unwrap();

        let full_history_1 = vec![0i64, 1, 2, 3];

        let full_history_2 = vec![4i64, 5, 6, 7];

        let windowed_history = vec![0i64, 1];

        assert_eq!(v.history(), full_history_1);

        assert_eq!(v.window(0, 2).history(), windowed_history);
        assert_eq!(e.history(), full_history_1);
        assert_eq!(e.window(0, 2).history(), windowed_history);

        g.add_edge(4, 1, 3, NO_PROPS, None).unwrap();
        g.add_edge(5, 1, 3, NO_PROPS, None).unwrap();
        g.add_edge(6, 1, 3, NO_PROPS, None).unwrap();
        g.add_edge(7, 1, 3, NO_PROPS, None).unwrap();

        assert_eq!(
            g.edges().history().collect_vec(),
            [full_history_1.clone(), full_history_2.clone()]
        );
        assert_eq!(
            g.nodes()
                .in_edges()
                .history()
                .map(|it| it.collect_vec())
                .collect_vec(),
            [vec![], vec![full_history_1], vec![full_history_2],]
        );

        assert_eq!(
            g.nodes().earliest_time().flatten().collect_vec(),
            [0, 0, 4,]
        );

        assert_eq!(g.nodes().latest_time().flatten().collect_vec(), [7, 3, 7]);

        assert_eq!(
            g.nodes()
                .neighbours()
                .latest_time()
                .map(|it| it.flatten().collect_vec())
                .collect_vec(),
            [vec![3, 7], vec![7], vec![7],]
        );

        assert_eq!(
            g.nodes()
                .neighbours()
                .earliest_time()
                .map(|it| it.flatten().collect_vec())
                .collect_vec(),
            [vec![0, 4,], vec![0], vec![0],]
        );
    }
}
