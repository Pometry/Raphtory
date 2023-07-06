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
//! It contains a `Graph`, a start time (`t_start`) and an end time (`t_end`).
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
//! graph.add_edge(0, 1, 2, [], None).unwrap();
//! graph.add_edge(1, 1, 3, [], None).unwrap();
//! graph.add_edge(2, 2, 3, [], None).unwrap();
//!
//!  let wg = graph.window(0, 1);
//!  assert_eq!(wg.edge(1, 2, None).unwrap().src().id(), 1);
//! ```

use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, vertices::vertex_ref::VertexRef, VID, EID},
        utils::time::IntoTime,
        Direction, Prop,
    },
    db::api::view::{
        internal::{
            Base, GraphOps, GraphWindowOps, InheritCoreOps, InheritMaterialize, TimeSemantics,
        },
        BoxedIter,
    },
    prelude::GraphViewOps,
};
use std::{
    cmp::{max, min},
    ops::Range,
};

/// A struct that represents a windowed view of a `Graph`.
#[derive(Debug, Clone)]
pub struct WindowedGraph<G: GraphViewOps> {
    /// The underlying `Graph` object.
    pub graph: G,
    /// The inclusive start time of the window.
    pub t_start: i64,
    /// The exclusive end time of the window.
    pub t_end: i64,
}

impl<G: GraphViewOps> Base for WindowedGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G: GraphViewOps> InheritCoreOps for WindowedGraph<G> {}

impl<G: GraphViewOps> InheritMaterialize for WindowedGraph<G> {}

impl<G: GraphViewOps> TimeSemantics for WindowedGraph<G> {
    fn vertex_earliest_time(&self, v: VID) -> Option<i64> {
        self.graph
            .vertex_earliest_time_window(v, self.t_start, self.t_end)
    }

    fn vertex_latest_time(&self, v: VID) -> Option<i64> {
        self.graph
            .vertex_latest_time_window(v, self.t_start, self.t_end)
    }

    fn view_start(&self) -> Option<i64> {
        Some(self.t_start)
    }

    fn view_end(&self) -> Option<i64> {
        Some(self.t_end)
    }

    fn earliest_time_global(&self) -> Option<i64> {
        self.graph.earliest_time_window(self.t_start, self.t_end)
    }

    fn latest_time_global(&self) -> Option<i64> {
        self.graph.latest_time_window(self.t_start, self.t_end)
    }

    fn earliest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.graph
            .earliest_time_window(self.actual_start(t_start), self.actual_end(t_end))
    }

    fn latest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.graph
            .latest_time_window(self.actual_start(t_start), self.actual_end(t_end))
    }

    fn vertex_earliest_time_window(&self, v: VID, t_start: i64, t_end: i64) -> Option<i64> {
        self.graph.vertex_earliest_time_window(
            v,
            self.actual_start(t_start),
            self.actual_end(t_end),
        )
    }

    fn vertex_latest_time_window(&self, v: VID, t_start: i64, t_end: i64) -> Option<i64> {
        self.graph
            .vertex_latest_time_window(v, self.actual_start(t_start), self.actual_end(t_end))
    }

    fn include_vertex_window(&self, v: VID, w: Range<i64>) -> bool {
        self.graph
            .include_vertex_window(v, self.actual_start(w.start)..self.actual_end(w.end))
    }

    fn include_edge_window(&self, e: EdgeRef, w: Range<i64>) -> bool {
        self.graph
            .include_edge_window(e, self.actual_start(w.start)..self.actual_end(w.end))
    }

    fn vertex_history(&self, v: VID) -> Vec<i64> {
        self.graph
            .vertex_history_window(v, self.t_start..self.t_end)
    }

    fn vertex_history_window(&self, v: VID, w: Range<i64>) -> Vec<i64> {
        self.graph
            .vertex_history_window(v, self.actual_start(w.start)..self.actual_end(w.end))
    }

    fn edge_t(&self, e: EdgeRef) -> BoxedIter<EdgeRef> {
        self.graph.edge_window_t(e, self.t_start..self.t_end)
    }

    fn edge_window_t(&self, e: EdgeRef, w: Range<i64>) -> BoxedIter<EdgeRef> {
        self.graph
            .edge_window_t(e, self.actual_start(w.start)..self.actual_end(w.end))
    }

    fn edge_earliest_time(&self, e: EdgeRef) -> Option<i64> {
        self.graph
            .edge_earliest_time_window(e, self.t_start..self.t_end)
    }

    fn edge_earliest_time_window(&self, e: EdgeRef, w: Range<i64>) -> Option<i64> {
        self.graph
            .edge_earliest_time_window(e, self.actual_start(w.start)..self.actual_end(w.end))
    }

    fn edge_latest_time(&self, e: EdgeRef) -> Option<i64> {
        self.graph
            .edge_latest_time_window(e, self.t_start..self.t_end)
    }

    fn edge_latest_time_window(&self, e: EdgeRef, w: Range<i64>) -> Option<i64> {
        self.graph
            .edge_latest_time_window(e, self.actual_start(w.start)..self.actual_end(w.end))
    }

    fn edge_deletion_history(&self, e: EdgeRef) -> Vec<i64> {
        self.graph
            .edge_deletion_history_window(e, self.t_start..self.t_end)
    }

    fn edge_deletion_history_window(&self, e: EdgeRef, w: Range<i64>) -> Vec<i64> {
        self.graph
            .edge_deletion_history_window(e, self.actual_start(w.start)..self.actual_end(w.end))
    }

    fn temporal_prop_vec(&self, name: &str) -> Vec<(i64, Prop)> {
        self.graph
            .temporal_prop_vec_window(name, self.t_start, self.t_end)
    }

    fn temporal_prop_vec_window(&self, name: &str, t_start: i64, t_end: i64) -> Vec<(i64, Prop)> {
        self.graph.temporal_prop_vec_window(
            name,
            self.actual_start(t_start),
            self.actual_end(t_end),
        )
    }

    fn temporal_vertex_prop_vec(&self, v: VID, name: &str) -> Vec<(i64, Prop)> {
        self.graph
            .temporal_vertex_prop_vec_window(v, name, self.t_start, self.t_end)
    }

    fn temporal_vertex_prop_vec_window(
        &self,
        v: VID,
        name: &str,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph.temporal_vertex_prop_vec_window(
            v,
            name,
            self.actual_start(t_start),
            self.actual_end(t_end),
        )
    }

    fn temporal_edge_prop_vec_window(
        &self,
        e: EdgeRef,
        name: &str,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph.temporal_edge_prop_vec_window(
            e,
            name,
            self.actual_start(t_start),
            self.actual_end(t_end),
        )
    }

    fn temporal_edge_prop_vec(&self, e: EdgeRef, name: &str) -> Vec<(i64, Prop)> {
        self.graph
            .temporal_edge_prop_vec_window(e, name, self.t_start, self.t_end)
    }
}

/// Implementation of the GraphViewInternalOps trait for WindowedGraph.
/// This trait provides operations to a `WindowedGraph` used internally by the `GraphWindowSet`.
/// *Note: All functions in this are bound by the time set in the windowed graph.
impl<G: GraphViewOps> GraphOps for WindowedGraph<G> {

    fn find_edge_id(&self, e_id: EID) -> Option<EdgeRef> {
        let e_ref = self.graph.find_edge_id(e_id)?;
        if self.graph.include_edge_window(e_ref, self.t_start .. self.t_end) {
            Some(e_ref)
        } else {
            None
        }
    }

    fn local_vertex_ref(&self, v: VertexRef) -> Option<VID> {
        self.graph
            .local_vertex_ref_window(v, self.t_start, self.t_end)
    }

    fn get_unique_layers_internal(&self) -> Vec<usize> {
        self.graph.get_unique_layers_internal()
    }

    fn get_layer_id(&self, key: Option<&str>) -> Option<usize> {
        self.graph.get_layer_id(key)
    }

    /// Returns the number of vertices in the windowed view.
    fn vertices_len(&self) -> usize {
        self.graph.vertices_len_window(self.t_start, self.t_end)
    }

    /// Returns the number of edges in the windowed view.
    fn edges_len(&self, layer: Option<usize>) -> usize {
        self.graph.edges_len_window(self.t_start, self.t_end, layer)
    }

    /// Check if there is an edge from src to dst in the window.
    ///
    /// # Arguments
    ///
    /// - `src` - The source vertex.
    /// - `dst` - The destination vertex.
    ///
    /// # Returns
    ///
    /// A result containing `true` if there is an edge from src to dst in the window, `false` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if either `src` or `dst` is not a valid vertex.
    fn has_edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> bool {
        self.graph
            .has_edge_ref_window(src, dst, self.t_start, self.t_end, layer)
    }

    /// Check if a vertex v exists in the window.
    ///
    /// # Arguments
    ///
    /// - `v` - The vertex to check.
    ///
    /// # Returns
    ///
    /// A result containing `true` if the vertex exists in the window, `false` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if `v` is not a valid vertex.
    fn has_vertex_ref(&self, v: VertexRef) -> bool {
        self.graph
            .has_vertex_ref_window(v, self.t_start, self.t_end)
    }

    /// Returns the number of edges from a vertex in the window.
    ///
    /// # Arguments
    ///
    /// - `v` - The vertex to check.
    /// - `d` - The direction of the edges to count.
    ///
    /// # Returns
    ///
    /// A result containing the number of edges from the vertex in the window.
    ///
    /// # Errors
    ///
    /// Returns an error if `v` is not a valid vertex.
    fn degree(&self, v: VID, d: Direction, layer: Option<usize>) -> usize {
        self.graph
            .degree_window(v, self.t_start, self.t_end, d, layer)
    }

    /// Get the reference of the vertex with ID v if it exists
    ///
    /// # Arguments
    ///
    /// - `v` - The ID of the vertex to get
    ///
    /// # Returns
    ///
    /// A result of an option containing the vertex reference if it exists, `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if `v` is not a valid vertex.
    fn vertex_ref(&self, v: u64) -> Option<VID> {
        self.graph.vertex_ref_window(v, self.t_start, self.t_end)
    }

    /// Get an iterator over the references of all vertices as references
    ///
    /// # Returns
    ///
    /// An iterator over the references of all vertices
    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VID> + Send> {
        self.graph.vertex_refs_window(self.t_start, self.t_end)
    }

    /// Get an iterator over the references of an edges as a reference
    ///
    /// # Arguments
    ///
    /// - `src` - The source vertex of the edge
    /// - `dst` - The destination vertex of the edge
    ///
    /// # Returns
    ///
    /// A result of an option containing the edge reference if it exists, `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if `src` or `dst` are not valid vertices.
    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> Option<EdgeRef> {
        self.graph
            .edge_ref_window(src, dst, self.t_start, self.t_end, layer)
    }

    /// Get an iterator of all edges as references
    ///
    /// # Returns
    ///
    /// An iterator over all edges as references
    fn edge_refs(&self, layer: Option<usize>) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.graph.edge_refs_window(self.t_start, self.t_end, layer)
    }

    fn vertex_edges(
        &self,
        v: VID,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.graph
            .vertex_edges_window(v, self.t_start, self.t_end, d, layer)
    }

    /// Get the neighbours of a vertex as references in a given direction
    ///
    /// # Arguments
    ///
    /// - `v` - The vertex to get the neighbours for
    /// - `d` - The direction of the edges
    ///
    /// # Returns
    ///
    /// An iterator over all neighbours in that vertex direction as references
    fn neighbours(
        &self,
        v: VID,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph
            .neighbours_window(v, self.t_start, self.t_end, d, layer)
    }
}

/// A windowed graph is a graph that only allows access to vertices and edges within a time window.
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
/// graph.add_edge(0, 1, 2, [], None).unwrap();
/// graph.add_edge(1, 2, 3, [], None).unwrap();
/// let windowed_graph = graph.window(0, 1);
/// ```
impl<G: GraphViewOps> WindowedGraph<G> {
    /// Create a new windowed graph
    ///
    /// # Arguments
    ///
    /// - `graph` - The graph to create the windowed graph from
    /// - `t_start` - The inclusive start time of the window.
    /// - `t_end` - The exclusive end time of the window.
    ///
    /// # Returns
    ///
    /// A new windowed graph
    pub fn new<T: IntoTime>(graph: G, t_start: T, t_end: T) -> Self {
        WindowedGraph {
            graph,
            t_start: t_start.into_time(),
            t_end: t_end.into_time(),
        }
    }

    /// the larger of `t_start` and `self.start()` (useful for creating nested windows)
    fn actual_start(&self, t_start: i64) -> i64 {
        max(t_start, self.t_start)
    }

    /// the smaller of `t_end` and `self.end()` (useful for creating nested windows)
    fn actual_end(&self, t_end: i64) -> i64 {
        min(t_end, self.t_end)
    }
}

#[cfg(test)]
mod views_test {

    use super::*;
    use crate::prelude::*;
    use itertools::Itertools;
    use quickcheck::TestResult;
    use rand::prelude::*;
    use rayon::prelude::*;

    #[test]
    fn windowed_graph_vertices_degree() {
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
            g.add_edge(*t, *src, *dst, [], None).unwrap();
        }

        let wg = WindowedGraph::new(g, -1, 1);

        let actual = wg
            .vertices()
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
            g.add_edge(t, src, dst, [], None).unwrap();
        }

        let wg = g.window(i64::MIN, i64::MAX);
        assert_eq!(wg.edge(1, 3, None).unwrap().src().id(), 1);
        assert_eq!(wg.edge(1, 3, None).unwrap().dst().id(), 3);
    }

    #[test]
    fn windowed_graph_vertex_edges() {
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
            g.add_edge(*t, *src, *dst, [], None).unwrap();
        }

        let wg = WindowedGraph::new(g, -1, 1);

        assert_eq!(wg.vertex(1).unwrap().id(), 1);
    }

    #[test]
    fn graph_has_vertex_check_fail() {
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
            g.add_vertex(*t, *v, [])
                .map_err(|err| println!("{:?}", err))
                .ok();
        }

        let wg = WindowedGraph::new(g, 1, 2);
        assert!(!wg.has_vertex(262))
    }

    #[quickcheck]
    fn windowed_graph_has_vertex(mut vs: Vec<(i64, u64)>) -> TestResult {
        if vs.is_empty() {
            return TestResult::discard();
        }

        vs.sort_by_key(|v| v.1); // Sorted by vertex
        vs.dedup_by_key(|v| v.1); // Have each vertex only once to avoid headaches
        vs.sort_by_key(|v| v.0); // Sorted by time

        let rand_start_index = thread_rng().gen_range(0..vs.len());
        let rand_end_index = thread_rng().gen_range(rand_start_index..vs.len());

        let g = Graph::new();

        for (t, v) in &vs {
            g.add_vertex(*t, *v, [])
                .map_err(|err| println!("{:?}", err))
                .ok();
        }

        let start = vs.get(rand_start_index).expect("start index in range").0;
        let end = vs.get(rand_end_index).expect("end index in range").0;

        let wg = WindowedGraph::new(g, start, end);

        let rand_test_index: usize = thread_rng().gen_range(0..vs.len());

        let (i, v) = vs.get(rand_test_index).expect("test index in range");
        if (start..end).contains(i) {
            if wg.has_vertex(*v) {
                TestResult::passed()
            } else {
                TestResult::error(format!(
                    "Vertex {:?} was not in window {:?}",
                    (i, v),
                    start..end
                ))
            }
        } else if !wg.has_vertex(*v) {
            TestResult::passed()
        } else {
            TestResult::error(format!(
                "Vertex {:?} was in window {:?}",
                (i, v),
                start..end
            ))
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
            g.add_edge(*t, e.0, e.1, [], None).unwrap();
        }

        let start = edges.get(rand_start_index).expect("start index in range").0;
        let end = edges.get(rand_end_index).expect("end index in range").0;

        let wg = WindowedGraph::new(g, start, end);

        let rand_test_index: usize = thread_rng().gen_range(0..edges.len());

        let (i, e) = edges.get(rand_test_index).expect("test index in range");
        if (start..end).contains(i) {
            if wg.has_edge(e.0, e.1, None) {
                TestResult::passed()
            } else {
                TestResult::error(format!(
                    "Edge {:?} was not in window {:?}",
                    (i, e),
                    start..end
                ))
            }
        } else if !wg.has_edge(e.0, e.1, None) {
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

        let wg = WindowedGraph::new(g, window.start, window.end);
        if wg.num_edges() != true_edge_count {
            println!(
                "failed, g.num_edges() = {}, true count = {}",
                wg.num_edges(),
                true_edge_count
            );
            println!("g.edges() = {:?}", wg.edges().collect_vec());
        }
        TestResult::from_bool(wg.num_edges() == true_edge_count)
    }

    #[quickcheck]
    fn trivial_window_has_all_edges(edges: Vec<(i64, u64, u64)>) -> bool {
        let g = Graph::new();
        edges
            .into_par_iter()
            .filter(|e| e.0 < i64::MAX)
            .for_each(|(t, src, dst)| {
                g.add_edge(t, src, dst, [("test".to_owned(), Prop::Bool(true))], None)
                    .unwrap()
            });
        let w = g.window(i64::MIN, i64::MAX);
        g.edges()
            .all(|e| w.has_edge(e.src().id(), e.dst().id(), None))
    }

    #[quickcheck]
    fn large_vertex_in_window(dsts: Vec<u64>) -> bool {
        let dsts: Vec<u64> = dsts.into_iter().unique().collect();
        let n = dsts.len();
        let g = Graph::new();

        for dst in dsts {
            let t = 1;
            g.add_edge(t, 0, dst, [], None).unwrap();
        }
        let w = g.window(i64::MIN, i64::MAX);
        w.num_edges() == n
    }

    #[test]
    fn windowed_graph_vertex_ids() {
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
            g.add_edge(*t, *src, *dst, [], None).unwrap();
        }

        let res: Vec<_> = (0..=3)
            .map(|i| {
                let wg = g.window(args[i].0, args[i].1);
                let mut e = wg.vertices().id().collect::<Vec<_>>();
                e.sort();
                e
            })
            .collect_vec();

        assert_eq!(res, expected);

        let g = Graph::new();
        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, [], None).unwrap();
        }
        let res: Vec<_> = (0..=3)
            .map(|i| {
                let wg = g.window(args[i].0, args[i].1);
                let mut e = wg.vertices().id().collect::<Vec<_>>();
                e.sort();
                e
            })
            .collect_vec();
        assert_eq!(res, expected);
    }

    #[test]
    fn windowed_graph_vertices() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = Graph::new();

        g.add_vertex(
            0,
            1,
            [
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(99.5)),
            ],
        )
        .map_err(|err| println!("{:?}", err))
        .ok();

        g.add_vertex(
            -1,
            2,
            [
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(10.0)),
            ],
        )
        .map_err(|err| println!("{:?}", err))
        .ok();

        g.add_vertex(
            6,
            3,
            [
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(76.2)),
            ],
        )
        .map_err(|err| println!("{:?}", err))
        .ok();

        for (t, src, dst) in &vs {
            g.add_edge(
                *t,
                *src,
                *dst,
                [("eprop".into(), Prop::Str("commons".into()))],
                None,
            )
            .unwrap();
        }

        let wg = g.window(-2, 0);

        let actual = wg.vertices().id().collect::<Vec<_>>();

        let expected = vec![1, 2];

        assert_eq!(actual, expected);

        // Check results from multiple graphs with different number of shards
        let g = Graph::new();

        g.add_vertex(
            0,
            1,
            [
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(99.5)),
            ],
        )
        .map_err(|err| println!("{:?}", err))
        .ok();

        g.add_vertex(
            -1,
            2,
            [
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(10.0)),
            ],
        )
        .map_err(|err| println!("{:?}", err))
        .ok();

        g.add_vertex(
            6,
            3,
            [
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(76.2)),
            ],
        )
        .map_err(|err| println!("{:?}", err))
        .ok();

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, [], None).unwrap();
        }

        let expected = wg.vertices().id().collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }
}
