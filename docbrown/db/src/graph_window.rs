use crate::graph::Graph;
use crate::perspective::{Perspective, PerspectiveSet};
use docbrown_core::{
    tgraph::{EdgeView, VertexView},
    Direction, Prop,
};

use crate::view_api;
pub use crate::view_api::edge::EdgeListOps;
pub use crate::view_api::vertex::{VertexListOps, VertexViewOps};
use crate::view_api::GraphViewOps;
use std::{collections::HashMap, sync::Arc};

pub struct GraphWindowSet {
    graph: Graph,
    perspectives: Box<dyn Iterator<Item = Perspective> + Send>,
}

impl GraphWindowSet {
    pub fn new(
        graph: Graph,
        perspectives: Box<dyn Iterator<Item = Perspective> + Send>,
    ) -> GraphWindowSet {
        GraphWindowSet {
            graph,
            perspectives,
        }
    }
}

impl Iterator for GraphWindowSet {
    type Item = WindowedGraph;
    fn next(&mut self) -> Option<Self::Item> {
        let perspective = self.perspectives.next()?;
        Some(WindowedGraph {
            graph: self.graph.clone(),
            t_start: perspective.start.unwrap_or(i64::MIN),
            t_end: perspective.end.unwrap_or(i64::MAX),
        })
    }
}

#[derive(Debug, Clone)]
pub struct WindowedGraph {
    pub(crate) graph: Graph,
    pub t_start: i64, // inclusive
    pub t_end: i64,   // exclusive
}

impl WindowedGraph {
    pub fn new(graph: Graph, t_start: i64, t_end: i64) -> Self {
        WindowedGraph {
            graph,
            t_start,
            t_end,
        }
    }

    pub fn fold_par<S, F, F2>(&self, f: F, agg: F2) -> Option<S>
    where
        S: Send,
        F: Fn(VertexView) -> S + Send + Sync + Copy,
        F2: Fn(S, S) -> S + Sync + Send + Copy,
    {
        self.graph.fold_par(self.t_start, self.t_end, f, agg)
    }

    pub fn vertex_window_par<O, F>(&self, f: F) -> Box<dyn Iterator<Item = O>>
    where
        O: Send + 'static,
        F: Fn(VertexView) -> O + Send + Sync + Copy,
    {
        self.graph.vertex_window_par(self.t_start, self.t_end, f)
    }

    pub fn neighbours_ids(&self, v: u64, d: Direction) -> Box<dyn Iterator<Item = u64> + Send> {
        self.graph
            .neighbours_ids_window(v, self.t_start, self.t_end, d)
    }

    pub fn vertex_ids(&self) -> Box<dyn Iterator<Item = u64> + Send> {
        self.graph.vertex_ids_window(self.t_start, self.t_end)
    }
}

impl GraphViewOps for WindowedGraph {
    type Vertex = WindowedVertex;
    type Vertices = Box<dyn Iterator<Item = WindowedVertex> + Send>;
    type Edge = WindowedEdge;
    type Edges = Box<dyn Iterator<Item = WindowedEdge> + Send>;

    fn len(&self) -> usize {
        // FIXME: This needs Optimising badly
        self.vertices().count()
    }

    fn edges_len(&self) -> usize {
        // FIXME: This needs Optimising badly
        self.edges().count()
    }

    fn has_vertex(&self, v: u64) -> bool {
        self.graph.has_vertex_window(v, self.t_start, self.t_end)
    }

    fn has_edge(&self, src: u64, dst: u64) -> bool {
        self.graph
            .has_edge_window(src, dst, self.t_start, self.t_end)
    }

    fn vertex(&self, v: u64) -> Option<WindowedVertex> {
        let graph_w = self.clone();
        self.graph
            .vertex_window(v, self.t_start, self.t_end)
            .map(|vv| WindowedVertex::from(vv, Arc::new(graph_w.clone())))
    }

    fn vertices(&self) -> Self::Vertices {
        let graph_w = self.clone();
        Box::new(
            self.graph
                .vertices_window(self.t_start, self.t_end)
                .map(move |vv| WindowedVertex::from(vv, Arc::new(graph_w.clone()))),
        )
    }

    fn edge(&self, src: u64, dst: u64) -> Option<WindowedEdge> {
        let graph_w = self.clone();
        self.graph
            .edge_window(src, dst, self.t_start, self.t_end)
            .map(|ev| WindowedEdge::from(ev, Arc::new(graph_w.clone())))
    }

    fn edges(&self) -> Self::Edges {
        Box::new(self.vertices().flat_map(|v| v.out_edges()))
    }
}

pub struct WindowedVertex {
    pub g_id: u64,
    pub graph_w: Arc<WindowedGraph>,
}

impl WindowedVertex {
    fn from(value: VertexView, graph_w: Arc<WindowedGraph>) -> Self {
        Self {
            g_id: value.g_id,
            graph_w,
        }
    }
}

impl VertexListOps for Box<dyn Iterator<Item = WindowedVertex> + Send> {
    type Vertex = WindowedVertex;
    type Edge = WindowedEdge;
    type EList = Box<dyn Iterator<Item = WindowedEdge> + Send>;
    type IterType = Box<dyn Iterator<Item = WindowedVertex> + Send>;
    type ValueIterType<U> = Box<dyn Iterator<Item = U> + Send>;

    fn id(self) -> Self::ValueIterType<u64> {
        Box::new(self.map(|v| v.id()))
    }

    fn prop(self, name: String) -> Self::ValueIterType<Vec<(i64, Prop)>> {
        Box::new(self.map(move |v| v.prop(name.clone())))
    }

    fn props(self) -> Self::ValueIterType<HashMap<String, Vec<(i64, Prop)>>> {
        Box::new(self.map(|v| v.props()))
    }

    fn degree(self) -> Self::ValueIterType<usize> {
        Box::new(self.map(|v| v.degree()))
    }

    fn in_degree(self) -> Self::ValueIterType<usize> {
        Box::new(self.map(|v| v.in_degree()))
    }

    fn out_degree(self) -> Self::ValueIterType<usize> {
        Box::new(self.map(|v| v.out_degree()))
    }

    fn edges(self) -> Self::EList {
        Box::new(self.flat_map(|v| v.edges()))
    }

    fn in_edges(self) -> Self::EList {
        Box::new(self.flat_map(|v| v.in_edges()))
    }

    fn out_edges(self) -> Self::EList {
        Box::new(self.flat_map(|v| v.out_edges()))
    }

    fn neighbours(self) -> Self {
        Box::new(self.flat_map(|v| v.neighbours()))
    }

    fn in_neighbours(self) -> Self {
        Box::new(self.flat_map(|v| v.in_neighbours()))
    }

    fn out_neighbours(self) -> Self {
        Box::new(self.flat_map(|v| v.out_neighbours()))
    }
}

impl EdgeListOps for Box<dyn Iterator<Item = WindowedEdge> + Send> {
    type Vertex = WindowedVertex;
    type Edge = WindowedEdge;
    type IterType = Box<dyn Iterator<Item = WindowedEdge> + Send>;
}

impl VertexViewOps for WindowedVertex {
    type Edge = WindowedEdge;
    type VList = Box<dyn Iterator<Item = WindowedVertex> + Send>;
    type EList = Box<dyn Iterator<Item = WindowedEdge> + Send>;

    fn id(&self) -> u64 {
        self.g_id
    }

    fn prop(&self, name: String) -> Vec<(i64, Prop)> {
        self.graph_w.graph.vertex_prop_vec_window(
            self.g_id,
            name,
            self.graph_w.t_start..self.graph_w.t_end,
        )
    }

    fn props(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph_w
            .graph
            .vertex_props_window(self.g_id, self.graph_w.t_start..self.graph_w.t_end)
    }

    fn degree(&self) -> usize {
        self.graph_w.graph.degree_window(
            self.g_id,
            self.graph_w.t_start,
            self.graph_w.t_end,
            Direction::BOTH,
        )
    }

    fn in_degree(&self) -> usize {
        self.graph_w.graph.degree_window(
            self.g_id,
            self.graph_w.t_start,
            self.graph_w.t_end,
            Direction::IN,
        )
    }

    fn out_degree(&self) -> usize {
        self.graph_w.graph.degree_window(
            self.g_id,
            self.graph_w.t_start,
            self.graph_w.t_end,
            Direction::OUT,
        )
    }

    fn edges(&self) -> Box<dyn Iterator<Item = WindowedEdge> + Send> {
        let wg = self.graph_w.clone();
        Box::new(
            self.graph_w
                .graph
                .vertex_edges_window(
                    self.g_id,
                    self.graph_w.t_start,
                    self.graph_w.t_end,
                    Direction::BOTH,
                )
                .map(move |te| WindowedEdge::from(te, wg.clone())),
        )
    }

    fn in_edges(&self) -> Box<dyn Iterator<Item = WindowedEdge> + Send> {
        let wg = self.graph_w.clone();
        Box::new(
            self.graph_w
                .graph
                .vertex_edges_window(
                    self.g_id,
                    self.graph_w.t_start,
                    self.graph_w.t_end,
                    Direction::IN,
                )
                .map(move |te| WindowedEdge::from(te, wg.clone())),
        )
    }

    fn out_edges(&self) -> Box<dyn Iterator<Item = WindowedEdge> + Send> {
        let wg = self.graph_w.clone();
        Box::new(
            self.graph_w
                .graph
                .vertex_edges_window(
                    self.g_id,
                    self.graph_w.t_start,
                    self.graph_w.t_end,
                    Direction::OUT,
                )
                .map(move |te| WindowedEdge::from(te, wg.clone())),
        )
    }

    fn neighbours(&self) -> Self::VList {
        let wg = self.graph_w.clone();
        Box::new(
            self.graph_w
                .graph
                .neighbours_window(
                    self.g_id,
                    self.graph_w.t_start,
                    self.graph_w.t_end,
                    Direction::BOTH,
                )
                .map(move |tv| WindowedVertex::from(tv, wg.clone())),
        )
    }

    fn in_neighbours(&self) -> Self::VList {
        let wg = self.graph_w.clone();
        Box::new(
            self.graph_w
                .graph
                .neighbours_window(
                    self.g_id,
                    self.graph_w.t_start,
                    self.graph_w.t_end,
                    Direction::IN,
                )
                .map(move |tv| WindowedVertex::from(tv, wg.clone())),
        )
    }

    fn out_neighbours(&self) -> Box<dyn Iterator<Item = WindowedVertex> + Send> {
        let wg = self.graph_w.clone();
        Box::new(
            self.graph_w
                .graph
                .neighbours_window(
                    self.g_id,
                    self.graph_w.t_start,
                    self.graph_w.t_end,
                    Direction::OUT,
                )
                .map(move |tv| WindowedVertex::from(tv, wg.clone())),
        )
    }
}

pub struct WindowedEdge {
    pub edge_id: usize,
    pub src: u64,
    pub dst: u64,
    pub time: Option<i64>,
    pub is_remote: bool,
    pub graph_w: Arc<WindowedGraph>,
}

impl WindowedEdge {
    fn from(value: EdgeView, graph_w: Arc<WindowedGraph>) -> Self {
        Self {
            edge_id: value.edge_id,
            src: value.src_g_id,
            dst: value.dst_g_id,
            time: value.time,
            is_remote: value.is_remote,
            graph_w,
        }
    }
}

impl view_api::edge::EdgeViewOps for WindowedEdge {
    type Vertex = WindowedVertex;

    fn prop(&self, name: String) -> Vec<(i64, Prop)> {
        self.graph_w.graph.edge_props_vec_window(
            self.src,
            self.edge_id,
            name,
            self.graph_w.t_start..self.graph_w.t_end,
        )
    }

    fn src(&self) -> Self::Vertex {
        self.graph_w.vertex(self.src).expect("src should exist")
    }

    fn dst(&self) -> Self::Vertex {
        self.graph_w.vertex(self.dst).expect("dest should exist")
    }
}

#[cfg(test)]
mod views_test {

    use std::collections::HashMap;

    use docbrown_core::Prop;
    use itertools::Itertools;
    use quickcheck::TestResult;
    use rand::Rng;

    use super::*;
    use crate::graph::Graph;

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

        let g = Graph::new(2);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let wg = WindowedGraph::new(g.into(), -1, 1);

        let actual = wg
            .vertices()
            .map(|v| (v.g_id, v.degree()))
            .collect::<Vec<_>>();

        let expected = vec![(2, 1), (1, 2)];

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

        let g = Graph::new(2);

        for (t, src, dst) in vs {
            g.add_edge(t, src, dst, &vec![]);
        }

        let wg = g.window(i64::MIN, i64::MAX);
        assert_eq!(wg.edge(1, 3).unwrap().src, 1);
        assert_eq!(wg.edge(1, 3).unwrap().dst, 3);
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

        let g = Graph::new(2);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let wg = WindowedGraph::new(g.into(), -1, 1);

        let v = wg.vertex(1).unwrap();
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
        let g = Graph::new(2);

        for (t, v) in &vs {
            g.add_vertex(*t, *v, &vec![]);
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

        let rand_start_index = rand::thread_rng().gen_range(0..vs.len());
        let rand_end_index = rand::thread_rng().gen_range(rand_start_index..vs.len());

        let g = Graph::new(2);

        for (t, v) in &vs {
            g.add_vertex(*t, *v, &vec![]);
        }

        let start = vs.get(rand_start_index).expect("start index in range").0;
        let end = vs.get(rand_end_index).expect("end index in range").0;

        let wg = WindowedGraph::new(g, start, end);

        let rand_test_index: usize = rand::thread_rng().gen_range(0..vs.len());

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
        } else {
            if !wg.has_vertex(*v) {
                TestResult::passed()
            } else {
                TestResult::error(format!(
                    "Vertex {:?} was in window {:?}",
                    (i, v),
                    start..end
                ))
            }
        }
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

        let g = Graph::new(1);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let res: Vec<_> = (0..=3)
            .map(|i| {
                let wg = g.window(args[i].0, args[i].1);
                let mut e = wg.vertex_ids().collect::<Vec<_>>();
                e.sort();
                e
            })
            .collect_vec();

        assert_eq!(res, expected);

        let g = Graph::new(3);
        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }
        let res: Vec<_> = (0..=3)
            .map(|i| {
                let wg = g.window(args[i].0, args[i].1);
                let mut e = wg.vertex_ids().collect::<Vec<_>>();
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

        let g = Graph::new(1);

        g.add_vertex(
            0,
            1,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(99.5)),
            ],
        );

        g.add_vertex(
            -1,
            2,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(10.0)),
            ],
        );

        g.add_vertex(
            6,
            3,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(76.2)),
            ],
        );

        for (t, src, dst) in &vs {
            g.add_edge(
                *t,
                *src,
                *dst,
                &vec![("eprop".into(), Prop::Str("commons".into()))],
            );
        }

        let wg = g.window(-2, 0);

        let actual = wg.vertices().map(|tv| tv.g_id).collect::<Vec<_>>();

        let hm: HashMap<String, Vec<(i64, Prop)>> = HashMap::new();
        let expected = vec![1, 2];

        assert_eq!(actual, expected);

        // Check results from multiple graphs with different number of shards
        let g = Graph::new(10);

        g.add_vertex(
            0,
            1,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(99.5)),
            ],
        );

        g.add_vertex(
            -1,
            2,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(10.0)),
            ],
        );

        g.add_vertex(
            6,
            3,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(76.2)),
            ],
        );

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let expected = wg.vertices().map(|tv| tv.g_id).collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }
}
