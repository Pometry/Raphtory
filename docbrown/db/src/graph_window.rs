use crate::graph::Graph;
use docbrown_core::{
    tgraph_shard::{TEdge, TVertex},
    Direction,
};

use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct WindowedGraph {
    pub(crate) graph: Arc<Graph>,
    pub t_start: i64,
    pub t_end: i64,
}

impl WindowedGraph {
    pub fn new(graph: Arc<Graph>, t_start: i64, t_end: i64) -> Self {
        WindowedGraph {
            graph,
            t_start,
            t_end,
        }
    }

    pub fn has_vertex(&self, v: u64) -> bool {
        self.graph.has_vertex_window(v, self.t_start, self.t_end)
    }

    pub fn vertex(&self, v: u64) -> Option<WindowedVertex> {
        let graph_w = self.clone();
        self.graph
            .vertex_window(v, self.t_start, self.t_end)
            .map(|tv| WindowedVertex::from(tv, Arc::new(graph_w.clone())))
    }

    pub fn vertex_ids(&self) -> Box<dyn Iterator<Item = u64> + Send> {
        self.graph.vertex_ids_window(self.t_start, self.t_end)
    }

    pub fn vertices(&self) -> Box<dyn Iterator<Item = WindowedVertex> + Send> {
        let graph_w = self.clone();
        Box::new(
            self.graph
                .vertices_window(self.t_start, self.t_end)
                .map(move |tv| WindowedVertex::from(tv, Arc::new(graph_w.clone()))),
        )
    }
}

pub struct WindowedVertex {
    pub g_id: u64,
    pub graph_w: Arc<WindowedGraph>,
}

impl WindowedVertex {
    fn from(value: TVertex, graph_w: Arc<WindowedGraph>) -> Self {
        Self {
            g_id: value.g_id,
            graph_w,
        }
    }
}

impl WindowedVertex {
    pub fn degree(&self) -> usize {
        self.graph_w.graph.degree_window(
            self.g_id,
            self.graph_w.t_start,
            self.graph_w.t_end,
            Direction::BOTH,
        )
    }

    pub fn in_degree(&self) -> usize {
        self.graph_w.graph.degree_window(
            self.g_id,
            self.graph_w.t_start,
            self.graph_w.t_end,
            Direction::IN,
        )
    }

    pub fn out_degree(&self) -> usize {
        self.graph_w.graph.degree_window(
            self.g_id,
            self.graph_w.t_start,
            self.graph_w.t_end,
            Direction::OUT,
        )
    }

    pub fn neighbours(&self) -> Box<dyn Iterator<Item = TEdge> + Send> {
        self.graph_w.graph.neighbours_window(
            self.g_id,
            self.graph_w.t_start,
            self.graph_w.t_end,
            Direction::BOTH,
        )
    }

    pub fn in_neighbours(&self) -> Box<dyn Iterator<Item = TEdge> + Send> {
        self.graph_w.graph.neighbours_window(
            self.g_id,
            self.graph_w.t_start,
            self.graph_w.t_end,
            Direction::IN,
        )
    }

    pub fn out_neighbours(&self) -> Box<dyn Iterator<Item = TEdge> + Send> {
        self.graph_w.graph.neighbours_window(
            self.g_id,
            self.graph_w.t_start,
            self.graph_w.t_end,
            Direction::OUT,
        )
    }
}

#[cfg(test)]
mod views_test {

    use std::collections::HashMap;

    use docbrown_core::Prop;
    use itertools::Itertools;
    use quickcheck::TestResult;
    use rand::Rng;

    use super::WindowedGraph;
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

    #[quickcheck]
    fn windowed_graph_has_vertex(mut vs: Vec<(i64, u64)>) -> TestResult {
        if vs.is_empty() {
            return TestResult::discard();
        }

        let g = Graph::new(2);

        for (t, v) in &vs {
            g.add_vertex(*t, *v, &vec![]);
        }

        vs.sort(); // Sorted by time
        vs.dedup();

        let rand_start_index = rand::thread_rng().gen_range(0..vs.len());
        let rand_end_index = rand::thread_rng().gen_range(0..vs.len());

        if rand_end_index < rand_start_index {
            return TestResult::discard();
        }

        let g = Graph::new(2);

        for (t, v) in &vs {
            g.add_vertex(*t, *v, &vec![]);
        }

        let start = vs.get(rand_start_index).unwrap().0;
        let end = vs.get(rand_end_index).unwrap().0;

        let wg = WindowedGraph::new(g.into(), start, end);
        if start == end {
            let v = vs.get(rand_start_index).unwrap().1;
            return TestResult::from_bool(!wg.has_vertex(v));
        }

        if rand_start_index == rand_end_index {
            let v = vs.get(rand_start_index).unwrap().1;
            return TestResult::from_bool(!wg.has_vertex(v));
        }

        let rand_index_within_rand_start_end: usize =
            rand::thread_rng().gen_range(rand_start_index..rand_end_index);

        let (i, v) = vs.get(rand_index_within_rand_start_end).unwrap();

        if *i == end {
            return TestResult::from_bool(!wg.has_vertex(*v));
        } else {
            return TestResult::from_bool(wg.has_vertex(*v));
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
