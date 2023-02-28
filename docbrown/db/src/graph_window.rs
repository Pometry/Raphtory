use crate::graph::Graph;
use docbrown_core::tgraph_shard::TVertex;

use std::sync::Arc;

pub struct WindowedGraph {
    gdb: Arc<Graph>,
    pub t_start: i64,
    pub t_end: i64,
}

impl WindowedGraph {
    pub fn new(gdb: Arc<Graph>, t_start: i64, t_end: i64) -> Self {
        WindowedGraph {
            gdb,
            t_start,
            t_end,
        }
    }

    pub fn vertex_ids(&self) -> Box<dyn Iterator<Item = u64> + Send> {
        self.gdb.vertex_ids_window(self.t_start, self.t_end)
    }

    pub fn vertices(&self) -> Box<dyn Iterator<Item = TVertex> + Send> {
        self.gdb.vertices_window(self.t_start, self.t_end)
    }
}

#[cfg(test)]
mod views_test {
    use super::WindowedGraph;
    use crate::graph::Graph;

    #[test]
    fn get_vertex_ids() {
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

        let wg = WindowedGraph::new(g.into(), 0, 7);

        let mut vw = wg.vertex_ids().collect::<Vec<_>>();
        vw.sort();
        assert_eq!(vw, vec![1, 2, 3])
    }

    #[test]
    fn get_vertices() {
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

        let mut vw = wg.vertices().map(|tv| tv.g_id).collect::<Vec<_>>();
        vw.sort();
        assert_eq!(vw, vec![1, 2])
    }
}
