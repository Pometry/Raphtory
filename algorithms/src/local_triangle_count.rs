use docbrown_core::Direction;
use docbrown_db::{graph::Graph, graph_window::WindowedGraph};
use itertools::Itertools;
use std::error::Error;

pub(crate) fn local_triangle_count(graph: &Graph, v: u64, t_start: i64, t_end: i64) -> u32 {
    let mut number_of_triangles: u32 = 0;
    let windowed_graph = graph.window(t_start, t_end);
    let vertex = windowed_graph.vertex(v).unwrap();

    if graph.window(t_start, t_end).has_vertex(v) && vertex.degree() >= 2 {
        graph
            .window(t_start, t_end)
            .vertex_ids()
            .combinations(2)
            .for_each(|v| {
                if graph.has_edge(v[0], v[1]) || (graph.has_edge(v[1], v[0])) {
                    number_of_triangles += 1;
                }
            })
    }

    number_of_triangles
}

#[cfg(test)]
mod triangle_count_tests {

    use docbrown_db::graph::Graph;

    use super::local_triangle_count;

    #[test]
    fn counts_triangles() {
        let g = Graph::new(1);
        let vs = vec![(1, 1, 2), (2, 1, 3), (3, 2, 1), (4, 3, 2)];

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let expected = vec![(3), (3), (3)];

        let actual = (1..=3)
            .map(|v| local_triangle_count(&g, v, 1, 5))
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }
}
