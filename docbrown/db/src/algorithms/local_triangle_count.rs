use crate::graph_window::WindowedGraph;
use crate::view_api::*;
use docbrown_core::Direction;
use itertools::Itertools;

pub fn local_triangle_count<G: GraphViewOps>(graph: &G, v: u64) -> usize {
    let vertex = graph.vertex(v).unwrap();

    let count = if vertex.degree() >= 2 {
        vertex
            .neighbours()
            .id()
            .combinations(2)
            .filter(|nb| graph.has_edge(nb[0], nb[1]) || graph.has_edge(nb[1], nb[0]))
            .count()
    } else {
        0
    };
    count
}

#[cfg(test)]
mod triangle_count_tests {

    use crate::graph::Graph;

    use super::local_triangle_count;

    #[test]
    fn counts_triangles() {
        let g = Graph::new(1);
        let vs = vec![(1, 1, 2), (2, 1, 3), (3, 2, 1), (4, 3, 2)];

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let windowed_graph = g.window(0, 5);
        let expected = vec![(1), (1), (1)];

        let actual = (1..=3)
            .map(|v| local_triangle_count(&windowed_graph, v))
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }
}
