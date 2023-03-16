use crate::graph_window::WindowedGraph;
use crate::view_api::*;

pub fn directed_graph_density<G: GraphViewOps>(graph: &G) -> f32 {
    graph.num_edges() as f32 / (graph.num_vertices() as f32 * (graph.num_vertices() as f32 - 1.0))
}

#[cfg(test)]
mod directed_graph_density_tests {
    use crate::graph::Graph;

    use super::directed_graph_density;

    #[test]
    fn low_graph_density() {
        let g = Graph::new(1);
        let windowed_graph = g.window(0, 7);
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (3, 2, 1),
            (4, 3, 2),
            (5, 1, 4),
            (6, 4, 5),
        ];

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let actual = directed_graph_density(&windowed_graph);
        let expected = 0.3;

        assert_eq!(actual, expected);
    }

    #[test]
    fn complete_graph_has_graph_density_of_one() {
        let g = Graph::new(1);
        let windowed_graph = g.window(0, 3);
        let vs = vec![(1, 1, 2), (2, 2, 1)];

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let actual = directed_graph_density(&windowed_graph);
        let expected = 1.0;

        assert_eq!(actual, expected);
    }
}
