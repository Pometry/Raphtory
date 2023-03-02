use crate::local_triangle_count::local_triangle_count;
use docbrown_db::{graph::Graph, graph_window::WindowedGraph};

pub fn local_clustering_coefficient(graph: &Graph, v: u64, t_start: i64, t_end: i64) -> u32 {
    let windowed_graph = graph.window(t_start, t_end);
    let vertex = windowed_graph.vertex(v).unwrap();

    let triangle_count = local_triangle_count(graph, v, t_start, t_end);
    let degree = vertex.degree() as u32;
    if degree > 1 {
        (2 * triangle_count) / (degree * (degree - 1))
    } else {
        0
    }
}

#[cfg(test)]
mod clustering_coefficient_tests {

    use docbrown_db::graph::Graph;

    use super::local_clustering_coefficient;

    #[test]
    fn clusters_of_triangles() {
        let g = Graph::new(1);
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

        let expected = vec![1, 5, 5, 5, 0];

        let actual = (1..=5)
            .map(|v| local_clustering_coefficient(&g, v, 1, 7))
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }
}
