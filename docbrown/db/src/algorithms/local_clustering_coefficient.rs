use docbrown_core::tgraph_shard::errors::GraphError;

use crate::algorithms::local_triangle_count::local_triangle_count;
use crate::view_api::*;

pub fn local_clustering_coefficient<G: GraphViewOps>(graph: &G, v: u64) -> Result<f32, GraphError> {
    let vertex = graph.vertex(v)?.unwrap();

    let triangle_count = local_triangle_count(graph, v)? as f32;

    let degree = vertex.degree()? as f32;
    if degree > 1.0 {
        Ok((2.0 * triangle_count) / (degree * (degree - 1.0) as f32))
    } else {
        Ok(0.0)
    }
}

#[cfg(test)]
mod clustering_coefficient_tests {
    use super::local_clustering_coefficient;
    use crate::graph::Graph;

    #[test]
    fn clusters_of_triangles() {
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

        let expected = vec![0.33333334, 1.0, 1.0, 0.0, 0.0];

        let actual = (1..=5)
            .map(|v| local_clustering_coefficient(&windowed_graph, v).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }
}
