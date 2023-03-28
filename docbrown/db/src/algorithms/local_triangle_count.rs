use crate::view_api::*;
use docbrown_core::tgraph_shard::errors::GraphError;
use itertools::Itertools;

pub fn local_triangle_count<G: GraphViewOps>(graph: &G, v: u64) -> Result<usize, GraphError> {
    let vertex = graph.vertex(v)?.unwrap();

    let count = if vertex.degree()? >= 2 {
        let r: Result<Vec<_>, _> = vertex
            .neighbours()
            .id()
            .into_iter()
            .combinations(2)
            .filter_map(|nb| match graph.has_edge(nb[0], nb[1]) {
                Ok(true) => Some(Ok(nb)),
                Ok(false) => match graph.has_edge(nb[1], nb[0]) {
                    Ok(true) => Some(Ok(nb)),
                    Ok(false) => None,
                    Err(e) => Some(Err(e)),
                },
                Err(e) => Some(Err(e)),
            })
            .collect();

        r.map(|t| t.len())?
    } else {
        0
    };

    Ok(count)
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
            .map(|v| local_triangle_count(&windowed_graph, v).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }
}
