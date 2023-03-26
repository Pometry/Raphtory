use crate::view_api::*;
use itertools::Itertools;
use rayon::prelude::*;

pub fn global_triangle_count<G: GraphViewOps>(graph: &G) -> usize {
    let count: usize = graph
        .vertices()
        .into_iter()
        .par_bridge()
        .map(|v| {
            v.neighbours()
                .id()
                .into_iter()
                .combinations(2)
                .filter(|nb| graph.has_edge(nb[0], nb[1]) || graph.has_edge(nb[1], nb[0]))
                .count()
        })
        .sum();
    count / 3
}

#[cfg(test)]
mod triangle_count_tests {

    use crate::graph::Graph;

    use super::global_triangle_count;

    #[test]
    fn counts_triangles() {
        let g = Graph::new(1);
        let vs = vec![(1, 1, 2), (2, 1, 3), (3, 2, 1), (4, 3, 2)];

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let windowed_graph = g.window(0, 5);
        let expected = 1;

        let actual = global_triangle_count(&windowed_graph);

        assert_eq!(actual, expected);
    }
}
