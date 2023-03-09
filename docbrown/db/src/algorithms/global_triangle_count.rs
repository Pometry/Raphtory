use crate::graph_window::WindowedGraph;
use docbrown_core::Direction;
use itertools::Itertools;
use rayon::prelude::*;

pub fn global_triangle_count(windowed_graph: &WindowedGraph) -> usize {
   
    let vertex_ids = windowed_graph.vertex_ids().collect::<Vec<_>>();

    vertex_ids.into_par_iter().map(|v| {
         windowed_graph.neighbours_ids(v, Direction::BOTH)
            .combinations(2)
            .map(|nb| {
                if windowed_graph.has_edge(nb[0], nb[1]) || (windowed_graph.has_edge(nb[1], nb[0]))
                {
                    1 as usize
                } else {
                    0 as usize
                }
            }).sum::<usize>()
        }).sum()
       
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
        let expected = 3;

        let actual = global_triangle_count(&windowed_graph);

        assert_eq!(actual, expected);
    }
}
