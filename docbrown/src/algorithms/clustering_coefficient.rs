use crate::algorithms::triangle_count::{TriangleCountS1, TriangleCountS2};
use crate::algorithms::triplet_count::TripletCount;
use crate::db::program::{GlobalEvalState, Program};
use crate::db::view_api::GraphViewOps;

/// Computes the global clustering coefficient of a graph. The global clustering coefficient is
/// defined as the number of triangles in the graph divided by the number of triplets in the graph.
///
/// # Arguments
///
/// * `g` - A reference to the graph
///
/// # Returns
///
/// The global clustering coefficient of the graph
///
/// # Example
///
/// ```rust
/// use docbrown::db::graph::Graph;
/// use docbrown::algorithms::clustering_coefficient::clustering_coefficient;
/// use docbrown::db::view_api::*;
/// let graph = Graph::new(2);
///  let edges = vec![
///      (1, 2),
///      (1, 3),
///      (1, 4),
///      (2, 1),
///      (2, 6),
///      (2, 7),
///  ];
///  for (src, dst) in edges {
///      graph.add_edge(0, src, dst, &vec![], None).expect("Unable to add edge");
///  }
///  let results = clustering_coefficient(&graph.at(1));
///  println!("global_clustering_coefficient: {}", results);
/// ```
///
pub fn clustering_coefficient<G: GraphViewOps>(g: &G) -> f64 {
    let mut gs = GlobalEvalState::new(g.clone(), false);
    let tc = TriangleCountS1 {};
    tc.run_step(g, &mut gs);
    let tc = TriangleCountS2 {};
    tc.run_step(g, &mut gs);
    let tc_val = tc.produce_output(g, &gs).unwrap_or(0);

    let mut gss = GlobalEvalState::new(g.clone(), false);
    let triplets = TripletCount {};
    triplets.run_step(g, &mut gss);
    let output = triplets.produce_output(g, &gss);

    if output == 0 || tc_val == 0 {
        0.0
    } else {
        3.0 * tc_val as f64 / output as f64
    }
}

#[cfg(test)]
mod cc_test {
    use super::*;
    use crate::db::graph::Graph;
    use crate::db::view_api::*;
    use pretty_assertions::assert_eq;

    /// Test the global clustering coefficient
    #[test]
    fn test_global_cc() {
        let graph = Graph::new(1);

        // Graph has 2 triangles and 20 triplets
        let edges = vec![
            (1, 2),
            (1, 3),
            (1, 4),
            (2, 1),
            (2, 6),
            (2, 7),
            (3, 1),
            (3, 4),
            (3, 7),
            (4, 1),
            (4, 3),
            (4, 5),
            (4, 6),
            (5, 4),
            (5, 6),
            (6, 4),
            (6, 5),
            (6, 2),
            (7, 2),
            (7, 3),
        ];

        for (src, dst) in edges {
            graph.add_edge(0, src, dst, &vec![], None);
        }

        let graph_at = graph.at(1);

        let exp_tri_count = 2.0;
        let exp_triplet_count = 20;

        let results = clustering_coefficient(&graph_at);
        let expected = 3.0 * exp_tri_count / exp_triplet_count as f64;
        assert_eq!(results, 0.3);
    }
}
