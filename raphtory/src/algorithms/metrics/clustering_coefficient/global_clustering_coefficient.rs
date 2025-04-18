use crate::{
    algorithms::motifs::{triangle_count::triangle_count, triplet_count::triplet_count},
    db::api::view::StaticGraphViewOps,
};

/// Computes the global clustering coefficient of a graph. The global clustering coefficient is
/// defined as the number of triangles in the graph divided by the number of triplets in the graph.
///
/// # Arguments
///
/// - `g` - A reference to the graph
///
/// # Returns
///
/// The global clustering coefficient of the graph
///
/// # Example
///
/// ```rust
/// use raphtory::algorithms::metrics::clustering_coefficient::global_clustering_coefficient::global_clustering_coefficient;
/// use raphtory::prelude::*;
/// let graph = Graph::new();
///  let edges = vec![
///      (1, 2),
///      (1, 3),
///      (1, 4),
///      (2, 1),
///      (2, 6),
///      (2, 7),
///  ];
///  for (src, dst) in edges {
///      graph.add_edge(0, src, dst, NO_PROPS, None).expect("Unable to add edge");
///  }
///  let results = global_clustering_coefficient(&graph.at(1));
///  println!("global_clustering_coefficient: {}", results);
/// ```
///
pub fn global_clustering_coefficient<G: StaticGraphViewOps>(g: &G) -> f64 {
    let tc_val = triangle_count(g, None);
    let output = triplet_count(g, None);

    if output == 0 || tc_val == 0 {
        0.0
    } else {
        3.0 * tc_val as f64 / output as f64
    }
}

#[cfg(test)]
mod cc_test {
    use super::*;
    use crate::{
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::NO_PROPS,
        test_storage,
    };
    use pretty_assertions::assert_eq;

    /// Test the global clustering coefficient
    #[test]
    fn test_global_cc() {
        let graph = Graph::new();

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
            graph.add_edge(0, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let results = global_clustering_coefficient(graph);
            assert_eq!(results, 0.3);
        });
    }
}
