use rustc_hash::FxHashSet;

use crate::algorithms::triplet_count::TripletCount;
use crate::core::state;
use crate::db::program::{GlobalEvalState, LocalState, Program};
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
/// use raphtory::db::graph::Graph;
/// use raphtory::algorithms::clustering_coefficient::clustering_coefficient;
/// use raphtory::db::view_api::*;
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

pub struct TriangleCountS1 {}

impl Program for TriangleCountS1 {
    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let neighbors_set = c.agg(state::def::hash_set(0));

        c.step(|s| {
            for t in s.neighbours() {
                if s.global_id() > t.global_id() {
                    t.update(&neighbors_set, s.global_id());
                }
            }
        });
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        let _ = c.agg(state::def::hash_set::<u64>(0));
        c.step(|_| false)
    }

    type Out = ();

    #[allow(unused_variables)]
    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
    }
}

pub struct TriangleCountS2 {}

impl Program for TriangleCountS2 {
    type Out = Option<usize>;
    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let neighbors_set = c.agg(state::def::hash_set::<u64>(0));
        let count = c.global_agg(state::def::sum::<usize>(1));

        c.step(|s| {
            for t in s.neighbours() {
                if s.global_id() > t.global_id() {
                    let intersection_count = {
                        // when using entry() we need to make sure the reference is released before we can update the state, otherwise we break the Rc<RefCell<_>> invariant
                        // where there can either be one mutable or many immutable references

                        match (
                            s.entry(&neighbors_set)
                                .read_ref()
                                .unwrap_or(&FxHashSet::default()),
                            t.entry(&neighbors_set)
                                .read_ref()
                                .unwrap_or(&FxHashSet::default()),
                        ) {
                            (s_set, t_set) => {
                                let intersection = s_set.intersection(t_set);
                                intersection.count()
                            }
                        }
                    };

                    s.global_update(&count, intersection_count);
                }
            }
        });
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        let _ = c.global_agg(state::def::sum::<usize>(1));
        c.step(|_| false)
    }

    #[allow(unused_variables)]
    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
        gs.read_global_state(&state::def::sum::<usize>(1))
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
            graph.add_edge(0, src, dst, &vec![], None).unwrap();
        }

        let graph_at = graph.at(1);

        let results = clustering_coefficient(&graph_at);
        assert_eq!(results, 0.3);
    }
}
