use crate::{
    core::{state::compute_state::ComputeStateVec, entities::VID},
    db::{
        api::view::{GraphViewOps, VertexViewOps},
        task::{
            context::Context,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
            vertex::eval_vertex::EvalVertexView,
        },
    },
};
use std::{cmp, collections::
    {HashMap, HashSet}};

#[derive(Clone, Debug)]
struct KCoreState {
    alive: bool,
}

impl KCoreState {
    fn new() -> Self {
        Self { alive:true }
    }
}

/// Determines which nodes are in the k-core for a given value of k
///
/// # Arguments
///
/// * `g` - A reference to the graph
/// * `k` - Value of k such that the returned vertices have degree > k (recursively)
/// * `iter_count` - The number of iterations to run
/// * `threads` - number of threads to run on
///
/// # Returns
///
/// A hash set of vertices in the k core
///
pub fn k_core_set<G>(
    graph: &G,
    k: usize,
    iter_count: usize,
    threads: Option<usize>,
) -> HashSet<VID>
where
    G: GraphViewOps,
{
    let ctx: Context<G, ComputeStateVec> = graph.into();

    let step1 = ATask::new(move |vv| {
        let deg = vv.degree();
        let state: &mut KCoreState = vv.get_mut();
        state.alive = if deg < k {false} else {true};
        Step::Continue
    });

    let step2 = ATask::new(
        move |vv: &mut EvalVertexView<'_, G, ComputeStateVec, KCoreState>| {
            let prev: bool = vv.prev().alive;
            let current = vv
                .neighbours()
                .into_iter()
                .map(|n| n.prev().alive)
                .filter(|b| *b)
                .count() >= k;
            let state: &mut KCoreState = vv.get_mut();
            if current != prev {
                state.alive = current;
                Step::Continue
            } else {
                Step::Done
            }
        },
    );

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    runner
        .run(
            vec![Job::new(step1)],
            vec![Job::read_only(step2)],
            KCoreState::new(),
            |_, _, _, local| {
                local
                    .iter()
                    .enumerate()
                    .map(|(v_ref, state)| (v_ref.into(), state.alive))
                    .collect::<HashMap<_, _>>()
            },
            threads,
            iter_count,
            None,
            None,
        )
        .into_iter()
        .filter(|(k, v)| *v)
        .map(|(k,v)| k)
        .collect()
}

mod k_core_test {
    use std::collections::HashSet;

    use crate::algorithms::triangle_count::triangle_count;
    use crate::prelude::*;
    use crate::algorithms::k_core::k_core_set;

    #[test]
    fn k_core_2() {
        let graph = Graph::new();

        let edges = vec![
            (1, 2, 1),
            (1, 3, 2),
            (1, 4, 3),
            (3, 1, 4),
            (3, 4, 5),
            (3, 5, 6),
            (4, 5, 7),
            (5, 6, 8),
            (5, 8, 9),
            (7, 5, 10),
            (8, 5, 11),
            (1, 9, 12),
            (9, 1, 13),
            (6, 3, 14),
            (4, 8, 15),
            (8, 3, 16),
            (5, 10, 17),
            (10, 5, 18),
            (10, 8, 19),
            (1, 11, 20),
            (11, 1, 21),
            (9, 11, 22),
            (11, 9, 23),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        let result = k_core_set(&graph, 2, usize::MAX, None);
        let subgraph = graph.subgraph(result.clone());
        let actual = vec!["1", "3", "4", "5", "6", "8", "9", "10", "11"]
        .into_iter()
        .map(|k| k.to_string())
        .collect::<HashSet<String>>();

        assert_eq!(actual,subgraph.vertices().name().collect::<HashSet<String>>());


    }
}