use num_traits::abs;
// the main execution unit of an algorithm
use rustc_hash::FxHashMap;

use crate::core::agg::{Init, Var};
use crate::core::state::{self, ComputeState, ComputeStateVec};

use crate::db::view_api::internal::GraphViewInternalOps;
use crate::db::view_api::GraphViewOps;

use super::context::{Context, GlobalState};
use super::eval_vertex::EvalVertexView;
use super::task_runner::TaskRunner;

pub trait Task<G, CS>
where
    G: GraphViewInternalOps + Send + Sync + Clone + 'static,
    CS: ComputeState,
{
    fn run(&self, vv: &EvalVertexView<G, CS>) -> Step;
}

#[derive(Debug, PartialEq)]
pub enum Step {
    Done,
    Continue,
}

pub struct ATask<G, CS, F>
where
    G: GraphViewInternalOps + Send + Sync + 'static,
    CS: ComputeState,
    F: Fn(&EvalVertexView<G, CS>) -> Step,
{
    f: F,
    _g: std::marker::PhantomData<G>,
    _cs: std::marker::PhantomData<CS>,
}

// determines if the task is executed for all vertices or only for updated vertices (vertices that had a state change since last sync)
pub enum Job<G, CS: ComputeState> {
    Read(Box<dyn Task<G, CS> + Sync + Send>),
    Write(Box<dyn Task<G, CS> + Sync + Send>),
    Check(Box<dyn Fn(&GlobalState<CS>) -> Step + Send + Sync + 'static>),
}

impl<G: GraphViewInternalOps + Send + Sync + Clone + 'static, CS: ComputeState> Job<G, CS> {
    fn new<T: Task<G, CS> + Send + Sync + 'static>(t: T) -> Self {
        Self::Write(Box::new(t))
    }

    fn read_only<T: Task<G, CS> + Send + Sync + 'static>(t: T) -> Self {
        Self::Read(Box::new(t))
    }
}

impl<G, CS, F> ATask<G, CS, F>
where
    G: GraphViewInternalOps + Send + Sync + 'static,
    CS: ComputeState,
    F: Fn(&EvalVertexView<G, CS>) -> Step,
{
    fn new(f: F) -> Self {
        Self {
            f,
            _g: std::marker::PhantomData,
            _cs: std::marker::PhantomData,
        }
    }
}

impl<G, CS, F> Task<G, CS> for ATask<G, CS, F>
where
    G: GraphViewInternalOps + Send + Sync + Clone + 'static,
    CS: ComputeState,
    F: Fn(&EvalVertexView<G, CS>) -> Step,
{
    fn run(&self, vv: &EvalVertexView<G, CS>) -> Step {
        (self.f)(vv)
    }
}

struct InitOneF32();
impl Init<Var<f32>> for InitOneF32 {
    fn init() -> Var<f32> {
        Var::new(1.0f32)
    }
}

#[allow(unused_variables)]
pub fn unweighted_page_rank<G>(g: &G, iter_count: usize, threads: usize) -> FxHashMap<u64, f32>
where
    G: GraphViewInternalOps + Send + Sync + Clone + 'static,
{
    let total_vertices = g.num_vertices();

    let mut ctx: Context<G, ComputeStateVec> = g.into();

    let MAX_DIFF = 0.01f32;
    let damping_factor = 0.85;

    let score = state::def::store::<f32>(0).init::<InitOneF32>();
    let recv_score = state::def::sum::<f32>(1);
    let max_diff = state::def::max::<f32>(2);

    ctx.agg(score);
    ctx.agg_reset(recv_score);
    ctx.global_agg_reset(max_diff);

    let step1 = ATask::new(move |vv| {
        let initial_score = 1f32 / total_vertices as f32;
        println!("v: {}, initial score: {}", vv.global_id(), initial_score);
        vv.update(&score, initial_score);
        Step::Continue
    });

    let step2 = ATask::new(move |s| {
        let out_degree = s.out_degree();
        if out_degree > 0 {
            let new_score = s.read(&score) / out_degree as f32;
            for t in s.neighbours_out() {
                t.update(&recv_score, new_score)
            }
        }
        Step::Continue
    });

    let step3 = ATask::new(move |s| {
        s.update(
            &score,
            (1f32 - damping_factor) + (damping_factor * s.read(&recv_score)),
        );
        let prev = s.read_prev(&score);
        let curr = s.read(&score);
        let md = abs(prev - curr);
        s.global_update(&max_diff, md);
        Step::Continue
    });

    let step4 = Job::Check(Box::new(move |state| {
        if state.read(&max_diff) > MAX_DIFF {
            Step::Continue
        } else {
            Step::Done
        }
    }));

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    let state = runner.run(
        vec![Job::new(step1)],
        vec![Job::new(step2), Job::new(step3), step4],
        Some(threads),
        iter_count,
        None,
    );

    let mut map: FxHashMap<u64, f32> = FxHashMap::default();

    state.fold_state_internal(
        runner.ctx.ss(),
        &mut map,
        &score,
        |res, shard, pid, score| {
            if let Some(v_ref) = g.lookup_by_pid_and_shard(pid, shard) {
                res.insert(v_ref.g_id, score);
            }
            res
        },
    );

    map
}

pub fn weakly_connected_components<G>(
    graph: &G,
    iter_count: usize,
    threads: usize,
) -> FxHashMap<u64, u64>
where
    G: GraphViewInternalOps + Send + Sync + Clone + 'static,
{
    let mut ctx: Context<G, ComputeStateVec> = graph.into();

    let min = state::def::min::<u64>(0);

    // setup the aggregator to be merged post execution
    ctx.agg(min.clone());

    let step1 = ATask::new(move |vv| {
        vv.update(&min, vv.global_id());

        for n in vv.neighbours() {
            let my_min = vv.read(&min);
            n.update(&min, my_min)
        }

        Step::Continue
    });

    let step2 = ATask::new(move |vv| {
        let current = vv.read(&min);
        let prev = vv.read_prev(&min);

        if current == prev {
            Step::Done
        } else {
            Step::Continue
        }
    });

    let tasks = vec![Job::new(step1), Job::read_only(step2)];
    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    let state = runner.run(vec![], tasks, Some(threads), iter_count, None);

    let mut map: FxHashMap<u64, u64> = FxHashMap::default();

    state.fold_state_internal(runner.ctx.ss(), &mut map, &min, |res, shard, pid, cc| {
        if let Some(v_ref) = graph.lookup_by_pid_and_shard(pid, shard) {
            res.insert(v_ref.g_id, cc);
        }
        res
    });

    map
}

#[cfg(test)]
mod page_rank_tests {
    use pretty_assertions::assert_eq;

    use crate::db::graph::Graph;

    use super::*;

    fn load_graph(n_shards: usize) -> Graph {
        let graph = Graph::new(n_shards);

        let edges = vec![(1, 2), (1, 4), (2, 3), (3, 1), (4, 1)];

        for (src, dst) in edges {
            graph.add_edge(0, src, dst, &vec![], None).unwrap();
        }
        graph
    }

    fn test_page_rank(n_shards: usize) {
        let graph = load_graph(n_shards);

        let results: FxHashMap<u64, f32> =
            unweighted_page_rank(&graph, 19, 2).into_iter().collect();

        assert_eq!(
            results,
            vec![
                (2, 0.78044075),
                (4, 0.78044075),
                (1, 1.4930439),
                (3, 0.8092761)
            ]
            // {8: 1.0, 5: 0.575, 2: 0.5, 7: 1.0, 4: 0.5, 1: 1.0, 3: 1.0}
            // vec![(8, 20.7725), (5, 29.76125), (2, 25.38375), (7, 20.7725), (4, 16.161251), (1, 21.133749), (3, 21.133749)]
            .into_iter()
            .collect::<FxHashMap<u64, f32>>()
        );
    }

    #[test]
    fn test_page_rank_1() {
        test_page_rank(1);
    }

    #[test]
    fn test_page_rank_2() {
        test_page_rank(2);
    }

    #[test]
    fn test_page_rank_3() {
        test_page_rank(3);
    }
}

#[cfg(test)]
mod weak_cc_tests {

    use std::{cmp::Reverse, iter::once};

    use itertools::{chain, Itertools};

    use crate::db::graph::Graph;

    use super::*;

    #[test]
    fn connected_components() {
        let graph = Graph::new(4);

        let edges = vec![
            (1, 2, 1),
            (2, 3, 2),
            (3, 4, 3),
            (3, 5, 4),
            (6, 5, 5),
            (7, 8, 6),
            (8, 7, 7),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, &vec![], None).unwrap();
        }

        let actual = weakly_connected_components(&graph, usize::MAX, 2);

        let expected: FxHashMap<u64, u64> = vec![
            (1, 1),
            (2, 1),
            (3, 1),
            (4, 1),
            (5, 1),
            (6, 1),
            (7, 7),
            (8, 7),
        ]
        .into_iter()
        .collect();

        assert_eq!(actual, expected)
    }

    #[test]
    fn simple_connected_components_2() {
        let graph = Graph::new(2);

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
            graph.add_edge(ts, src, dst, &vec![], None).unwrap();
        }

        let results: FxHashMap<u64, u64> = weakly_connected_components(&graph, usize::MAX, 4);

        assert_eq!(
            results,
            vec![
                (1, 1),
                (2, 1),
                (3, 1),
                (4, 1),
                (5, 1),
                (6, 1),
                (7, 1),
                (8, 1),
                (9, 1),
                (10, 1),
                (11, 1),
            ]
            .into_iter()
            .collect::<FxHashMap<u64, u64>>()
        );
    }

    // connected components on a graph with 1 node and a self loop
    #[test]
    fn simple_connected_components_3() {
        let graph = Graph::new(2);

        let edges = vec![(1, 1, 1)];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, &vec![], None).unwrap();
        }

        let results: FxHashMap<u64, u64> = weakly_connected_components(&graph, usize::MAX, 2);

        assert_eq!(
            results,
            vec![(1, 1),].into_iter().collect::<FxHashMap<u64, u64>>()
        );
    }

    #[quickcheck]
    fn circle_graph_the_smallest_value_is_the_cc(vs: Vec<u64>) {
        if vs.len() > 0 {
            let vs = vs.into_iter().unique().collect::<Vec<u64>>();

            let smallest = vs.iter().min().unwrap();

            let first = vs[0];
            // pairs of vertices from vs one after the next
            let edges = vs
                .iter()
                .zip(chain!(vs.iter().skip(1), once(&first)))
                .map(|(a, b)| (*a, *b))
                .collect::<Vec<(u64, u64)>>();

            assert_eq!(edges[0].0, first);
            assert_eq!(edges.last().unwrap().1, first);

            let graph = Graph::new(4);

            for (src, dst) in edges.iter() {
                graph.add_edge(0, *src, *dst, &vec![], None).unwrap();
            }

            // now we do connected components over window 0..1

            let components: FxHashMap<u64, u64> =
                weakly_connected_components(&graph, usize::MAX, 4);

            let actual = components
                .iter()
                .group_by(|(_, cc)| *cc)
                .into_iter()
                .map(|(cc, group)| (cc, Reverse(group.count())))
                .sorted_by(|l, r| l.1.cmp(&r.1))
                .map(|(cc, count)| (*cc, count.0))
                .take(1)
                .next();

            assert_eq!(
                actual,
                Some((*smallest, edges.len())),
                "actual: {:?}",
                actual
            );
        }
    }
}
