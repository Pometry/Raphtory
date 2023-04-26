// the main execution unit of an algorithm

use std::borrow::Cow;
use std::cell::RefCell;
use std::cmp::min;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use once_cell::sync::Lazy;
use rayon::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};
use replace_with::{replace_with_and_return, replace_with_or_abort};
use rustc_hash::FxHashMap;

use crate::core::agg::{Accumulator, AndDef};
use crate::core::state::{
    self, AccId, ComputeState, ComputeStateVec, ShuffleComputeState, StateType,
};
use crate::core::tgraph::VertexRef;
use crate::core::utils::get_shard_id_from_global_vid;

use super::vertex::VertexView;
use super::view_api::internal::GraphViewInternalOps;

enum Sheep<T: Clone> {
    Borrow(Arc<T>),
    Own(T),
}

impl<T: Clone> Deref for Sheep<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            Sheep::Borrow(a) => &a,
            Sheep::Own(t) => &t,
        }
    }
}

impl<T: Clone> DerefMut for Sheep<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Sheep::Own(ref mut own) => return own,
            _ => {
                replace_with_or_abort(self, |mut _self| match _self {
                    Sheep::Borrow(a) => {
                        let b = Arc::try_unwrap(a).unwrap_or_else(|a| (*a).clone());
                        Sheep::Own(b)
                    }
                    _ => unreachable!(),
                });
                self.deref_mut()
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Step {
    Done,
    Continue,
}

pub trait Task<G, CS>
where
    G: GraphViewInternalOps + Send + Sync + Clone + 'static,
    CS: ComputeState,
{
    fn run(&self, vv: &EvalVertexView<G, CS>) -> Step;
}

pub struct ATask<G, CS, F>
where
    G: GraphViewInternalOps + Send + Sync + 'static,
    CS: ComputeState,
    F: Fn(&EvalVertexView<G, CS>) -> Step,
{
    f: F,
    task_type: TaskType,
    _g: std::marker::PhantomData<G>,
    _cs: std::marker::PhantomData<CS>,
}

// determines if the task is executed for all vertices or only for updated vertices (vertices that had a state change since last sync)
pub enum TaskType {
    ALL,
    UPDATED_ONLY,
}

impl<G, CS, F> ATask<G, CS, F>
where
    G: GraphViewInternalOps + Send + Sync + 'static,
    CS: ComputeState,
    F: Fn(&EvalVertexView<G, CS>) -> Step,
{
    fn new(task_type: TaskType, f: F) -> Self {
        Self {
            f,
            task_type,
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

type MergeFn<CS> =
    Arc<dyn Fn(&mut ShuffleComputeState<CS>, &ShuffleComputeState<CS>, usize) + Send + Sync>;

// state for the execution of a task
pub struct Context<G, CS>
where
    G: GraphViewInternalOps + Send + Sync + 'static,
    CS: ComputeState,
{
    ss: usize,
    g: Arc<G>,
    merge_fns: Vec<MergeFn<CS>>,
    resetable_states: Vec<u32>,
}

impl<G, CS> Context<G, CS>
where
    G: GraphViewInternalOps + Send + Sync + 'static,
    CS: ComputeState,
{
    pub fn agg<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        id: AccId<A, IN, OUT, ACC>,
    ) {
        let fn_merge: MergeFn<CS> = Arc::new(move |a, b, ss| a.merge_mut_2(b, id, ss));

        self.merge_fns.push(fn_merge);
    }

    pub fn agg_reset<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        id: AccId<A, IN, OUT, ACC>,
    ) {
        let fn_merge: MergeFn<CS> = Arc::new(move |a, b, ss| a.merge_mut_2(b, id, ss));

        self.merge_fns.push(fn_merge);
        self.resetable_states.push(id.id());
    }

    pub fn global_agg<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        id: AccId<A, IN, OUT, ACC>,
    ) {
        let fn_merge: MergeFn<CS> = Arc::new(move |a, b, ss| a.merge_mut_global(b, id, ss));

        self.merge_fns.push(fn_merge);
    }

    pub fn global_agg_reset<
        A: StateType,
        IN: 'static,
        OUT: 'static,
        ACC: Accumulator<A, IN, OUT>,
    >(
        &mut self,
        id: AccId<A, IN, OUT, ACC>,
    ) {
        let fn_merge: MergeFn<CS> = Arc::new(move |a, b, ss| a.merge_mut_global(b, id, ss));

        self.merge_fns.push(fn_merge);
        self.resetable_states.push(id.id());
    }
}

impl<G: GraphViewInternalOps + Send + Sync + Clone + 'static, CS: ComputeState> From<&G>
    for Context<G, CS>
{
    fn from(g: &G) -> Self {
        Self {
            ss: 0,
            g: Arc::new(g.clone()),
            merge_fns: vec![],
            resetable_states: vec![],
        }
    }
}

pub struct TaskRunner<G: GraphViewInternalOps + Send + Sync + Clone + 'static, CS: ComputeState> {
    tasks: Vec<Box<dyn Task<G, CS> + Sync + Send>>,
    ctx: Context<G, CS>,
}

impl<G: GraphViewInternalOps + Send + Sync + Clone + 'static, CS: ComputeState> TaskRunner<G, CS> {
    pub fn new(tasks: Vec<Box<dyn Task<G, CS> + Sync + Send>>, ctx: Context<G, CS>) -> Self {
        Self { tasks, ctx: ctx }
    }

    fn ss(&self) -> usize {
        self.ctx.ss
    }

    fn merge_states(
        &self,
        mut a: Arc<ShuffleComputeState<CS>>,
        b: Arc<ShuffleComputeState<CS>>,
    ) -> Arc<ShuffleComputeState<CS>> {
        let left = Arc::get_mut(&mut a)
            .expect("merge_states: get_mut should always work, no other reference can exist");
        for merge_fn in self.ctx.merge_fns.iter() {
            merge_fn(left, &b, self.ctx.ss);
        }
        a
    }

    fn make_total_state<F: Fn() -> Arc<Option<ShuffleComputeState<CS>>>>(
        &self,
        num_threads: usize,
        f: F,
    ) -> Vec<(usize, Arc<Option<ShuffleComputeState<CS>>>)> {
        vec![f(); num_threads]
            .into_iter()
            .enumerate()
            .collect::<Vec<_>>()
    }

    pub fn run_task_list(
        &mut self,
        total_state: Arc<Option<ShuffleComputeState<CS>>>,
        num_threads: usize,
        num_shards: usize,
        all_stop: &AccId<bool, bool, bool, AndDef>,
    ) -> (bool, Arc<Option<ShuffleComputeState<CS>>>) {
        POOL.install(move || {
            let mut new_total_state = total_state;
            let mut done = false;

            let num_tasks = min(num_shards, num_threads);

            for task in self.tasks.iter() {
                let mut task_states = self.make_total_state(num_tasks, || new_total_state.clone());

                let updated_state = task_states
                    .par_iter_mut()
                    .map(|(job_id, state)| {
                        let a = Arc::make_mut(state).take().unwrap();
                        let sheep_a = Sheep::Borrow(Arc::new(a));
                        let rc_local_state =
                            Rc::new(RefCell::new(sheep_a)); // make_mut clones the state

                        for shard in 0..num_shards {
                            if shard % num_tasks == *job_id {
                                for vertex in self.ctx.g.vertices_shard(shard) {
                                    let vv = EvalVertexView::new(
                                        self.ctx.ss,
                                        vertex,
                                        self.ctx.g.clone(),
                                        rc_local_state.clone(),
                                    );

                                    match task.run(&vv) {
                                        Step::Done => {
                                            rc_local_state.borrow_mut().accumulate_global(
                                                self.ctx.ss,
                                                true,
                                                all_stop,
                                            );
                                        }
                                        Step::Continue => {
                                            rc_local_state.borrow_mut().accumulate_global(
                                                self.ctx.ss,
                                                false,
                                                all_stop,
                                            );
                                        }
                                    }
                                }
                            }
                        }

                        let state: ShuffleComputeState<CS> =
                            Rc::try_unwrap(rc_local_state).unwrap().into_inner();

                        Arc::new(state)
                    })
                    .reduce_with(|a, b| self.merge_states(a, b));

                let full_stop = updated_state
                    .as_ref()
                    .and_then(|state| state.read_global(self.ctx.ss, &all_stop))
                    .unwrap_or(false);

                if full_stop {
                    done = true;
                }

                // restore the shape of new_total_state Vec<(usize, Arc<Option<ShuffleComputeState<CS>>>)> from updated state using num_threads for vec len
                if let Some(arc_state) = updated_state {
                    let mut state = Arc::try_unwrap(arc_state)
                        .expect("should be able to unwrap Arc, no other reference can exist");

                    // we reset the all_stop state in between states to figure out if it's time to stop
                    if !done {
                        state.reset_global_states(self.ctx.ss + 1, &vec![all_stop.id()]);
                    }
                    new_total_state = Arc::new(Some(state));
                }

                if done {
                    break;
                }
            }

            (done, new_total_state)
        })
    }

    pub fn run(
        &mut self,
        num_threads: Option<usize>,
        steps: usize,
    ) -> Arc<Option<ShuffleComputeState<CS>>> {
        // say we go over all the vertices on all the threads but we partition the vertices
        // on each thread and we only run the function if the vertex is in the partition

        let graph_shards = self.ctx.g.num_shards();

        let pool = num_threads
            .map(|nt| custom_pool(nt))
            .unwrap_or_else(|| POOL.clone());

        let num_threads = pool.current_num_threads();

        let all_stop = state::def::and(u32::MAX);
        self.ctx.global_agg_reset(all_stop);

        let mut total_state = Arc::new(Some(ShuffleComputeState::new(graph_shards)));

        // the only benefit in this case is that we do not clone when we run with 1 thread

        let mut done = false;
        while !done && self.ctx.ss < steps {
            (done, total_state) =
                self.run_task_list(total_state, num_threads, graph_shards, &all_stop);
            // copy and reset the state from the step that just ended

            Arc::get_mut(&mut total_state)
                .and_then(|s| s.as_mut())
                .map(|s| {
                    s.copy_over_next_ss(self.ctx.ss);
                    s.reset_states(self.ctx.ss, &self.ctx.resetable_states);
                });

            self.ctx.ss += 1;
        }

        total_state
    }

    // use the global id of the vertex to determine if it's present in the partition
    fn is_vertex_in_job(&self, vertex: &VertexView<G>, n_jobs: usize, job_id: usize) -> bool {
        get_shard_id_from_global_vid(vertex.vertex.g_id, n_jobs) == job_id
    }
}

pub static POOL: Lazy<Arc<ThreadPool>> = Lazy::new(|| {
    let num_threads = std::env::var("DOCBROWN_MAX_THREADS")
        .map(|s| {
            s.parse::<usize>()
                .expect("DOCBROWN_MAX_THREADS must be a number")
        })
        .unwrap_or_else(|_| {
            std::thread::available_parallelism()
                .unwrap_or(std::num::NonZeroUsize::new(1).unwrap())
                .get()
        });

    let pool = ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .unwrap();

    Arc::new(pool)
});

pub fn custom_pool(n_threads: usize) -> Arc<ThreadPool> {
    let pool = ThreadPoolBuilder::new()
        .num_threads(n_threads)
        .build()
        .unwrap();

    Arc::new(pool)
}

pub struct EvalVertexView<G: GraphViewInternalOps + Send + Sync + Clone + 'static, CS: ComputeState>
{
    ss: usize,
    vv: VertexRef,
    g: Arc<G>,
    state: Rc<RefCell<ShuffleComputeState<CS>>>,
}

impl<G: GraphViewInternalOps + Send + Sync + Clone + 'static, CS: ComputeState>
    EvalVertexView<G, CS>
{
    fn new(
        ss: usize,
        vertex: VertexRef,
        g: Arc<G>,
        state: Rc<RefCell<ShuffleComputeState<CS>>>,
    ) -> Self {
        Self {
            ss,
            vv: vertex,
            g,
            state,
        }
    }

    fn global_id(&self) -> u64 {
        self.vv.g_id
    }

    // TODO: do we always look-up the pid in the graph? or when calling neighbours we look-it up?
    fn pid(&self) -> usize {
        if let Some(pid) = self.vv.pid {
            pid
        } else {
            self.g
                .vertex_ref(self.global_id())
                .and_then(|v_ref| v_ref.pid)
                .unwrap()
        }
    }

    pub fn neighbours(&self) -> impl Iterator<Item = Self> + '_ {
        self.g
            .neighbours(self.vv, crate::core::Direction::BOTH, None)
            .map(move |vv| EvalVertexView::new(self.ss, vv, self.g.clone(), self.state.clone()))
    }

    pub fn update<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &self,
        id: &AccId<A, IN, OUT, ACC>,
        a: IN,
    ) {
        self.state
            .borrow_mut()
            .accumulate_into_pid(self.ss, self.global_id(), self.pid(), a, id)
    }

    /// Read the current value of the vertex state using the given accumulator.
    /// Returns a default value if the value is not present.
    pub fn read<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AccId<A, IN, OUT, ACC>,
    ) -> OUT
    where
        A: StateType,
        OUT: std::fmt::Debug,
    {
        self.state
            .borrow()
            .read_with_pid(self.ss, self.global_id(), self.pid(), agg_r)
            .unwrap_or(ACC::finish(&ACC::zero()))
    }

    /// Read the prev value of the vertex state using the given accumulator.
    /// Returns a default value if the value is not present.
    pub fn read_prev<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AccId<A, IN, OUT, ACC>,
    ) -> OUT
    where
        A: StateType,
        OUT: std::fmt::Debug,
    {
        self.state
            .borrow()
            .read_with_pid(self.ss + 1, self.global_id(), self.pid(), agg_r)
            .unwrap_or(ACC::finish(&ACC::zero()))
    }
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

    let step1 = ATask::new(TaskType::ALL, move |vv| {
        vv.update(&min, vv.global_id());

        for n in vv.neighbours() {
            let my_min = vv.read(&min);
            n.update(&min, my_min)
        }

        Step::Continue
    });

    let step2 = ATask::new(TaskType::ALL, move |vv| {
        let current = vv.read(&min);
        let prev = vv.read_prev(&min);

        if current == prev {
            Step::Done
        } else {
            Step::Continue
        }
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(vec![Box::new(step1), Box::new(step2)], ctx);

    let results = runner.run(Some(threads), iter_count);

    if let Some(state) = results.as_ref() {
        let mut map: FxHashMap<u64, u64> = FxHashMap::default();

        state.fold_state_internal(runner.ctx.ss, &mut map, &min, |res, shard, pid, cc| {
            if let Some(v_ref) = graph.lookup_by_pid_and_shard(pid, shard) {
                res.insert(v_ref.g_id, cc);
            }
            res
        });

        map
    } else {
        FxHashMap::default()
    }
}

#[cfg(test)]
mod tasks_tests {

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

        let window = 0..25;

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

            let window = 0..1;

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
