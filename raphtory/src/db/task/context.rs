use super::task_state::{Global, Shard};
use crate::{
    core::{
        entities::VID,
        state::{
            accumulator_id::AccId, agg::Accumulator, compute_state::ComputeState,
            shuffle_state::ShuffleComputeState, StateType,
        },
    },
    db::{api::view::GraphViewOps, graph::vertex::VertexView},
};
use std::{fmt::Debug, sync::Arc};

type MergeFn<CS> =
    Arc<dyn Fn(&mut ShuffleComputeState<CS>, &ShuffleComputeState<CS>, usize) + Send + Sync>;

pub struct Context<G, CS>
where
    G: GraphViewOps,
    CS: ComputeState,
{
    ss: usize,
    g: G,
    merge_fns: Vec<MergeFn<CS>>,
    resetable_states: Vec<u32>,
}

impl<G, CS> Context<G, CS>
where
    G: GraphViewOps,
    CS: ComputeState,
{
    pub fn new_local_state<O: Debug + Default, F: Fn(VertexView<G>) -> O>(
        &self,
        init_f: F,
    ) -> Vec<O> {
        let n = self.g.unfiltered_num_vertices();
        let mut new_state = Vec::with_capacity(n);
        for i in 0..n {
            match self.g.vertex(VID(i)) {
                Some(v) => new_state.push(init_f(v)),
                None => new_state.push(O::default()),
            }
        }
        new_state
    }
    pub fn ss(&self) -> usize {
        self.ss
    }

    pub fn graph(&self) -> G {
        self.g.clone()
    }

    pub fn increment_ss(&mut self) {
        self.ss += 1;
    }

    pub fn resetable_states(&self) -> &[u32] {
        &self.resetable_states
    }

    pub(crate) fn run_merge_shard(&self, a: Shard<CS>, b: Shard<CS>) -> Shard<CS> {
        self.run_merge(a.unwrap(), b.unwrap()).into()
    }

    pub(crate) fn run_merge_global(&self, a: Global<CS>, b: Global<CS>) -> Global<CS> {
        self.run_merge(a.unwrap(), b.unwrap()).into()
    }

    pub(crate) fn run_merge(
        &self,
        mut a: Arc<ShuffleComputeState<CS>>,
        mut b: Arc<ShuffleComputeState<CS>>,
    ) -> Arc<ShuffleComputeState<CS>> {
        if let Some(left) = Arc::get_mut(&mut a) {
            for merge_fn in self.merge_fns.iter() {
                merge_fn(left, &b, self.ss);
            }
            a
        } else if let Some(right) = Arc::get_mut(&mut b) {
            for merge_fn in self.merge_fns.iter() {
                merge_fn(right, &a, self.ss);
            }
            b
        } else {
            // none of the states have been changes so just return one of them
            a
        }
    }

    pub fn agg<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        id: AccId<A, IN, OUT, ACC>,
    ) {
        let fn_merge: MergeFn<CS> = Arc::new(move |a, b, ss| a.merge_mut(b, id, ss));

        self.merge_fns.push(fn_merge);
    }

    pub fn agg_reset<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        id: AccId<A, IN, OUT, ACC>,
    ) {
        let fn_merge: MergeFn<CS> = Arc::new(move |a, b, ss| a.merge_mut(b, id, ss));

        self.merge_fns.push(fn_merge);
        self.resetable_states.push(id.id());
    }

    pub fn local_reset<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        id: AccId<A, IN, OUT, ACC>,
    ) {
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

impl<G: GraphViewOps, CS: ComputeState> From<&G> for Context<G, CS> {
    fn from(g: &G) -> Self {
        Self {
            ss: 0,
            g: g.clone(),
            merge_fns: vec![],
            resetable_states: vec![],
        }
    }
}

pub struct GlobalState<CS: ComputeState> {
    state: Global<CS>,
    ss: usize,
}

impl<CS: ComputeState> GlobalState<CS> {
    pub fn finalize<A: StateType, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_def: &AccId<A, IN, OUT, ACC>,
    ) -> OUT
    where
        OUT: StateType + Default,
        A: 'static,
    {
        // ss needs to be incremented because the loop ran once and at the end it incremented the state thus
        // the value is on the previous ss
        self.state
            .inner()
            .read_global(self.ss + 1, agg_def)
            .unwrap_or_default()
    }

    pub(crate) fn new(state: Global<CS>, ss: usize) -> Self {
        Self { state, ss }
    }

    pub fn read<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        acc_id: &AccId<A, IN, OUT, ACC>,
    ) -> OUT
    where
        A: StateType,
        OUT: std::fmt::Debug,
    {
        self.state
            .inner()
            .read_global(self.ss, acc_id)
            .unwrap_or(ACC::finish(&ACC::zero()))
    }

    pub fn read_prev<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        acc_id: &AccId<A, IN, OUT, ACC>,
    ) -> OUT
    where
        A: StateType,
        OUT: std::fmt::Debug,
    {
        self.state
            .inner()
            .read_global(self.ss + 1, acc_id)
            .unwrap_or(ACC::finish(&ACC::zero()))
    }
}
