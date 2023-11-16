use super::{
    accumulator_id::AccId,
    compute_state::ComputeState,
    morcel_state::{MorcelComputeState, GLOBAL_STATE_KEY},
    StateType,
};
use crate::{
    core::state::agg::Accumulator,
    db::{
        api::view::GraphViewOps,
        task::task_state::{Global, Shard},
    },
};
use std::{borrow::Borrow, collections::HashMap, sync::Arc};

#[derive(Debug, Clone)]
pub struct ShuffleComputeState<CS: ComputeState + Send> {
    morcel_size: usize,
    pub global: MorcelComputeState<CS>,
    pub parts: Vec<MorcelComputeState<CS>>,
}

// every partition has a struct as such
impl<CS: ComputeState + Send + Sync> ShuffleComputeState<CS> {
    fn resolve_pid(&self, p_id: usize) -> (usize, usize) {
        let morcel_id = p_id / self.morcel_size;
        let offset = p_id % self.morcel_size;
        (morcel_id, offset)
    }

    pub fn merge_mut<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        other: &Self,
        agg_ref: AccId<A, IN, OUT, ACC>,
        ss: usize,
    ) where
        A: StateType,
    {
        // zip the two partitions
        // merge each shard
        assert_eq!(self.parts.len(), other.parts.len());
        self.parts
            .iter_mut()
            .zip(other.parts.iter())
            .for_each(|(s, o)| s.merge(o, &agg_ref, ss));
    }

    pub fn set_from_other<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        other: &Self,
        agg_ref: &AccId<A, IN, OUT, ACC>,
        ss: usize,
    ) where
        A: StateType,
    {
        // zip the two partitions
        // merge each shard
        assert_eq!(self.parts.len(), other.parts.len());
        self.parts
            .iter_mut()
            .zip(other.parts.iter())
            .for_each(|(s, o)| s.set_from_other(o, agg_ref, ss));
    }

    pub fn merge_mut_global<
        A,
        IN,
        OUT,
        ACC: Accumulator<A, IN, OUT>,
        B: Borrow<AccId<A, IN, OUT, ACC>>,
    >(
        &mut self,
        other: &Self,
        agg_ref: B,
        ss: usize,
    ) where
        A: StateType,
    {
        self.global.merge(&other.global, agg_ref.borrow(), ss);
    }

    pub fn copy_over_next_ss(&mut self, ss: usize) {
        self.parts.iter_mut().for_each(|p| p.copy_over_next_ss(ss));
    }

    pub fn reset_states(&mut self, ss: usize, states: &[u32]) {
        self.global.reset_states(ss, states);
        self.parts
            .iter_mut()
            .for_each(|p| p.reset_states(ss, states));
    }

    pub fn reset_global_states(&mut self, ss: usize, states: &Vec<u32>) {
        self.global.reset_states(ss, states);
    }

    pub fn new(total_len: usize, n_parts: usize, morcel_size: usize) -> Self {
        let last_one_size = if morcel_size == 0 {
            1
        } else {
            total_len % morcel_size
        };
        let mut parts: Vec<MorcelComputeState<CS>> = (0..n_parts - 1)
            .into_iter()
            .map(|_| MorcelComputeState::new(morcel_size))
            .collect();

        if last_one_size != 0 {
            parts.push(MorcelComputeState::new(last_one_size));
        } else {
            parts.push(MorcelComputeState::new(morcel_size));
        }

        Self {
            morcel_size,
            parts,
            global: MorcelComputeState::new(1),
        }
    }

    pub fn global() -> Self {
        Self {
            morcel_size: 1,
            parts: vec![],
            global: MorcelComputeState::new(1),
        }
    }

    pub fn accumulate_into<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        ss: usize,
        p_id: usize,
        a: IN,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) where
        A: StateType,
    {
        let (morcel_id, offset) = self.resolve_pid(p_id);
        self.parts[morcel_id].accumulate_into(ss, offset, a, agg_ref)
    }

    pub fn read_with_pid<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        ss: usize,
        p_id: usize,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) -> Option<OUT>
    where
        A: StateType,
        OUT: std::fmt::Debug,
    {
        let (morcel_id, offset) = self.resolve_pid(p_id);
        self.parts[morcel_id].read::<A, IN, OUT, ACC>(offset, agg_ref.id(), ss)
    }

    pub fn accumulate_global<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        ss: usize,
        a: IN,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) where
        A: StateType,
    {
        self.global
            .accumulate_into(ss, GLOBAL_STATE_KEY, a, agg_ref)
    }

    pub fn read<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        ss: usize,
        p_id: usize,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) -> Option<OUT>
    where
        A: StateType,
        OUT: std::fmt::Debug,
    {
        let (morcel_id, offset) = self.resolve_pid(p_id);
        self.parts[morcel_id].read::<A, IN, OUT, ACC>(offset, agg_ref.id(), ss)
    }

    pub fn read_ref<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        ss: usize,
        p_id: usize,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) -> Option<&A>
    where
        A: StateType,
    {
        let (morcel_id, offset) = self.resolve_pid(p_id);
        self.parts[morcel_id].read_ref::<A, IN, OUT, ACC>(offset, agg_ref.id(), ss)
    }

    pub fn read_global<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        ss: usize,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) -> Option<OUT>
    where
        A: StateType,
        OUT: std::fmt::Debug,
    {
        self.global
            .read::<A, IN, OUT, ACC>(GLOBAL_STATE_KEY, agg_ref.id(), ss)
    }

    pub fn finalize<A, B, F, IN, OUT, ACC: Accumulator<A, IN, OUT>, G: GraphViewOps>(
        &self,
        agg_def: &AccId<A, IN, OUT, ACC>,
        ss: usize,
        _g: &G,
        f: F,
    ) -> HashMap<usize, B>
    where
        OUT: StateType,
        A: StateType,
        F: Fn(OUT) -> B + Copy,
    {
        self.iter(ss, *agg_def)
            .map(|(v_id, a)| {
                let out = a
                    .map(|a| ACC::finish(a))
                    .unwrap_or_else(|| ACC::finish(&ACC::zero()));
                (v_id, f(out))
            })
            .collect()
    }

    pub fn iter<'a, A: StateType, IN: 'a, OUT: 'a, ACC: Accumulator<A, IN, OUT>>(
        &'a self,
        ss: usize,
        acc_id: AccId<A, IN, OUT, ACC>,
    ) -> impl Iterator<Item = (usize, Option<&A>)> + 'a {
        self.parts
            .iter()
            .flat_map(move |part| part.iter(ss, &acc_id))
            .enumerate()
    }

    pub fn iter_out<'a, A: StateType, IN: 'a, OUT: 'a, ACC: Accumulator<A, IN, OUT>>(
        &'a self,
        ss: usize,
        acc_id: AccId<A, IN, OUT, ACC>,
    ) -> impl Iterator<Item = (usize, OUT)> + 'a {
        self.iter(ss, acc_id).map(|(id, a)| {
            let out = a
                .map(|a| ACC::finish(a))
                .unwrap_or_else(|| ACC::finish(&ACC::zero()));
            (id, out)
        })
    }
}

pub struct EvalGlobalState<CS: ComputeState + Send> {
    ss: usize,
    pub(crate) global_state: Global<CS>,
}

impl<CS: ComputeState + Send> EvalGlobalState<CS> {
    pub fn new(ss: usize, global_state: Global<CS>) -> EvalGlobalState<CS> {
        Self { ss, global_state }
    }

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
        self.global_state
            .inner()
            .read_global(self.ss + 1, agg_def)
            .unwrap_or_default()
    }
}

#[derive(Debug)]
pub struct EvalShardState<G: GraphViewOps, CS: ComputeState + Send> {
    ss: usize,
    g: G,
    shard_states: Shard<CS>,
}

impl<G: GraphViewOps, CS: ComputeState + Send> EvalShardState<G, CS> {
    pub fn new(ss: usize, g: G, shard_states: Shard<CS>) -> EvalShardState<G, CS> {
        Self {
            ss,
            g,
            shard_states,
        }
    }

    pub fn finalize<A, B, F, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        self,
        agg_def: &AccId<A, IN, OUT, ACC>,
        f: F,
    ) -> HashMap<usize, B>
    where
        OUT: StateType,
        A: StateType,
        F: Fn(OUT) -> B + Copy,
    {
        let inner = self.shard_states.consume();
        if let Ok(inner) = inner {
            inner.finalize(agg_def, self.ss, &self.g, f)
        } else {
            HashMap::new()
        }
    }

    pub fn values(&self) -> &Shard<CS> {
        &self.shard_states
    }
}

pub struct EvalLocalState<G: GraphViewOps, CS: ComputeState + Send> {
    ss: usize,
    g: G,
    local_states: Vec<Arc<Option<ShuffleComputeState<CS>>>>,
}

impl<G: GraphViewOps, CS: ComputeState + Send> EvalLocalState<G, CS> {
    pub fn new(
        ss: usize,
        g: G,
        local_states: Vec<Arc<Option<ShuffleComputeState<CS>>>>,
    ) -> EvalLocalState<G, CS> {
        Self {
            ss,
            g,
            local_states,
        }
    }

    pub fn finalize<A, B, F, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        self,
        agg_def: &AccId<A, IN, OUT, ACC>,
        f: F,
    ) -> HashMap<usize, B>
    where
        OUT: StateType,
        A: StateType,
        F: Fn(OUT) -> B + Copy,
    {
        self.local_states
            .into_iter()
            .flat_map(|state| {
                if let Some(state) = Arc::try_unwrap(state).ok().flatten() {
                    state.finalize(agg_def, self.ss, &self.g, f)
                } else {
                    HashMap::<usize, B>::new()
                }
            })
            .collect()
    }
}
