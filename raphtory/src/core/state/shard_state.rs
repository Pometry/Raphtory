use super::{accumulator_id::AccId, compute_state::ComputeState, StateType};
use crate::core::agg::Accumulator;
use crate::db::view_api::GraphViewOps;
use rustc_hash::FxHashMap;
use std::collections::HashMap;

pub const GLOBAL_STATE_KEY: usize = 0;

#[derive(Debug, Clone)]
pub struct ShardComputeState<CS: ComputeState + Send> {
    pub(crate) states: FxHashMap<u32, CS>,
}

impl<CS: ComputeState + Send + Clone> ShardComputeState<CS> {
    pub(crate) fn copy_over_next_ss(&mut self, ss: usize) {
        for (_, state) in self.states.iter_mut() {
            state.clone_current_into_other(ss);
        }
    }

    pub(crate) fn reset_states(&mut self, ss: usize, states: &[u32]) {
        for (id, state) in self.states.iter_mut() {
            if states.contains(id) {
                state.reset_resetable_states(ss);
            }
        }
    }

    pub(crate) fn fold<A, IN, OUT, ACC: Accumulator<A, IN, OUT>, F, B>(
        &self,
        ss: usize,
        b: B,
        agg_ref: &AccId<A, IN, OUT, ACC>,
        f: F,
    ) -> B
    where
        F: FnOnce(B, &u64, OUT) -> B + Copy,
        A: 'static,
        B: std::fmt::Debug,
        OUT: StateType,
    {
        if let Some(state) = self.states.get(&agg_ref.id()) {
            state.fold::<A, IN, OUT, ACC, F, B>(ss, b, f)
        } else {
            b
        }
    }

    pub fn read_vec<A, IN, OUT, ACC: Accumulator<A, IN, OUT>, G: GraphViewOps>(
        &self,
        ss: usize,
        agg_ref: &AccId<A, IN, OUT, ACC>,
        shard_id: usize,
        g: &G,
    ) -> Option<HashMap<String, OUT>>
    where
        OUT: StateType,
        A: 'static,
    {
        let cs = self.states.get(&agg_ref.id())?;
        Some(cs.finalize::<A, IN, OUT, ACC, G>(ss, shard_id, g))
    }

    pub(crate) fn set_from_other<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        other: &Self,
        agg_ref: &AccId<A, IN, OUT, ACC>,
        _ss: usize,
    ) where
        A: StateType,
    {
        match (
            self.states.get_mut(&agg_ref.id()),
            other.states.get(&agg_ref.id()),
        ) {
            (Some(self_cs), Some(other_cs)) => {
                *self_cs = other_cs.clone();
            }
            (None, Some(other_cs)) => {
                self.states.insert(agg_ref.id(), other_cs.clone());
            }
            _ => {}
        }
    }

    pub(crate) fn merge<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        other: &Self,
        agg_ref: &AccId<A, IN, OUT, ACC>,
        ss: usize,
    ) where
        A: StateType,
    {
        match (
            self.states.get_mut(&agg_ref.id()),
            other.states.get(&agg_ref.id()),
        ) {
            (Some(self_cs), Some(other_cs)) => {
                self_cs.merge::<A, IN, OUT, ACC>(other_cs, ss);
            }
            (None, Some(other_cs)) => {
                self.states.insert(agg_ref.id(), other_cs.clone());
            }
            _ => {}
        }
    }

    pub(crate) fn read<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        i: usize,
        id: u32,
        ss: usize,
    ) -> Option<OUT>
    where
        A: StateType,
        OUT: std::fmt::Debug,
    {
        let state = self.states.get(&id)?;
        state.read::<A, IN, OUT, ACC>(ss, i)
    }

    pub(crate) fn read_ref<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        i: usize,
        id: u32,
        ss: usize,
    ) -> Option<&A>
    where
        A: StateType,
    {
        let state = self.states.get(&id)?;
        state.read_ref::<A, IN, OUT, ACC>(ss, i)
    }

    pub(crate) fn new() -> Self {
        ShardComputeState {
            states: FxHashMap::default(),
        }
    }

    pub(crate) fn accumulate_into<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        ss: usize,
        key: usize,
        a: IN,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) where
        A: StateType,
    {
        let state = self
            .states
            .entry(agg_ref.id())
            .or_insert_with(|| CS::new_mutable_primitive(ACC::zero()));
        state.agg::<A, IN, OUT, ACC>(ss, a, key);
    }
}

impl<CS: ComputeState + Send> ShardComputeState<CS> {
    pub fn finalize<A, IN, OUT, ACC: Accumulator<A, IN, OUT>, G: GraphViewOps>(
        &self,
        ss: usize,
        agg_ref: &AccId<A, IN, OUT, ACC>,
        shard_id: usize,
        g: &G,
    ) -> HashMap<String, OUT>
    where
        OUT: StateType,
        A: 'static,
    {
        self.states
            .get(&agg_ref.id())
            .map(|s| s.finalize::<A, IN, OUT, ACC, G>(ss, shard_id, g))
            .unwrap_or(HashMap::<String, OUT>::default())
    }
}
