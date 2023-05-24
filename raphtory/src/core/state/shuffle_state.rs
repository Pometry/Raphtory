use crate::db::task::task_state::{Global, Shard};
use crate::db::view_api::GraphViewOps;
use crate::{
    core::{agg::Accumulator, utils::get_shard_id_from_global_vid},
    db::view_api::internal::GraphViewInternalOps,
};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;

use super::{
    accumulator_id::AccId,
    compute_state::ComputeState,
    shard_state::{ShardComputeState, GLOBAL_STATE_KEY},
    StateType,
};

#[derive(Debug, Clone)]
pub struct ShuffleComputeState<CS: ComputeState + Send> {
    pub global: ShardComputeState<CS>,
    pub parts: Vec<ShardComputeState<CS>>,
}

// every partition has a struct as such
impl<CS: ComputeState + Send + Sync> ShuffleComputeState<CS> {
    pub fn fold_state<A, IN, OUT, ACC: Accumulator<A, IN, OUT>, B, F>(
        &self,
        ss: usize,
        b: B,
        agg_ref: &AccId<A, IN, OUT, ACC>,
        f: F,
    ) -> B
    where
        A: StateType,
        B: std::fmt::Debug,
        OUT: StateType,
        F: Fn(B, &u64, OUT) -> B + Copy,
    {
        let out_b = self
            .parts
            .iter()
            .fold(b, |b, part| part.fold(ss, b, agg_ref, f));
        out_b
    }

    pub fn fold_state_internal<A, IN, OUT, ACC: Accumulator<A, IN, OUT>, B, F>(
        &self,
        ss: usize,
        b: B,
        agg_ref: &AccId<A, IN, OUT, ACC>,
        f: F,
    ) -> B
    where
        A: StateType,
        B: std::fmt::Debug,
        OUT: StateType,
        F: Fn(B, usize, usize, OUT) -> B + Copy,
    {
        let out_b = self.parts.iter().enumerate().fold(b, |b, (part_id, part)| {
            part.fold(ss, b, agg_ref, |b, id, out| {
                f(b, part_id, *id as usize, out)
            })
        });
        out_b
    }

    pub fn merge_mut<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
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
            .for_each(|(s, o)| s.merge(o, agg_ref, ss));
    }

    pub fn merge_mut_2<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
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

    pub fn new(n_parts: usize) -> Self {
        Self {
            parts: (0..n_parts)
                .into_iter()
                .map(|_| ShardComputeState::new())
                .collect(),
            global: ShardComputeState::new(),
        }
    }

    pub fn keys(&self, part_num: usize) -> impl Iterator<Item = u64> + '_ {
        self.parts[part_num]
            .states
            .iter()
            .flat_map(|(_, cs)| cs.iter_keys())
    }

    pub fn changed_keys(&self, part_num: usize, ss: usize) -> impl Iterator<Item = u64> + '_ {
        self.parts[part_num]
            .states
            .iter()
            .flat_map(move |(_, cs)| cs.iter_keys_changed(ss))
    }

    pub fn accumulate_into<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        ss: usize,
        into: usize,
        a: IN,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) where
        A: StateType,
    {
        let part = get_shard_id_from_global_vid(into as u64, self.parts.len());
        self.parts[part].accumulate_into(ss, into, a, agg_ref)
    }

    pub fn accumulate_into_pid<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        ss: usize,
        g_id: u64,
        p_id: usize,
        a: IN,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) where
        A: StateType,
    {
        let part = get_shard_id_from_global_vid(g_id, self.parts.len());
        self.parts[part].accumulate_into(ss, p_id, a, agg_ref)
    }

    pub fn read_with_pid<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        ss: usize,
        g_id: u64,
        p_id: usize,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) -> Option<OUT>
    where
        A: StateType,
        OUT: std::fmt::Debug,
    {
        let part = get_shard_id_from_global_vid(g_id, self.parts.len());
        self.parts[part].read::<A, IN, OUT, ACC>(p_id, agg_ref.id(), ss)
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
    // reads the value from K if it's set we return Ok(a) else we return Err(zero) from the monoid
    pub fn read<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        ss: usize,
        into: usize,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) -> Option<OUT>
    where
        A: StateType,
        OUT: std::fmt::Debug,
    {
        let part = get_shard_id_from_global_vid(into as u64, self.parts.len());
        self.parts[part].read::<A, IN, OUT, ACC>(into, agg_ref.id(), ss)
    }

    pub fn read_ref<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        ss: usize,
        into: usize,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) -> Option<&A>
    where
        A: StateType,
    {
        let part = get_shard_id_from_global_vid(into as u64, self.parts.len());
        self.parts[part].read_ref::<A, IN, OUT, ACC>(into, agg_ref.id(), ss)
    }

    pub fn read_ref_with_pid<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        ss: usize,
        g_id: u64,
        p_id: usize,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) -> Option<&A>
    where
        A: StateType,
    {
        let part = get_shard_id_from_global_vid(g_id, self.parts.len());
        self.parts[part].read_ref::<A, IN, OUT, ACC>(p_id, agg_ref.id(), ss)
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

    pub fn read_vec_partition<A, IN, OUT, ACC: Accumulator<A, IN, OUT>, G: GraphViewInternalOps>(
        &self,
        ss: usize,
        agg_def: &AccId<A, IN, OUT, ACC>,
        g: &G,
    ) -> Vec<HashMap<String, OUT>>
    where
        OUT: StateType,
        A: 'static,
    {
        self.parts
            .iter()
            .enumerate()
            .flat_map(|(shard_id, part)| part.read_vec(ss, agg_def, shard_id, g))
            .collect()
    }
}

impl<CS: ComputeState + Send> ShuffleComputeState<CS> {
    pub fn finalize<A, B, F, IN, OUT, ACC: Accumulator<A, IN, OUT>, G: GraphViewOps>(
        &self,
        agg_def: &AccId<A, IN, OUT, ACC>,
        ss: usize,
        g: &G,
        f: F,
    ) -> HashMap<String, B>
    where
        OUT: StateType,
        A: 'static,
        F: Fn(OUT) -> B + Copy,
    {
        let r = self
            .parts
            .iter()
            .enumerate()
            .map(|(shard_id, part)| part.finalize(ss, &agg_def, shard_id, g));

        r.into_iter()
            .flat_map(|c| c.into_iter().map(|(k, v)| (k, f(v))))
            .collect()
    }
}

pub struct EvalGlobalState<G: GraphViewOps, CS: ComputeState + Send> {
    ss: usize,
    g: G,
    global_state: Global<CS>,
}

impl<G: GraphViewOps, CS: ComputeState + Send> EvalGlobalState<G, CS> {
    pub fn new(ss: usize, g: G, global_state: Global<CS>) -> EvalGlobalState<G, CS> {
        Self {
            ss,
            g,
            global_state,
        }
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
        &self,
        agg_def: &AccId<A, IN, OUT, ACC>,
        f: F,
    ) -> HashMap<String, B>
    where
        OUT: StateType,
        A: 'static,
        F: Fn(OUT) -> B + Copy,
    {
        self.shard_states
            .inner()
            .finalize(agg_def, self.ss, &self.g, f)
    }

    pub fn values(self) -> Shard<CS> {
        self.shard_states
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
        &self,
        agg_def: &AccId<A, IN, OUT, ACC>,
        f: F,
    ) -> HashMap<String, B>
    where
        OUT: StateType,
        A: 'static,
        F: Fn(OUT) -> B + Copy,
    {
        self.local_states
            .iter()
            .flat_map(|state| {
                if let Some(state) = state.as_ref() {
                    state.finalize(agg_def, self.ss, &self.g, f)
                } else {
                    HashMap::<String, B>::new()
                }
            })
            .collect()
    }
}
