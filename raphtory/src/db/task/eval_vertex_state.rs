use std::{borrow::{Cow, Borrow}, cell::RefCell, rc::Rc};

use crate::core::{
    state::{compute_state::ComputeState, shuffle_state::ShuffleComputeState},
    vertex_ref::LocalVertexRef,
};

pub(crate) struct EVState<'a, CS: ComputeState> {
    pub(crate) shard_state: Cow<'a, ShuffleComputeState<CS>>,
    pub(crate) global_state: Cow<'a, ShuffleComputeState<CS>>,
    pub(crate) local_state_prev: &'a Vec<Option<(LocalVertexRef, f64)>>,
    shard_size: usize,
}

impl<'a, CS: ComputeState> EVState<'a, CS> {
    pub fn rc_from(
        shard_state: Cow<'a, ShuffleComputeState<CS>>,
        global_state: Cow<'a, ShuffleComputeState<CS>>,
        local_state_prev: &'a Vec<Option<(LocalVertexRef, f64)>>,
        shard_size: usize,
    ) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self {
            shard_state,
            global_state,
            local_state_prev,
            shard_size,
        }))
    }

    pub fn read_prev(&self, v: &LocalVertexRef) -> Option<&f64> {
        let LocalVertexRef { shard_id, pid } = v;
        let i = self.shard_size * *shard_id + *pid;
        self.local_state_prev[i].as_ref().map(|(_, val)| val)
    }

    pub(crate) fn shard_mut(&mut self) -> &mut ShuffleComputeState<CS> {
        self.shard_state.to_mut()
    }

    pub(crate) fn global_mut(&mut self) -> &mut ShuffleComputeState<CS> {
        self.global_state.to_mut()
    }

    pub(crate) fn shard(&self) -> &ShuffleComputeState<CS> {
        &self.shard_state
    }

    pub(crate) fn global(&self) -> &ShuffleComputeState<CS> {
        &self.global_state
    }
}
