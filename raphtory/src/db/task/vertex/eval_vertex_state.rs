use std::{borrow::Cow, cell::RefCell, rc::Rc};

use crate::core::state::{compute_state::ComputeState, shuffle_state::ShuffleComputeState};

#[derive(Debug)]
pub(crate) struct EVState<'a, CS: ComputeState> {
    pub(crate) shard_state: Cow<'a, ShuffleComputeState<CS>>,
    pub(crate) global_state: Cow<'a, ShuffleComputeState<CS>>,
}

impl<'a, CS: ComputeState> EVState<'a, CS> {
    pub fn rc_from(
        shard_state: Cow<'a, ShuffleComputeState<CS>>,
        global_state: Cow<'a, ShuffleComputeState<CS>>,
    ) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self {
            shard_state,
            global_state,
        }))
    }

    pub fn restore_states(
        self,
    ) -> (
        Cow<'a, ShuffleComputeState<CS>>,
        Cow<'a, ShuffleComputeState<CS>>,
    ) {
        (self.shard_state, self.global_state)
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
