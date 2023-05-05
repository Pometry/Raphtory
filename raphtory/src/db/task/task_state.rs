use std::{borrow::Cow, cell::RefCell, rc::Rc, sync::Arc};

use crate::core::state::{compute_state::ComputeState, shuffle_state::ShuffleComputeState};

// this only contains the global state and it is synchronized after each task run
#[derive(Clone, Debug)]
pub struct Global<CS: ComputeState>(Arc<ShuffleComputeState<CS>>);

// this contains the vertex specific shard state and it is synchronized after each task run
#[derive(Clone, Debug)]
pub struct Shard<CS: ComputeState>(Arc<ShuffleComputeState<CS>>);

// this contains the local shard state global and vertex specific and it is not synchronized
#[derive(Clone, Debug)]
pub(crate) struct Local<CS: ComputeState>(Arc<Option<ShuffleComputeState<CS>>>);

impl<CS: ComputeState> Shard<CS> {
    pub(crate) fn new(graph_shards: usize) -> Self {
        Self(Arc::new(ShuffleComputeState::new(graph_shards)))
    }

    pub(crate) fn as_cow_rc(&self) -> Rc<RefCell<Cow<'_, ShuffleComputeState<CS>>>> {
        Rc::new(RefCell::new(Cow::Borrowed(&*self.0)))
    }

    pub(crate) fn as_cow_arc(&self) -> Arc<RefCell<Cow<'_, ShuffleComputeState<CS>>>> {
        Arc::new(RefCell::new(Cow::Borrowed(&*self.0)))
    }

    pub(crate) fn from_state(state: ShuffleComputeState<CS>) -> Shard<CS> {
        Self(Arc::new(state))
    }

    pub fn unwrap(self) -> Arc<ShuffleComputeState<CS>> {
        self.0
    }

    pub fn inner(&self) -> &ShuffleComputeState<CS> {
        &self.0
    }

    pub fn reset(&mut self, ss: usize, resetable_states: &[u32]) {
        Arc::get_mut(&mut self.0).map(|s| {
            s.copy_over_next_ss(ss);
            s.reset_states(ss, resetable_states);
        });
    }
}

impl<CS: ComputeState> From<Arc<ShuffleComputeState<CS>>> for Shard<CS> {
    fn from(state: Arc<ShuffleComputeState<CS>>) -> Self {
        Self(state)
    }
}

impl<CS: ComputeState> Global<CS> {
    pub(crate) fn new() -> Self {
        Self(Arc::new(ShuffleComputeState::new(0)))
    }

    pub(crate) fn as_cow_rc(&self) -> Rc<RefCell<Cow<'_, ShuffleComputeState<CS>>>> {
        Rc::new(RefCell::new(Cow::Borrowed(&*self.0)))
    }

    pub(crate) fn as_cow_arc(&self) -> Arc<RefCell<Cow<'_, ShuffleComputeState<CS>>>> {
        Arc::new(RefCell::new(Cow::Borrowed(&*self.0)))
    }

    pub(crate) fn from_state(global_state: ShuffleComputeState<CS>) -> Global<CS> {
        Self(Arc::new(global_state))
    }

    pub fn unwrap(self) -> Arc<ShuffleComputeState<CS>> {
        self.0
    }

    pub fn inner(&self) -> &ShuffleComputeState<CS> {
        &self.0
    }

    pub fn reset(&mut self, ss: usize, resetable_states: &[u32]) {
        Arc::get_mut(&mut self.0).map(|s| {
            s.copy_over_next_ss(ss);
            s.reset_states(ss, resetable_states);
        });
    }
}

impl<CS: ComputeState> From<Arc<ShuffleComputeState<CS>>> for Global<CS> {
    fn from(state: Arc<ShuffleComputeState<CS>>) -> Self {
        Self(state)
    }
}
