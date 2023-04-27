use std::{borrow::Cow, cell::RefCell, rc::Rc, sync::Arc};

use crate::{
    core::{
        state::{ComputeState, ShuffleComputeState, StateType, AccId},
        tgraph::VertexRef, agg::Accumulator,
    },
    db::view_api::internal::GraphViewInternalOps,
};

pub struct EvalVertexView<
    'a,
    G: GraphViewInternalOps + Send + Sync + Clone + 'static,
    CS: ComputeState,
> {
    ss: usize,
    vv: VertexRef,
    g: Arc<G>,
    state: Rc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
}

impl<'a, G: GraphViewInternalOps + Send + Sync + Clone + 'static, CS: ComputeState>
    EvalVertexView<'a, G, CS>
{
    pub fn new(
        ss: usize,
        vertex: VertexRef,
        g: Arc<G>,
        state: Rc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
    ) -> Self {
        Self {
            ss,
            vv: vertex,
            g,
            state,
        }
    }

    pub fn global_id(&self) -> u64 {
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

    pub fn neighbours(&self) -> impl Iterator<Item = EvalVertexView<'a, G, CS>> + '_ {
        self.g
            .neighbours(self.vv, crate::core::Direction::BOTH, None)
            .map(move |vv| EvalVertexView::new(self.ss, vv, self.g.clone(), self.state.clone()))
    }

    pub fn update<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &self,
        id: &AccId<A, IN, OUT, ACC>,
        a: IN,
    ) {
        let mut ref_cow = self.state.borrow_mut();
        let owned_mut = ref_cow.to_mut();
        owned_mut.accumulate_into_pid(self.ss, self.global_id(), self.pid(), a, id);
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
