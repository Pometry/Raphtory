use rand_distr::weighted_alias::AliasableWeight;
use std::{
    borrow::Cow,
    cell::{Ref, RefCell},
    rc::Rc,
    sync::Arc,
};

use crate::core::tgraph::EdgeRef;
use crate::db::edge::EdgeView;
use crate::db::graph_window::WindowedGraph;
use crate::db::vertex::VertexView;
use crate::db::view_api::{TimeOps, VertexViewOps};
use crate::{
    core::{
        agg::Accumulator,
        state::{
            accumulator_id::AccId, compute_state::ComputeState, shuffle_state::ShuffleComputeState,
            StateType,
        },
        tgraph::VertexRef,
    },
    db::view_api::internal::GraphViewInternalOps,
};

pub struct EvalVertexView<
    'a,
    G: GraphViewInternalOps + Send + Sync + Clone + 'static,
    CS: ComputeState,
> {
    ss: usize,
    vv: VertexView<G>,
    shard_state: Rc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
    global_state: Rc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
    local_state: Rc<RefCell<ShuffleComputeState<CS>>>,
}

impl<'a, G: GraphViewInternalOps + Send + Sync + Clone + 'static, CS: ComputeState>
    EvalVertexView<'a, G, CS>
{
    pub fn new(
        ss: usize,
        vertex: VertexRef,
        g: Arc<G>,
        shard_state: Rc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
        global_state: Rc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
        local_state: Rc<RefCell<ShuffleComputeState<CS>>>,
    ) -> Self {
        Self {
            ss,
            vv: VertexView::new(g, vertex),
            shard_state,
            global_state,
            local_state,
        }
    }

    pub fn new2(
        ss: usize,
        vertex: VertexView<G>,
        shard_state: Rc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
        global_state: Rc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
        local_state: Rc<RefCell<ShuffleComputeState<CS>>>,
    ) -> Self {
        Self {
            ss,
            vv: vertex,
            shard_state,
            global_state,
            local_state,
        }
    }

    pub fn global_id(&self) -> u64 {
        self.vv.id()
    }

    // TODO: do we always look-up the pid in the graph? or when calling neighbours we look-it up?
    fn pid(&self) -> usize {
        if let Some(pid) = self.vv.vertex.pid {
            pid
        } else {
            self.vv
                .graph
                .vertex_ref(self.global_id())
                .and_then(|v_ref| v_ref.pid)
                .unwrap()
        }
    }

    pub fn name(&self) -> String {
        self.vv.name()
    }

    pub fn out_degree(&self) -> usize {
        self.vv
            .graph
            .degree(self.vv.vertex, crate::core::Direction::OUT, None)
    }

    pub fn out_edges(
        &self,
        after: i64,
    ) -> impl Iterator<Item = EvalEdgeView<'a, WindowedGraph<G>, CS>> + '_ {
        self.vv.window(after, i64::MAX).out_edges().map(move |ev| {
            EvalEdgeView::new2(
                self.ss,
                ev,
                self.shard_state.clone(),
                self.global_state.clone(),
                self.local_state.clone(),
            )
        })
    }

    pub fn neighbours(&self) -> impl Iterator<Item = EvalVertexView<'a, G, CS>> + '_ {
        self.vv
            .graph
            .neighbours(self.vv.vertex, crate::core::Direction::BOTH, None)
            .map(move |vv| {
                EvalVertexView::new(
                    self.ss,
                    vv,
                    self.vv.graph.clone(),
                    self.shard_state.clone(),
                    self.global_state.clone(),
                    self.local_state.clone(),
                )
            })
    }

    pub fn neighbours_out(&self) -> impl Iterator<Item = EvalVertexView<'a, G, CS>> + '_ {
        self.vv
            .graph
            .neighbours(self.vv.vertex, crate::core::Direction::OUT, None)
            .map(move |vv| {
                EvalVertexView::new(
                    self.ss,
                    vv,
                    self.vv.graph.clone(),
                    self.shard_state.clone(),
                    self.global_state.clone(),
                    self.local_state.clone(),
                )
            })
    }

    pub fn update<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &self,
        id: &AccId<A, IN, OUT, ACC>,
        a: IN,
    ) {
        let mut ref_cow = self.shard_state.borrow_mut();
        let owned_mut = ref_cow.to_mut();
        owned_mut.accumulate_into_pid(self.ss, self.global_id(), self.pid(), a, id);
    }

    pub fn update_local<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &self,
        id: &AccId<A, IN, OUT, ACC>,
        a: IN,
    ) {
        self.local_state.borrow_mut().accumulate_into_pid(
            self.ss,
            self.global_id(),
            self.pid(),
            a,
            id,
        );
    }

    pub fn global_update<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &self,
        id: &AccId<A, IN, OUT, ACC>,
        a: IN,
    ) {
        let mut ref_cow = self.global_state.borrow_mut();
        let owned_mut = ref_cow.to_mut();
        owned_mut.accumulate_global(self.ss, a, id);
    }

    /// Reads the global state for a given accumulator, returned value is the global
    /// accumulated value for all shards. If the state does not exist, returns None.
    ///
    /// # Arguments
    ///
    /// * `agg` - A reference to the `AccId` struct representing the accumulator.
    ///
    /// # Type Parameters
    ///
    /// * `A` - The type of the state that the accumulator uses.
    /// * `IN` - The input type of the accumulator.
    /// * `OUT` - The output type of the accumulator.
    /// * `ACC` - The type of the accumulator.
    ///
    /// # Return Value
    ///
    /// An optional `OUT` value representing the global state for the accumulator.
    pub fn read_global_state<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg: &AccId<A, IN, OUT, ACC>,
    ) -> Option<OUT>
    where
        OUT: StateType,
        A: StateType,
    {
        self.global_state.borrow().read_global(self.ss, agg)
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
        self.shard_state
            .borrow()
            .read_with_pid(self.ss, self.global_id(), self.pid(), agg_r)
            .unwrap_or(ACC::finish(&ACC::zero()))
    }

    /// Read the current value of the vertex state using the given accumulator.
    /// Returns a default value if the value is not present.
    pub fn entry<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AccId<A, IN, OUT, ACC>,
    ) -> Entry<'_, '_, A, IN, OUT, ACC, CS>
    where
        A: StateType,
        OUT: std::fmt::Debug,
    {
        Entry::new(
            self.shard_state.borrow(),
            *agg_r,
            self.pid(),
            self.global_id(),
            self.ss,
        )
    }

    /// Read the current value of the vertex state using the given accumulator.
    /// Returns a default value if the value is not present.
    pub fn read_local<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AccId<A, IN, OUT, ACC>,
    ) -> OUT
    where
        A: StateType,
        OUT: std::fmt::Debug,
    {
        self.local_state
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
        self.shard_state
            .borrow()
            .read_with_pid(self.ss + 1, self.global_id(), self.pid(), agg_r)
            .unwrap_or(ACC::finish(&ACC::zero()))
    }

    /// Read the prev value of the vertex state using the given accumulator.
    /// Returns a default value if the value is not present.
    pub fn read_local_prev<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AccId<A, IN, OUT, ACC>,
    ) -> OUT
    where
        A: StateType,
        OUT: std::fmt::Debug,
    {
        self.local_state
            .borrow()
            .read_with_pid(self.ss + 1, self.global_id(), self.pid(), agg_r)
            .unwrap_or(ACC::finish(&ACC::zero()))
    }

    pub fn read_global_state_prev<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AccId<A, IN, OUT, ACC>,
    ) -> OUT
        where
            A: StateType,
            OUT: std::fmt::Debug,
    {
        self.global_state
            .borrow()
            .read_global(self.ss + 1, agg_r)
            .unwrap_or(ACC::finish(&ACC::zero()))
    }
}

pub struct EvalEdgeView<
    'a,
    G: GraphViewInternalOps + Send + Sync + Clone + 'static,
    CS: ComputeState,
> {
    ss: usize,
    ev: EdgeView<G>,
    shard_state: Rc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
    global_state: Rc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
    local_state: Rc<RefCell<ShuffleComputeState<CS>>>,
}

impl<'a, G: GraphViewInternalOps + Send + Sync + Clone + 'static, CS: ComputeState>
    EvalEdgeView<'a, G, CS>
{
    pub fn new(
        ss: usize,
        edge: EdgeRef,
        g: Arc<G>,
        shard_state: Rc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
        global_state: Rc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
        local_state: Rc<RefCell<ShuffleComputeState<CS>>>,
    ) -> Self {
        Self {
            ss,
            ev: EdgeView::new(g, edge),
            shard_state,
            global_state,
            local_state,
        }
    }

    pub fn new2(
        ss: usize,
        ev: EdgeView<G>,
        shard_state: Rc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
        global_state: Rc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
        local_state: Rc<RefCell<ShuffleComputeState<CS>>>,
    ) -> Self {
        Self {
            ss,
            ev,
            shard_state,
            global_state,
            local_state,
        }
    }

    pub fn dst(&self) -> EvalVertexView<'a, G, CS> {
        EvalVertexView::new2(
            self.ss,
            self.ev.dst(),
            self.shard_state.clone(),
            self.global_state.clone(),
            self.local_state.clone(),
        )
    }

    pub fn src(&self) -> EvalVertexView<'a, G, CS> {
        EvalVertexView::new2(
            self.ss,
            self.ev.src(),
            self.shard_state.clone(),
            self.global_state.clone(),
            self.local_state.clone(),
        )
    }

    pub fn history(&self) -> Vec<i64> {
        self.ev.history()
    }
}

/// Represents an entry in the shuffle table.
///
/// The entry contains a reference to a `ShuffleComputeState` and an `AccId` representing the accumulator
/// for which the entry is being accessed. It also contains the index of the entry in the shuffle table
/// and the super-step counter.
pub struct Entry<'a, 'b, A: StateType, IN, OUT, ACC: Accumulator<A, IN, OUT>, CS: ComputeState> {
    state: Ref<'a, Cow<'b, ShuffleComputeState<CS>>>,
    acc_id: AccId<A, IN, OUT, ACC>,
    pid: usize,
    gid: u64,
    ss: usize,
}

// Entry implementation has read_ref function to access Option<&A>
impl<'a, 'b, A: StateType, IN, OUT, ACC: Accumulator<A, IN, OUT>, CS: ComputeState>
    Entry<'a, 'b, A, IN, OUT, ACC, CS>
{
    /// Creates a new `Entry` instance.
    ///
    /// # Arguments
    ///
    /// * `state` - A reference to a `ShuffleComputeState` instance.
    /// * `acc_id` - An `AccId` representing the accumulator for which the entry is being accessed.
    /// * `i` - The index of the entry in the shuffle table.
    /// * `ss` - The super-step counter.
    pub fn new(
        state: Ref<'a, Cow<'b, ShuffleComputeState<CS>>>,
        acc_id: AccId<A, IN, OUT, ACC>,
        pid: usize,
        gid: u64,
        ss: usize,
    ) -> Entry<'a, 'b, A, IN, OUT, ACC, CS> {
        Entry {
            state,
            acc_id,
            pid,
            gid,
            ss,
        }
    }

    /// Returns a reference to the value stored in the `Entry` if it exists.
    pub fn read_ref(&self) -> Option<&A> {
        self.state
            .read_ref_with_pid(self.ss, self.gid, self.pid, &self.acc_id)
    }
}
