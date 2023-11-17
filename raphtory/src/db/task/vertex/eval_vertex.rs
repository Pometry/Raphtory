use crate::{
    core::entities::{edges::edge_ref::EdgeRef, LayerIds},
    db::{
        api::view::{
            internal::{EdgeFilter, TimeSemantics},
            BaseVertexViewOps,
        },
        graph::{
            path::{Operation, PathFromVertex},
            views::{layer_graph::LayeredGraph, window_graph::WindowedGraph},
        },
    },
    prelude::{Layer, LayerOps},
};
use crate::{
    core::{
        entities::VID,
        state::{accumulator_id::AccId, agg::Accumulator, compute_state::ComputeState, StateType},
        utils::time::IntoTime,
        Direction,
    },
    db::{
        api::{
            properties::Properties,
            view::{
                BoxedIter, EdgeListOps, EdgeViewOps, GraphViewOps, TimeOps, VertexListOps,
                VertexViewOps,
            },
        },
        graph::{
            edge::EdgeView,
            // path::{Operations, PathFromVertex},
            vertex::VertexView,
        },
        task::{
            edge::eval_edge::EvalEdgeView, task_state::Local2, vertex::eval_vertex_state::EVState,
        },
    },
};
use itertools::Itertools;
use std::{
    cell::{Ref, RefCell},
    marker::PhantomData,
    ops::Range,
    rc::Rc,
};

pub struct EvalVertexView<
    'a,
    G: GraphViewOps + 'a,
    GH: GraphViewOps + 'a,
    CS: ComputeState,
    S: 'static,
> {
    ss: usize,
    vertex: VertexView<G, GH>,
    local_state: Option<&'a mut S>,
    local_state_prev: &'a Local2<'a, S>,
    vertex_state: Rc<RefCell<EVState<'a, CS>>>,
}

impl<'a, G: GraphViewOps + 'a, CS: ComputeState, S> EvalVertexView<'a, G, G, CS, S> {
    pub(crate) fn new_local(
        ss: usize,
        v_ref: VID,
        g: G,
        local_state: Option<&'a mut S>,
        local_state_prev: &'a Local2<'a, S>,
        vertex_state: Rc<RefCell<EVState<'a, CS>>>,
    ) -> Self {
        let vertex = VertexView {
            base_graph: g.clone(),
            graph: g,
            vertex: v_ref,
        };
        Self {
            ss,
            vertex,
            local_state,
            local_state_prev,
            vertex_state,
        }
    }
}

impl<'a, G: GraphViewOps, GH: GraphViewOps, CS: ComputeState, S> Clone
    for EvalVertexView<'a, G, GH, CS, S>
{
    fn clone(&self) -> Self {
        EvalVertexView::new_from_vertex(
            self.ss,
            self.vertex.clone(),
            None,
            self.local_state_prev,
            self.vertex_state.clone(),
        )
    }
}

impl<'a, G: GraphViewOps, GH: GraphViewOps, CS: ComputeState, S> EvalVertexView<'a, G, GH, CS, S> {
    pub fn prev(&self) -> &S {
        let VID(i) = self.vertex.vertex;
        &self.local_state_prev.state[i]
    }

    pub fn get_mut(&mut self) -> &mut S {
        match &mut self.local_state {
            Some(state) => state,
            None => panic!("unwrap on None state"),
        }
    }

    pub fn get(&self) -> &S {
        match &self.local_state {
            Some(state) => state,
            None => panic!("unwrap on None state"),
        }
    }

    pub(crate) fn new_from_vertex(
        ss: usize,
        vertex: VertexView<G, GH>,
        local_state: Option<&'a mut S>,
        local_state_prev: &'a Local2<'a, S>,
        vertex_state: Rc<RefCell<EVState<'a, CS>>>,
    ) -> Self {
        Self {
            ss,
            vertex,
            local_state,
            local_state_prev,
            vertex_state,
        }
    }

    pub(crate) fn update_vertex<GHH: GraphViewOps + 'a>(
        &self,
        vertex: VertexView<G, GHH>,
    ) -> EvalVertexView<'a, G, GHH, CS, S> {
        EvalVertexView::new_from_vertex(
            self.ss,
            vertex,
            None,
            self.local_state_prev,
            self.vertex_state.clone(),
        )
    }

    fn pid(&self) -> usize {
        let VID(i) = self.vertex.vertex;
        i
    }

    pub fn update<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &self,
        id: &AccId<A, IN, OUT, ACC>,
        a: IN,
    ) {
        self.vertex_state
            .borrow_mut()
            .shard_mut()
            .accumulate_into(self.ss, self.pid(), a, id);
    }

    pub fn global_update<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &self,
        id: &AccId<A, IN, OUT, ACC>,
        a: IN,
    ) {
        self.vertex_state
            .borrow_mut()
            .global_mut()
            .accumulate_global(self.ss, a, id);
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
        self.vertex_state
            .borrow()
            .global()
            .read_global(self.ss, agg)
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
        self.vertex_state
            .borrow()
            .shard()
            .read_with_pid(self.ss, self.pid(), agg_r)
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
            self.vertex_state.borrow(),
            *agg_r,
            &self.vertex.vertex,
            self.ss,
        )
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
        self.vertex_state
            .borrow()
            .shard()
            .read_with_pid(self.ss + 1, self.pid(), agg_r)
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
        self.vertex_state
            .borrow()
            .global()
            .read_global(self.ss + 1, agg_r)
            .unwrap_or(ACC::finish(&ACC::zero()))
    }
}

pub struct EvalPathFromVertex<'a, G: GraphViewOps + 'a, GH: GraphViewOps + 'a, CS: ComputeState, S>
{
    path: PathFromVertex<G, GH>,
    ss: usize,
    vertex_state: Rc<RefCell<EVState<'a, CS>>>,
    local_state_prev: &'a Local2<'a, S>,
    _s: PhantomData<S>,
}

impl<'a, G: GraphViewOps, GH: GraphViewOps, CS: ComputeState, S: 'static>
    EvalPathFromVertex<'a, G, GH, CS, S>
{
    pub fn iter(&self) -> impl Iterator<Item = EvalVertexView<'a, G, GH, CS, S>> + 'a {
        let local = self.local_state_prev;
        let vertex_state = self.vertex_state.clone();
        let ss = self.ss;
        self.path
            .iter()
            .map(move |v| EvalVertexView::new_from_vertex(ss, v, None, local, vertex_state.clone()))
    }
}

impl<'a, G: GraphViewOps + 'a, GH: GraphViewOps + 'a, CS: ComputeState, S: 'static> IntoIterator
    for EvalPathFromVertex<'a, G, GH, CS, S>
{
    type Item = EvalVertexView<'a, G, GH, CS, S>;
    type IntoIter = Box<dyn Iterator<Item = EvalVertexView<'a, G, GH, CS, S>> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

impl<'a, G: GraphViewOps, GH: GraphViewOps, CS: ComputeState, S: 'static> Clone
    for EvalPathFromVertex<'a, G, GH, CS, S>
{
    fn clone(&self) -> Self {
        EvalPathFromVertex {
            path: self.path.clone(),
            ss: self.ss,
            vertex_state: self.vertex_state.clone(),
            local_state_prev: self.local_state_prev,
            _s: Default::default(),
        }
    }
}

impl<'a, G: GraphViewOps + 'a, GH: GraphViewOps + 'a, CS: ComputeState, S: 'static>
    BaseVertexViewOps for EvalPathFromVertex<'a, G, GH, CS, S>
{
    type BaseGraph = G;
    type Graph = GH;
    type ValueType<T> = Box<dyn Iterator<Item = T> + 'a>;
    type PropType = VertexView<GH, GH>;
    type PathType = EvalPathFromVertex<'a, G, G, CS, S>;
    type Edge = EvalEdgeView<'a, G, CS, S>;
    type EList = Box<dyn Iterator<Item = Self::Edge> + 'a>;

    fn map<O, F: for<'b> Fn(&'b Self::Graph, VID) -> O + Send + Sync>(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        self.path.map(op)
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.path.as_props()
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send,
        F: for<'b> Fn(&'b Self::Graph, VID) -> I + Send + Sync,
    >(
        &self,
        op: F,
    ) -> Self::EList {
        let local_state_prev = self.local_state_prev;
        let vertex_state = self.vertex_state.clone();
        let ss = self.ss;
        Box::new(
            self.path
                .map_edges(op)
                .map(|e| EvalEdgeView::new(ss, e, vertex_state, local_state_prev)),
        )
    }

    fn hop<I: Iterator<Item = VID> + Send, F: for<'b> Fn(&'b Self::Graph, VID) -> I>(
        &self,
        op: F,
    ) -> Self::PathType {
        todo!()
    }
}

impl<'a, G: GraphViewOps, GH: GraphViewOps, CS: ComputeState, S> TimeOps
    for EvalVertexView<'a, G, GH, CS, S>
{
    type WindowedViewType = EvalVertexView<'a, G, WindowedGraph<GH>, CS, S>;

    fn start(&self) -> Option<i64> {
        self.vertex.start()
    }

    fn end(&self) -> Option<i64> {
        self.vertex.end()
    }

    fn window<T: IntoTime>(&self, start: T, end: T) -> Self::WindowedViewType {
        let vertex = self.vertex.window(start, end);
        self.update_vertex(vertex)
    }
}

impl<'a, G: GraphViewOps, GH: GraphViewOps, CS: ComputeState, S> LayerOps
    for EvalVertexView<'a, G, GH, CS, S>
{
    type LayeredViewType = EvalVertexView<'a, G, LayeredGraph<GH>, CS, S>;

    fn layer<L: Into<Layer>>(&self, name: L) -> Option<Self::LayeredViewType> {
        let vertex = self.vertex.layer(name)?;
        Some(self.update_vertex(vertex))
    }
}

impl<'a, G: GraphViewOps, GH: GraphViewOps, CS: ComputeState, S> BaseVertexViewOps
    for EvalVertexView<'a, G, GH, CS, S>
{
    type BaseGraph = G;
    type Graph = GH;
    type ValueType<T> = T;
    type PropType = VertexView<G, GH>;
    type PathType = EvalPathFromVertex<'a, G, G, CS, S>;
    type Edge = EvalEdgeView<'a, G, CS, S>;
    type EList = Box<dyn Iterator<Item = Self::Edge> + 'a>;

    fn map<O, F: for<'b> Fn(&'b Self::Graph, VID) -> O>(&self, op: F) -> Self::ValueType<O> {
        todo!()
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        todo!()
    }

    fn map_edges<I: Iterator<Item = EdgeRef>, F: for<'b> Fn(&'b Self::Graph, VID) -> I>(
        &self,
        op: F,
    ) -> Self::EList {
        todo!()
    }

    fn hop<I: Iterator<Item = VID> + Send, F: for<'b> Fn(&'b Self::Graph, VID) -> I>(
        &self,
        op: F,
    ) -> Self::PathType {
        todo!()
    }
}

/// Represents an entry in the shuffle table.
///
/// The entry contains a reference to a `ShuffleComputeState` and an `AccId` representing the accumulator
/// for which the entry is being accessed. It also contains the index of the entry in the shuffle table
/// and the super-step counter.
pub struct Entry<'a, 'b, A: StateType, IN, OUT, ACC: Accumulator<A, IN, OUT>, CS: ComputeState> {
    state: Ref<'a, EVState<'b, CS>>,
    acc_id: AccId<A, IN, OUT, ACC>,
    v_ref: &'a VID,
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
    pub(crate) fn new(
        state: Ref<'a, EVState<'b, CS>>,
        acc_id: AccId<A, IN, OUT, ACC>,
        v_ref: &'a VID,
        ss: usize,
    ) -> Entry<'a, 'b, A, IN, OUT, ACC, CS> {
        Entry {
            state,
            acc_id,
            v_ref,
            ss,
        }
    }

    /// Returns a reference to the value stored in the `Entry` if it exists.
    pub fn read_ref(&self) -> Option<&A> {
        self.state
            .shard()
            .read_ref(self.ss, (*self.v_ref).into(), &self.acc_id)
    }
}
