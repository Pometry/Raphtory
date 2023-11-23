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
            view::{BoxedIter, EdgeListOps, EdgeViewOps, TimeOps, VertexListOps, VertexViewOps},
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
use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds},
        state::compute_state::ComputeStateVec,
    },
    db::{
        api::view::{
            internal::{EdgeFilter, OneHopFilter, TimeSemantics},
            BaseVertexViewOps, StaticGraphViewOps,
        },
        graph::{
            path::{Operation, PathFromVertex},
            views::{layer_graph::LayeredGraph, window_graph::WindowedGraph},
        },
    },
    prelude::{GraphViewOps, Layer, LayerOps},
};
use itertools::Itertools;
use std::{
    cell::{Ref, RefCell},
    marker::PhantomData,
    ops::Range,
    rc::Rc,
};

pub struct EvalVertexView<'graph, 'a: 'graph, G, S, GH = &'graph G, CS: Clone = ComputeStateVec> {
    ss: usize,
    vertex: VertexView<&'graph G, GH>,
    local_state: Option<&'graph mut S>,
    local_state_prev: &'graph Local2<'a, S>,
    vertex_state: Rc<RefCell<EVState<'a, CS>>>,
}

impl<'graph, 'a: 'graph, G: GraphViewOps<'graph>, CS: ComputeState + 'a, S>
    EvalVertexView<'graph, 'a, G, S, &'graph G, CS>
{
    pub(crate) fn new_local(
        ss: usize,
        v_ref: VID,
        g: &'graph G,
        local_state: Option<&'graph mut S>,
        local_state_prev: &'graph Local2<'a, S>,
        vertex_state: Rc<RefCell<EVState<'a, CS>>>,
    ) -> Self {
        let vertex = VertexView {
            base_graph: g,
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

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        S,
        CS: ComputeState,
        GH: GraphViewOps<'graph>,
    > Clone for EvalVertexView<'graph, 'a, G, S, GH, CS>
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

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
        GH: GraphViewOps<'graph>,
    > EvalVertexView<'graph, 'a, G, S, GH, CS>
{
    pub fn graph(&self) -> GH {
        self.vertex.graph.clone()
    }
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
        vertex: VertexView<&'graph G, GH>,
        local_state: Option<&'graph mut S>,
        local_state_prev: &'graph Local2<'a, S>,
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

    pub(crate) fn update_vertex<GHH: GraphViewOps<'graph>>(
        &self,
        vertex: VertexView<&'graph G, GHH>,
    ) -> EvalVertexView<'graph, 'a, G, S, GHH, CS> {
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

pub struct EvalPathFromVertex<
    'graph,
    'a: 'graph,
    G: GraphViewOps<'graph>,
    GH: GraphViewOps<'graph>,
    CS: ComputeState,
    S,
> {
    path: PathFromVertex<'graph, &'graph G, GH>,
    ss: usize,
    vertex_state: Rc<RefCell<EVState<'a, CS>>>,
    local_state_prev: &'graph Local2<'a, S>,
}

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
        GH: GraphViewOps<'graph>,
    > EvalPathFromVertex<'graph, 'a, G, GH, CS, S>
{
    pub fn iter(&self) -> impl Iterator<Item = EvalVertexView<'graph, 'a, G, S, GH, CS>> + 'graph {
        let local = self.local_state_prev;
        let vertex_state = self.vertex_state.clone();
        let ss = self.ss;
        self.path
            .iter()
            .map(move |v| EvalVertexView::new_from_vertex(ss, v, None, local, vertex_state.clone()))
    }
}

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
        GH: GraphViewOps<'graph>,
    > IntoIterator for EvalPathFromVertex<'graph, 'a, G, GH, CS, S>
{
    type Item = EvalVertexView<'graph, 'a, G, S, GH, CS>;
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'graph>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
        GH: GraphViewOps<'graph>,
    > Clone for EvalPathFromVertex<'graph, 'a, G, GH, CS, S>
{
    fn clone(&self) -> Self {
        EvalPathFromVertex {
            path: self.path.clone(),
            ss: self.ss,
            vertex_state: self.vertex_state.clone(),
            local_state_prev: self.local_state_prev,
        }
    }
}

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
        GH: GraphViewOps<'graph>,
    > BaseVertexViewOps<'graph> for EvalPathFromVertex<'graph, 'a, G, GH, CS, S>
{
    type BaseGraph = &'graph G;
    type Graph = GH;
    type ValueType<T: 'graph> = Box<dyn Iterator<Item = T> + 'graph>;
    type PropType = VertexView<GH, GH>;
    type PathType = EvalPathFromVertex<'graph, 'a, G, &'graph G, CS, S>;
    type Edge = EvalEdgeView<'graph, 'a, G, CS, S>;
    type EList = Box<dyn Iterator<Item = Self::Edge> + 'graph>;

    fn map<O: 'graph, F: for<'b> Fn(&'b Self::Graph, VID) -> O + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        self.path.map(op)
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.path.as_props()
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + 'graph,
        F: for<'b> Fn(&'b Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
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
                .map(move |e| EvalEdgeView::new(ss, e, vertex_state.clone(), local_state_prev)),
        )
    }

    fn hop<I: Iterator<Item = VID> + Send, F: for<'b> Fn(&'b Self::Graph, VID) -> I>(
        &self,
        op: F,
    ) -> Self::PathType {
        todo!()
    }
}

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
        GH: GraphViewOps<'graph>,
    > OneHopFilter<'graph> for EvalPathFromVertex<'graph, 'a, G, GH, CS, S>
{
    type Graph = GH;
    type Filtered<GHH: GraphViewOps<'graph>> = EvalPathFromVertex<'graph, 'a, G, GHH, CS, S>;

    fn current_filter(&self) -> &Self::Graph {
        self.path.current_filter()
    }

    fn one_hop_filtered<GHH: GraphViewOps<'graph>>(
        &self,
        filtered_graph: GHH,
    ) -> Self::Filtered<GHH> {
        let path = self.path.one_hop_filtered(filtered_graph);
        let local_state_prev = self.local_state_prev;
        let vertex_state = self.vertex_state.clone();
        let ss = self.ss;
        EvalPathFromVertex {
            path,
            ss,
            vertex_state,
            local_state_prev,
        }
    }
}

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
        GH: GraphViewOps<'graph>,
    > OneHopFilter<'graph> for EvalVertexView<'graph, 'a, G, S, GH, CS>
{
    type Graph = GH;
    type Filtered<GHH: GraphViewOps<'graph>> = EvalVertexView<'graph, 'a, G, S, GHH, CS>;

    fn current_filter(&self) -> &Self::Graph {
        &self.vertex.graph
    }

    fn one_hop_filtered<GHH: GraphViewOps<'graph>>(
        &self,
        filtered_graph: GHH,
    ) -> Self::Filtered<GHH> {
        let vertex = self.vertex.one_hop_filtered(filtered_graph);
        self.update_vertex(vertex)
    }
}

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
        GH: GraphViewOps<'graph>,
    > BaseVertexViewOps<'graph> for EvalVertexView<'graph, 'a, G, S, GH, CS>
{
    type BaseGraph = &'graph G;
    type Graph = GH;
    type ValueType<T: 'graph> = T;
    type PropType = VertexView<G, GH>;
    type PathType = EvalPathFromVertex<'graph, 'a, G, &'graph G, CS, S>;
    type Edge = EvalEdgeView<'graph, 'a, G, CS, S>;
    type EList = Box<dyn Iterator<Item = Self::Edge> + 'graph>;

    fn map<O: 'graph, F: for<'b> Fn(&'b Self::Graph, VID) -> O + 'graph>(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        todo!()
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        todo!()
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + 'graph,
        F: for<'b> Fn(&'b Self::Graph, VID) -> I + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::EList {
        todo!()
    }

    fn hop<
        I: Iterator<Item = VID> + Send + 'graph,
        F: for<'b> Fn(&'b Self::Graph, VID) -> I + 'graph,
    >(
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
