use crate::core::time::IntoTime;
use crate::core::vertex_ref::VertexRef;
use crate::core::Prop;
use crate::db::edge::EdgeView;
use crate::db::graph_window::WindowedGraph;
use crate::db::path::PathFromVertex;
use crate::db::task::eval_edge::EvalEdgeView;
use crate::db::vertex::VertexView;
use crate::db::view_api::{BoxedIter, TimeOps, VertexListOps, VertexViewOps};
use crate::{
    core::{
        agg::Accumulator,
        state::{
            accumulator_id::AccId, compute_state::ComputeState, shuffle_state::ShuffleComputeState,
            StateType,
        },
        vertex_ref::LocalVertexRef,
    },
    db::view_api::GraphViewOps,
};
use std::collections::HashMap;
use std::{
    cell::{Ref, RefCell},
    rc::Rc,
    sync::Arc,
};

use super::eval_vertex_state::EVState;

pub struct EvalVertexView<'a, G: GraphViewOps, CS: ComputeState> {
    ss: usize,
    vv: VertexView<G>,
    local_state: Option<&'a mut f64>,
    vertex_state: Rc<RefCell<EVState<'a, CS>>>,
}

impl<'a, G: GraphViewOps, CS: ComputeState> EvalVertexView<'a, G, CS> {
    pub fn unwrap_local_state(&mut self) -> &mut f64 {
        match &mut self.local_state {
            Some(state) => state,
            None => panic!("unwrap on None state"),
        }
    }

    pub(crate) fn new_local(
        ss: usize,
        vertex: LocalVertexRef,
        g: Arc<G>,
        local_state: Option<&'a mut f64>,
        vertex_state: Rc<RefCell<EVState<'a, CS>>>,
    ) -> Self {
        Self {
            ss,
            vv: VertexView::new_local(g, vertex),
            local_state,
            vertex_state,
        }
    }

    pub fn new_edge(&self, e: EdgeView<G>) -> EvalEdgeView<'a, G, CS> {
        EvalEdgeView::new_(self.ss, e, self.vertex_state.clone())
    }

    pub(crate) fn new_from_view(
        ss: usize,
        vv: VertexView<G>,
        local_state: Option<&'a mut f64>,
        vertex_state: Rc<RefCell<EVState<'a, CS>>>,
    ) -> Self {
        Self {
            ss,
            vv,
            local_state,
            vertex_state,
        }
    }

    pub(crate) fn new(
        ss: usize,
        vertex: VertexRef,
        g: Arc<G>,
        local_state: Option<&'a mut f64>,
        vertex_state: Rc<RefCell<EVState<'a, CS>>>,
    ) -> Self {
        let vref = g.localise_vertex_unchecked(vertex);
        Self {
            ss,
            vv: VertexView::new_local(g, vref),
            local_state,
            vertex_state,
        }
    }

    fn pid(&self) -> usize {
        self.vv.vertex.pid
    }

    pub fn update<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &self,
        id: &AccId<A, IN, OUT, ACC>,
        a: IN,
    ) {
        self.vertex_state
            .borrow_mut()
            .shard_mut()
            .accumulate_into_pid(self.ss, self.id(), self.pid(), a, id);
    }

    pub fn update_local<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &self,
        id: &AccId<A, IN, OUT, ACC>,
        a: IN,
    ) {
        // self.local_state
        //     .borrow_mut()
        //     .accumulate_into_pid(self.ss, self.id(), self.pid(), a, id);
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
            .read_with_pid(self.ss, self.id(), self.pid(), agg_r)
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
            &self.vv.vertex,
            self.id(),
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
        // self.local_state
        //     .borrow()
        //     .read_with_pid(self.ss, self.id(), self.pid(), agg_r)
        //     .unwrap_or(ACC::finish(&ACC::zero()))
        todo!()
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
            .read_with_pid(self.ss + 1, self.id(), self.pid(), agg_r)
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
        // self.local_state
        //     .borrow()
        //     .read_with_pid(self.ss + 1, self.id(), self.pid(), agg_r)
        //     .unwrap_or(ACC::finish(&ACC::zero()))
        todo!()
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

pub struct EvalPathFromVertex<'a, G: GraphViewOps, CS: ComputeState> {
    path: PathFromVertex<G>,
    ss: usize,
    vertex_state: Rc<RefCell<EVState<'a, CS>>>,
}

impl<'a, G: GraphViewOps, CS: ComputeState> EvalPathFromVertex<'a, G, CS> {
    fn update_path(&self, path: PathFromVertex<G>) -> Self {
        EvalPathFromVertex {
            path,
            ss: self.ss,
            vertex_state: self.vertex_state.clone(),
        }
    }

    fn new_from_path_and_vertex(
        path: PathFromVertex<G>,
        vertex: &EvalVertexView<'a, G, CS>,
    ) -> EvalPathFromVertex<'a, G, CS> {
        EvalPathFromVertex {
            path,
            ss: vertex.ss,
            vertex_state: vertex.vertex_state.clone(),
        }
    }

    pub fn iter(&'a self) -> Box<dyn Iterator<Item = EvalVertexView<'a, G, CS>> + 'a> {
        Box::new(
            self.path.iter().map(|v| {
                EvalVertexView::new_from_view(self.ss, v, None, self.vertex_state.clone())
            }),
        )
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState> IntoIterator for EvalPathFromVertex<'a, G, CS> {
    type Item = EvalVertexView<'a, G, CS>;
    type IntoIter = Box<dyn Iterator<Item = EvalVertexView<'a, G, CS>> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        // carefully decouple the lifetimes!
        let path = self.path.clone();
        let vertex_state = self.vertex_state.clone();
        let ss = self.ss;
        Box::new(
            path.iter()
                .map(move |v| EvalVertexView::new_from_view(ss, v, None, vertex_state.clone())),
        )
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState> TimeOps for EvalPathFromVertex<'a, G, CS> {
    type WindowedViewType = EvalPathFromVertex<'a, WindowedGraph<G>, CS>;

    fn start(&self) -> Option<i64> {
        self.path.start()
    }

    fn end(&self) -> Option<i64> {
        self.path.end()
    }

    fn window<T: IntoTime>(&self, t_start: T, t_end: T) -> Self::WindowedViewType {
        EvalPathFromVertex {
            path: self.path.window(t_start, t_end),
            ss: self.ss,
            vertex_state: self.vertex_state.clone(),
        }
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState> VertexViewOps for EvalPathFromVertex<'a, G, CS> {
    type Graph = G;
    type ValueType<T> = BoxedIter<T>;
    type PathType<'b> = EvalPathFromVertex<'a, G, CS> where Self: 'b;
    type EList = BoxedIter<EdgeView<G>>;

    fn id(&self) -> Self::ValueType<u64> {
        self.path.id()
    }

    fn name(&self) -> Self::ValueType<String> {
        self.path.name()
    }

    fn earliest_time(&self) -> Self::ValueType<Option<i64>> {
        self.path.earliest_time()
    }

    fn latest_time(&self) -> Self::ValueType<Option<i64>> {
        self.path.latest_time()
    }

    fn property(&self, name: String, include_static: bool) -> Self::ValueType<Option<Prop>> {
        self.path.property(name, include_static)
    }

    fn history(&self) -> Self::ValueType<Vec<i64>> {
        self.path.history()
    }

    fn property_history(&self, name: String) -> Self::ValueType<Vec<(i64, Prop)>> {
        self.path.property_history(name)
    }

    fn properties(&self, include_static: bool) -> Self::ValueType<HashMap<String, Prop>> {
        self.path.properties(include_static)
    }

    fn property_histories(&self) -> Self::ValueType<HashMap<String, Vec<(i64, Prop)>>> {
        self.path.property_histories()
    }

    fn property_names(&self, include_static: bool) -> Self::ValueType<Vec<String>> {
        self.path.property_names(include_static)
    }

    fn has_property(&self, name: String, include_static: bool) -> Self::ValueType<bool> {
        self.path.has_property(name, include_static)
    }

    fn has_static_property(&self, name: String) -> Self::ValueType<bool> {
        self.path.has_static_property(name)
    }

    fn static_property(&self, name: String) -> Self::ValueType<Option<Prop>> {
        self.path.static_property(name)
    }

    fn degree(&self) -> Self::ValueType<usize> {
        self.path.degree()
    }

    fn in_degree(&self) -> Self::ValueType<usize> {
        self.path.in_degree()
    }

    fn out_degree(&self) -> Self::ValueType<usize> {
        self.path.out_degree()
    }

    fn edges(&self) -> Self::EList {
        self.path.edges()
    }

    fn in_edges(&self) -> Self::EList {
        self.path.in_edges()
    }

    fn out_edges(&self) -> Self::EList {
        self.path.out_edges()
    }

    fn neighbours(&self) -> Self::PathType<'a> {
        self.update_path(self.path.neighbours())
    }

    fn in_neighbours(&self) -> Self::PathType<'_> {
        self.update_path(self.path.in_neighbours())
    }

    fn out_neighbours(&self) -> Self::PathType<'_> {
        self.update_path(self.path.out_neighbours())
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState> TimeOps for EvalVertexView<'a, G, CS> {
    type WindowedViewType = EvalVertexView<'a, WindowedGraph<G>, CS>;

    fn start(&self) -> Option<i64> {
        self.vv.start()
    }

    fn end(&self) -> Option<i64> {
        self.vv.end()
    }

    fn window<T: IntoTime>(&self, t_start: T, t_end: T) -> Self::WindowedViewType {
        EvalVertexView::new_local(
            self.ss,
            self.vv.vertex,
            Arc::from(self.vv.graph.window(t_start, t_end)),
            None,
            self.vertex_state.clone(),
        )
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState> VertexViewOps for EvalVertexView<'a, G, CS> {
    type Graph = G;
    type ValueType<T> = T;
    type PathType<'b> = EvalPathFromVertex<'a, G, CS> where Self: 'b;
    type EList = Box<dyn Iterator<Item = EvalEdgeView<'a, G, CS>> + 'a>;

    fn id(&self) -> Self::ValueType<u64> {
        self.vv.id()
    }

    fn name(&self) -> Self::ValueType<String> {
        self.vv.name()
    }

    fn earliest_time(&self) -> Self::ValueType<Option<i64>> {
        self.vv.earliest_time()
    }

    fn latest_time(&self) -> Self::ValueType<Option<i64>> {
        self.vv.latest_time()
    }

    fn property(&self, name: String, include_static: bool) -> Self::ValueType<Option<Prop>> {
        self.vv.property(name, include_static)
    }

    fn history(&self) -> Self::ValueType<Vec<i64>> {
        self.vv.history()
    }

    fn property_history(&self, name: String) -> Self::ValueType<Vec<(i64, Prop)>> {
        self.vv.property_history(name)
    }

    fn properties(&self, include_static: bool) -> Self::ValueType<HashMap<String, Prop>> {
        self.vv.properties(include_static)
    }

    fn property_histories(&self) -> Self::ValueType<HashMap<String, Vec<(i64, Prop)>>> {
        self.vv.property_histories()
    }

    fn property_names(&self, include_static: bool) -> Self::ValueType<Vec<String>> {
        self.vv.property_names(include_static)
    }

    fn has_property(&self, name: String, include_static: bool) -> Self::ValueType<bool> {
        self.vv.has_property(name, include_static)
    }

    fn has_static_property(&self, name: String) -> Self::ValueType<bool> {
        self.vv.has_static_property(name)
    }

    fn static_property(&self, name: String) -> Self::ValueType<Option<Prop>> {
        self.vv.static_property(name)
    }

    fn degree(&self) -> Self::ValueType<usize> {
        self.vv.degree()
    }

    fn in_degree(&self) -> Self::ValueType<usize> {
        self.vv.in_degree()
    }

    fn out_degree(&self) -> Self::ValueType<usize> {
        self.vv.out_degree()
    }

    fn edges(&self) -> Self::EList {
        let ss = self.ss;
        let vertex_state = self.vertex_state.clone();
        Box::new(
            self.vv
                .edges()
                .map(move |e| EvalEdgeView::new_(ss, e, vertex_state.clone())),
        )
    }

    fn in_edges(&self) -> Self::EList {
        let ss = self.ss;
        let vertex_state = self.vertex_state.clone();
        Box::new(
            self.vv
                .in_edges()
                .map(move |e| EvalEdgeView::new_(ss, e, vertex_state.clone())),
        )
    }

    fn out_edges(&self) -> Self::EList {
        let vertex_state = self.vertex_state.clone();
        let ss = self.ss;
        Box::new(
            self.vv
                .out_edges()
                .map(move |e| EvalEdgeView::new_(ss, e, vertex_state.clone())),
        )
    }

    fn neighbours(&self) -> Self::PathType<'_> {
        EvalPathFromVertex::new_from_path_and_vertex(self.vv.neighbours(), self)
    }

    fn in_neighbours(&self) -> Self::PathType<'_> {
        EvalPathFromVertex::new_from_path_and_vertex(self.vv.in_neighbours(), self)
    }

    fn out_neighbours(&self) -> Self::PathType<'_> {
        EvalPathFromVertex::new_from_path_and_vertex(self.vv.out_neighbours(), self)
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
    v_ref: &'a LocalVertexRef,
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
    pub(crate) fn new(
        state: Ref<'a, EVState<'b, CS>>,
        acc_id: AccId<A, IN, OUT, ACC>,
        v_ref: &'a LocalVertexRef,
        gid: u64,
        ss: usize,
    ) -> Entry<'a, 'b, A, IN, OUT, ACC, CS> {
        Entry {
            state,
            acc_id,
            v_ref,
            gid,
            ss,
        }
    }

    /// Returns a reference to the value stored in the `Entry` if it exists.
    pub fn read_ref(&self) -> Option<&A> {
        self.state
            .shard()
            .read_ref_with_pid(self.ss, self.gid, self.v_ref.pid, &self.acc_id)
    }

    pub fn read_local_prev(&'a self) -> Option<&'a f64> {
        self
            .state
            .read_prev(self.v_ref)
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState> VertexListOps
    for Box<dyn Iterator<Item = EvalVertexView<'a, G, CS>> + 'a>
{
    type Graph = G;
    type Vertex = EvalVertexView<'a, G, CS>;
    type IterType<T> = Box<dyn Iterator<Item = T> + 'a>;
    type EList = Box<dyn Iterator<Item = EvalEdgeView<'a, G, CS>> + 'a>;
    type ValueType<T> = T;

    fn earliest_time(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|v| v.earliest_time()))
    }

    fn latest_time(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|v| v.latest_time()))
    }

    fn window(
        self,
        t_start: i64,
        t_end: i64,
    ) -> Self::IterType<<Self::Vertex as TimeOps>::WindowedViewType> {
        Box::new(self.map(move |v| v.window(t_start, t_end)))
    }

    fn id(self) -> Self::IterType<u64> {
        Box::new(self.map(|v| v.id()))
    }

    fn name(self) -> Self::IterType<String> {
        Box::new(self.map(|v| v.name()))
    }

    fn property(self, name: String, include_static: bool) -> Self::IterType<Option<Prop>> {
        Box::new(self.map(move |v| v.property(name.clone(), include_static)))
    }

    fn property_history(self, name: String) -> Self::IterType<Vec<(i64, Prop)>> {
        Box::new(self.map(move |v| v.property_history(name.clone())))
    }

    fn properties(self, include_static: bool) -> Self::IterType<HashMap<String, Prop>> {
        Box::new(self.map(move |v| v.properties(include_static)))
    }

    fn history(self) -> Self::IterType<Vec<i64>> {
        Box::new(self.map(|v| v.history()))
    }

    fn property_histories(self) -> Self::IterType<HashMap<String, Vec<(i64, Prop)>>> {
        Box::new(self.map(|v| v.property_histories()))
    }

    fn property_names(self, include_static: bool) -> Self::IterType<Vec<String>> {
        Box::new(self.map(move |v| v.property_names(include_static)))
    }

    fn has_property(self, name: String, include_static: bool) -> Self::IterType<bool> {
        Box::new(self.map(move |v| v.has_property(name.clone(), include_static)))
    }

    fn has_static_property(self, name: String) -> Self::IterType<bool> {
        Box::new(self.map(move |v| v.has_static_property(name.clone())))
    }

    fn static_property(self, name: String) -> Self::IterType<Option<Prop>> {
        Box::new(self.map(move |v| v.static_property(name.clone())))
    }

    fn degree(self) -> Self::IterType<usize> {
        Box::new(self.map(|v| v.degree()))
    }

    fn in_degree(self) -> Self::IterType<usize> {
        Box::new(self.map(|v| v.in_degree()))
    }

    fn out_degree(self) -> Self::IterType<usize> {
        Box::new(self.map(|v| v.out_degree()))
    }

    fn edges(self) -> Self::EList {
        Box::new(self.flat_map(|v| v.edges()))
    }

    fn in_edges(self) -> Self::EList {
        Box::new(self.flat_map(|v| v.in_edges()))
    }

    fn out_edges(self) -> Self::EList {
        Box::new(self.flat_map(|v| v.out_edges()))
    }

    fn neighbours(self) -> Self {
        Box::new(self.flat_map(|v| v.neighbours()))
    }

    fn in_neighbours(self) -> Self {
        Box::new(self.flat_map(|v| v.in_neighbours()))
    }

    fn out_neighbours(self) -> Self {
        Box::new(self.flat_map(|v| v.out_neighbours()))
    }
}
