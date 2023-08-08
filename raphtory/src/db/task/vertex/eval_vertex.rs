use crate::{
    core::{
        entities::{LayerIds, VID},
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
            path::{Operations, PathFromVertex},
            vertex::VertexView,
        },
        task::{
            edge::eval_edge::EvalEdgeView,
            task_state::Local2,
            vertex::{
                eval_vertex_state::EVState,
                window_eval_vertex::{WindowEvalPathFromVertex, WindowEvalVertex},
            },
        },
    },
};
use itertools::Itertools;
use std::{
    cell::{Ref, RefCell},
    marker::PhantomData,
    rc::Rc,
};

pub struct EvalVertexView<'a, G: GraphViewOps, CS: ComputeState, S: 'static> {
    ss: usize,
    vertex: VID,
    pub(crate) graph: &'a G,
    local_state: Option<&'a mut S>,
    local_state_prev: &'a Local2<'a, S>,
    vertex_state: Rc<RefCell<EVState<'a, CS>>>,
}

impl<'a, G: GraphViewOps, CS: ComputeState, S> EvalVertexView<'a, G, CS, S> {
    pub fn prev(&self) -> &S {
        let i: usize = self.vertex.into();
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

    pub(crate) fn new_local(
        ss: usize,
        v_ref: VID,
        g: &'a G,
        local_state: Option<&'a mut S>,
        local_state_prev: &'a Local2<'a, S>,
        vertex_state: Rc<RefCell<EVState<'a, CS>>>,
    ) -> Self {
        Self {
            ss,
            vertex: v_ref,
            graph: g,
            local_state,
            local_state_prev,
            vertex_state,
        }
    }

    fn pid(&self) -> usize {
        self.vertex.into()
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
        Entry::new(self.vertex_state.borrow(), *agg_r, &self.vertex, self.ss)
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

pub struct EvalPathFromVertex<'a, G: GraphViewOps, CS: ComputeState, S> {
    path: PathFromVertex<G>,
    ss: usize,
    g: &'a G,
    vertex_state: Rc<RefCell<EVState<'a, CS>>>,
    local_state_prev: &'a Local2<'a, S>,
    _s: PhantomData<S>,
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> EvalPathFromVertex<'a, G, CS, S> {
    fn update_path(&self, path: PathFromVertex<G>) -> Self {
        EvalPathFromVertex {
            path,
            ss: self.ss,
            g: self.g,
            vertex_state: self.vertex_state.clone(),
            local_state_prev: self.local_state_prev,
            _s: PhantomData,
        }
    }

    fn new_from_path_and_vertex(
        path: PathFromVertex<G>,
        vertex: &EvalVertexView<'a, G, CS, S>,
    ) -> EvalPathFromVertex<'a, G, CS, S> {
        EvalPathFromVertex {
            path,
            ss: vertex.ss,
            g: vertex.graph,
            vertex_state: vertex.vertex_state.clone(),
            local_state_prev: vertex.local_state_prev,
            _s: PhantomData,
        }
    }

    pub fn iter(&'a self) -> Box<dyn Iterator<Item = EvalVertexView<'a, G, CS, S>> + 'a> {
        Box::new(self.path.iter_refs().map(|v| {
            EvalVertexView::new_local(
                self.ss,
                v,
                self.g,
                None,
                self.local_state_prev,
                self.vertex_state.clone(),
            )
        }))
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> IntoIterator
    for EvalPathFromVertex<'a, G, CS, S>
{
    type Item = EvalVertexView<'a, G, CS, S>;
    type IntoIter = Box<dyn Iterator<Item = EvalVertexView<'a, G, CS, S>> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        // carefully decouple the lifetimes!
        let path = self.path.clone();
        let vertex_state = self.vertex_state.clone();
        let ss = self.ss;
        let g: &G = self.g;
        Box::new(path.iter_refs().map(move |v| {
            EvalVertexView::new_local(ss, v, g, None, self.local_state_prev, vertex_state.clone())
        }))
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> TimeOps
    for EvalPathFromVertex<'a, G, CS, S>
{
    type WindowedViewType = WindowEvalPathFromVertex<'a, G, CS, S>;

    fn start(&self) -> Option<i64> {
        self.path.start()
    }

    fn end(&self) -> Option<i64> {
        self.path.end()
    }

    fn window<T: IntoTime>(&self, t_start: T, t_end: T) -> Self::WindowedViewType {
        WindowEvalPathFromVertex::new(
            self.path.clone(),
            self.ss,
            self.g,
            self.vertex_state.clone(),
            self.local_state_prev,
            t_start.into_time(),
            t_end.into_time(),
        )
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> VertexViewOps
    for EvalPathFromVertex<'a, G, CS, S>
{
    type Graph = G;
    type ValueType<T> = BoxedIter<T>;
    type PathType<'b> = EvalPathFromVertex<'a, G, CS, S> where Self: 'b;
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

    fn history(&self) -> Self::ValueType<Vec<i64>> {
        self.path.history()
    }

    fn properties(&self) -> Self::ValueType<Properties<VertexView<G>>> {
        self.path.properties()
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

impl<'a, G: GraphViewOps, CS: ComputeState, S> TimeOps for EvalVertexView<'a, G, CS, S> {
    type WindowedViewType = WindowEvalVertex<'a, G, CS, S>;

    fn start(&self) -> Option<i64> {
        self.graph.start()
    }

    fn end(&self) -> Option<i64> {
        self.graph.end()
    }

    fn window<T: IntoTime>(&self, t_start: T, t_end: T) -> Self::WindowedViewType {
        WindowEvalVertex::new(
            self.ss,
            self.vertex,
            self.graph,
            None,
            self.local_state_prev,
            self.vertex_state.clone(),
            t_start.into_time(),
            t_end.into_time(),
        )
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> VertexViewOps
    for EvalVertexView<'a, G, CS, S>
{
    type Graph = G;
    type ValueType<T> = T;
    type PathType<'b> = EvalPathFromVertex<'a, G, CS, S> where Self: 'b;
    type EList = Box<dyn Iterator<Item = EvalEdgeView<'a, G, CS, S>> + 'a>;

    fn id(&self) -> Self::ValueType<u64> {
        self.graph.vertex_id(self.vertex)
    }

    fn name(&self) -> Self::ValueType<String> {
        self.graph.vertex_name(self.vertex)
    }

    fn earliest_time(&self) -> Self::ValueType<Option<i64>> {
        self.graph.vertex_earliest_time(self.vertex)
    }

    fn latest_time(&self) -> Self::ValueType<Option<i64>> {
        self.graph.vertex_latest_time(self.vertex)
    }

    fn history(&self) -> Self::ValueType<Vec<i64>> {
        self.edges()
            .map(|e| e.explode().earliest_time().flatten())
            .kmerge()
            .dedup()
            .collect()
    }

    fn properties(&self) -> Self::ValueType<Properties<VertexView<G>>> {
        //FIXME: need to implement this properly without cloning the graph...
        Properties::new(VertexView::new_local(self.graph.clone(), self.vertex))
    }

    fn degree(&self) -> Self::ValueType<usize> {
        self.graph
            .degree(self.vertex, Direction::BOTH, LayerIds::All)
    }

    fn in_degree(&self) -> Self::ValueType<usize> {
        self.graph.degree(self.vertex, Direction::IN, LayerIds::All)
    }

    fn out_degree(&self) -> Self::ValueType<usize> {
        self.graph
            .degree(self.vertex, Direction::OUT, LayerIds::All)
    }

    fn edges(&self) -> Self::EList {
        let ss = self.ss;
        let vertex_state = self.vertex_state.clone();
        let local = self.local_state_prev;
        let graph = self.graph;
        Box::new(
            self.graph
                .vertex_edges(self.vertex, Direction::BOTH, LayerIds::All)
                .map(move |e| EvalEdgeView::new_(ss, e, graph, local, vertex_state.clone())),
        )
    }

    fn in_edges(&self) -> Self::EList {
        let ss = self.ss;
        let vertex_state = self.vertex_state.clone();
        let local = self.local_state_prev;
        let graph = self.graph;
        Box::new(
            self.graph
                .vertex_edges(self.vertex, Direction::IN, LayerIds::All)
                .map(move |e| EvalEdgeView::new_(ss, e, graph, local, vertex_state.clone())),
        )
    }

    fn out_edges(&self) -> Self::EList {
        let ss = self.ss;
        let vertex_state = self.vertex_state.clone();
        let local = self.local_state_prev;
        let graph = self.graph;
        Box::new(
            self.graph
                .vertex_edges(self.vertex, Direction::OUT, LayerIds::All)
                .map(move |e| EvalEdgeView::new_(ss, e, graph, local, vertex_state.clone())),
        )
    }

    fn neighbours(&self) -> Self::PathType<'_> {
        let neighbours = PathFromVertex::new(
            self.graph.clone(),
            self.vertex,
            Operations::Neighbours {
                dir: Direction::BOTH,
            },
        );

        EvalPathFromVertex::new_from_path_and_vertex(neighbours, self)
    }

    fn in_neighbours(&self) -> Self::PathType<'_> {
        let neighbours = PathFromVertex::new(
            self.graph.clone(),
            self.vertex,
            Operations::Neighbours { dir: Direction::IN },
        );

        EvalPathFromVertex::new_from_path_and_vertex(neighbours, self)
    }

    fn out_neighbours(&self) -> Self::PathType<'_> {
        let neighbours = PathFromVertex::new(
            self.graph.clone(),
            self.vertex,
            Operations::Neighbours {
                dir: Direction::OUT,
            },
        );

        EvalPathFromVertex::new_from_path_and_vertex(neighbours, self)
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

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> VertexListOps
    for Box<dyn Iterator<Item = EvalVertexView<'a, G, CS, S>> + 'a>
{
    type Graph = G;
    type Vertex = EvalVertexView<'a, G, CS, S>;
    type IterType<T> = Box<dyn Iterator<Item = T> + 'a>;
    type EList = Box<dyn Iterator<Item = EvalEdgeView<'a, G, CS, S>> + 'a>;
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

    fn properties(self) -> Self::IterType<Properties<VertexView<G>>> {
        Box::new(self.map(move |v| v.properties()))
    }

    fn history(self) -> Self::IterType<Vec<i64>> {
        Box::new(self.map(|v| v.history()))
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
