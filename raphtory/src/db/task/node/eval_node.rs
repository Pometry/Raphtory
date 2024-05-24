use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, VID},
        state::{
            accumulator_id::AccId,
            agg::Accumulator,
            compute_state::{ComputeState, ComputeStateVec},
            StateType,
        },
    },
    db::{
        api::{
            properties::Properties,
            storage::storage_ops::GraphStorage,
            view::{internal::OneHopFilter, BaseNodeViewOps, BoxedLIter, IntoDynBoxed},
        },
        graph::{edges::Edges, node::NodeView, path::PathFromNode},
        task::{
            edge::eval_edges::EvalEdges, node::eval_node_state::EVState, task_state::PrevLocalState,
        },
    },
    prelude::{GraphViewOps, NodeTypesFilter},
};
use std::{
    cell::{Ref, RefCell},
    rc::Rc,
    sync::Arc,
};

pub struct EvalNodeView<'graph, 'a: 'graph, G, S, GH = &'graph G, CS: Clone = ComputeStateVec> {
    pub(crate) ss: usize,
    pub(crate) node: VID,
    pub(crate) base_graph: &'graph G,
    pub(crate) graph: GH,
    pub(crate) storage: &'graph GraphStorage,
    pub(crate) local_state: Option<&'graph mut S>,
    pub(crate) local_state_prev: &'graph PrevLocalState<'a, S>,
    pub(crate) node_state: Rc<RefCell<EVState<'a, CS>>>,
}

impl<'graph, 'a: 'graph, G: GraphViewOps<'graph>, CS: ComputeState + 'a, S>
    EvalNodeView<'graph, 'a, G, S, &'graph G, CS>
{
    pub(crate) fn new_local(
        ss: usize,
        node: VID,
        g: &'graph G,
        storage: &'graph GraphStorage,
        local_state: Option<&'graph mut S>,
        local_state_prev: &'graph PrevLocalState<'a, S>,
        node_state: Rc<RefCell<EVState<'a, CS>>>,
    ) -> Self {
        Self {
            ss,
            node,
            base_graph: g,
            graph: g,
            storage,
            local_state,
            local_state_prev,
            node_state,
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
    > Clone for EvalNodeView<'graph, 'a, G, S, GH, CS>
{
    fn clone(&self) -> Self {
        EvalNodeView::new_filtered(
            self.ss,
            self.node,
            self.base_graph,
            self.graph.clone(),
            self.storage,
            None,
            self.local_state_prev,
            self.node_state.clone(),
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
    > EvalNodeView<'graph, 'a, G, S, GH, CS>
{
    pub fn graph(&self) -> GH {
        self.graph.clone()
    }
    pub fn prev(&self) -> &S {
        let VID(i) = self.node;
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

    pub(crate) fn new_filtered(
        ss: usize,
        node: VID,
        base_graph: &'graph G,
        graph: GH,
        storage: &'graph GraphStorage,
        local_state: Option<&'graph mut S>,
        local_state_prev: &'graph PrevLocalState<'a, S>,
        node_state: Rc<RefCell<EVState<'a, CS>>>,
    ) -> Self {
        Self {
            ss,
            node,
            base_graph,
            graph,
            storage,
            local_state,
            local_state_prev,
            node_state,
        }
    }

    fn pid(&self) -> usize {
        let VID(i) = self.node;
        i
    }

    pub fn update<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &self,
        id: &AccId<A, IN, OUT, ACC>,
        a: IN,
    ) {
        self.node_state
            .borrow_mut()
            .shard_mut()
            .accumulate_into(self.ss, self.pid(), a, id);
    }

    pub fn global_update<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &self,
        id: &AccId<A, IN, OUT, ACC>,
        a: IN,
    ) {
        self.node_state
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
        self.node_state.borrow().global().read_global(self.ss, agg)
    }

    /// Read the current value of the node state using the given accumulator.
    /// Returns a default value if the value is not present.
    pub fn read<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AccId<A, IN, OUT, ACC>,
    ) -> OUT
    where
        A: StateType,
        OUT: std::fmt::Debug,
    {
        self.node_state
            .borrow()
            .shard()
            .read_with_pid(self.ss, self.pid(), agg_r)
            .unwrap_or(ACC::finish(&ACC::zero()))
    }

    /// Read the current value of the node state using the given accumulator.
    /// Returns a default value if the value is not present.
    pub fn entry<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AccId<A, IN, OUT, ACC>,
    ) -> Entry<'_, '_, A, IN, OUT, ACC, CS>
    where
        A: StateType,
        OUT: std::fmt::Debug,
    {
        Entry::new(self.node_state.borrow(), *agg_r, &self.node, self.ss)
    }

    /// Read the prev value of the node state using the given accumulator.
    /// Returns a default value if the value is not present.
    pub fn read_prev<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AccId<A, IN, OUT, ACC>,
    ) -> OUT
    where
        A: StateType,
        OUT: std::fmt::Debug,
    {
        self.node_state
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
        self.node_state
            .borrow()
            .global()
            .read_global(self.ss + 1, agg_r)
            .unwrap_or(ACC::finish(&ACC::zero()))
    }
}

pub struct EvalPathFromNode<
    'graph,
    'a: 'graph,
    G: GraphViewOps<'graph>,
    GH: GraphViewOps<'graph>,
    CS: ComputeState,
    S,
> {
    pub graph: GH,
    pub(crate) base_graph: &'graph G,
    pub(crate) op: Arc<dyn Fn() -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>,
    pub(crate) storage: &'graph GraphStorage,
    pub(crate) ss: usize,
    pub(crate) node_state: Rc<RefCell<EVState<'a, CS>>>,
    pub(crate) local_state_prev: &'graph PrevLocalState<'a, S>,
}

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
        GH: GraphViewOps<'graph>,
    > EvalPathFromNode<'graph, 'a, G, GH, CS, S>
{
    fn iter_refs(&self) -> impl Iterator<Item = VID> + 'graph {
        (self.op)()
    }

    pub fn iter(&self) -> impl Iterator<Item = EvalNodeView<'graph, 'a, G, S, GH, CS>> + 'graph {
        let local = self.local_state_prev;
        let node_state = self.node_state.clone();
        let ss = self.ss;
        let base_graph = self.base_graph;
        let graph = self.graph.clone();
        let storage = self.storage;
        self.iter_refs().map(move |v| {
            EvalNodeView::new_filtered(
                ss,
                v,
                base_graph,
                graph.clone(),
                storage,
                None,
                local,
                node_state.clone(),
            )
        })
    }
}

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
        GH: GraphViewOps<'graph>,
    > IntoIterator for EvalPathFromNode<'graph, 'a, G, GH, CS, S>
{
    type Item = EvalNodeView<'graph, 'a, G, S, GH, CS>;
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
    > Clone for EvalPathFromNode<'graph, 'a, G, GH, CS, S>
{
    fn clone(&self) -> Self {
        EvalPathFromNode {
            graph: self.graph.clone(),
            base_graph: self.base_graph,
            op: self.op.clone(),
            storage: self.storage,
            ss: self.ss,
            node_state: self.node_state.clone(),
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
    > BaseNodeViewOps<'graph> for EvalPathFromNode<'graph, 'a, G, GH, CS, S>
{
    type BaseGraph = &'graph G;
    type Graph = GH;
    type ValueType<T: 'graph> = Box<dyn Iterator<Item = T> + 'graph>;
    type PropType = NodeView<GH, GH>;
    type PathType = EvalPathFromNode<'graph, 'a, G, &'graph G, CS, S>;
    type Edges = EvalEdges<'graph, 'a, G, GH, CS, S>;

    fn map<
        O: 'graph,
        F: Fn(&GraphStorage, &Self::Graph, VID) -> O + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        let graph = self.graph.clone();
        let storage = self.storage;
        Box::new(self.iter_refs().map(move |node| op(storage, &graph, node)))
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.map(|_cg, g, v| Properties::new(NodeView::new_internal(g.clone(), v)))
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + 'graph,
        F: Fn(&GraphStorage, &Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Edges {
        let local_state_prev = self.local_state_prev;
        let node_state = self.node_state.clone();
        let ss = self.ss;
        let storage = self.storage;
        let path = PathFromNode::new_one_hop_filtered(
            self.base_graph,
            self.graph.clone(),
            self.op.clone(),
        );
        let edges = path.map_edges(op);
        EvalEdges {
            ss,
            edges,
            node_state,
            local_state_prev,
            storage,
        }
    }

    fn hop<
        I: Iterator<Item = VID> + Send + 'graph,
        F: Fn(&GraphStorage, &Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::PathType {
        let old_op = self.op.clone();
        let graph = self.graph.clone();
        let storage = self.storage;
        let new_op = Arc::new(move || {
            let op = op.clone();
            let graph = graph.clone();
            old_op()
                .flat_map(move |vv| op(storage, &graph, vv))
                .into_dyn_boxed()
        });
        let ss = self.ss;
        let node_state = self.node_state.clone();
        let local_state_prev = self.local_state_prev;
        EvalPathFromNode {
            graph: self.base_graph,
            base_graph: self.base_graph,
            op: new_op,
            storage,
            ss,
            node_state,
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
    > OneHopFilter<'graph> for EvalPathFromNode<'graph, 'a, G, GH, CS, S>
{
    type BaseGraph = &'graph G;
    type FilteredGraph = GH;
    type Filtered<GHH: GraphViewOps<'graph>> = EvalPathFromNode<'graph, 'a, G, GHH, CS, S>;

    fn current_filter(&self) -> &Self::FilteredGraph {
        &self.graph
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.base_graph
    }

    fn one_hop_filtered<GHH: GraphViewOps<'graph>>(
        &self,
        filtered_graph: GHH,
    ) -> Self::Filtered<GHH> {
        let storage = self.storage;
        let local_state_prev = self.local_state_prev;
        let node_state = self.node_state.clone();
        let ss = self.ss;
        EvalPathFromNode {
            graph: filtered_graph,
            base_graph: self.base_graph,
            op: self.op.clone(),
            storage,
            ss,
            node_state,
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
    > OneHopFilter<'graph> for EvalNodeView<'graph, 'a, G, S, GH, CS>
{
    type BaseGraph = &'graph G;
    type FilteredGraph = GH;
    type Filtered<GHH: GraphViewOps<'graph>> = EvalNodeView<'graph, 'a, G, S, GHH, CS>;

    fn current_filter(&self) -> &Self::FilteredGraph {
        &self.graph
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.base_graph
    }

    fn one_hop_filtered<GHH: GraphViewOps<'graph>>(
        &self,
        filtered_graph: GHH,
    ) -> Self::Filtered<GHH> {
        EvalNodeView::new_filtered(
            self.ss,
            self.node,
            self.base_graph,
            filtered_graph,
            self.storage,
            None,
            self.local_state_prev,
            self.node_state.clone(),
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
    > BaseNodeViewOps<'graph> for EvalNodeView<'graph, 'a, G, S, GH, CS>
{
    type BaseGraph = &'graph G;
    type Graph = GH;
    type ValueType<T>  = T where T: 'graph;
    type PropType = NodeView<GH>;
    type PathType = EvalPathFromNode<'graph, 'a, G, &'graph G, CS, S>;
    type Edges = EvalEdges<'graph, 'a, G, GH, CS, S>;

    fn map<
        O: 'graph,
        F: Fn(&GraphStorage, &Self::Graph, VID) -> O + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        op(self.storage, &self.graph, self.node)
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        Properties::new(NodeView::new_internal(self.graph.clone(), self.node))
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + 'graph,
        F: Fn(&GraphStorage, &Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Edges {
        let ss = self.ss;
        let local_state_prev = self.local_state_prev;
        let node_state = self.node_state.clone();
        let node = self.node;
        let storage = self.storage;
        let graph = self.graph.clone();
        let edges = Arc::new(move || op(storage, &graph, node).into_dyn_boxed());
        let edges = Edges {
            base_graph: self.base_graph,
            graph: self.graph.clone(),
            edges,
        };
        EvalEdges {
            ss,
            edges,
            node_state,
            local_state_prev,
            storage,
        }
    }

    fn hop<
        I: Iterator<Item = VID> + Send + 'graph,
        F: Fn(&GraphStorage, &Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::PathType {
        let graph = self.graph.clone();
        let node = self.node;
        let storage = self.storage;

        let path_op = Arc::new(move || op(storage, &graph, node).into_dyn_boxed());
        let ss = self.ss;
        let local_state_prev = self.local_state_prev;
        let node_state = self.node_state.clone();
        EvalPathFromNode {
            graph: self.base_graph,
            base_graph: self.base_graph,
            op: path_op,
            storage,
            local_state_prev,
            node_state,
            ss,
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
    > NodeTypesFilter<'graph> for EvalPathFromNode<'graph, 'a, G, GH, CS, S>
{
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
