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
            state::NodeOp,
            view::{
                internal::{BaseFilter, GraphView},
                BaseNodeViewOps, BoxedLIter, IntoDynBoxed,
            },
        },
        graph::{create_node_type_filter, edges::Edges, node::NodeView, path::PathFromNode},
        task::{
            edge::eval_edges::EvalEdges, eval_graph::EvalGraph, node::eval_node_state::EVState,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_storage::graph::graph::GraphStorage;
use std::{
    cell::{Ref, RefCell, RefMut},
    sync::Arc,
};

pub struct EvalNodeView<'graph, 'a: 'graph, G, S, CS: Clone = ComputeStateVec> {
    pub node: VID,
    pub(crate) eval_graph: EvalGraph<'graph, 'a, G, S, CS>,
    pub(crate) local_state: Option<&'graph mut S>,
}

impl<'graph, 'a: 'graph, G: GraphViewOps<'graph>, CS: ComputeState + 'a, S>
    EvalNodeView<'graph, 'a, G, S, CS>
{
    pub(crate) fn new_local(
        node: VID,
        eval_graph: EvalGraph<'graph, 'a, G, S, CS>,
        local_state: Option<&'graph mut S>,
    ) -> Self {
        Self {
            node,
            eval_graph,
            local_state,
        }
    }
}

impl<'graph, 'a: 'graph, G: GraphViewOps<'graph>, S, CS: ComputeState> Clone
    for EvalNodeView<'graph, 'a, G, S, CS>
{
    fn clone(&self) -> Self {
        Self {
            node: self.node,
            eval_graph: self.eval_graph.clone(),
            local_state: None,
        }
    }
}

impl<'graph, 'a: 'graph, G: GraphViewOps<'graph>, S, CS: ComputeState + 'a>
    EvalNodeView<'graph, 'a, G, S, CS>
{
    pub fn graph(&self) -> EvalGraph<'graph, 'a, G, S, CS> {
        self.eval_graph.clone()
    }

    pub fn prev(&self) -> &S {
        let VID(i) = self.node;
        &self.eval_graph.local_state_prev.state[i]
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
        node: VID,
        eval_graph: EvalGraph<'graph, 'a, G, S, CS>,
        local_state: Option<&'graph mut S>,
    ) -> Self {
        Self {
            node,
            eval_graph,
            local_state,
        }
    }

    fn pid(&self) -> usize {
        let VID(i) = self.node;
        i
    }

    fn node_state(&self) -> Ref<'_, EVState<'a, CS>> {
        RefCell::borrow(&self.eval_graph.node_state)
    }

    fn node_state_mut(&self) -> RefMut<'_, EVState<'a, CS>> {
        RefCell::borrow_mut(&self.eval_graph.node_state)
    }

    pub fn update<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &self,
        id: &AccId<A, IN, OUT, ACC>,
        a: IN,
    ) {
        self.node_state_mut()
            .shard_mut()
            .accumulate_into(self.eval_graph.ss, self.pid(), a, id);
    }

    pub fn global_update<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &self,
        id: &AccId<A, IN, OUT, ACC>,
        a: IN,
    ) {
        self.node_state_mut()
            .global_mut()
            .accumulate_global(self.eval_graph.ss, a, id);
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
        self.node_state()
            .global()
            .read_global(self.eval_graph.ss, agg)
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
        self.node_state()
            .shard()
            .read_with_pid(self.eval_graph.ss, self.pid(), agg_r)
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
        Entry::new(self.node_state(), *agg_r, &self.node, self.eval_graph.ss)
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
        self.node_state()
            .shard()
            .read_with_pid(self.eval_graph.ss + 1, self.pid(), agg_r)
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
        self.node_state()
            .global()
            .read_global(self.eval_graph.ss + 1, agg_r)
            .unwrap_or(ACC::finish(&ACC::zero()))
    }
}

pub struct EvalPathFromNode<'graph, 'a: 'graph, G: GraphViewOps<'graph>, CS: ComputeState, S> {
    pub(crate) eval_graph: EvalGraph<'graph, 'a, G, S, CS>,
    pub(crate) op: Arc<dyn Fn() -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>,
}

impl<'graph, 'a: 'graph, G: GraphViewOps<'graph>, S, CS: ComputeState + 'a>
    EvalPathFromNode<'graph, 'a, G, CS, S>
{
    fn iter_refs(&self) -> impl Iterator<Item = VID> + 'graph {
        (self.op)()
    }

    pub fn iter(&self) -> impl Iterator<Item = EvalNodeView<'graph, 'a, G, S, CS>> + 'graph {
        let base_graph = self.eval_graph.clone();
        self.iter_refs()
            .map(move |v| EvalNodeView::new_filtered(v, base_graph.clone(), None))
    }

    pub fn type_filter<I: IntoIterator<Item = V>, V: AsRef<str>>(&self, node_types: I) -> Self {
        let node_types_filter = create_node_type_filter(
            self.eval_graph.base_graph.node_meta().node_type_meta(),
            node_types,
        );

        let base_graph = self.eval_graph.base_graph.clone();
        let old_op = self.op.clone();

        EvalPathFromNode {
            eval_graph: self.eval_graph.clone(),
            op: Arc::new(move || {
                let base_graph = base_graph.clone();
                let node_types_filter = node_types_filter.clone();
                old_op()
                    .filter(move |v| {
                        let node_type_id = base_graph.node_type_id(*v);
                        node_types_filter[node_type_id]
                    })
                    .into_dyn_boxed()
            }),
        }
    }
}

impl<'graph, 'a: 'graph, G: GraphViewOps<'graph>, S, CS: ComputeState + 'a> IntoIterator
    for EvalPathFromNode<'graph, 'a, G, CS, S>
{
    type Item = EvalNodeView<'graph, 'a, G, S, CS>;
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'graph>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

impl<'graph, 'a: 'graph, G: GraphViewOps<'graph>, S, CS: ComputeState + 'a> Clone
    for EvalPathFromNode<'graph, 'a, G, CS, S>
{
    fn clone(&self) -> Self {
        EvalPathFromNode {
            eval_graph: self.eval_graph.clone(),
            op: self.op.clone(),
        }
    }
}

impl<'graph, 'a: 'graph, G: GraphViewOps<'graph>, S: 'static, CS: ComputeState + 'a>
    BaseNodeViewOps<'graph> for EvalPathFromNode<'graph, 'a, G, CS, S>
{
    type Graph = G;
    type ValueType<T: NodeOp + 'graph> = Box<dyn Iterator<Item = T::Output> + 'graph>;
    type PropType = NodeView<'graph, G>;
    type PathType = EvalPathFromNode<'graph, 'a, G, CS, S>;
    type Edges = EvalEdges<'graph, 'a, G, CS, S>;

    fn graph(&self) -> &Self::Graph {
        &self.eval_graph.base_graph
    }

    fn map<F: NodeOp + 'graph>(&self, op: F) -> Self::ValueType<F>
    where
        <F as NodeOp>::Output: 'graph,
    {
        let storage = self.eval_graph.storage;
        Box::new(self.iter_refs().map(move |node| op.apply(storage, node)))
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
        F: Fn(&GraphStorage, &Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Edges {
        let local_state_prev = self.eval_graph.local_state_prev;
        let node_state = self.eval_graph.node_state.clone();
        let ss = self.eval_graph.ss;
        let storage = self.eval_graph.storage;
        let path =
            PathFromNode::new_one_hop_filtered(self.eval_graph.base_graph.clone(), self.op.clone());
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
        I: Iterator<Item = VID> + Send + Sync + 'graph,
        F: Fn(&GraphStorage, &Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::PathType {
        let old_op = self.op.clone();
        let graph = self.eval_graph.base_graph.clone();
        let storage = self.eval_graph.storage;
        let new_op = Arc::new(move || {
            let op = op.clone();
            let graph = graph.clone();
            old_op()
                .flat_map(move |vv| op(storage, &graph, vv))
                .into_dyn_boxed()
        });

        EvalPathFromNode {
            eval_graph: self.eval_graph.clone(),
            op: new_op,
        }
    }
}

impl<'graph, 'a, S, CS, Current> BaseFilter<'graph> for EvalPathFromNode<'graph, 'a, Current, CS, S>
where
    'a: 'graph,
    Current: GraphViewOps<'graph>,
    CS: ComputeState + 'a,
    S: 'static,
{
    type BaseGraph = Current;
    type Filtered<Next: GraphViewOps<'graph>> = EvalPathFromNode<'graph, 'a, Next, CS, S>;

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.eval_graph.base_graph
    }

    fn apply_filter<Next: GraphViewOps<'graph>>(
        &self,
        filtered_graph: Next,
    ) -> Self::Filtered<Next> {
        EvalPathFromNode {
            eval_graph: self.eval_graph.apply_filter(filtered_graph),
            op: self.op.clone(),
        }
    }
}

impl<'graph, 'a, Current, S, CS> BaseFilter<'graph> for EvalNodeView<'graph, 'a, Current, S, CS>
where
    'a: 'graph,
    Current: GraphViewOps<'graph>,
    CS: ComputeState + 'a,
    S: 'static,
{
    type BaseGraph = Current;
    type Filtered<Next: GraphViewOps<'graph>> = EvalNodeView<'graph, 'a, Next, S, CS>;

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.eval_graph.base_graph
    }

    fn apply_filter<Next: GraphViewOps<'graph>>(
        &self,
        filtered_graph: Next,
    ) -> Self::Filtered<Next> {
        EvalNodeView::new_filtered(
            self.node,
            self.eval_graph.apply_filter(filtered_graph),
            None,
        )
    }
}

impl<'graph, 'a: 'graph, G: GraphView + 'graph, S: 'static, CS: ComputeState + 'a>
    BaseNodeViewOps<'graph> for EvalNodeView<'graph, 'a, G, S, CS>
{
    type Graph = G;
    type ValueType<T: NodeOp>
        = T::Output
    where
        T: 'graph;
    type PropType = NodeView<'graph, G>;
    type PathType = EvalPathFromNode<'graph, 'a, G, CS, S>;
    type Edges = EvalEdges<'graph, 'a, G, CS, S>;

    fn graph(&self) -> &Self::Graph {
        &self.eval_graph.base_graph
    }

    fn map<F: NodeOp + 'graph>(&self, op: F) -> Self::ValueType<F>
    where
        <F as NodeOp>::Output: 'graph,
    {
        op.apply(self.eval_graph.storage, self.node)
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
        F: Fn(&GraphStorage, &Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Edges {
        let ss = self.eval_graph.ss;
        let local_state_prev = self.eval_graph.local_state_prev;
        let node_state = self.eval_graph.node_state.clone();
        let node = self.node;
        let storage = self.eval_graph.storage;
        let graph = self.eval_graph.base_graph.clone();
        let edges = Arc::new(move || op(storage, &graph, node).into_dyn_boxed());
        let edges = Edges {
            base_graph: self.eval_graph.base_graph.clone(),
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
        I: Iterator<Item = VID> + Send + Sync + 'graph,
        F: Fn(&GraphStorage, &Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::PathType {
        let graph = self.eval_graph.base_graph.clone();
        let node = self.node;
        let storage = self.eval_graph.storage;
        let path_op = Arc::new(move || op(storage, &graph, node).into_dyn_boxed());
        EvalPathFromNode {
            eval_graph: self.eval_graph.clone(),
            op: path_op,
        }
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
