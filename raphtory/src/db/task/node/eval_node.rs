use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
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
            view::{
                internal::{InternalLayerOps, OneHopFilter},
                BaseNodeViewOps,
            },
        },
        graph::{node::NodeView, path::PathFromNode},
        task::{edge::eval_edge::EvalEdgeView, node::eval_node_state::EVState, task_state::Local2},
    },
    prelude::{GraphViewOps, Layer},
};

use std::{
    cell::{Ref, RefCell},
    rc::Rc,
};

pub struct EvalNodeView<'graph, 'a: 'graph, G, S, GH = &'graph G, CS: Clone = ComputeStateVec> {
    ss: usize,
    node: NodeView<&'graph G, GH>,
    local_state: Option<&'graph mut S>,
    local_state_prev: &'graph Local2<'a, S>,
    node_state: Rc<RefCell<EVState<'a, CS>>>,
}

impl<'graph, 'a: 'graph, G: GraphViewOps<'graph>, CS: ComputeState + 'a, S>
    EvalNodeView<'graph, 'a, G, S, &'graph G, CS>
{
    pub(crate) fn new_local(
        ss: usize,
        v_ref: VID,
        g: &'graph G,
        local_state: Option<&'graph mut S>,
        local_state_prev: &'graph Local2<'a, S>,
        node_state: Rc<RefCell<EVState<'a, CS>>>,
    ) -> Self {
        let node = NodeView {
            base_graph: g,
            graph: g,
            node: v_ref,
        };
        Self {
            ss,
            node,
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
        EvalNodeView::new_from_node(
            self.ss,
            self.node.clone(),
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
        self.node.graph.clone()
    }
    pub fn prev(&self) -> &S {
        let VID(i) = self.node.node;
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

    pub(crate) fn new_from_node(
        ss: usize,
        node: NodeView<&'graph G, GH>,
        local_state: Option<&'graph mut S>,
        local_state_prev: &'graph Local2<'a, S>,
        node_state: Rc<RefCell<EVState<'a, CS>>>,
    ) -> Self {
        Self {
            ss,
            node,
            local_state,
            local_state_prev,
            node_state,
        }
    }

    pub(crate) fn update_node<GHH: GraphViewOps<'graph>>(
        &self,
        node: NodeView<&'graph G, GHH>,
    ) -> EvalNodeView<'graph, 'a, G, S, GHH, CS> {
        EvalNodeView::new_from_node(
            self.ss,
            node,
            None,
            self.local_state_prev,
            self.node_state.clone(),
        )
    }

    fn pid(&self) -> usize {
        let VID(i) = self.node.node;
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
        Entry::new(self.node_state.borrow(), *agg_r, &self.node.node, self.ss)
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
    path: PathFromNode<'graph, &'graph G, GH>,
    ss: usize,
    node_state: Rc<RefCell<EVState<'a, CS>>>,
    local_state_prev: &'graph Local2<'a, S>,
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
    pub fn iter(&self) -> impl Iterator<Item = EvalNodeView<'graph, 'a, G, S, GH, CS>> + 'graph {
        let local = self.local_state_prev;
        let node_state = self.node_state.clone();
        let ss = self.ss;
        self.path
            .iter()
            .map(move |v| EvalNodeView::new_from_node(ss, v, None, local, node_state.clone()))
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
            path: self.path.clone(),
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
    > InternalLayerOps for EvalPathFromNode<'graph, 'a, G, GH, CS, S>
{
    fn layer_ids(&self) -> LayerIds {
        self.path.layer_ids()
    }

    fn layer_ids_from_names(&self, key: Layer) -> LayerIds {
        self.path.layer_ids_from_names(key)
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
    type Edge = EvalEdgeView<'graph, 'a, G, GH, CS, S>;
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
        let node_state = self.node_state.clone();
        let ss = self.ss;
        Box::new(
            self.path
                .map_edges(op)
                .map(move |e| EvalEdgeView::new(ss, e, node_state.clone(), local_state_prev)),
        )
    }

    fn hop<
        I: Iterator<Item = VID> + Send + 'graph,
        F: for<'b> Fn(&'b Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::PathType {
        let path = self.path.hop(op);
        let ss = self.ss;
        let node_state = self.node_state.clone();
        let local_state_prev = self.local_state_prev;
        EvalPathFromNode {
            path,
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
        self.path.current_filter()
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.path.base_graph
    }

    fn one_hop_filtered<GHH: GraphViewOps<'graph>>(
        &self,
        filtered_graph: GHH,
    ) -> Self::Filtered<GHH> {
        let path = self.path.one_hop_filtered(filtered_graph);
        let local_state_prev = self.local_state_prev;
        let node_state = self.node_state.clone();
        let ss = self.ss;
        EvalPathFromNode {
            path,
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
        &self.node.graph
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.node.base_graph
    }

    fn one_hop_filtered<GHH: GraphViewOps<'graph>>(
        &self,
        filtered_graph: GHH,
    ) -> Self::Filtered<GHH> {
        let node = self.node.one_hop_filtered(filtered_graph);
        self.update_node(node)
    }
}

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
        GH: GraphViewOps<'graph>,
    > InternalLayerOps for EvalNodeView<'graph, 'a, G, S, GH, CS>
{
    fn layer_ids(&self) -> LayerIds {
        self.node.layer_ids()
    }

    fn layer_ids_from_names(&self, key: Layer) -> LayerIds {
        self.node.layer_ids_from_names(key)
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
    type PathType = EvalPathFromNode<'graph, 'a, G, &'graph G, CS, S>;
    type PropType = NodeView<&'graph G, GH>;
    type Edge = EvalEdgeView<'graph, 'a, G, GH, CS, S>;
    type EList = Box<dyn Iterator<Item = Self::Edge> + 'graph>;

    fn map<O: 'graph, F: Fn(&Self::Graph, VID) -> O + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        self.node.map(op)
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.node.as_props()
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + 'graph,
        F: for<'b> Fn(&'b Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::EList {
        let ss = self.ss;
        let local_state_prev = self.local_state_prev;
        let node_state = self.node_state.clone();
        Box::new(
            self.node
                .map_edges(op)
                .map(move |e| EvalEdgeView::new(ss, e, node_state.clone(), local_state_prev)),
        )
    }

    fn hop<
        I: Iterator<Item = VID> + Send + 'graph,
        F: for<'b> Fn(&'b Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::PathType {
        let path = self.node.hop(op);
        let ss = self.ss;
        let local_state_prev = self.local_state_prev;
        let node_state = self.node_state.clone();
        EvalPathFromNode {
            path,
            local_state_prev,
            node_state,
            ss,
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
