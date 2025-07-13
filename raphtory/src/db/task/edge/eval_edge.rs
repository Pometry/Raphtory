use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, VID},
        state::compute_state::ComputeState,
    },
    db::{
        api::{
            properties::Properties,
            view::{internal::BaseFilter, *},
        },
        graph::edge::EdgeView,
        task::{
            node::{eval_node::EvalNodeView, eval_node_state::EVState},
            task_state::PrevLocalState,
        },
    },
};

use crate::db::task::edge::eval_edges::EvalEdges;

use crate::db::task::eval_graph::EvalGraph;
use raphtory_storage::graph::graph::GraphStorage;
use std::{cell::RefCell, rc::Rc};

pub struct EvalEdgeView<'graph, 'a, G, CS: Clone, S> {
    pub(crate) ss: usize,
    pub(crate) edge: EdgeView<G>,
    pub(crate) storage: &'graph GraphStorage,
    pub(crate) node_state: Rc<RefCell<EVState<'a, CS>>>,
    pub(crate) local_state_prev: &'graph PrevLocalState<'a, S>,
}

impl<'graph, 'a: 'graph, G: GraphViewOps<'graph>, S, CS: ComputeState + 'a>
    EvalEdgeView<'graph, 'a, G, CS, S>
{
    pub(crate) fn new(
        ss: usize,
        edge: EdgeView<G>,
        storage: &'graph GraphStorage,
        node_state: Rc<RefCell<EVState<'a, CS>>>,
        local_state_prev: &'graph PrevLocalState<'a, S>,
    ) -> Self {
        Self {
            ss,
            edge,
            storage,
            node_state,
            local_state_prev,
        }
    }
}

impl<'graph, 'a: 'graph, G: GraphViewOps<'graph>, S: 'static, CS: ComputeState + 'a>
    BaseEdgeViewOps<'graph> for EvalEdgeView<'graph, 'a, G, CS, S>
{
    type Graph = G;
    type ValueType<T>
        = T
    where
        T: 'graph;
    type PropType = EdgeView<G>;
    type Nodes = EvalNodeView<'graph, 'a, G, S, CS>;
    type Exploded = EvalEdges<'graph, 'a, G, G, CS, S>;

    fn map<O: 'graph, F: Fn(&Self::Graph, EdgeRef) -> O + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        self.edge.map(op)
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.edge.as_props()
    }

    fn map_nodes<F: for<'b> Fn(&'b Self::Graph, EdgeRef) -> VID + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::Nodes {
        let node = self.edge.map_nodes(op);
        let ss = self.ss;
        let node_state = self.node_state.clone();
        let local_state_prev = self.local_state_prev;
        let storage = self.storage;
        let base_graph = self.edge.graph.clone();
        let eval_graph = EvalGraph {
            ss,
            base_graph,
            storage,
            local_state_prev,
            node_state,
        };
        EvalNodeView {
            node: node.node,
            eval_graph,
            local_state: None,
        }
    }

    fn map_exploded<
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
        F: for<'b> Fn(&'b Self::Graph, EdgeRef) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Exploded {
        let edges = self.edge.map_exploded(op);
        let ss = self.ss;
        let node_state = self.node_state.clone();
        let local_state_prev = self.local_state_prev;
        let storage = self.storage;
        EvalEdges {
            ss,
            edges,
            storage,
            node_state,
            local_state_prev,
        }
    }
}

impl<'graph, 'a: 'graph, G: GraphViewOps<'graph>, S, CS: ComputeState + 'a> Clone
    for EvalEdgeView<'graph, 'a, G, CS, S>
{
    fn clone(&self) -> Self {
        Self {
            ss: self.ss,
            edge: self.edge.clone(),
            storage: self.storage,
            node_state: self.node_state.clone(),
            local_state_prev: self.local_state_prev,
        }
    }
}

impl<'graph, 'a: 'graph, Current, S, CS> BaseFilter<'graph>
    for EvalEdgeView<'graph, 'a, Current, CS, S>
where
    'a: 'graph,
    Current: GraphViewOps<'graph>,
    CS: ComputeState + 'a,
{
    type BaseGraph = Current;
    type Filtered<Next: GraphViewOps<'graph>> = EvalEdgeView<'graph, 'a, Next, CS, S>;

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.edge.graph
    }

    fn apply_filter<Next: GraphViewOps<'graph>>(
        &self,
        filtered_graph: Next,
    ) -> Self::Filtered<Next> {
        let edge = self.edge.apply_filter(filtered_graph);
        EvalEdgeView::new(
            self.ss,
            edge,
            self.storage,
            self.node_state.clone(),
            self.local_state_prev,
        )
    }
}
