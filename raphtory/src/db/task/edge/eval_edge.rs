use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, VID},
        state::compute_state::ComputeState,
    },
    db::{
        api::{
            properties::Properties,
            view::{internal::OneHopFilter, *},
        },
        graph::edge::EdgeView,
        task::{
            node::{eval_node::EvalNodeView, eval_node_state::EVState},
            task_state::PrevLocalState,
        },
    },
};

use crate::db::task::edge::eval_edges::EvalEdges;

use crate::db::{api::storage::graph::storage_ops::GraphStorage, task::eval_graph::EvalGraph};
use std::{cell::RefCell, rc::Rc};

pub struct EvalEdgeView<'graph, 'a, G, GH, CS: Clone, S> {
    pub(crate) ss: usize,
    pub(crate) edge: EdgeView<&'graph G, GH>,
    pub(crate) storage: &'graph GraphStorage,
    pub(crate) node_state: Rc<RefCell<EVState<'a, CS>>>,
    pub(crate) local_state_prev: &'graph PrevLocalState<'a, S>,
}

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
    > EvalEdgeView<'graph, 'a, G, GH, CS, S>
{
    pub(crate) fn new(
        ss: usize,
        edge: EdgeView<&'graph G, GH>,
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

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
    > ResetFilter<'graph> for EvalEdgeView<'graph, 'a, G, GH, CS, S>
{
}

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
    > BaseEdgeViewOps<'graph> for EvalEdgeView<'graph, 'a, G, GH, CS, S>
{
    type BaseGraph = &'graph G;
    type Graph = GH;
    type ValueType<T> = T where T: 'graph;
    type PropType = EdgeView<&'graph G, GH>;
    type Nodes = EvalNodeView<'graph, 'a, G, S, &'graph G, CS>;
    type Exploded = EvalEdges<'graph, 'a, G, GH, CS, S>;

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
        let base_graph = self.edge.base_graph;
        let eval_graph = EvalGraph {
            ss,
            base_graph,
            storage,
            local_state_prev,
            node_state,
        };
        EvalNodeView {
            node: node.node,
            graph: node.base_graph,
            eval_graph,
            local_state: None,
        }
    }

    fn map_exploded<
        I: Iterator<Item = EdgeRef> + Send + 'graph,
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

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
    > Clone for EvalEdgeView<'graph, 'a, G, GH, CS, S>
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

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
    > OneHopFilter<'graph> for EvalEdgeView<'graph, 'a, G, GH, CS, S>
{
    type BaseGraph = &'graph G;
    type FilteredGraph = GH;
    type Filtered<GHH: GraphViewOps<'graph>> = EvalEdgeView<'graph, 'a, G, GHH, CS, S>;

    fn current_filter(&self) -> &Self::FilteredGraph {
        &self.edge.graph
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.edge.base_graph
    }

    fn one_hop_filtered<GHH: GraphViewOps<'graph>>(
        &self,
        filtered_graph: GHH,
    ) -> Self::Filtered<GHH> {
        let edge = self.edge.one_hop_filtered(filtered_graph);
        EvalEdgeView::new(
            self.ss,
            edge,
            self.storage,
            self.node_state.clone(),
            self.local_state_prev,
        )
    }
}
