use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, VID},
        state::compute_state::ComputeState,
    },
    db::{
        api::{
            properties::Properties,
            storage::storage_ops::GraphStorage,
            view::{internal::OneHopFilter, BaseEdgeViewOps, BoxedLIter},
        },
        graph::edges::Edges,
        task::{
            edge::eval_edge::EvalEdgeView,
            eval_graph::EvalGraph,
            node::{eval_node::EvalPathFromNode, eval_node_state::EVState},
            task_state::PrevLocalState,
        },
    },
    prelude::{GraphViewOps, ResetFilter},
};
use std::{cell::RefCell, rc::Rc};

pub struct EvalEdges<'graph, 'a, G, GH, CS: Clone, S> {
    pub(crate) ss: usize,
    pub(crate) edges: Edges<'graph, &'graph G, GH>,
    pub(crate) storage: &'graph GraphStorage,
    pub(crate) node_state: Rc<RefCell<EVState<'a, CS>>>,
    pub(crate) local_state_prev: &'graph PrevLocalState<'a, S>,
}

impl<'graph, 'a: 'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>, CS: Clone, S> Clone
    for EvalEdges<'graph, 'a, G, GH, CS, S>
{
    fn clone(&self) -> Self {
        Self {
            ss: self.ss,
            edges: self.edges.clone(),
            node_state: self.node_state.clone(),
            local_state_prev: self.local_state_prev,
        }
    }
}

impl<'graph, 'a: 'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>, CS: Clone, S>
    OneHopFilter<'graph> for EvalEdges<'graph, 'a, G, GH, CS, S>
{
    type BaseGraph = &'graph G;
    type FilteredGraph = GH;
    type Filtered<GHH: GraphViewOps<'graph> + 'graph> = EvalEdges<'graph, 'a, G, GHH, CS, S>;

    fn current_filter(&self) -> &Self::FilteredGraph {
        &self.edges.graph
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.edges.base_graph
    }

    fn one_hop_filtered<GHH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GHH,
    ) -> Self::Filtered<GHH> {
        let edges = self.edges.one_hop_filtered(filtered_graph);
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
        'a,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        CS: Clone + ComputeState,
        S,
    > EvalEdges<'graph, 'a, G, GH, CS, S>
{
    pub fn iter(&self) -> impl Iterator<Item = EvalEdgeView<'graph, 'a, G, GH, CS, S>> + 'graph {
        let node_state = self.node_state.clone();
        let ss = self.ss;
        let local_state_prev = self.local_state_prev;
        let storage = self.storage;
        self.edges.iter().map(move |edge| EvalEdgeView {
            ss,
            edge,
            storage,
            node_state: node_state.clone(),
            local_state_prev,
        })
    }
}

impl<
        'graph,
        'a,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        CS: Clone + ComputeState,
        S,
    > IntoIterator for EvalEdges<'graph, 'a, G, GH, CS, S>
{
    type Item = EvalEdgeView<'graph, 'a, G, GH, CS, S>;
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'graph>;

    fn into_iter(self) -> Self::IntoIter {
        let node_state = self.node_state;
        let ss = self.ss;
        let local_state_prev = self.local_state_prev;
        let storage = self.storage;
        Box::new(self.edges.iter().map(move |edge| EvalEdgeView {
            ss,
            edge,
            storage,
            node_state: node_state.clone(),
            local_state_prev,
        }))
    }
}

impl<
        'graph,
        'a,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        CS: Clone + ComputeState,
        S,
    > ResetFilter<'graph> for EvalEdges<'graph, 'a, G, GH, CS, S>
{
}

impl<
        'graph,
        'a,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        CS: Clone + ComputeState,
        S,
    > BaseEdgeViewOps<'graph> for EvalEdges<'graph, 'a, G, GH, CS, S>
{
    type BaseGraph = &'graph G;
    type Graph = GH;
    type ValueType<T> = BoxedLIter<'graph, T> where T: 'graph;
    type PropType = <Edges<'graph, &'graph G, GH> as BaseEdgeViewOps<'graph>>::PropType;
    type Nodes = EvalPathFromNode<'graph, 'a, G, &'graph G, CS, S>;
    type Exploded = EvalEdges<'graph, 'a, G, GH, CS, S>;

    fn map<O: 'graph, F: Fn(&Self::Graph, EdgeRef) -> O + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        self.edges.map(op)
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.edges.as_props()
    }

    fn map_nodes<F: for<'b> Fn(&'b Self::Graph, EdgeRef) -> VID + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::Nodes {
        let ss = self.ss;
        let node_state = self.node_state.clone();
        let local_state_prev = self.local_state_prev;
        let path = self.edges.map_nodes(op);
        let base_graph = self.edges.base_graph;
        let storage = self.storage;
        let eval_graph = EvalGraph {
            ss,
            base_graph,
            storage,
            local_state_prev,
            node_state,
        };
        EvalPathFromNode {
            graph: base_graph,
            base_graph: eval_graph,
            op: path.op,
        }
    }

    fn map_exploded<
        I: Iterator<Item = EdgeRef> + Send + 'graph,
        F: for<'b> Fn(&'b Self::Graph, EdgeRef) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Exploded {
        let ss = self.ss;
        let node_state = self.node_state.clone();
        let local_state_prev = self.local_state_prev;
        let edges = self.edges.map_exploded(op);
        let storage = self.storage;
        Self {
            ss,
            storage,
            node_state,
            local_state_prev,
            edges,
        }
    }
}
