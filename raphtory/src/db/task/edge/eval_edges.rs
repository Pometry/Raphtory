use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, VID},
        state::compute_state::ComputeState,
    },
    db::{
        api::{
            properties::Properties,
            view::{internal::OneHopFilter, BaseEdgeViewOps, BoxedLIter},
        },
        graph::edges::Edges,
        task::{
            edge::eval_edge::EvalEdgeView,
            node::{eval_node::EvalPathFromNode, eval_node_state::EVState},
            task_state::Local2,
        },
    },
    prelude::{GraphViewOps, ResetFilter},
};
use std::{cell::RefCell, rc::Rc};

pub struct EvalEdges<'graph, 'a, G, GH, CS: Clone, S> {
    pub(crate) ss: usize,
    pub(crate) edges: Edges<'graph, &'graph G, GH>,
    pub(crate) node_state: Rc<RefCell<EVState<'a, CS>>>,
    pub(crate) local_state_prev: &'graph Local2<'a, S>,
}

impl<'graph, 'a: 'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>, CS: Clone, S> Clone
    for EvalEdges<'graph, 'a, G, GH, CS, S>
{
    fn clone(&self) -> Self {
        todo!()
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
        EvalEdges {
            ss,
            edges,
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
        self.edges.iter().map(move |edge| EvalEdgeView {
            ss,
            edge,
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
        Box::new(self.edges.iter().map(move |edge| EvalEdgeView {
            ss,
            edge,
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
        EvalPathFromNode {
            path,
            ss,
            node_state,
            local_state_prev,
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
        Self {
            ss,
            node_state,
            local_state_prev,
            edges,
        }
    }
}
