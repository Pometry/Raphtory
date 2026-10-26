use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, VID},
        state::compute_state::ComputeState,
    },
    db::{
        api::{
            properties::{Metadata, Properties},
            state::Index,
            view::{
                internal::{DynGraphArc, GraphView, InternalFilter},
                BaseEdgeViewOps, BoxedLIter, Select,
            },
        },
        graph::{
            edges::{Edge, EdgeItem, Edges, ExplodedEdge},
            views::filter::CreateFilter,
        },
        task::{
            edge::eval_edge::EvalEdgeView,
            eval_graph::EvalGraph,
            node::{eval_node::EvalPathFromNode, eval_node_state::EVState},
            task_state::PrevLocalState,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_storage::graph::graph::GraphStorage;
use std::{cell::RefCell, rc::Rc};

pub struct EvalEdges<'graph, 'a, G, CS: Clone, S, K = Edge> {
    pub(crate) ss: usize,
    pub(crate) edges: Edges<'graph, G, K>,
    pub(crate) storage: &'graph GraphStorage,
    pub(crate) index: &'graph Index<VID>,
    pub(crate) node_state: Rc<RefCell<EVState<'a, CS>>>,
    pub(crate) local_state_prev: &'graph PrevLocalState<'a, S>,
}

impl<'graph, 'a: 'graph, G: GraphViewOps<'graph>, CS: Clone, S, K: EdgeItem> Clone
    for EvalEdges<'graph, 'a, G, CS, S, K>
{
    fn clone(&self) -> Self {
        Self {
            ss: self.ss,
            edges: self.edges.clone(),
            storage: self.storage,
            index: self.index,
            node_state: self.node_state.clone(),
            local_state_prev: self.local_state_prev,
        }
    }
}

impl<'graph, 'a, Current, CS, S, K> InternalFilter<'graph>
    for EvalEdges<'graph, 'a, Current, CS, S, K>
where
    'a: 'graph,
    Current: GraphViewOps<'graph>,
    CS: Clone,
    K: EdgeItem,
{
    type Graph = Current;
    type Filtered<Next: GraphViewOps<'graph> + 'graph> = EvalEdges<'graph, 'a, Next, CS, S, K>;

    fn base_graph(&self) -> &Self::Graph {
        &self.edges.base_graph
    }

    fn apply_filter<Next: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: Next,
    ) -> Self::Filtered<Next> {
        let edges = self.edges.apply_filter(filtered_graph);
        let ss = self.ss;
        let node_state = self.node_state.clone();
        let local_state_prev = self.local_state_prev;
        let storage = self.storage;
        let index = self.index;
        EvalEdges {
            ss,
            edges,
            storage,
            index,
            node_state,
            local_state_prev,
        }
    }
}

impl<'graph, 'a, G: GraphViewOps<'graph>, CS: Clone + ComputeState, S, K: EdgeItem>
    EvalEdges<'graph, 'a, G, CS, S, K>
{
    pub fn iter(&self) -> impl Iterator<Item = EvalEdgeView<'graph, 'a, G, CS, S>> + 'graph {
        let node_state = self.node_state.clone();
        let ss = self.ss;
        let local_state_prev = self.local_state_prev;
        let storage = self.storage;
        let index = self.index;
        self.edges
            .clone()
            .into_iter()
            .map(move |edge| EvalEdgeView {
                ss,
                edge,
                storage,
                index,
                node_state: node_state.clone(),
                local_state_prev,
            })
    }
}

impl<'graph, 'a, G: GraphViewOps<'graph>, CS: Clone + ComputeState, S, K: EdgeItem> IntoIterator
    for EvalEdges<'graph, 'a, G, CS, S, K>
{
    type Item = EvalEdgeView<'graph, 'a, G, CS, S>;
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'graph>;

    fn into_iter(self) -> Self::IntoIter {
        let node_state = self.node_state;
        let ss = self.ss;
        let local_state_prev = self.local_state_prev;
        let storage = self.storage;
        let index = self.index;
        Box::new(self.edges.into_iter().map(move |edge| EvalEdgeView {
            ss,
            edge,
            storage,
            index,
            node_state: node_state.clone(),
            local_state_prev,
        }))
    }
}

impl<'graph, 'a, G: GraphViewOps<'graph>, CS: Clone + ComputeState, S: 'static, K: EdgeItem>
    BaseEdgeViewOps<'graph> for EvalEdges<'graph, 'a, G, CS, S, K>
{
    type Graph = G;
    type ValueType<T>
        = BoxedLIter<'graph, T>
    where
        T: 'graph;
    type PropType = <Edges<'graph, G, K> as BaseEdgeViewOps<'graph>>::PropType;
    type Nodes = EvalPathFromNode<'graph, 'a, G, CS, S>;
    type Exploded = EvalEdges<'graph, 'a, G, CS, S, ExplodedEdge>;

    fn map<O: 'graph, F: Fn(&Self::Graph, EdgeRef) -> O + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        self.edges.map(op)
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.edges.as_props()
    }

    fn as_metadata(&self) -> Self::ValueType<Metadata<'graph, Self::PropType>> {
        self.edges.as_metadata()
    }

    fn map_nodes<F: Fn(EdgeRef) -> VID + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::Nodes {
        let ss = self.ss;
        let node_state = self.node_state.clone();
        let local_state_prev = self.local_state_prev;
        let path = self.edges.map_nodes(op);
        let base_graph = self.edges.base_graph.clone();
        let storage = self.storage;
        let index = self.index;
        let eval_graph = EvalGraph {
            ss,
            base_graph,
            storage,
            index,
            local_state_prev,
            node_state,
        };
        EvalPathFromNode { eval_graph, path }
    }

    fn map_exploded<
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
        F: Fn(&DynGraphArc<'graph>, EdgeRef) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Exploded {
        let ss = self.ss;
        let node_state = self.node_state.clone();
        let local_state_prev = self.local_state_prev;
        let edges = self.edges.map_exploded(op);
        let storage = self.storage;
        let index = self.index;
        EvalEdges {
            ss,
            storage,
            index,
            node_state,
            local_state_prev,
            edges,
        }
    }
}

impl<'graph, 'a, G: GraphView + 'graph, CS: Clone + ComputeState, S, K: EdgeItem> Select<'graph>
    for EvalEdges<'graph, 'a, G, CS, S, K>
{
    type IterFiltered<Filter: CreateFilter + 'graph> = EvalEdges<'graph, 'a, G, CS, S, K>;

    fn select<F: CreateFilter + 'graph>(
        &self,
        filter: F,
    ) -> Result<Self::IterFiltered<F>, GraphError> {
        let edges = self.edges.select(filter)?;
        let ss = self.ss;
        let node_state = self.node_state.clone();
        let local_state_prev = self.local_state_prev;
        let storage = self.storage;
        let index = self.index;
        Ok(EvalEdges {
            ss,
            storage,
            index,
            node_state,
            local_state_prev,
            edges,
        })
    }
}
