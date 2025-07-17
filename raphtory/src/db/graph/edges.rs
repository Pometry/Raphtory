use crate::{
    core::entities::{edges::edge_ref::EdgeRef, VID},
    db::{
        api::{
            properties::Properties,
            view::{
                internal::{BaseFilter, Static},
                BaseEdgeViewOps, BoxedLIter, DynamicGraph, IntoDynBoxed, IntoDynamic,
                StaticGraphViewOps,
            },
        },
        graph::{
            edge::EdgeView,
            path::{PathFromGraph, PathFromNode},
        },
    },
    prelude::GraphViewOps,
};
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

#[derive(Clone)]
pub struct Edges<'graph, G, GH = G> {
    pub(crate) base_graph: G,
    pub(crate) graph: GH,
    pub(crate) edges: Arc<dyn Fn() -> BoxedLIter<'graph, EdgeRef> + Send + Sync + 'graph>,
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Debug for Edges<'graph, G, GH> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<'graph, Current, G> BaseFilter<'graph> for Edges<'graph, Current, G>
where
    Current: GraphViewOps<'graph>,
    G: GraphViewOps<'graph>,
{
    type BaseGraph = Current;
    type Filtered<Next: GraphViewOps<'graph> + 'graph> = Edges<'graph, Next, G>;

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.base_graph
    }

    fn apply_filter<Next: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: Next,
    ) -> Self::Filtered<Next> {
        Edges {
            base_graph: filtered_graph,
            graph: self.graph.clone(),
            edges: self.edges.clone(),
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Edges<'graph, G, GH> {
    pub fn new(
        base_graph: G,
        graph: GH,
        edges: Arc<dyn Fn() -> BoxedLIter<'graph, EdgeRef> + Send + Sync + 'graph>,
    ) -> Self {
        Edges {
            base_graph,
            graph,
            edges,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = EdgeView<&G>> + '_ {
        let graph = &self.base_graph;
        (self.edges)().map(move |e| EdgeView::new_filtered(graph, e))
    }

    pub fn len(&self) -> usize {
        self.iter().count()
    }

    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    pub fn collect(&self) -> Vec<EdgeView<G>> {
        self.iter().map(|e| e.cloned()).collect()
    }

    pub fn get_const_prop_id(&self, prop_name: &str) -> Option<usize> {
        self.graph.edge_meta().get_prop_id(prop_name, true)
    }

    pub fn get_temporal_prop_id(&self, prop_name: &str) -> Option<usize> {
        self.graph.edge_meta().get_prop_id(prop_name, false)
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> IntoIterator
    for Edges<'graph, G, GH>
{
    type Item = EdgeView<G>;
    type IntoIter = BoxedLIter<'graph, EdgeView<G>>;

    fn into_iter(self) -> Self::IntoIter {
        let base_graph = self.base_graph.clone();
        Box::new((self.edges)().map(move |e| EdgeView::new_filtered(base_graph.clone(), e)))
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> BaseEdgeViewOps<'graph>
    for Edges<'graph, G, GH>
{
    type Graph = G;
    type ValueType<T>
        = BoxedLIter<'graph, T>
    where
        T: 'graph;
    type PropType = EdgeView<G>;
    type Nodes = PathFromNode<'graph, G, G>;
    type Exploded = Self;

    fn map<O: 'graph, F: Fn(&Self::Graph, EdgeRef) -> O + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        let graph = self.base_graph.clone();
        (self.edges)().map(move |e| op(&graph, e)).into_dyn_boxed()
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.map(|g, e| Properties::new(EdgeView::new(g.clone(), e)))
    }

    fn map_nodes<F: for<'a> Fn(&'a Self::Graph, EdgeRef) -> VID + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::Nodes {
        let graph = self.base_graph.clone();
        let edges = self.edges.clone();
        PathFromNode::new(self.base_graph.clone(), move || {
            let graph = graph.clone();
            let op = op.clone();
            edges().map(move |e| op(&graph, e)).into_dyn_boxed()
        })
    }

    fn map_exploded<
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
        F: for<'a> Fn(&'a Self::Graph, EdgeRef) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Exploded {
        let graph = self.base_graph.clone();
        let edges = self.edges.clone();
        let edges = Arc::new(move || {
            let graph = graph.clone();
            let op = op.clone();
            edges().flat_map(move |e| op(&graph, e)).into_dyn_boxed()
        });
        Edges {
            base_graph: self.base_graph.clone(),
            graph: self.graph.clone(),
            edges,
        }
    }
}

impl<G: StaticGraphViewOps + IntoDynamic + Static, GH: StaticGraphViewOps + IntoDynamic>
    From<Edges<'static, G, GH>> for Edges<'static, DynamicGraph>
{
    fn from(value: Edges<'static, G, GH>) -> Self {
        Edges {
            base_graph: value.base_graph.into_dynamic(),
            graph: value.graph.into_dynamic(),
            edges: value.edges,
        }
    }
}

#[derive(Clone)]
pub struct NestedEdges<'graph, G, GH = G> {
    pub(crate) base_graph: G,
    pub(crate) graph: GH,
    pub(crate) nodes: Arc<dyn Fn() -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>,
    pub(crate) edges: Arc<dyn Fn(VID) -> BoxedLIter<'graph, EdgeRef> + Send + Sync + 'graph>,
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> NestedEdges<'graph, G, GH> {
    pub fn len(&self) -> usize {
        (self.nodes)().count()
    }

    pub fn is_empty(&self) -> bool {
        (self.nodes)().next().is_none()
    }

    pub fn iter(&self) -> impl Iterator<Item = Edges<'graph, G, GH>> + 'graph {
        let base_graph = self.base_graph.clone();
        let graph = self.graph.clone();
        let edges = self.edges.clone();
        (self.nodes)().map(move |n| {
            let edge_fn = edges.clone();
            Edges {
                base_graph: base_graph.clone(),
                graph: graph.clone(),
                edges: Arc::new(move || edge_fn(n)),
            }
        })
    }

    pub fn collect(&self) -> Vec<Vec<EdgeView<G>>> {
        self.iter().map(|edges| edges.collect()).collect()
    }
}

impl<'graph, G, Current> BaseFilter<'graph> for NestedEdges<'graph, Current, G>
where
    G: GraphViewOps<'graph>,
    Current: GraphViewOps<'graph>,
{
    type BaseGraph = Current;
    type Filtered<Next: GraphViewOps<'graph> + 'graph> = NestedEdges<'graph, Next, G>;

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.base_graph
    }

    fn apply_filter<Next: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: Next,
    ) -> Self::Filtered<Next> {
        NestedEdges {
            base_graph: filtered_graph,
            graph: self.graph.clone(),
            nodes: self.nodes.clone(),
            edges: self.edges.clone(),
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> BaseEdgeViewOps<'graph>
    for NestedEdges<'graph, G, GH>
{
    type Graph = G;
    type ValueType<T>
        = BoxedLIter<'graph, BoxedLIter<'graph, T>>
    where
        T: 'graph;
    type PropType = EdgeView<G>;
    type Nodes = PathFromGraph<'graph, G, G>;
    type Exploded = Self;

    fn map<O: 'graph, F: Fn(&Self::Graph, EdgeRef) -> O + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        let graph = self.base_graph.clone();
        let edges = self.edges.clone();
        (self.nodes)()
            .map(move |n| {
                let graph = graph.clone();
                let op = op.clone();
                edges(n).map(move |e| op(&graph, e)).into_dyn_boxed()
            })
            .into_dyn_boxed()
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.map(|g, e| Properties::new(EdgeView::new(g.clone(), e)))
    }

    fn map_nodes<F: for<'a> Fn(&'a Self::Graph, EdgeRef) -> VID + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::Nodes {
        let graph = self.base_graph.clone();
        let edges = self.edges.clone();
        let edges = move |n| {
            let graph = graph.clone();
            let op = op.clone();
            edges(n).map(move |e| op(&graph, e)).into_dyn_boxed()
        };
        PathFromGraph::new(self.base_graph.clone(), self.nodes.clone(), edges)
    }

    fn map_exploded<
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
        F: for<'a> Fn(&'a Self::Graph, EdgeRef) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Exploded {
        let graph = self.base_graph.clone();
        let edges = self.edges.clone();
        let edges = Arc::new(move |n: VID| {
            let graph = graph.clone();
            let op = op.clone();
            edges(n).flat_map(move |e| op(&graph, e)).into_dyn_boxed()
        });
        NestedEdges {
            base_graph: self.base_graph.clone(),
            graph: self.graph.clone(),
            nodes: self.nodes.clone(),
            edges,
        }
    }
}
