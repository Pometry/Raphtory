use crate::{
    core::entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
    db::{
        api::{
            properties::Properties,
            view::{
                internal::OneHopFilter, BaseEdgeViewOps, BaseNodeViewOps, BoxedLIter, IntoDynBoxed,
            },
        },
        graph::{
            edge::EdgeView,
            path::{PathFromGraph, PathFromNode},
        },
    },
    prelude::{EdgeViewOps, GraphViewOps, Layer},
};
use std::sync::Arc;

#[derive(Clone)]
pub struct Edges<'graph, G, GH = G> {
    pub(crate) base_graph: G,
    pub(crate) graph: GH,
    pub(crate) edges: Arc<dyn Fn() -> BoxedLIter<'graph, EdgeRef> + Send + Sync + 'graph>,
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> OneHopFilter<'graph>
    for Edges<'graph, G, GH>
{
    type Graph = GH;
    type Filtered<GHH: GraphViewOps<'graph> + 'graph> = Edges<'graph, G, GHH>;

    fn current_filter(&self) -> &Self::Graph {
        &self.graph
    }

    fn one_hop_filtered<GHH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GHH,
    ) -> Self::Filtered<GHH> {
        let base_graph = self.base_graph.clone();
        let edges = self.edges.clone();
        Edges {
            base_graph,
            graph: filtered_graph,
            edges,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Edges<'graph, G, GH> {
    pub fn iter(&self) -> impl Iterator<Item = EdgeView<G, GH>> + 'graph {
        let base_graph = self.base_graph.clone();
        let graph = self.graph.clone();
        (self.edges)().map(move |e| EdgeView::new_filtered(base_graph.clone(), graph.clone(), e))
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> IntoIterator
    for Edges<'graph, G, GH>
{
    type Item = EdgeView<G, GH>;
    type IntoIter = BoxedLIter<'graph, EdgeView<G, GH>>;

    fn into_iter(self) -> Self::IntoIter {
        let base_graph = self.base_graph.clone();
        let graph = self.graph.clone();
        Box::new(
            (self.edges)()
                .map(move |e| EdgeView::new_filtered(base_graph.clone(), graph.clone(), e)),
        )
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> BaseEdgeViewOps<'graph>
    for Edges<'graph, G, GH>
{
    type BaseGraph = G;
    type Graph = GH;
    type ValueType<T> = BoxedLIter<'graph, T> where T: 'graph;
    type PropType = EdgeView<GH>;
    type Nodes = PathFromNode<'graph, G, G>;
    type Exploded = Self;

    fn map<O: 'graph, F: Fn(&Self::Graph, EdgeRef) -> O + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        let graph = self.graph.clone();
        (self.edges)().map(move |e| op(&graph, e)).into_dyn_boxed()
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.map(|g, e| Properties::new(EdgeView::new(g.clone(), e)))
    }

    fn map_nodes<F: for<'a> Fn(&'a Self::Graph, EdgeRef) -> VID + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::Nodes {
        let graph = self.graph.clone();
        let edges = self.edges.clone();
        PathFromNode::new(self.base_graph.clone(), move || {
            let graph = graph.clone();
            let op = op.clone();
            edges().map(move |e| op(&graph, e)).into_dyn_boxed()
        })
    }

    fn map_exploded<
        I: Iterator<Item = EdgeRef> + Send + 'graph,
        F: for<'a> Fn(&'a Self::Graph, EdgeRef) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Exploded {
        let graph = self.graph.clone();
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

#[derive(Clone)]
pub struct NestedEdges<'graph, G, GH = G> {
    pub(crate) base_graph: G,
    pub(crate) graph: GH,
    pub(crate) nodes: Arc<dyn Fn() -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>,
    pub(crate) edges: Arc<dyn Fn(VID) -> BoxedLIter<'graph, EdgeRef> + Send + Sync + 'graph>,
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> OneHopFilter<'graph>
    for NestedEdges<'graph, G, GH>
{
    type Graph = GH;
    type Filtered<GHH: GraphViewOps<'graph> + 'graph> = NestedEdges<'graph, G, GHH>;

    fn current_filter(&self) -> &Self::Graph {
        &self.graph
    }

    fn one_hop_filtered<GHH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GHH,
    ) -> Self::Filtered<GHH> {
        let base_graph = self.base_graph.clone();
        let edges = self.edges.clone();
        let nodes = self.nodes.clone();
        NestedEdges {
            base_graph,
            graph: filtered_graph,
            nodes,
            edges,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> BaseEdgeViewOps<'graph>
    for NestedEdges<'graph, G, GH>
{
    type BaseGraph = G;
    type Graph = GH;
    type ValueType<T> = BoxedLIter<'graph, BoxedLIter<'graph, T>> where T: 'graph;
    type PropType = EdgeView<GH>;
    type Nodes = PathFromGraph<'graph, G, G>;
    type Exploded = Self;

    fn map<O: 'graph, F: Fn(&Self::Graph, EdgeRef) -> O + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        let graph = self.graph.clone();
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
        let graph = self.graph.clone();
        let base_graph = self.base_graph.clone();
        let edges = self.edges.clone();
        let nodes = self.nodes.clone();
        PathFromGraph::new(base_graph, nodes, move |n| {
            let graph = graph.clone();
            let op = op.clone();
            edges(n).map(move |e| op(&graph, e)).into_dyn_boxed()
        })
    }

    fn map_exploded<
        I: Iterator<Item = EdgeRef> + Send + 'graph,
        F: for<'a> Fn(&'a Self::Graph, EdgeRef) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Exploded {
        let graph = self.graph.clone();
        let edges = self.edges.clone();
        let edges = Arc::new(move |n: VID| {
            let graph = graph.clone();
            let op = op.clone();
            edges(n).flat_map(move |e| op(&graph, e)).into_dyn_boxed()
        });
        let nodes = self.nodes.clone();
        NestedEdges {
            base_graph: self.base_graph.clone(),
            graph: self.graph.clone(),
            nodes,
            edges,
        }
    }
}
