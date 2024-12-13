use crate::{
    core::entities::{edges::edge_ref::EdgeRef, VID},
    db::{
        api::{
            properties::Properties,
            view::{
                internal::{OneHopFilter, Static},
                BaseEdgeViewOps, BoxedLIter, DynamicGraph, IntoDynBoxed, IntoDynamic,
                StaticGraphViewOps,
            },
        },
        graph::{
            edge::EdgeView,
            path::{PathFromGraph, PathFromNode},
        },
    },
    prelude::{GraphViewOps, ResetFilter},
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

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> OneHopFilter<'graph>
    for Edges<'graph, G, GH>
{
    type BaseGraph = G;
    type FilteredGraph = GH;
    type Filtered<GHH: GraphViewOps<'graph> + 'graph> = Edges<'graph, G, GHH>;

    fn current_filter(&self) -> &Self::FilteredGraph {
        &self.graph
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.base_graph
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
    pub fn iter(&self) -> impl Iterator<Item = EdgeView<&G, &GH>> + '_ {
        let base_graph = &self.base_graph;
        let graph = &self.graph;
        (self.edges)().map(move |e| EdgeView::new_filtered(base_graph, graph, e))
    }

    pub fn len(&self) -> usize {
        self.iter().count()
    }

    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    pub fn collect(&self) -> Vec<EdgeView<G, GH>> {
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

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> ResetFilter<'graph>
    for Edges<'graph, G, GH>
{
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> BaseEdgeViewOps<'graph>
    for Edges<'graph, G, GH>
{
    type BaseGraph = G;
    type Graph = GH;
    type ValueType<T>
        = BoxedLIter<'graph, T>
    where
        T: 'graph;
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
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
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

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic + Static>
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

    pub fn collect(&self) -> Vec<Vec<EdgeView<G, GH>>> {
        self.iter().map(|edges| edges.collect()).collect()
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> OneHopFilter<'graph>
    for NestedEdges<'graph, G, GH>
{
    type BaseGraph = G;
    type FilteredGraph = GH;
    type Filtered<GHH: GraphViewOps<'graph> + 'graph> = NestedEdges<'graph, G, GHH>;

    fn current_filter(&self) -> &Self::FilteredGraph {
        &self.graph
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.base_graph
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

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> ResetFilter<'graph>
    for NestedEdges<'graph, G, GH>
{
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> BaseEdgeViewOps<'graph>
    for NestedEdges<'graph, G, GH>
{
    type BaseGraph = G;
    type Graph = GH;
    type ValueType<T>
        = BoxedLIter<'graph, BoxedLIter<'graph, T>>
    where
        T: 'graph;
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
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
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
