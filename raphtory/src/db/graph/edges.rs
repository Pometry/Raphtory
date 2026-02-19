use crate::{
    core::entities::{edges::edge_ref::EdgeRef, VID},
    db::{
        api::{
            properties::{Metadata, Properties},
            view::{
                internal::{FilterOps, InternalEdgeSelect, InternalFilter, Static},
                BaseEdgeViewOps, BoxedLIter, DynamicGraph, IntoDynBoxed, IntoDynamic,
                StaticGraphViewOps,
            },
        },
        graph::{
            edge::EdgeView,
            path::{PathFromGraph, PathFromNode},
            views::layer_graph::LayeredGraph,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::LayerIds;
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

#[derive(Clone)]
pub struct Edges<'graph, G> {
    pub(crate) base_graph: G,
    pub(crate) edges: Arc<dyn Fn() -> BoxedLIter<'graph, EdgeRef> + Send + Sync + 'graph>,
}

impl<'graph, G: IntoDynamic> Edges<'graph, G> {
    pub fn into_dyn(self) -> Edges<'graph, DynamicGraph> {
        Edges {
            base_graph: self.base_graph.into_dynamic(),
            edges: self.edges,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>> Debug for Edges<'graph, G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<'graph, Current> InternalFilter<'graph> for Edges<'graph, Current>
where
    Current: GraphViewOps<'graph>,
{
    type Graph = Current;
    type Filtered<Next: GraphViewOps<'graph> + 'graph> = Edges<'graph, Next>;

    fn base_graph(&self) -> &Self::Graph {
        &self.base_graph
    }

    fn apply_filter<Next: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: Next,
    ) -> Self::Filtered<Next> {
        Edges {
            base_graph: filtered_graph,
            edges: self.edges.clone(),
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>> Edges<'graph, G> {
    pub fn new(
        base_graph: G,
        edges: Arc<dyn Fn() -> BoxedLIter<'graph, EdgeRef> + Send + Sync + 'graph>,
    ) -> Self {
        Edges { base_graph, edges }
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

    /// Collect all nodes into a vec
    pub fn collect(&self) -> Vec<EdgeView<G>> {
        self.iter().map(|e| e.cloned()).collect()
    }

    pub fn get_metadata_id(&self, prop_name: &str) -> Option<usize> {
        self.base_graph.edge_meta().get_prop_id(prop_name, true)
    }

    pub fn get_temporal_prop_id(&self, prop_name: &str) -> Option<usize> {
        self.base_graph.edge_meta().get_prop_id(prop_name, false)
    }
}

impl<'graph, G: GraphViewOps<'graph>> IntoIterator for Edges<'graph, G> {
    type Item = EdgeView<G>;
    type IntoIter = BoxedLIter<'graph, EdgeView<G>>;

    fn into_iter(self) -> Self::IntoIter {
        let base_graph = self.base_graph.clone();
        Box::new((self.edges)().map(move |e| EdgeView::new_filtered(base_graph.clone(), e)))
    }
}

impl<'graph, G: GraphViewOps<'graph>> BaseEdgeViewOps<'graph> for Edges<'graph, G> {
    type Graph = G;
    type ValueType<T>
        = BoxedLIter<'graph, T>
    where
        T: 'graph;
    type PropType = EdgeView<G>;
    type Nodes = PathFromNode<'graph, G>;
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

    fn as_metadata(&self) -> Self::ValueType<Metadata<'graph, Self::PropType>> {
        self.map(|g, e| Metadata::new(EdgeView::new(g.clone(), e)))
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
            edges,
        }
    }
}

impl<G: StaticGraphViewOps + IntoDynamic + Static> From<Edges<'static, G>>
    for Edges<'static, DynamicGraph>
{
    fn from(value: Edges<'static, G>) -> Self {
        Edges {
            base_graph: value.base_graph.into_dynamic(),
            edges: value.edges,
        }
    }
}

impl<'graph, G> InternalEdgeSelect<'graph> for Edges<'graph, G>
where
    G: GraphViewOps<'graph> + 'graph,
{
    type IterGraph = G;
    type IterFiltered<FilteredGraph: GraphViewOps<'graph> + 'graph> = Edges<'graph, G>;

    fn iter_graph(&self) -> &Self::IterGraph {
        &self.base_graph
    }

    fn apply_iter_filter<FilteredGraph: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: FilteredGraph,
    ) -> Self::IterFiltered<FilteredGraph> {
        let edges = self.edges.clone();
        Edges {
            base_graph: self.base_graph.clone(),
            edges: Arc::new(move || {
                let filtered_graph = filtered_graph.clone();
                let edges_locked = filtered_graph.core_edges();
                Box::new(edges().filter(move |e_ref| match e_ref.layer() {
                    Some(l) => match e_ref.time() {
                        Some(t) => {
                            filtered_graph.filter_exploded_edge(e_ref.pid().with_layer(l), t)
                        }
                        None => {
                            let lg = LayeredGraph::new(&filtered_graph, LayerIds::One(l.0));
                            lg.filter_edge(edges_locked.edge(e_ref.pid()))
                        }
                    },
                    None => filtered_graph.filter_edge(edges_locked.edge(e_ref.pid())),
                }))
            }),
        }
    }
}

#[derive(Clone)]
pub struct NestedEdges<'graph, G> {
    pub(crate) graph: G,
    pub(crate) nodes: Arc<dyn Fn() -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>,
    pub(crate) edges: Arc<dyn Fn(VID) -> BoxedLIter<'graph, EdgeRef> + Send + Sync + 'graph>,
}

impl<'graph, G: GraphViewOps<'graph>> NestedEdges<'graph, G> {
    pub fn new(
        graph: G,
        nodes: Arc<dyn Fn() -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>,
        edges: Arc<dyn Fn(VID) -> BoxedLIter<'graph, EdgeRef> + Send + Sync + 'graph>,
    ) -> Self {
        NestedEdges {
            graph,
            nodes,
            edges,
        }
    }

    pub fn len(&self) -> usize {
        (self.nodes)().count()
    }

    pub fn is_empty(&self) -> bool {
        (self.nodes)().next().is_none()
    }

    pub fn iter(&self) -> impl Iterator<Item = Edges<'graph, G>> + 'graph {
        let base_graph = self.graph.clone();
        let edges = self.edges.clone();
        (self.nodes)().map(move |n| {
            let edge_fn = edges.clone();
            Edges {
                base_graph: base_graph.clone(),
                edges: Arc::new(move || edge_fn(n)),
            }
        })
    }

    pub fn collect(&self) -> Vec<Vec<EdgeView<G>>> {
        self.iter().map(|edges| edges.collect()).collect()
    }
}

impl<'graph, Current> InternalFilter<'graph> for NestedEdges<'graph, Current>
where
    Current: GraphViewOps<'graph>,
{
    type Graph = Current;
    type Filtered<Next: GraphViewOps<'graph> + 'graph> = NestedEdges<'graph, Next>;

    fn base_graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn apply_filter<Next: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: Next,
    ) -> Self::Filtered<Next> {
        NestedEdges {
            graph: filtered_graph,
            nodes: self.nodes.clone(),
            edges: self.edges.clone(),
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>> BaseEdgeViewOps<'graph> for NestedEdges<'graph, G> {
    type Graph = G;
    type ValueType<T>
        = BoxedLIter<'graph, BoxedLIter<'graph, T>>
    where
        T: 'graph;
    type PropType = EdgeView<G>;
    type Nodes = PathFromGraph<'graph, G>;
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

    fn as_metadata(&self) -> Self::ValueType<Metadata<'graph, Self::PropType>> {
        self.map(|g, e| Metadata::new(EdgeView::new(g.clone(), e)))
    }

    fn map_nodes<F: for<'a> Fn(&'a Self::Graph, EdgeRef) -> VID + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::Nodes {
        let graph = self.graph.clone();
        let edges = self.edges.clone();
        let edges = move |n| {
            let graph = graph.clone();
            let op = op.clone();
            edges(n).map(move |e| op(&graph, e)).into_dyn_boxed()
        };
        PathFromGraph::new(self.graph.clone(), self.nodes.clone(), edges)
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
        NestedEdges {
            graph: self.graph.clone(),
            nodes: self.nodes.clone(),
            edges,
        }
    }
}

impl<'graph, G> InternalEdgeSelect<'graph> for NestedEdges<'graph, G>
where
    G: GraphViewOps<'graph> + 'graph,
{
    type IterGraph = G;
    type IterFiltered<FilteredGraph: GraphViewOps<'graph> + 'graph> = NestedEdges<'graph, G>;

    fn iter_graph(&self) -> &Self::IterGraph {
        &self.graph
    }

    fn apply_iter_filter<FilteredGraph: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: FilteredGraph,
    ) -> Self::IterFiltered<FilteredGraph> {
        let edges = self.edges.clone();
        NestedEdges {
            graph: self.graph.clone(),
            nodes: self.nodes.clone(),
            edges: Arc::new(move |vid| {
                let filtered_graph = filtered_graph.clone();
                let edges_locked = filtered_graph.core_edges();
                Box::new(edges(vid).filter(move |e_ref| match e_ref.layer() {
                    Some(l) => match e_ref.time() {
                        Some(t) => {
                            filtered_graph.filter_exploded_edge(e_ref.pid().with_layer(l), t)
                        }
                        None => {
                            let lg = LayeredGraph::new(&filtered_graph, LayerIds::One(l.0));
                            lg.filter_edge(edges_locked.edge(e_ref.pid()))
                        }
                    },
                    None => filtered_graph.filter_edge(edges_locked.edge(e_ref.pid())),
                }))
            }),
        }
    }
}
