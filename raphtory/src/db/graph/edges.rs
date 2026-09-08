use crate::{
    core::entities::{edges::edge_ref::EdgeRef, VID},
    db::{
        api::{
            properties::{Metadata, Properties},
            view::{
                internal::{DynGraphArc, GraphView, InternalFilter, Static},
                sort::{compare_edge, EdgeSortBy},
                BaseEdgeViewOps, BoxableGraphView, BoxedLIter, DynamicGraph, IntoDynBoxed,
                IntoDynamic, Select, StaticGraphViewOps,
            },
        },
        graph::{
            edge::EdgeView,
            path::{PathFromGraph, PathFromNode},
            views::filter::CreateFilter,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use itertools::Itertools;
use std::{
    cmp::Ordering,
    fmt::{Debug, Formatter},
    sync::Arc,
};

pub type EdgeOp<'graph> = Arc<
    dyn Fn(Arc<dyn BoxableGraphView + 'graph>) -> BoxedLIter<'graph, EdgeRef>
        + Send
        + Sync
        + 'graph,
>;

#[derive(Clone)]
pub struct Edges<'graph, G> {
    pub(crate) base_graph: G,
    pub(crate) select: DynGraphArc<'graph>,
    pub(crate) edges: EdgeOp<'graph>,
}

impl<G: IntoDynamic> Edges<'static, G> {
    pub fn into_dyn(self) -> Edges<'static, DynamicGraph> {
        Edges {
            base_graph: self.base_graph.into_dynamic(),
            select: self.select,
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
            select: self.select.clone(),
            edges: self.edges.clone(),
        }
    }
}

impl<'graph, G: GraphView + 'graph> Edges<'graph, G> {
    pub fn new(base_graph: G, edges: EdgeOp<'graph>) -> Self {
        let select = Arc::new(base_graph.clone()) as DynGraphArc<'graph>;
        Edges {
            base_graph,
            select,
            edges,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = EdgeView<&G>> + '_ {
        let graph = &self.base_graph;
        let select = self.select.clone();
        (self.edges)(select).map(move |e| EdgeView::new_filtered(graph, e))
    }

    /// Reorder this collection by an ordered list of sort keys: members
    /// compare by the first key, ties break to the next. Returns a new
    /// collection backed by an explicit edge list in the sorted order.
    pub fn sorted(&self, sort_bys: &[EdgeSortBy]) -> Self {
        let sorted: Arc<[EdgeRef]> = self
            .iter()
            .sorted_by(|a, b| {
                sort_bys.iter().fold(Ordering::Equal, |current, sort_by| {
                    current.then_with(|| compare_edge(a, b, sort_by))
                })
            })
            .map(|edge_view| edge_view.edge)
            .collect();
        Edges::new(
            self.base_graph.clone(),
            Arc::new(move |_| {
                let sorted = sorted.clone();
                (0..sorted.len()).map(move |i| sorted[i]).into_dyn_boxed()
            }),
        )
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
        Box::new(
            (self.edges)(self.select).map(move |e| EdgeView::new_filtered(base_graph.clone(), e)),
        )
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
        (self.edges)(self.select.clone())
            .map(move |e| op(&graph, e))
            .into_dyn_boxed()
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.map(|g, e| Properties::new(EdgeView::new(g.clone(), e)))
    }

    fn as_metadata(&self) -> Self::ValueType<Metadata<'graph, Self::PropType>> {
        self.map(|g, e| Metadata::new(EdgeView::new(g.clone(), e)))
    }

    fn map_nodes<F: Fn(EdgeRef) -> VID + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::Nodes {
        let edges = self.edges.clone();
        let select = self.select.clone();
        PathFromNode::new_one_hop_filtered(
            self.base_graph.clone(),
            select,
            Arc::new(move |graph| {
                let op = op.clone();
                edges(graph).map(move |e| op(e)).into_dyn_boxed()
            }),
        )
    }

    fn map_exploded<
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
        F: Fn(&DynGraphArc<'graph>, EdgeRef) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Exploded {
        let edges = self.edges.clone();
        let edges = Arc::new(move |graph: DynGraphArc<'graph>| {
            let graph = graph.clone();
            let op = op.clone();
            edges(graph.clone())
                .flat_map(move |e| op(&graph, e))
                .into_dyn_boxed()
        });
        let select = self.select.clone();
        Edges {
            base_graph: self.base_graph.clone(),
            select,
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
            select: value.select,
            edges: value.edges,
        }
    }
}

impl<'graph, G: GraphView + 'graph> Select<'graph> for Edges<'graph, G> {
    type IterFiltered<Filter: CreateFilter + 'graph> = Edges<'graph, G>;

    fn select<F: CreateFilter + 'graph>(
        &self,
        filter: F,
    ) -> Result<Self::IterFiltered<F>, GraphError> {
        // Chain onto the current select rather than AND a fresh filter with the base graph:
        // AndFilteredGraph inherits time semantics from its base, so a time view (window/before/
        // after/snapshot) on the right operand is silently dropped and the collection fails open.
        let filtered_graph = filter.filter_graph_view(self.select.clone())?;
        let filtered_graph = filter.create_filter(self.select.clone(), filtered_graph)?;
        Ok(Edges {
            base_graph: self.base_graph.clone(),
            select: Arc::new(filtered_graph),
            edges: self.edges.clone(),
        })
    }
}

pub type NestedEdgeOp<'graph> =
    Arc<dyn Fn(DynGraphArc<'graph>, VID) -> BoxedLIter<'graph, EdgeRef> + Send + Sync + 'graph>;

#[derive(Clone)]
pub struct NestedEdges<'graph, G> {
    pub(crate) graph: G,
    pub(crate) select: DynGraphArc<'graph>,
    pub(crate) nodes: Arc<dyn Fn() -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>,
    pub(crate) edges: NestedEdgeOp<'graph>,
}

impl<'graph, G: GraphViewOps<'graph>> NestedEdges<'graph, G> {
    pub fn new(
        graph: G,
        nodes: Arc<dyn Fn() -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>,
        edges: NestedEdgeOp<'graph>,
    ) -> Self {
        let select = Arc::new(graph.clone());
        NestedEdges {
            graph,
            select,
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
        let select = self.select.clone();
        (self.nodes)().map(move |n| {
            let edge_fn = edges.clone();
            Edges {
                base_graph: base_graph.clone(),
                select: select.clone(),
                edges: Arc::new(move |graph| edge_fn(graph, n)),
            }
        })
    }

    pub fn collect(&self) -> Vec<Vec<EdgeView<G>>> {
        self.iter().map(|edges| edges.collect()).collect()
    }
}

impl<'graph, G: IntoDynamic> NestedEdges<'graph, G> {
    pub fn into_dyn(self) -> NestedEdges<'graph, DynamicGraph> {
        NestedEdges {
            graph: self.graph.into_dynamic(),
            select: self.select,
            nodes: self.nodes,
            edges: self.edges,
        }
    }
}

impl<G: StaticGraphViewOps + IntoDynamic + Static> From<NestedEdges<'static, G>>
    for NestedEdges<'static, DynamicGraph>
{
    fn from(value: NestedEdges<'static, G>) -> Self {
        NestedEdges {
            graph: value.graph.into_dynamic(),
            select: value.select,
            nodes: value.nodes,
            edges: value.edges,
        }
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
            select: self.select.clone(),
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
        let select = self.select.clone();
        (self.nodes)()
            .map(move |n| {
                let graph = graph.clone();
                let op = op.clone();
                edges(select.clone(), n)
                    .map(move |e| op(&graph, e))
                    .into_dyn_boxed()
            })
            .into_dyn_boxed()
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.map(|g, e| Properties::new(EdgeView::new(g.clone(), e)))
    }

    fn as_metadata(&self) -> Self::ValueType<Metadata<'graph, Self::PropType>> {
        self.map(|g, e| Metadata::new(EdgeView::new(g.clone(), e)))
    }

    fn map_nodes<F: Fn(EdgeRef) -> VID + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::Nodes {
        let edges = self.edges.clone();
        let select = self.select.clone();
        let edges = Arc::new(move |graph: DynGraphArc<'graph>, n| {
            let op = op.clone();
            edges(graph, n).map(move |e| op(e)).into_dyn_boxed()
        });
        PathFromGraph::new_filtered(self.graph.clone(), select, self.nodes.clone(), edges)
    }

    fn map_exploded<
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
        F: Fn(&DynGraphArc<'graph>, EdgeRef) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Exploded {
        let edges = self.edges.clone();
        let select = self.select.clone();
        let edges = Arc::new(move |graph: DynGraphArc<'graph>, n: VID| {
            let graph = graph.clone();
            let op = op.clone();
            edges(graph.clone(), n)
                .flat_map(move |e| op(&graph, e))
                .into_dyn_boxed()
        });
        NestedEdges {
            graph: self.graph.clone(),
            nodes: self.nodes.clone(),
            select,
            edges,
        }
    }
}

impl<'graph, G: GraphView + 'graph> Select<'graph> for NestedEdges<'graph, G> {
    type IterFiltered<Filter: CreateFilter + 'graph> = NestedEdges<'graph, G>;

    fn select<F: CreateFilter + 'graph>(
        &self,
        filter: F,
    ) -> Result<Self::IterFiltered<F>, GraphError> {
        let filtered_graph = filter.filter_graph_view(self.select.clone())?;
        let filtered_graph = filter.create_filter(self.select.clone(), filtered_graph)?;
        Ok(NestedEdges {
            graph: self.graph.clone(),
            nodes: self.nodes.clone(),
            select: Arc::new(filtered_graph),
            edges: self.edges.clone(),
        })
    }
}
