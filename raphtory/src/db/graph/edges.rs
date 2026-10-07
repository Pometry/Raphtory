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
            views::filter::{
                edge_expr_filtered_graph::EdgeExprFilteredGraph,
                exploded_edge_expr_filtered_graph::ExplodedEdgeExprFilteredGraph, CreateFilter,
            },
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use itertools::Itertools;
use std::{
    cmp::Ordering,
    fmt::{Debug, Formatter},
    marker::PhantomData,
    sync::Arc,
};

pub type EdgeOp<'graph> = Arc<
    dyn Fn(Arc<dyn BoxableGraphView + 'graph>) -> BoxedLIter<'graph, EdgeRef>
        + Send
        + Sync
        + 'graph,
>;

/// What a collection holds: edges, or exploded edges. The kind decides what
/// `select` asks its question of. A predicate on a collection of edges is
/// asked once per edge, and the exploded edges of a kept edge all pass; on a
/// collection of exploded edges it is asked once per exploded edge.
pub trait EdgeItem: Copy + Debug + Default + Send + Sync + 'static {
    /// `select` with the items that pass `filter` taken out, on top of the
    /// selections already made.
    fn select<'graph, F: CreateFilter + 'graph>(
        select: DynGraphArc<'graph>,
        filter: F,
    ) -> Result<DynGraphArc<'graph>, GraphError>;
}

/// The items of a collection are edges.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Edge;

/// The items of a collection are exploded edges.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExplodedEdge;

impl EdgeItem for Edge {
    fn select<'graph, F: CreateFilter + 'graph>(
        select: DynGraphArc<'graph>,
        filter: F,
    ) -> Result<DynGraphArc<'graph>, GraphError> {
        let filter = filter.create_edge_filter(select.clone())?;
        Ok(Arc::new(EdgeExprFilteredGraph::new(select, filter)))
    }
}

impl EdgeItem for ExplodedEdge {
    fn select<'graph, F: CreateFilter + 'graph>(
        select: DynGraphArc<'graph>,
        filter: F,
    ) -> Result<DynGraphArc<'graph>, GraphError> {
        let filter = filter.create_edge_filter(select.clone())?;
        Ok(Arc::new(ExplodedEdgeExprFilteredGraph::new(select, filter)))
    }
}

/// A collection of exploded edges: what `explode()` and `explode_layers()` return.
pub type ExplodedEdges<'graph, G> = Edges<'graph, G, ExplodedEdge>;

#[derive(Clone)]
pub struct Edges<'graph, G, K = Edge> {
    pub(crate) base_graph: G,
    pub(crate) select: DynGraphArc<'graph>,
    pub(crate) edges: EdgeOp<'graph>,
    pub(crate) kind: PhantomData<K>,
}

impl<G: IntoDynamic, K: EdgeItem> Edges<'static, G, K> {
    pub fn into_dyn(self) -> Edges<'static, DynamicGraph, K> {
        Edges {
            base_graph: self.base_graph.into_dynamic(),
            select: self.select,
            edges: self.edges,
            kind: PhantomData,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, K: EdgeItem> Debug for Edges<'graph, G, K> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<'graph, Current, K> InternalFilter<'graph> for Edges<'graph, Current, K>
where
    Current: GraphViewOps<'graph>,
    K: EdgeItem,
{
    type Graph = Current;
    type Filtered<Next: GraphViewOps<'graph> + 'graph> = Edges<'graph, Next, K>;

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
            kind: PhantomData,
        }
    }
}

impl<'graph, G: GraphView + 'graph, K: EdgeItem> Edges<'graph, G, K> {
    pub fn new(base_graph: G, edges: EdgeOp<'graph>) -> Self {
        let select = Arc::new(base_graph.clone()) as DynGraphArc<'graph>;
        Edges {
            base_graph,
            select,
            edges,
            kind: PhantomData,
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

impl<'graph, G: GraphViewOps<'graph>, K: EdgeItem> IntoIterator for Edges<'graph, G, K> {
    type Item = EdgeView<G>;
    type IntoIter = BoxedLIter<'graph, EdgeView<G>>;

    fn into_iter(self) -> Self::IntoIter {
        let base_graph = self.base_graph.clone();
        Box::new(
            (self.edges)(self.select).map(move |e| EdgeView::new_filtered(base_graph.clone(), e)),
        )
    }
}

impl<'graph, G: GraphViewOps<'graph>, K: EdgeItem> BaseEdgeViewOps<'graph> for Edges<'graph, G, K> {
    type Graph = G;
    type ValueType<T>
        = BoxedLIter<'graph, T>
    where
        T: 'graph;
    type PropType = EdgeView<G>;
    type Nodes = PathFromNode<'graph, G>;
    type Exploded = ExplodedEdges<'graph, G>;

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
            kind: PhantomData,
        }
    }
}

impl<G: StaticGraphViewOps + IntoDynamic + Static, K: EdgeItem> From<Edges<'static, G, K>>
    for Edges<'static, DynamicGraph, K>
{
    fn from(value: Edges<'static, G, K>) -> Self {
        Edges {
            base_graph: value.base_graph.into_dynamic(),
            select: value.select,
            edges: value.edges,
            kind: PhantomData,
        }
    }
}

impl<'graph, G: GraphView + 'graph, K: EdgeItem> Select<'graph> for Edges<'graph, G, K> {
    type IterFiltered<Filter: CreateFilter + 'graph> = Edges<'graph, G, K>;

    fn select<F: CreateFilter + 'graph>(
        &self,
        filter: F,
    ) -> Result<Self::IterFiltered<F>, GraphError> {
        // Chain onto the current select so every earlier selection keeps its say;
        // the kind of item decides what the predicate is asked about.
        Ok(Edges {
            base_graph: self.base_graph.clone(),
            select: K::select(self.select.clone(), filter)?,
            edges: self.edges.clone(),
            kind: PhantomData,
        })
    }
}

pub type NestedEdgeOp<'graph> =
    Arc<dyn Fn(DynGraphArc<'graph>, VID) -> BoxedLIter<'graph, EdgeRef> + Send + Sync + 'graph>;

/// A collection of exploded edges per node: what `explode()` returns on nested edges.
pub type NestedExplodedEdges<'graph, G> = NestedEdges<'graph, G, ExplodedEdge>;

#[derive(Clone)]
pub struct NestedEdges<'graph, G, K = Edge> {
    pub(crate) graph: G,
    pub(crate) select: DynGraphArc<'graph>,
    pub(crate) nodes: Arc<dyn Fn() -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>,
    pub(crate) edges: NestedEdgeOp<'graph>,
    pub(crate) kind: PhantomData<K>,
}

impl<'graph, G: GraphViewOps<'graph>, K: EdgeItem> NestedEdges<'graph, G, K> {
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
            kind: PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        (self.nodes)().count()
    }

    pub fn is_empty(&self) -> bool {
        (self.nodes)().next().is_none()
    }

    pub fn iter(&self) -> impl Iterator<Item = Edges<'graph, G, K>> + 'graph {
        let base_graph = self.graph.clone();
        let edges = self.edges.clone();
        let select = self.select.clone();
        (self.nodes)().map(move |n| {
            let edge_fn = edges.clone();
            Edges {
                base_graph: base_graph.clone(),
                select: select.clone(),
                edges: Arc::new(move |graph| edge_fn(graph, n)),
                kind: PhantomData,
            }
        })
    }

    pub fn collect(&self) -> Vec<Vec<EdgeView<G>>> {
        self.iter().map(|edges| edges.collect()).collect()
    }
}

impl<'graph, G: IntoDynamic, K: EdgeItem> NestedEdges<'graph, G, K> {
    pub fn into_dyn(self) -> NestedEdges<'graph, DynamicGraph, K> {
        NestedEdges {
            graph: self.graph.into_dynamic(),
            select: self.select,
            nodes: self.nodes,
            edges: self.edges,
            kind: PhantomData,
        }
    }
}

impl<G: StaticGraphViewOps + IntoDynamic + Static, K: EdgeItem> From<NestedEdges<'static, G, K>>
    for NestedEdges<'static, DynamicGraph, K>
{
    fn from(value: NestedEdges<'static, G, K>) -> Self {
        NestedEdges {
            graph: value.graph.into_dynamic(),
            select: value.select,
            nodes: value.nodes,
            edges: value.edges,
            kind: PhantomData,
        }
    }
}

impl<'graph, Current, K> InternalFilter<'graph> for NestedEdges<'graph, Current, K>
where
    Current: GraphViewOps<'graph>,
    K: EdgeItem,
{
    type Graph = Current;
    type Filtered<Next: GraphViewOps<'graph> + 'graph> = NestedEdges<'graph, Next, K>;

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
            kind: PhantomData,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, K: EdgeItem> BaseEdgeViewOps<'graph>
    for NestedEdges<'graph, G, K>
{
    type Graph = G;
    type ValueType<T>
        = BoxedLIter<'graph, BoxedLIter<'graph, T>>
    where
        T: 'graph;
    type PropType = EdgeView<G>;
    type Nodes = PathFromGraph<'graph, G>;
    type Exploded = NestedExplodedEdges<'graph, G>;

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
            kind: PhantomData,
        }
    }
}

impl<'graph, G: GraphView + 'graph, K: EdgeItem> Select<'graph> for NestedEdges<'graph, G, K> {
    type IterFiltered<Filter: CreateFilter + 'graph> = NestedEdges<'graph, G, K>;

    fn select<F: CreateFilter + 'graph>(
        &self,
        filter: F,
    ) -> Result<Self::IterFiltered<F>, GraphError> {
        Ok(NestedEdges {
            graph: self.graph.clone(),
            nodes: self.nodes.clone(),
            select: K::select(self.select.clone(), filter)?,
            edges: self.edges.clone(),
            kind: PhantomData,
        })
    }
}
