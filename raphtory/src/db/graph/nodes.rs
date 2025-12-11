use crate::{
    core::entities::{edges::edge_ref::EdgeRef, nodes::node_ref::AsNodeRef, VID},
    db::{
        api::{
            state::{
                ops::{
                    filter::{AndOp, NodeTypeFilterOp, NO_FILTER},
                    Const, IntoDynNodeOp, NodeFilterOp, NodeOp,
                },
                Index, LazyNodeState,
            },
            view::{
                internal::{FilterOps, InternalFilter, InternalNodeSelect, NodeList},
                BaseNodeViewOps, BoxedLIter, DynamicGraph, IntoDynBoxed, IntoDynamic,
            },
        },
        graph::{edges::NestedEdges, node::NodeView, path::PathFromGraph},
    },
    prelude::*,
};
use raphtory_storage::{core_ops::is_view_compatible, graph::graph::GraphStorage};
use rayon::iter::ParallelIterator;
use std::{
    collections::HashSet,
    fmt::{Debug, Formatter},
    hash::{BuildHasher, Hash},
    marker::PhantomData,
    sync::Arc,
};

#[derive(Clone)]
pub struct Nodes<'graph, G, GH = G, F = Const<bool>> {
    pub(crate) base_graph: G,
    pub(crate) graph: GH,
    pub(crate) predicate: F,
    pub(crate) nodes: Option<Index<VID>>,
    _marker: PhantomData<&'graph ()>,
}

impl<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + Clone + 'graph,
        V: AsNodeRef + Hash + Eq,
        S: BuildHasher,
    > PartialEq<HashSet<V, S>> for Nodes<'graph, G, GH, F>
{
    fn eq(&self, other: &HashSet<V, S>) -> bool {
        self.len() == other.len() && other.iter().all(|o| self.contains(o))
    }
}

impl<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + Clone + 'graph,
        V: AsNodeRef,
    > PartialEq<Vec<V>> for Nodes<'graph, G, GH, F>
{
    fn eq(&self, other: &Vec<V>) -> bool {
        self.iter_refs()
            .eq(other.iter().filter_map(|o| self.get(o).map(|n| n.node)))
    }
}

impl<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + Clone + 'graph + Debug,
    > Debug for Nodes<'graph, G, GH, F>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + Clone + 'graph,
    > PartialEq for Nodes<'graph, G, GH, F>
{
    fn eq(&self, other: &Self) -> bool {
        if is_view_compatible(&self.graph, &other.graph) {
            // same storage, can use internal ids
            self.iter_refs().eq(other.iter_refs())
        } else {
            // different storage, use external ids
            self.id().iter_values().eq(other.id().iter_values())
        }
    }
}

pub trait IntoDynNodes {
    fn into_dyn(self)
        -> Nodes<'static, DynamicGraph, DynamicGraph, Arc<dyn NodeOp<Output = bool>>>;
}

impl<G: IntoDynamic, GH: IntoDynamic, F: NodeFilterOp + IntoDynNodeOp + 'static> IntoDynNodes
    for Nodes<'static, G, GH, F>
{
    fn into_dyn(
        self,
    ) -> Nodes<'static, DynamicGraph, DynamicGraph, Arc<dyn NodeOp<Output = bool>>> {
        Nodes {
            base_graph: self.base_graph.into_dynamic(),
            graph: self.graph.into_dynamic(),
            predicate: self.predicate.into_dynamic(),
            nodes: self.nodes,
            _marker: Default::default(),
        }
    }
}

impl<'graph, G> Nodes<'graph, G>
where
    G: GraphViewOps<'graph> + Clone,
{
    pub fn new(graph: G) -> Self {
        Self {
            base_graph: graph.clone(),
            graph: graph.clone(),
            predicate: NO_FILTER,
            nodes: None,
            _marker: PhantomData,
        }
    }
}

impl<'graph, G, GH, F> Nodes<'graph, G, GH, F>
where
    G: GraphViewOps<'graph> + 'graph,
    GH: GraphViewOps<'graph> + 'graph,
    F: NodeFilterOp + Clone + 'graph,
{
    pub fn new_filtered(base_graph: G, graph: GH, predicate: F, nodes: Option<Index<VID>>) -> Self {
        Self {
            base_graph,
            graph,
            predicate,
            nodes,
            _marker: PhantomData,
        }
    }

    pub fn node_list(&self) -> NodeList {
        match self.nodes.clone() {
            None => self.graph.node_list(),
            Some(elems) => NodeList::List { elems },
        }
    }

    pub fn indexed(&self, index: Index<VID>) -> Nodes<'graph, G, GH, F> {
        Nodes::new_filtered(
            self.base_graph.clone(),
            self.graph.clone(),
            self.predicate.clone(),
            Some(index),
        )
    }

    pub(crate) fn par_iter_refs(&self) -> impl ParallelIterator<Item = VID> + 'graph {
        let g = self.base_graph.core_graph().lock();
        let view = self.base_graph.clone();
        let node_select = self.predicate.clone();
        self.node_list().into_par_iter().filter(move |&vid| {
            let node = g.core_node(vid);
            view.filter_node(node.as_ref()) && node_select.apply(&g, vid)
        })
    }

    #[inline]
    pub(crate) fn iter_refs(&self) -> impl Iterator<Item = VID> + Send + Sync + 'graph {
        let g = self.base_graph.core_graph().lock();
        let view = self.base_graph.clone();
        let selector = self.predicate.clone();
        self.node_list().into_iter().filter(move |&vid| {
            let node = g.core_node(vid);
            view.filter_node(node.as_ref()) && selector.apply(&g, vid)
        })
    }

    fn iter_vids(&self, g: GraphStorage) -> impl Iterator<Item = VID> + Send + Sync + 'graph {
        let g = self.base_graph.core_graph().clone();
        let view = self.base_graph.clone();
        let selector = self.predicate.clone();
        self.node_list().into_iter().filter(move |&vid| {
            let node = g.core_node(vid);
            view.filter_node(node.as_ref()) && selector.apply(&g, vid)
        })
    }

    #[inline]
    pub(crate) fn iter_refs_unlocked(&self) -> impl Iterator<Item = VID> + Send + Sync + 'graph {
        let g = self.graph.core_graph().clone();
        self.iter_vids(g)
    }

    pub fn iter(&self) -> impl Iterator<Item = NodeView<&GH>> + use<'_, 'graph, G, GH, F> {
        self.iter_refs()
            .map(|v| NodeView::new_internal(&self.graph, v))
    }

    pub fn iter_unlocked(&self) -> impl Iterator<Item = NodeView<&GH>> + use<'_, 'graph, G, GH, F> {
        self.iter_refs_unlocked()
            .map(|v| NodeView::new_internal(&self.graph, v))
    }

    pub fn iter_owned(&self) -> BoxedLIter<'graph, NodeView<'graph, GH>> {
        let graph = self.graph.clone();
        self.iter_refs()
            .map(move |v| NodeView::new_internal(graph.clone(), v))
            .into_dyn_boxed()
    }

    pub fn iter_owned_unlocked(&self) -> BoxedLIter<'graph, NodeView<'graph, GH>> {
        let g = self.graph.clone();
        self.iter_refs_unlocked()
            .map(move |v| NodeView::new_internal(g.clone(), v))
            .into_dyn_boxed()
    }

    pub fn par_iter(
        &self,
    ) -> impl ParallelIterator<Item = NodeView<&GH>> + use<'_, 'graph, G, GH, F> {
        self.par_iter_refs()
            .map(|v| NodeView::new_internal(&self.graph, v))
    }

    pub fn into_par_iter(self) -> impl ParallelIterator<Item = NodeView<'graph, GH>> + 'graph {
        self.par_iter_refs()
            .map(move |n| NodeView::new_internal(self.graph.clone(), n))
    }

    /// Returns the number of nodes in the graph.
    #[inline]
    pub fn len(&self) -> usize {
        match self.nodes.as_ref() {
            None => {
                if self.is_list_filtered() {
                    self.par_iter_refs().count()
                } else {
                    self.graph.node_list().len()
                }
            }
            Some(nodes) => {
                if self.is_filtered() {
                    self.par_iter_refs().count()
                } else {
                    nodes.len()
                }
            }
        }
    }

    /// Returns true if the graph contains no nodes.
    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    pub fn get<V: AsNodeRef>(&self, node: V) -> Option<NodeView<'graph, GH>> {
        let vid = self.graph.internalise_node(node.as_node_ref())?;
        self.contains(vid)
            .then(|| NodeView::new_internal(self.graph.clone(), vid))
    }

    pub fn type_filter<I: IntoIterator<Item = V>, V: AsRef<str>>(
        &self,
        node_types: I,
    ) -> Nodes<'graph, G, GH, AndOp<F, NodeTypeFilterOp>> {
        let node_types_filter = NodeTypeFilterOp::new_from_values(node_types, &self.graph);
        let predicate = self.predicate.clone().and(node_types_filter);
        Nodes {
            base_graph: self.base_graph.clone(),
            graph: self.graph.clone(),
            predicate,
            nodes: self.nodes.clone(),
            _marker: PhantomData,
        }
    }

    pub fn id_filter(
        &self,
        nodes: impl IntoIterator<Item = impl AsNodeRef>,
    ) -> Nodes<'graph, G, GH, F> {
        let index: Index<_> = nodes
            .into_iter()
            .filter_map(|n| self.graph.node(n).map(|n| n.node))
            .collect();
        self.indexed(index)
    }

    /// Collect nodes into a vec
    pub fn collect(&self) -> Vec<NodeView<'graph, GH>> {
        self.iter_owned().collect()
    }

    pub fn get_metadata_id(&self, prop_name: &str) -> Option<usize> {
        self.graph.node_meta().get_prop_id(prop_name, true)
    }

    pub fn get_temporal_prop_id(&self, prop_name: &str) -> Option<usize> {
        self.graph.node_meta().get_prop_id(prop_name, false)
    }

    pub fn is_list_filtered(&self) -> bool {
        !self.graph.node_list_trusted() || self.predicate.is_filtered()
    }

    pub fn is_filtered(&self) -> bool {
        self.graph.filtered() || self.predicate.is_filtered()
    }

    pub fn contains<V: AsNodeRef>(&self, node: V) -> bool {
        (&self.base_graph)
            .node(node)
            .filter(|node| {
                self.nodes
                    .as_ref()
                    .map(|nodes| nodes.contains(&node.node))
                    .unwrap_or(true)
                    && self
                        .predicate
                        .apply(self.base_graph.core_graph(), node.node)
            })
            .is_some()
    }
}

impl<'graph, G, GH, F> InternalNodeSelect<'graph> for Nodes<'graph, G, GH, F>
where
    G: GraphViewOps<'graph> + 'graph,
    GH: GraphViewOps<'graph> + 'graph,
    F: NodeFilterOp + 'graph,
{
    type IterGraph = GH;
    type IterFiltered<Filter: NodeFilterOp + 'graph> = Nodes<'graph, G, GH, AndOp<F, Filter>>;

    fn iter_graph(&self) -> &Self::IterGraph {
        &self.graph
    }

    fn apply_iter_filter<Filter: NodeFilterOp + 'graph>(
        &self,
        filter: Filter,
    ) -> Self::IterFiltered<Filter> {
        let predicate = self.predicate.clone().and(filter);
        Nodes {
            base_graph: self.base_graph.clone(),
            graph: self.graph.clone(),
            predicate,
            nodes: self.nodes.clone(),
            _marker: Default::default(),
        }
    }
}

impl<'graph, G, GH, F> BaseNodeViewOps<'graph> for Nodes<'graph, G, GH, F>
where
    G: GraphViewOps<'graph> + 'graph,
    GH: GraphViewOps<'graph> + 'graph,
    F: NodeFilterOp + 'graph,
{
    type Graph = GH;
    type ValueType<T: NodeOp + 'graph> = LazyNodeState<'graph, T, G, GH, F>;
    type PropType = NodeView<'graph, G>;
    type PathType = PathFromGraph<'graph, GH>;
    type Edges = NestedEdges<'graph, GH>;

    fn graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn map<T: NodeOp + 'graph>(&self, op: T) -> Self::ValueType<T> {
        LazyNodeState::new(op, self.clone())
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
        T: Fn(&GraphStorage, &Self::Graph, VID) -> I + Send + Sync + 'graph,
    >(
        &self,
        op: T,
    ) -> Self::Edges {
        let graph = self.graph.clone();
        let nodes = self.clone();
        let nodes = Arc::new(move || nodes.iter_refs().into_dyn_boxed());
        let edges = Arc::new(move |node: VID| {
            let cg = graph.core_graph();
            op(cg, &graph, node).into_dyn_boxed()
        });
        NestedEdges {
            graph: self.graph.clone(),
            nodes,
            edges,
        }
    }

    fn hop<
        I: Iterator<Item = VID> + Send + Sync + 'graph,
        T: Fn(&GraphStorage, &Self::Graph, VID) -> I + Send + Sync + 'graph,
    >(
        &self,
        op: T,
    ) -> Self::PathType {
        let graph = self.graph.clone();
        let nodes = self.clone();
        let nodes = Arc::new(move || nodes.iter_refs().into_dyn_boxed());
        PathFromGraph::new(self.graph.clone(), nodes, move |v| {
            let cg = graph.core_graph();
            op(cg, &graph, v).into_dyn_boxed()
        })
    }
}

impl<'graph, G, Current, F> InternalFilter<'graph> for Nodes<'graph, G, Current, F>
where
    G: GraphViewOps<'graph> + 'graph,
    Current: GraphViewOps<'graph> + 'graph,
    F: NodeFilterOp + Clone + 'graph,
{
    type Graph = Current;
    type Filtered<Next: GraphViewOps<'graph>> = Nodes<'graph, G, Next, F>;

    fn base_graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn apply_filter<Next: GraphViewOps<'graph>>(
        &self,
        filtered_graph: Next,
    ) -> Self::Filtered<Next> {
        Nodes {
            base_graph: self.base_graph.clone(),
            graph: filtered_graph,
            predicate: self.predicate.clone(),
            nodes: self.nodes.clone(),
            _marker: PhantomData,
        }
    }
}

impl<'graph, G, GH, F> IntoIterator for Nodes<'graph, G, GH, F>
where
    G: GraphViewOps<'graph> + 'graph,
    GH: GraphViewOps<'graph> + 'graph,
    F: NodeFilterOp + 'graph,
{
    type Item = NodeView<'graph, GH>;
    type IntoIter = BoxedLIter<'graph, Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter_owned())
    }
}
