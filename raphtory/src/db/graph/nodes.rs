use crate::{
    core::entities::{edges::edge_ref::EdgeRef, nodes::node_ref::AsNodeRef, VID},
    db::{
        api::{
            state::LazyNodeState,
            storage::graph::storage_ops::GraphStorage,
            view::{
                internal::{OneHopFilter, Static},
                BaseNodeViewOps, BoxedLIter, DynamicGraph, IntoDynBoxed, IntoDynamic,
            },
        },
        graph::{edges::NestedEdges, node::NodeView, path::PathFromGraph},
    },
    prelude::*,
};

use crate::db::{
    api::state::{Index, NodeOp},
    graph::{create_node_type_filter, views::node_subgraph::NodeSubgraph},
};
use either::Either;
use rayon::iter::ParallelIterator;
use std::{
    collections::HashSet,
    fmt::{Debug, Formatter},
    hash::{BuildHasher, Hash},
    marker::PhantomData,
    sync::Arc,
};

#[derive(Clone)]
pub struct Nodes<'graph, G, GH = G> {
    pub(crate) base_graph: G,
    pub(crate) graph: GH,
    pub(crate) nodes: Option<Index<VID>>,
    pub(crate) node_types_filter: Option<Arc<[bool]>>,
    _marker: PhantomData<&'graph ()>,
}

impl<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        V: AsNodeRef + Hash + Eq,
        S: BuildHasher,
    > PartialEq<HashSet<V, S>> for Nodes<'graph, G, GH>
{
    fn eq(&self, other: &HashSet<V, S>) -> bool {
        self.len() == other.len() && other.iter().all(|o| self.contains(o))
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>, V: AsNodeRef> PartialEq<Vec<V>>
    for Nodes<'graph, G, GH>
{
    fn eq(&self, other: &Vec<V>) -> bool {
        self.iter_refs()
            .eq(other.iter().filter_map(|o| self.get(o).map(|n| n.node)))
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph> + Debug> Debug
    for Nodes<'graph, G, GH>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> PartialEq for Nodes<'graph, G, GH> {
    fn eq(&self, other: &Self) -> bool {
        if self.base_graph.core_graph().graph_id() == other.base_graph.core_graph().graph_id() {
            // same storage, can use internal ids
            self.iter_refs().eq(other.iter_refs())
        } else {
            // different storage, use external ids
            self.id().iter_values().eq(other.id().iter_values())
        }
    }
}

impl<'graph, G: IntoDynamic, GH: IntoDynamic> Nodes<'graph, G, GH> {
    pub fn into_dyn(self) -> Nodes<'graph, DynamicGraph> {
        Nodes {
            base_graph: self.base_graph.into_dynamic(),
            graph: self.graph.into_dynamic(),
            nodes: self.nodes,
            node_types_filter: self.node_types_filter,
            _marker: Default::default(),
        }
    }
}

impl<'graph, G, GH> From<Nodes<'graph, G, GH>> for Nodes<'graph, DynamicGraph, DynamicGraph>
where
    G: GraphViewOps<'graph> + IntoDynamic,
    GH: GraphViewOps<'graph> + IntoDynamic + Static,
{
    fn from(value: Nodes<'graph, G, GH>) -> Self {
        let base_graph = value.base_graph.into_dynamic();
        let graph = value.graph.into_dynamic();
        Nodes {
            base_graph,
            graph,
            nodes: value.nodes,
            node_types_filter: value.node_types_filter,
            _marker: PhantomData,
        }
    }
}

impl<'graph, G> Nodes<'graph, G, G>
where
    G: GraphViewOps<'graph> + Clone,
{
    pub fn new(graph: G) -> Self {
        let base_graph = graph.clone();
        Self {
            base_graph,
            graph,
            nodes: None,
            node_types_filter: None,
            _marker: PhantomData,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> EdgePropertyFilterOps<'graph>
    for Nodes<'graph, G, GH>
{
}
impl<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph> + ExplodedEdgePropertyFilterOps<'graph>,
    > ExplodedEdgePropertyFilterOps<'graph> for Nodes<'graph, G, GH>
{
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> NodePropertyFilterOps<'graph>
    for Nodes<'graph, G, GH>
{
}

impl<'graph, G, GH> Nodes<'graph, G, GH>
where
    G: GraphViewOps<'graph> + 'graph,
    GH: GraphViewOps<'graph> + 'graph,
{
    pub fn new_filtered(
        base_graph: G,
        graph: GH,
        nodes: Option<Index<VID>>,
        node_types_filter: Option<Arc<[bool]>>,
    ) -> Self {
        Self {
            base_graph,
            graph,
            nodes,
            node_types_filter,
            _marker: PhantomData,
        }
    }

    pub(crate) fn par_iter_refs(&self) -> impl ParallelIterator<Item = VID> + 'graph {
        let g = self.graph.core_graph().lock();
        let node_types_filter = self.node_types_filter.clone();
        match self.nodes.clone() {
            None => Either::Left(g.into_nodes_par(self.graph.clone(), node_types_filter)),
            Some(nodes) => {
                let gs = NodeSubgraph {
                    graph: self.graph.clone(),
                    nodes,
                };
                Either::Right(g.into_nodes_par(gs, node_types_filter))
            }
        }
    }

    pub fn indexed(&self, index: Index<VID>) -> Nodes<'graph, G, GH> {
        Nodes::new_filtered(
            self.base_graph.clone(),
            self.graph.clone(),
            Some(index),
            self.node_types_filter.clone(),
        )
    }

    #[inline]
    pub(crate) fn iter_refs(&self) -> impl Iterator<Item = VID> + Send + Sync + 'graph {
        let g = self.graph.core_graph().lock();
        let node_types_filter = self.node_types_filter.clone();
        match self.nodes.clone() {
            None => g.into_nodes_iter(self.graph.clone(), node_types_filter),
            Some(nodes) => {
                let gs = NodeSubgraph {
                    graph: self.graph.clone(),
                    nodes,
                };
                g.into_nodes_iter(gs, node_types_filter)
            }
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = NodeView<&G, &GH>> + use<'_, 'graph, G, GH> {
        self.iter_refs()
            .map(|v| NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, v))
    }

    pub fn iter_owned(&self) -> BoxedLIter<'graph, NodeView<G, GH>> {
        let base_graph = self.base_graph.clone();
        let g = self.graph.clone();
        self.iter_refs()
            .map(move |v| NodeView::new_one_hop_filtered(base_graph.clone(), g.clone(), v))
            .into_dyn_boxed()
    }

    pub fn par_iter(
        &self,
    ) -> impl ParallelIterator<Item = NodeView<&G, &GH>> + use<'_, 'graph, G, GH> {
        self.par_iter_refs()
            .map(|v| NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, v))
    }

    pub fn into_par_iter(self) -> impl ParallelIterator<Item = NodeView<G, GH>> + 'graph {
        self.par_iter_refs().map(move |n| {
            NodeView::new_one_hop_filtered(self.base_graph.clone(), self.graph.clone(), n)
        })
    }

    /// Returns the number of nodes in the graph.
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    /// Returns true if the graph contains no nodes.
    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    pub fn get<V: AsNodeRef>(&self, node: V) -> Option<NodeView<G, GH>> {
        let vid = self.graph.internalise_node(node.as_node_ref())?;
        self.contains(vid).then(|| {
            NodeView::new_one_hop_filtered(self.base_graph.clone(), self.graph.clone(), vid)
        })
    }

    pub fn type_filter(&self, node_types: &[impl AsRef<str>]) -> Nodes<'graph, G, GH> {
        let node_types_filter = Some(create_node_type_filter(
            self.graph.node_meta().node_type_meta(),
            node_types,
        ));
        Nodes {
            base_graph: self.base_graph.clone(),
            graph: self.graph.clone(),
            nodes: self.nodes.clone(),
            node_types_filter,
            _marker: PhantomData,
        }
    }

    pub fn collect(&self) -> Vec<NodeView<G, GH>> {
        self.iter_owned().collect()
    }

    pub fn get_const_prop_id(&self, prop_name: &str) -> Option<usize> {
        self.graph.node_meta().get_prop_id(prop_name, true)
    }

    pub fn get_temporal_prop_id(&self, prop_name: &str) -> Option<usize> {
        self.graph.node_meta().get_prop_id(prop_name, false)
    }

    pub fn is_filtered(&self) -> bool {
        self.node_types_filter.is_some() || self.graph.nodes_filtered()
    }

    pub fn contains<V: AsNodeRef>(&self, node: V) -> bool {
        (&self.graph())
            .node(node)
            .filter(|node| {
                self.node_types_filter
                    .as_ref()
                    .map(|filter| filter[node.node_type_id()])
                    .unwrap_or(true)
                    && self
                        .nodes
                        .as_ref()
                        .map(|nodes| nodes.contains(&node.node))
                        .unwrap_or(true)
            })
            .is_some()
    }
}

impl<'graph, G, GH> BaseNodeViewOps<'graph> for Nodes<'graph, G, GH>
where
    G: GraphViewOps<'graph> + 'graph,
    GH: GraphViewOps<'graph> + 'graph,
{
    type BaseGraph = G;
    type Graph = GH;
    type ValueType<T: NodeOp + 'graph> = LazyNodeState<'graph, T, G, GH>;
    type PropType = NodeView<GH, GH>;
    type PathType = PathFromGraph<'graph, G, G>;
    type Edges = NestedEdges<'graph, G, GH>;

    fn graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn map<F: NodeOp + 'graph>(&self, op: F) -> Self::ValueType<F>
    where
        <F as NodeOp>::Output: 'graph,
    {
        LazyNodeState::new(op, self.clone())
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
        F: Fn(&GraphStorage, &Self::Graph, VID) -> I + Send + Sync + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Edges {
        let graph = self.graph.clone();
        let base_graph = self.base_graph.clone();
        let nodes = self.clone();
        let nodes = Arc::new(move || nodes.iter_refs().into_dyn_boxed());
        let edges = Arc::new(move |node: VID| {
            let cg = graph.core_graph();
            op(cg, &graph, node).into_dyn_boxed()
        });
        let graph = self.graph.clone();
        NestedEdges {
            base_graph,
            graph,
            nodes,
            edges,
        }
    }

    fn hop<
        I: Iterator<Item = VID> + Send + Sync + 'graph,
        F: Fn(&GraphStorage, &Self::Graph, VID) -> I + Send + Sync + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::PathType {
        let graph = self.graph.clone();
        let nodes = self.clone();
        let nodes = Arc::new(move || nodes.iter_refs().into_dyn_boxed());
        PathFromGraph::new(self.base_graph.clone(), nodes, move |v| {
            let cg = graph.core_graph();
            op(cg, &graph, v).into_dyn_boxed()
        })
    }
}

impl<'graph, G, GH> OneHopFilter<'graph> for Nodes<'graph, G, GH>
where
    G: GraphViewOps<'graph> + 'graph,
    GH: GraphViewOps<'graph> + 'graph,
{
    type BaseGraph = G;
    type FilteredGraph = GH;
    type Filtered<GHH: GraphViewOps<'graph>> = Nodes<'graph, G, GHH>;

    fn current_filter(&self) -> &Self::FilteredGraph {
        &self.graph
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.base_graph
    }

    fn one_hop_filtered<GHH: GraphViewOps<'graph>>(
        &self,
        filtered_graph: GHH,
    ) -> Self::Filtered<GHH> {
        let base_graph = self.base_graph.clone();
        Nodes {
            base_graph,
            graph: filtered_graph,
            nodes: self.nodes.clone(),
            node_types_filter: self.node_types_filter.clone(),
            _marker: PhantomData,
        }
    }
}

impl<'graph, G, GH> IntoIterator for Nodes<'graph, G, GH>
where
    G: GraphViewOps<'graph> + 'graph,
    GH: GraphViewOps<'graph> + 'graph,
{
    type Item = NodeView<G, GH>;
    type IntoIter = BoxedLIter<'graph, Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter_owned())
    }
}
