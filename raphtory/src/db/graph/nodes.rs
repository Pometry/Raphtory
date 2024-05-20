use crate::{
    core::entities::{edges::edge_ref::EdgeRef, nodes::node_ref::NodeRef, VID},
    db::{
        api::{
            properties::Properties,
            view::{
                internal::{OneHopFilter, Static},
                BaseNodeViewOps, BoxedLIter, DynamicGraph, IntoDynBoxed, IntoDynamic,
            },
        },
        graph::{edges::NestedEdges, node::NodeView, path::PathFromGraph},
    },
    prelude::*,
};

use crate::db::api::{storage::locked::LockedGraph, view::internal::NodeTypeFilter};
use rayon::iter::ParallelIterator;
use std::{marker::PhantomData, sync::Arc};

#[derive(Clone)]
pub struct Nodes<'graph, G, GH = G> {
    pub(crate) base_graph: G,
    pub(crate) graph: GH,
    node_types: Arc<[usize]>,
    _marker: PhantomData<&'graph ()>,
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
            node_types: value.node_types,
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
            node_types: [].into(),
            _marker: PhantomData,
        }
    }
}

impl<'graph, G, GH> Nodes<'graph, G, GH>
where
    G: GraphViewOps<'graph> + 'graph,
    GH: GraphViewOps<'graph> + 'graph,
{
    pub fn new_filtered(base_graph: G, graph: GH) -> Self {
        Self {
            base_graph,
            graph,
            node_types: [].into(),
            _marker: PhantomData,
        }
    }

    #[inline]
    fn iter_refs(&self) -> impl Iterator<Item = VID> + 'graph {
        let g = self.graph.core_graph();
        g.into_nodes_iter(self.graph.clone())
    }

    pub fn iter(&self) -> BoxedLIter<'graph, NodeView<G, GH>> {
        let base_graph = self.base_graph.clone();
        let g = self.graph.clone();
        self.iter_refs()
            .map(move |v| NodeView::new_one_hop_filtered(base_graph.clone(), g.clone(), v))
            .into_dyn_boxed()
    }

    pub fn par_iter(&self) -> impl ParallelIterator<Item = NodeView<&G, &GH>> + '_ {
        let cg = self.graph.core_graph();
        cg.into_nodes_par(&self.graph)
            .map(|v| NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, v))
    }

    /// Returns the number of nodes in the graph.
    pub fn len(&self) -> usize {
        self.graph.count_nodes()
    }

    /// Returns true if the graph contains no nodes.
    pub fn is_empty(&self) -> bool {
        self.graph.is_empty()
    }

    pub fn get<N: Into<NodeRef>>(&self, node: N) -> Option<NodeView<G, GH>> {
        let vid = self.graph.internalise_node(node.into())?;
        Some(NodeView::new_one_hop_filtered(
            self.base_graph.clone(),
            self.graph.clone(),
            vid,
        ))
    }

    pub fn type_filter<I: IntoIterator<Item = V>, V: AsRef<str>>(
        &self,
        node_types: I,
    ) -> BoxedLIter<'graph, NodeView<G, G>>
    where
        I::IntoIter: Send + Sync + 'graph,
        V: Send + Sync + 'graph,
    {
        let base_graph = self.base_graph.clone();
        node_types
            .into_iter()
            .flat_map(move |nt| {
                base_graph.nodes().into_iter().filter_map(move |node| {
                    if nt.as_ref() == node.node_type()?.as_ref() {
                        Some(node)
                    } else {
                        None
                    }
                })
            })
            .into_dyn_boxed()
    }

    pub fn collect(&self) -> Vec<NodeView<G, GH>> {
        self.iter().collect()
    }
}

impl<'graph, G, GH> BaseNodeViewOps<'graph> for Nodes<'graph, G, GH>
where
    G: GraphViewOps<'graph> + 'graph,
    GH: GraphViewOps<'graph> + 'graph,
{
    type BaseGraph = G;
    type Graph = GH;
    type ValueType<T: 'graph> = BoxedLIter<'graph, T>;
    type PropType = NodeView<GH, GH>;
    type PathType = PathFromGraph<'graph, G, G>;
    type Edges = NestedEdges<'graph, G, GH>;

    fn map<O: 'graph, F: Fn(&LockedGraph, &Self::Graph, VID) -> O + Send + Sync + 'graph>(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        let g = self.graph.clone();
        let cg = g.core_graph();
        Box::new(self.iter_refs().map(move |v| op(&cg, &g, v)))
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.map(|_cg, g, v| Properties::new(NodeView::new_internal(g.clone(), v)))
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + 'graph,
        F: Fn(&LockedGraph, &Self::Graph, VID) -> I + Send + Sync + 'graph,
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
            op(&cg, &graph, node).into_dyn_boxed()
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
        I: Iterator<Item = VID> + Send + 'graph,
        F: Fn(&LockedGraph, &Self::Graph, VID) -> I + Send + Sync + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::PathType {
        let graph = self.graph.clone();
        let nodes = self.clone();
        let nodes: Arc<dyn Fn() -> BoxedLIter<'graph, VID> + Send + Sync> =
            if !nodes.node_types.is_empty() {
                Arc::new(move || {
                    let nodes = nodes.clone();
                    let node_types = nodes.node_types.clone();
                    nodes
                        .iter_refs()
                        .filter(move |v| {
                            let node_types = node_types.clone();
                            let node_type = nodes.base_graph().node_type_id(*v);
                            node_types.contains(&node_type)
                        })
                        .into_dyn_boxed()
                })
            } else {
                Arc::new(move || nodes.iter_refs().into_dyn_boxed())
            };

        PathFromGraph::new(self.base_graph.clone(), nodes, move |v| {
            let cg = graph.core_graph();
            op(&cg, &graph, v).into_dyn_boxed()
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
            node_types: self.node_types.clone(),
            _marker: PhantomData,
        }
    }
}

impl<'graph, G, GH> NodeTypeFilter<'graph, G, GH> for Nodes<'graph, G, GH>
where
    G: GraphViewOps<'graph> + 'graph,
    GH: GraphViewOps<'graph> + 'graph,
{
    fn node_type_filter(&self, node_types: &[impl AsRef<str>]) -> Nodes<'graph, G, GH> {
        let node_types = node_types
            .iter()
            .filter_map(|nt| self.graph.node_meta().get_node_type_id(nt.as_ref()))
            .collect();

        Nodes {
            base_graph: self.base_graph.clone(),
            graph: self.graph.clone(),
            node_types,
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
        Box::new(self.iter())
    }
}
