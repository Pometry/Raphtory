use crate::{
    core::entities::{edges::edge_ref::EdgeRef, nodes::node_ref::AsNodeRef, VID},
    db::{
        api::{
            properties::Properties,
            state::LazyNodeState,
            storage::storage_ops::GraphStorage,
            view::{
                internal::{OneHopFilter, Static},
                BaseNodeViewOps, BoxedLIter, DynamicGraph, IntoDynBoxed, IntoDynamic,
            },
        },
        graph::{edges::NestedEdges, node::NodeView, path::PathFromGraph},
    },
    prelude::*,
};

use crate::db::graph::create_node_type_filter;
use rayon::iter::ParallelIterator;
use std::{marker::PhantomData, sync::Arc};

#[derive(Clone)]
pub struct Nodes<'graph, G, GH = G> {
    pub(crate) base_graph: G,
    pub(crate) graph: GH,
    pub(crate) node_types_filter: Option<Arc<[bool]>>,
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
            node_types_filter: None,
            _marker: PhantomData,
        }
    }
}

impl<'graph, G, GH> Nodes<'graph, G, GH>
where
    G: GraphViewOps<'graph> + 'graph,
    GH: GraphViewOps<'graph> + 'graph,
{
    pub fn new_filtered(base_graph: G, graph: GH, node_types_filter: Option<Arc<[bool]>>) -> Self {
        Self {
            base_graph,
            graph,
            node_types_filter,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub(crate) fn iter_refs(&self) -> impl Iterator<Item = VID> + 'graph {
        let g = self.graph.core_graph().lock();
        let node_types_filter = self.node_types_filter.clone();
        g.into_nodes_iter(self.graph.clone(), node_types_filter)
    }

    pub fn iter(&self) -> BoxedLIter<'graph, NodeView<G, GH>> {
        let base_graph = self.base_graph.clone();
        let g = self.graph.clone();
        self.iter_refs()
            .map(move |v| NodeView::new_one_hop_filtered(base_graph.clone(), g.clone(), v))
            .into_dyn_boxed()
    }

    pub fn par_iter(&self) -> impl ParallelIterator<Item = NodeView<&G, &GH>> + '_ {
        let cg = self.graph.core_graph().lock();
        let node_types_filter = self.node_types_filter.clone();
        cg.into_nodes_par(&self.graph, node_types_filter)
            .map(|v| NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, v))
    }

    pub fn into_par_iter(self) -> impl ParallelIterator<Item = NodeView<G, GH>> + 'graph {
        let cg = self.graph.core_graph().lock();
        cg.into_nodes_par(self.graph.clone(), self.node_types_filter)
            .map(move |n| {
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
        Some(NodeView::new_one_hop_filtered(
            self.base_graph.clone(),
            self.graph.clone(),
            vid,
        ))
    }

    pub fn type_filter(&self, node_types: &[impl AsRef<str>]) -> Nodes<'graph, G, GH> {
        let node_types_filter = Some(create_node_type_filter(
            self.graph.node_meta().node_type_meta(),
            node_types,
        ));
        println!(
            "node_types_filter = {:?}",
            node_types_filter.as_ref().unwrap()
        );
        println!(
            "node_types = {:?}",
            self.graph.nodes().node_type().collect_vec()
        );
        println!(
            "node_type_ids = {:?}",
            self.graph.nodes().node_type_id().collect_vec()
        );
        Nodes {
            base_graph: self.base_graph.clone(),
            graph: self.graph.clone(),
            node_types_filter,
            _marker: PhantomData,
        }
    }

    pub fn collect(&self) -> Vec<NodeView<G, GH>> {
        self.iter().collect()
    }

    pub fn get_const_prop_id(&self, prop_name: &str) -> Option<usize> {
        self.graph.node_meta().get_prop_id(prop_name, true)
    }

    pub fn get_temporal_prop_id(&self, prop_name: &str) -> Option<usize> {
        self.graph.node_meta().get_prop_id(prop_name, false)
    }
}

impl<'graph, G, GH> BaseNodeViewOps<'graph> for Nodes<'graph, G, GH>
where
    G: GraphViewOps<'graph> + 'graph,
    GH: GraphViewOps<'graph> + 'graph,
{
    type BaseGraph = G;
    type Graph = GH;
    type ValueType<T: 'graph> = LazyNodeState<'graph, T, G, GH>;
    type PropType = NodeView<GH, GH>;
    type PathType = PathFromGraph<'graph, G, G>;
    type Edges = NestedEdges<'graph, G, GH>;

    fn map<
        O: Clone + Send + Sync + 'graph,
        F: Fn(&GraphStorage, &Self::Graph, VID) -> O + Send + Sync + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        let g = self.graph.clone();
        let bg = self.base_graph.clone();
        LazyNodeState::new(bg, g, self.node_types_filter.clone(), op)
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.map(|_cg, g, v| Properties::new(NodeView::new_internal(g.clone(), v)))
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + 'graph,
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
        Box::new(self.iter())
    }
}
