use crate::{
    core::entities::nodes::node_ref::NodeRef,
    db::{
        api::{properties::Properties, view::Layer},
        graph::{
            edge::EdgeView,
            // path::{Operations, PathFromGraph},
            node::NodeView,
        },
    },
    prelude::*,
};
use crate::{
    core::entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
    db::{
        api::view::{
            internal::{InternalLayerOps, OneHopFilter, Static},
            BaseNodeViewOps, BoxedLIter, DynamicGraph, IntoDynBoxed, IntoDynamic,
        },
        graph::path::PathFromGraph,
    },
};
use std::marker::PhantomData;

#[derive(Clone)]
pub struct Nodes<'graph, G, GH = G> {
    pub(crate) base_graph: G,
    pub(crate) graph: GH,
    _marker: PhantomData<&'graph G>,
}

impl<
        'graph,
        G: GraphViewOps<'graph> + IntoDynamic,
        GH: GraphViewOps<'graph> + IntoDynamic + Static,
    > From<Nodes<'graph, G, GH>> for Nodes<'graph, DynamicGraph, DynamicGraph>
{
    fn from(value: Nodes<'graph, G, GH>) -> Self {
        Nodes::new_filtered(value.base_graph.into_dynamic(), value.graph.into_dynamic())
    }
}

impl<'graph, G: GraphViewOps<'graph>> Nodes<'graph, G, G> {
    pub fn new(graph: G) -> Nodes<'graph, G, G> {
        let base_graph = graph.clone();
        Self {
            base_graph,
            graph,
            _marker: PhantomData,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Nodes<'graph, G, GH> {
    pub fn new_filtered(base_graph: G, graph: GH) -> Self {
        Self {
            base_graph,
            graph,
            _marker: PhantomData,
        }
    }
    #[inline]
    fn iter_refs(&self) -> impl Iterator<Item = VID> + 'graph {
        self.graph
            .node_refs(self.graph.layer_ids(), self.graph.edge_filter())
    }
    pub fn iter(&self) -> BoxedLIter<'graph, NodeView<G, GH>> {
        let base_graph = self.base_graph.clone();
        let g = self.graph.clone();
        self.iter_refs()
            .map(move |v| NodeView::new_one_hop_filtered(base_graph.clone(), g.clone(), v))
            .into_dyn_boxed()
    }

    /// Returns the number of nodes in the graph.
    pub fn len(&self) -> usize {
        self.graph.count_nodes()
    }

    /// Returns true if the graph contains no nodes.
    pub fn is_empty(&self) -> bool {
        self.graph.is_empty()
    }

    pub fn get<V: Into<NodeRef>>(&self, node: V) -> Option<NodeView<G, GH>> {
        let vid = self.graph.internalise_node(node.into())?;
        Some(NodeView::new_one_hop_filtered(
            self.base_graph.clone(),
            self.graph.clone(),
            vid,
        ))
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> InternalLayerOps
    for Nodes<'graph, G, GH>
{
    fn layer_ids(&self) -> LayerIds {
        self.graph.layer_ids()
    }

    fn layer_ids_from_names(&self, key: Layer) -> LayerIds {
        self.graph.layer_ids_from_names(key)
    }
}

impl<'graph, G: GraphViewOps<'graph> + 'graph, GH: GraphViewOps<'graph> + 'graph>
    BaseNodeViewOps<'graph> for Nodes<'graph, G, GH>
{
    type BaseGraph = G;
    type Graph = GH;
    type ValueType<T: 'graph> = BoxedLIter<'graph, T>;
    type PropType = NodeView<GH, GH>;
    type PathType = PathFromGraph<'graph, G, G>;
    type Edge = EdgeView<G, GH>;
    type EList = BoxedLIter<'graph, BoxedLIter<'graph, EdgeView<G, GH>>>;

    fn map<O: 'graph, F: for<'a> Fn(&'a Self::Graph, VID) -> O + Send + Sync + 'graph>(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        let g = self.graph.clone();
        Box::new(self.iter_refs().map(move |v| op(&g, v)))
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.map(|g, v| Properties::new(NodeView::new_internal(g.clone(), v)))
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + 'graph,
        F: for<'a> Fn(&'a Self::Graph, VID) -> I + Send + Sync + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::EList {
        let graph = self.graph.clone();
        let base_graph = self.base_graph.clone();
        self.iter_refs()
            .map(move |v| {
                let base_graph = base_graph.clone();
                let graph = graph.clone();
                op(&graph, v)
                    .map(move |edge| {
                        EdgeView::new_filtered(base_graph.clone(), graph.clone(), edge)
                    })
                    .into_dyn_boxed()
            })
            .into_dyn_boxed()
    }

    fn hop<
        I: Iterator<Item = VID> + Send + 'graph,
        F: for<'a> Fn(&'a Self::Graph, VID) -> I + Send + Sync + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::PathType {
        let graph = self.graph.clone();
        PathFromGraph::new(self.base_graph.clone(), move |v| {
            op(&graph, v).into_dyn_boxed()
        })
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> OneHopFilter<'graph>
    for Nodes<'graph, G, GH>
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
            _marker: PhantomData,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph> + 'graph, GH: GraphViewOps<'graph> + 'graph> IntoIterator
    for Nodes<'graph, G, GH>
{
    type Item = NodeView<G, GH>;
    type IntoIter = BoxedLIter<'graph, Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter().into_dyn_boxed()
    }
}
