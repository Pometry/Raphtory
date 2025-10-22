use crate::{
    core::entities::{edges::edge_ref::EdgeRef, VID},
    db::{
        api::{
            state::{Index, LazyNodeState, NodeOp},
            view::{
                history::History,
                internal::{NodeList, OneHopFilter},
                BaseNodeViewOps, BoxedLIter, DynamicGraph, ExplodedEdgePropertyFilterOps,
                IntoDynBoxed,
            },
        },
        graph::{
            create_node_type_filter,
            edges::{Edges, NestedEdges},
            node::NodeView,
            nodes::Nodes,
            views::{
                filter::node_type_filtered_graph::NodeTypeFilteredGraph, layer_graph::LayeredGraph,
                window_graph::WindowedGraph,
            },
        },
    },
    prelude::*,
};
use raphtory_storage::graph::graph::GraphStorage;
use std::{marker::PhantomData, sync::Arc};

#[derive(Clone)]
pub struct PathFromGraph<'graph, G, GH> {
    pub(crate) base_graph: G,
    pub(crate) graph: GH,
    pub(crate) nodes: Arc<dyn Fn() -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>,
    pub(crate) op: Arc<dyn Fn(VID) -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>,
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> EdgePropertyFilterOps<'graph>
    for PathFromGraph<'graph, G, GH>
{
}
impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    ExplodedEdgePropertyFilterOps<'graph> for PathFromGraph<'graph, G, GH>
{
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> NodePropertyFilterOps<'graph>
    for PathFromGraph<'graph, G, GH>
{
}

impl<'graph, G: GraphViewOps<'graph>> PathFromGraph<'graph, G, G> {
    pub fn new<OP: Fn(VID) -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>(
        graph: G,
        nodes: Arc<dyn Fn() -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>,
        op: OP,
    ) -> Self {
        let base_graph = graph.clone();
        let op = Arc::new(op);
        PathFromGraph {
            graph,
            base_graph,
            nodes,
            op,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> PathFromGraph<'graph, G, GH> {
    fn new_filtered<OP: Fn(VID) -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>(
        base_graph: G,
        graph: GH,
        nodes: Arc<dyn Fn() -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>,
        op: OP,
    ) -> Self {
        let op = Arc::new(op);
        PathFromGraph {
            graph,
            base_graph,
            nodes,
            op,
        }
    }

    fn base_iter(&self) -> BoxedLIter<'graph, VID> {
        (self.nodes)()
    }

    pub fn iter(&self) -> impl Iterator<Item = PathFromNode<'graph, G, GH>> + Send + 'graph {
        let graph = self.graph.clone();
        let base_graph = self.base_graph.clone();
        let op = self.op.clone();
        // FIXME: Temporarily collect VIDs, will need to update
        self.base_iter().map(move |v| {
            let op = op.clone();
            let nodes: Index<_> = op(v).collect();
            PathFromNode::new_one_hop_filtered(base_graph.clone(), graph.clone(), nodes)
        })
    }

    pub fn iter_refs(&self) -> impl Iterator<Item = BoxedLIter<'graph, VID>> + Send + 'graph {
        let op = self.op.clone();
        self.base_iter().map(move |vid| op(vid))
    }

    pub fn total_count(&self) -> usize {
        self.iter_refs().flatten().count()
    }

    pub fn len(&self) -> usize {
        self.iter_refs().count()
    }

    pub fn is_all_empty(&self) -> bool {
        self.iter_refs().flatten().next().is_none()
    }

    pub fn is_empty(&self) -> bool {
        self.iter_refs().next().is_none()
    }

    pub fn type_filter<I: IntoIterator<Item = V>, V: AsRef<str>>(
        &self,
        node_types: I,
    ) -> PathFromGraph<'graph, G, GH> {
        let node_types_filter =
            create_node_type_filter(self.graph.node_meta().node_type_meta(), node_types);

        let base_graph = self.base_graph.clone();
        let old_op = self.op.clone();

        PathFromGraph::new_filtered(
            self.base_graph.clone(),
            self.graph.clone(),
            self.nodes.clone(),
            move |vid| {
                let base_graph = base_graph.clone();
                let node_types_filter = node_types_filter.clone();
                old_op(vid)
                    .filter(move |v| {
                        let node_type_id = base_graph.node_type_id(*v);
                        node_types_filter[node_type_id]
                    })
                    .into_dyn_boxed()
            },
        )
    }

    pub fn collect(&self) -> Vec<Vec<NodeView<'graph, G, GH>>> {
        self.iter().map(|path| path.collect()).collect()
    }

    pub fn combined_history(&self) -> History<'graph, Self> {
        History::new(self.clone())
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> BaseNodeViewOps<'graph>
    for PathFromGraph<'graph, G, GH>
{
    type BaseGraph = G;
    type Graph = GH;
    type ValueType<T: NodeOp + 'graph> = BoxedLIter<'graph, BoxedLIter<'graph, T::Output>>;
    type PropType = NodeView<'graph, GH, GH>;
    type PathType = PathFromGraph<'graph, G, G>;
    type Edges = NestedEdges<'graph, G, GH>;

    fn graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn map<F: NodeOp + Clone + 'graph>(&self, op: F) -> Self::ValueType<F>
    where
        <F as NodeOp>::Output: 'graph,
    {
        let storage = self.graph.core_graph().lock();
        self.iter_refs()
            .map(move |it| {
                let op = op.clone();
                let storage = storage.clone();
                it.map(move |node| op.apply(&storage, node))
                    .into_dyn_boxed()
            })
            .into_dyn_boxed()
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
        F: Fn(&GraphStorage, &Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Edges {
        let graph = self.graph.clone();
        let base_graph = self.base_graph.clone();
        let nodes = self.nodes.clone();
        let node_op = self.op.clone();
        let edges = Arc::new(move |node: VID| {
            let op = op.clone();
            let graph = graph.clone();
            node_op(node)
                .flat_map(move |node| op(graph.core_graph(), &graph, node))
                .into_dyn_boxed()
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
        F: Fn(&GraphStorage, &Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::PathType {
        let old_op = self.op.clone();
        let nodes = self.nodes.clone();
        let graph = self.graph.clone();
        PathFromGraph::new(self.base_graph.clone(), nodes, move |v| {
            let op = op.clone();
            let graph = graph.clone();
            Box::new(old_op(v).flat_map(move |vv| op(graph.core_graph(), &graph, vv)))
        })
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> IntoIterator
    for PathFromGraph<'graph, G, GH>
{
    type Item = PathFromNode<'graph, G, GH>;
    type IntoIter = BoxedLIter<'graph, Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        let graph = self.graph;
        let base_graph = self.base_graph;
        let op = self.op;
        // FIXME: Temporarily collect VIDs, will need to update
        (self.nodes)()
            .map(move |node| {
                let op = op.clone();
                let nodes: Index<_> = op(node).collect();
                PathFromNode::new_one_hop_filtered(base_graph.clone(), graph.clone(), nodes)
            })
            .into_dyn_boxed()
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> OneHopFilter<'graph>
    for PathFromGraph<'graph, G, GH>
{
    type BaseGraph = G;
    type FilteredGraph = GH;
    type Filtered<GHH: GraphViewOps<'graph>> = PathFromGraph<'graph, G, GHH>;

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
        let nodes = self.nodes.clone();
        let op = self.op.clone();
        PathFromGraph {
            graph: filtered_graph,
            base_graph,
            nodes,
            op,
        }
    }
}

impl From<PathFromNode<'static, DynamicGraph, LayeredGraph<DynamicGraph>>>
    for PathFromNode<'static, DynamicGraph, DynamicGraph>
{
    fn from(value: PathFromNode<'static, DynamicGraph, LayeredGraph<DynamicGraph>>) -> Self {
        PathFromNode::new(DynamicGraph::new(value.graph.clone()), value.nodes.clone())
    }
}

impl From<PathFromNode<'static, DynamicGraph, WindowedGraph<DynamicGraph>>>
    for PathFromNode<'static, DynamicGraph, DynamicGraph>
{
    fn from(value: PathFromNode<'static, DynamicGraph, WindowedGraph<DynamicGraph>>) -> Self {
        PathFromNode::new(DynamicGraph::new(value.graph.clone()), value.nodes.clone())
    }
}

impl From<PathFromNode<'static, DynamicGraph, NodeTypeFilteredGraph<DynamicGraph>>>
    for PathFromNode<'static, DynamicGraph, DynamicGraph>
{
    fn from(
        value: PathFromNode<'static, DynamicGraph, NodeTypeFilteredGraph<DynamicGraph>>,
    ) -> Self {
        PathFromNode::new(DynamicGraph::new(value.graph.clone()), value.nodes.clone())
    }
}

#[derive(Clone)]
pub struct PathFromNode<'graph, G, GH> {
    pub graph: GH,
    pub(crate) base_graph: G,
    pub(crate) nodes: Index<VID>,
    pub(crate) node_types_filter: Option<Arc<[bool]>>,
    pub(crate) _marker: PhantomData<&'graph ()>,
    // pub(crate) op: Arc<dyn Fn() -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>,
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> EdgePropertyFilterOps<'graph>
    for PathFromNode<'graph, G, GH>
{
}
impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    ExplodedEdgePropertyFilterOps<'graph> for PathFromNode<'graph, G, GH>
{
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> NodePropertyFilterOps<'graph>
    for PathFromNode<'graph, G, GH>
{
}

impl<'graph, G: GraphViewOps<'graph>> PathFromNode<'graph, G, G> {
    pub(crate) fn new(graph: G, nodes: Index<VID>) -> PathFromNode<'graph, G, G> {
        let base_graph = graph.clone();
        PathFromNode {
            base_graph,
            graph,
            nodes,
            node_types_filter: None,
            _marker: PhantomData,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> PathFromNode<'graph, G, GH> {
    pub(crate) fn new_one_hop_filtered(base_graph: G, graph: GH, nodes: Index<VID>) -> Self {
        Self {
            base_graph,
            graph,
            nodes,
            node_types_filter: None,
            _marker: PhantomData,
        }
    }

    pub fn node_list(&self) -> NodeList {
        let elems = self.nodes.clone();
        NodeList::List { elems }
    }

    pub fn iter_refs(&self) -> impl Iterator<Item = VID> + Send + Sync + 'graph {
        let base_graph = self.base_graph.clone();
        let node_types_filter = self.node_types_filter.clone();
        // FIXME: Nodes equivalent locks graph, should this be locking the graph? same for all iter_ functions
        self.nodes.clone().into_iter().filter(move |vid| {
            node_types_filter
                .as_ref()
                .is_none_or(|type_filter| type_filter[base_graph.node_type_id(*vid)])
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = NodeView<'_, &G, &GH>> + use<'_, 'graph, G, GH> {
        self.iter_refs()
            .map(|v| NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, v))
    }

    pub fn iter_owned(&self) -> BoxedLIter<'graph, NodeView<'graph, G, GH>> {
        let base_graph = self.base_graph.clone();
        let g = self.graph.clone();
        self.iter_refs()
            .map(move |v| NodeView::new_one_hop_filtered(base_graph.clone(), g.clone(), v))
            .into_dyn_boxed()
    }

    pub fn len(&self) -> usize {
        if self.is_filtered() {
            self.iter_refs().count()
        } else {
            self.nodes.len()
        }
    }

    pub fn is_filtered(&self) -> bool {
        // FIXME: Previous implementation didn't check if self.graph.filtered(), but Nodes equivalent does
        self.node_types_filter.is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    pub fn type_filter<I: IntoIterator<Item = V>, V: AsRef<str>>(
        &self,
        node_types: I,
    ) -> PathFromNode<'graph, G, GH> {
        let node_types_filter =
            create_node_type_filter(self.graph.node_meta().node_type_meta(), node_types);

        PathFromNode {
            base_graph: self.base_graph.clone(),
            graph: self.graph.clone(),
            nodes: self.nodes.clone(),
            node_types_filter: Some(node_types_filter),
            _marker: PhantomData,
        }
    }

    /// Collect all nodes into a list
    ///
    /// Returns:
    ///     list[NodeView]: the list of nodes
    pub fn collect(&self) -> Vec<NodeView<'graph, G, GH>> {
        self.iter_owned().collect()
    }

    pub fn combined_history(&self) -> History<'graph, Self> {
        History::new(self.clone())
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> BaseNodeViewOps<'graph>
    for PathFromNode<'graph, G, GH>
{
    type BaseGraph = G;
    type Graph = GH;
    type ValueType<T: NodeOp + 'graph> = LazyNodeState<'graph, T, G, GH>;
    type PropType = NodeView<'graph, GH, GH>;
    type PathType = PathFromNode<'graph, G, G>;
    type Edges = Edges<'graph, G, GH>;

    fn graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn map<F: NodeOp + 'graph>(&self, op: F) -> Self::ValueType<F>
    where
        <F as NodeOp>::Output: 'graph,
    {
        let nodes = Nodes::new_filtered(
            self.base_graph.clone(),
            self.graph.clone(),
            Some(self.nodes.clone()),
            self.node_types_filter.clone(),
        );
        LazyNodeState::new(op, nodes)
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
        F: Fn(&GraphStorage, &Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Edges {
        let graph = self.graph.clone();
        let base_graph = self.base_graph.clone();
        let nodes = self.clone();
        let nodes = Arc::new(move || nodes.iter_refs());
        let edges = Arc::new(move || {
            let graph = graph.clone();
            let op = op.clone();
            nodes()
                .flat_map(move |node| op(graph.core_graph(), &graph, node))
                .into_dyn_boxed()
        });
        let graph = self.graph.clone();
        Edges {
            graph,
            base_graph,
            edges,
        }
    }

    fn hop<
        I: Iterator<Item = VID> + Send + Sync + 'graph,
        F: Fn(&GraphStorage, &Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::PathType {
        let graph = self.graph.clone();
        let nodes: Index<_> = self
            .iter_refs()
            .flat_map(move |v| op(graph.core_graph(), &graph, v))
            .collect();

        PathFromNode::new(self.base_graph.clone(), nodes)
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> IntoIterator
    for PathFromNode<'graph, G, GH>
{
    type Item = NodeView<'graph, G, GH>;
    type IntoIter = BoxedLIter<'graph, Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_owned()
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> OneHopFilter<'graph>
    for PathFromNode<'graph, G, GH>
{
    type BaseGraph = G;
    type FilteredGraph = GH;
    type Filtered<GHH: GraphViewOps<'graph>> = PathFromNode<'graph, G, GHH>;

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
        PathFromNode {
            base_graph: self.base_graph.clone(),
            graph: filtered_graph,
            nodes: self.nodes.clone(),
            node_types_filter: self.node_types_filter.clone(),
            _marker: PhantomData,
        }
    }
}

#[cfg(test)]
mod test {
    use raphtory_api::core::entities::GID;

    use crate::prelude::*;

    #[test]
    fn test_node_view_ops() {
        let g = Graph::new();

        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();

        let n = Vec::from_iter(g.node(1).unwrap().neighbours().id().iter_values());
        assert_eq!(n, [GID::U64(2)])
    }
}
