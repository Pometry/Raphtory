use crate::{
    core::entities::{edges::edge_ref::EdgeRef, VID},
    db::{
        api::{
            state::NodeOp,
            view::{
                internal::{BaseFilter, OneHopFilter},
                BaseNodeViewOps, BoxedLIter, DynamicGraph, IntoDynBoxed,
            },
        },
        graph::{
            create_node_type_filter,
            edges::{Edges, NestedEdges},
            node::NodeView,
            views::{
                filter::node_type_filtered_graph::NodeTypeFilteredGraph, layer_graph::LayeredGraph,
                window_graph::WindowedGraph,
            },
        },
    },
    prelude::*,
};
use raphtory_storage::graph::graph::GraphStorage;
use std::sync::Arc;

#[derive(Clone)]
pub struct PathFromGraph<'graph, G, GH> {
    pub(crate) base_graph: G,
    pub(crate) one_hop_graph: GH,
    pub(crate) nodes: Arc<dyn Fn() -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>,
    pub(crate) op: Arc<dyn Fn(VID) -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>,
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
            one_hop_graph: graph,
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
            one_hop_graph: graph,
            base_graph,
            nodes,
            op,
        }
    }

    fn base_iter(&self) -> BoxedLIter<'graph, VID> {
        (self.nodes)()
    }

    pub fn iter(&self) -> impl Iterator<Item = PathFromNode<'graph, G, GH>> + Send + 'graph {
        let base_graph = self.base_graph.clone();
        let graph = self.one_hop_graph.clone();
        let op = self.op.clone();
        self.base_iter().map(move |v| {
            let op = op.clone();
            let node_op = Arc::new(move || op(v));
            PathFromNode::new_one_hop_filtered(base_graph.clone(), graph.clone(), node_op)
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
            create_node_type_filter(self.one_hop_graph.node_meta().node_type_meta(), node_types);

        let base_graph = self.base_graph.clone();
        let old_op = self.op.clone();

        PathFromGraph::new_filtered(
            self.base_graph.clone(),
            self.one_hop_graph.clone(),
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

    pub fn collect(&self) -> Vec<Vec<NodeView<'graph, G>>> {
        self.iter().map(|path| path.collect()).collect()
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> BaseNodeViewOps<'graph>
    for PathFromGraph<'graph, G, GH>
{
    type Graph = G;
    type ValueType<T: NodeOp + 'graph> = BoxedLIter<'graph, BoxedLIter<'graph, T::Output>>;
    type PropType = NodeView<'graph, GH>;
    type PathType = PathFromGraph<'graph, G, G>;
    type Edges = NestedEdges<'graph, G, GH>;

    fn graph(&self) -> &Self::Graph {
        &self.base_graph
    }

    fn map<F: NodeOp + Clone + 'graph>(&self, op: F) -> Self::ValueType<F>
    where
        <F as NodeOp>::Output: 'graph,
    {
        let storage = self.one_hop_graph.core_graph().lock();
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
        let base_graph = self.base_graph.clone();
        let node_op = self.op.clone();
        let edges = Arc::new(move |node: VID| {
            let op = op.clone();
            let base_graph = base_graph.clone();
            node_op(node)
                .flat_map(move |node| op(base_graph.core_graph(), &base_graph, node))
                .into_dyn_boxed()
        });
        NestedEdges {
            base_graph: self.base_graph.clone(),
            graph: self.one_hop_graph.clone(),
            nodes: self.nodes.clone(),
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
        let base_graph = self.base_graph.clone();
        PathFromGraph::new(self.base_graph.clone(), nodes, move |v| {
            let op = op.clone();
            let base_graph = base_graph.clone();
            Box::new(old_op(v).flat_map(move |vv| op(base_graph.core_graph(), &base_graph, vv)))
        })
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> IntoIterator
    for PathFromGraph<'graph, G, GH>
{
    type Item = PathFromNode<'graph, G, GH>;
    type IntoIter = BoxedLIter<'graph, Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        let base_graph = self.base_graph;
        let graph = self.one_hop_graph;
        let op = self.op;
        (self.nodes)()
            .map(move |node| {
                let op = op.clone();
                let node_op = Arc::new(move || op(node));
                PathFromNode::new_one_hop_filtered(base_graph.clone(), graph.clone(), node_op)
            })
            .into_dyn_boxed()
    }
}

impl<'graph, Current, G> BaseFilter<'graph> for PathFromGraph<'graph, Current, G>
where
    Current: GraphViewOps<'graph>,
    G: GraphViewOps<'graph>,
{
    type BaseGraph = Current;
    type Filtered<Next: GraphViewOps<'graph>> = PathFromGraph<'graph, Next, G>;

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.base_graph
    }

    fn apply_filter<Next: GraphViewOps<'graph>>(
        &self,
        filtered_graph: Next,
    ) -> Self::Filtered<Next> {
        PathFromGraph {
            base_graph: filtered_graph,
            one_hop_graph: self.one_hop_graph.clone(),
            nodes: self.nodes.clone(),
            op: self.op.clone(),
        }
    }
}

impl<'graph, Current, G> OneHopFilter<'graph> for PathFromGraph<'graph, G, Current>
where
    G: GraphViewOps<'graph>,
    Current: GraphViewOps<'graph>,
{
    type OneHopGraph = Current;
    type OneHopFiltered<Next: GraphViewOps<'graph> + 'graph> = PathFromGraph<'graph, G, Next>;

    fn one_hop_graph(&self) -> &Self::OneHopGraph {
        &self.one_hop_graph
    }

    fn apply_one_hop_filter<Next: GraphViewOps<'graph>>(
        &self,
        filtered_graph: Next,
    ) -> Self::OneHopFiltered<Next> {
        PathFromGraph {
            base_graph: self.base_graph.clone(),
            one_hop_graph: filtered_graph,
            nodes: self.nodes.clone(),
            op: self.op.clone(),
        }
    }
}

impl From<PathFromNode<'static, DynamicGraph, LayeredGraph<DynamicGraph>>>
    for PathFromNode<'static, DynamicGraph, DynamicGraph>
{
    fn from(value: PathFromNode<'static, DynamicGraph, LayeredGraph<DynamicGraph>>) -> Self {
        PathFromNode::new(DynamicGraph::new(value.one_hop_graph.clone()), move || {
            (value.op)()
        })
    }
}

impl From<PathFromNode<'static, DynamicGraph, WindowedGraph<DynamicGraph>>>
    for PathFromNode<'static, DynamicGraph, DynamicGraph>
{
    fn from(value: PathFromNode<'static, DynamicGraph, WindowedGraph<DynamicGraph>>) -> Self {
        PathFromNode::new(DynamicGraph::new(value.one_hop_graph.clone()), move || {
            (value.op)()
        })
    }
}

impl From<PathFromNode<'static, DynamicGraph, NodeTypeFilteredGraph<DynamicGraph>>>
    for PathFromNode<'static, DynamicGraph, DynamicGraph>
{
    fn from(
        value: PathFromNode<'static, DynamicGraph, NodeTypeFilteredGraph<DynamicGraph>>,
    ) -> Self {
        PathFromNode::new(DynamicGraph::new(value.one_hop_graph.clone()), move || {
            (value.op)()
        })
    }
}

#[derive(Clone)]
pub struct PathFromNode<'graph, G, GH> {
    pub(crate) base_graph: G,
    pub one_hop_graph: GH,
    pub(crate) op: Arc<dyn Fn() -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>,
}

impl<'graph, G: GraphViewOps<'graph>> PathFromNode<'graph, G, G> {
    pub(crate) fn new<OP: Fn() -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>(
        graph: G,
        op: OP,
    ) -> PathFromNode<'graph, G, G> {
        let base_graph = graph.clone();
        let op = Arc::new(op);
        PathFromNode {
            base_graph,
            one_hop_graph: graph,
            op,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> PathFromNode<'graph, G, GH> {
    pub(crate) fn new_one_hop_filtered(
        base_graph: G,
        graph: GH,
        op: Arc<dyn Fn() -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>,
    ) -> Self {
        Self {
            base_graph,
            one_hop_graph: graph,
            op,
        }
    }

    pub fn iter_refs(&self) -> BoxedLIter<'graph, VID> {
        (self.op)()
    }

    pub fn iter(&self) -> BoxedLIter<'graph, NodeView<'graph, G>> {
        let base_graph = self.base_graph.clone();
        let iter = self
            .iter_refs()
            .map(move |node| NodeView::new_one_hop_filtered(base_graph.clone(), node));
        Box::new(iter)
    }

    pub fn len(&self) -> usize {
        self.iter().count()
    }

    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    pub fn type_filter<I: IntoIterator<Item = V>, V: AsRef<str>>(
        &self,
        node_types: I,
    ) -> PathFromNode<'graph, G, GH> {
        let node_types_filter =
            create_node_type_filter(self.one_hop_graph.node_meta().node_type_meta(), node_types);

        let base_graph = self.base_graph.clone();
        let old_op = self.op.clone();

        PathFromNode {
            base_graph: self.base_graph.clone(),
            one_hop_graph: self.one_hop_graph.clone(),
            op: Arc::new(move || {
                let base_graph = base_graph.clone();
                let node_types_filter = node_types_filter.clone();
                old_op()
                    .filter(move |v| {
                        let node_type_id = base_graph.node_type_id(*v);
                        node_types_filter[node_type_id]
                    })
                    .into_dyn_boxed()
            }),
        }
    }

    pub fn collect(&self) -> Vec<NodeView<'graph, G>> {
        self.iter().collect()
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> BaseNodeViewOps<'graph>
    for PathFromNode<'graph, G, GH>
{
    type Graph = G;
    type ValueType<T: NodeOp + 'graph> = BoxedLIter<'graph, T::Output>;
    type PropType = NodeView<'graph, G>;
    type PathType = PathFromNode<'graph, G, G>;
    type Edges = Edges<'graph, G, G>;

    fn graph(&self) -> &Self::Graph {
        &self.base_graph
    }

    fn map<F: NodeOp + 'graph>(&self, op: F) -> Self::ValueType<F>
    where
        <F as NodeOp>::Output: 'graph,
    {
        let storage = self.one_hop_graph.core_graph().lock();
        Box::new(self.iter_refs().map(move |node| op.apply(&storage, node)))
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
        F: Fn(&GraphStorage, &Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Edges {
        let node_op = self.op.clone();
        let base_graph = Arc::new(self.base_graph.clone());
        let edges = Arc::new(move || {
            let op = op.clone();
            let base_graph = base_graph.clone();
            node_op()
                .flat_map(move |node| op(base_graph.core_graph(), &base_graph, node))
                .into_dyn_boxed()
        });
        Edges {
            base_graph: self.base_graph.clone(),
            graph: self.base_graph.clone(),
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
        let base_graph = Arc::new(self.base_graph.clone());
        PathFromNode::new(self.base_graph.clone(), move || {
            let op = op.clone();
            let old_op = old_op.clone();
            let base_graph = base_graph.clone();
            old_op()
                .flat_map(move |vv| op(base_graph.core_graph(), &base_graph, vv))
                .into_dyn_boxed()
        })
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> IntoIterator
    for PathFromNode<'graph, G, GH>
{
    type Item = NodeView<'graph, G>;
    type IntoIter = BoxedLIter<'graph, NodeView<'graph, G>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'graph, Current, G> BaseFilter<'graph> for PathFromNode<'graph, Current, G>
where
    Current: GraphViewOps<'graph>,
    G: GraphViewOps<'graph>,
{
    type BaseGraph = Current;
    type Filtered<Next: GraphViewOps<'graph>> = PathFromNode<'graph, Next, G>;

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.base_graph
    }

    fn apply_filter<Next: GraphViewOps<'graph>>(
        &self,
        filtered_graph: Next,
    ) -> Self::Filtered<Next> {
        PathFromNode {
            base_graph: filtered_graph,
            one_hop_graph: self.one_hop_graph.clone(),
            op: self.op.clone(),
        }
    }
}

impl<'graph, Current, G> OneHopFilter<'graph> for PathFromNode<'graph, G, Current>
where
    G: GraphViewOps<'graph>,
    Current: GraphViewOps<'graph>,
{
    type OneHopGraph = Current;
    type OneHopFiltered<Next: GraphViewOps<'graph>> = PathFromNode<'graph, G, Next>;

    fn one_hop_graph(&self) -> &Self::OneHopGraph {
        &self.one_hop_graph
    }

    fn apply_one_hop_filter<Next: GraphViewOps<'graph>>(
        &self,
        filtered_graph: Next,
    ) -> Self::OneHopFiltered<Next> {
        PathFromNode {
            base_graph: self.base_graph.clone(),
            one_hop_graph: filtered_graph,
            op: self.op.clone(),
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

        let n = Vec::from_iter(g.node(1).unwrap().neighbours().id());
        assert_eq!(n, [GID::U64(2)])
    }
}
