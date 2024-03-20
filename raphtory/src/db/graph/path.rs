use crate::{
    core::entities::{edges::edge_ref::EdgeRef, VID},
    db::{
        api::{
            properties::Properties,
            view::{internal::OneHopFilter, BaseNodeViewOps, BoxedLIter, IntoDynBoxed},
        },
        graph::{
            edges::{Edges, NestedEdges},
            node::NodeView,
        },
    },
    prelude::*,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct PathFromGraph<'graph, G, GH> {
    pub(crate) graph: GH,
    pub(crate) base_graph: G,
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
            graph,
            base_graph,
            nodes,
            op,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> PathFromGraph<'graph, G, GH> {
    fn base_iter(&self) -> BoxedLIter<'graph, VID> {
        (self.nodes)()
    }

    pub fn iter(&self) -> impl Iterator<Item = PathFromNode<'graph, G, GH>> + Send + 'graph {
        let graph = self.graph.clone();
        let base_graph = self.base_graph.clone();
        let op = self.op.clone();
        self.base_iter().map(move |node| {
            let op = op.clone();
            let node_op = Arc::new(move || op(node));
            PathFromNode::new_one_hop_filtered(base_graph.clone(), graph.clone(), node_op)
        })
    }

    pub fn iter_refs(&self) -> impl Iterator<Item = BoxedLIter<'graph, VID>> + Send + 'graph {
        let op = self.op.clone();
        self.base_iter().map(move |vid| op(vid))
    }

    pub fn len(&self) -> usize {
        self.iter().count()
    }

    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    pub fn collect(&self) -> Vec<Vec<NodeView<G, GH>>> {
        self.iter().map(|path| path.collect()).collect()
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> BaseNodeViewOps<'graph>
    for PathFromGraph<'graph, G, GH>
{
    type BaseGraph = G;
    type Graph = GH;
    type ValueType<T: 'graph> = BoxedLIter<'graph, BoxedLIter<'graph, T>>;
    type PropType = NodeView<GH, GH>;
    type PathType = PathFromGraph<'graph, G, G>;
    type Edges = NestedEdges<'graph, G, GH>;

    fn map<O: 'graph, F: for<'a> Fn(&'a Self::Graph, VID) -> O + Send + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        let graph = self.graph.clone();
        self.iter_refs()
            .map(move |it| {
                let graph = graph.clone();
                let op = op.clone();
                it.map(move |node| op(&graph, node)).into_dyn_boxed()
            })
            .into_dyn_boxed()
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.map(|g, v| Properties::new(NodeView::new_internal(g.clone(), v)))
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + 'graph,
        F: for<'a> Fn(&'a Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
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
                .flat_map(move |node| op(&graph, node))
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
        I: Iterator<Item = VID> + Send + 'graph,
        F: for<'a> Fn(&'a Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
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
            Box::new(old_op(v).flat_map(move |vv| op(&graph, vv)))
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
        graph
            .core_graph()
            .nodes_iter(&graph)
            .map(move |node| {
                let op = op.clone();
                let node_op = Arc::new(move || op(node));
                PathFromNode::new_one_hop_filtered(base_graph.clone(), graph.clone(), node_op)
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

#[derive(Clone)]
pub struct PathFromNode<'graph, G, GH> {
    pub graph: GH,
    pub(crate) base_graph: G,
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
            graph,
            op,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> PathFromNode<'graph, G, GH> {
    pub fn iter_refs(&self) -> BoxedLIter<'graph, VID> {
        (self.op)()
    }

    pub fn iter(&self) -> BoxedLIter<'graph, NodeView<G, GH>> {
        let graph = self.graph.clone();
        let base_graph = self.base_graph.clone();
        let iter = self.iter_refs().map(move |node| {
            NodeView::new_one_hop_filtered(base_graph.clone(), graph.clone(), node)
        });
        Box::new(iter)
    }

    pub fn len(&self) -> usize {
        self.iter().count()
    }

    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    pub fn collect(&self) -> Vec<NodeView<G, GH>> {
        self.iter().collect()
    }

    pub(crate) fn new_one_hop_filtered(
        base_graph: G,
        graph: GH,
        op: Arc<dyn Fn() -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>,
    ) -> Self {
        Self {
            base_graph,
            graph,
            op,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> BaseNodeViewOps<'graph>
    for PathFromNode<'graph, G, GH>
{
    type BaseGraph = G;
    type Graph = GH;
    type ValueType<T: 'graph> = BoxedLIter<'graph, T>;
    type PropType = NodeView<GH, GH>;
    type PathType = PathFromNode<'graph, G, G>;
    type Edges = Edges<'graph, G, GH>;

    fn map<O: 'graph, F: for<'a> Fn(&'a Self::Graph, VID) -> O + Send + 'graph>(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        let graph = self.graph.clone();
        Box::new(self.iter_refs().map(move |node| op(&graph, node)))
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.map(|g, v| Properties::new(NodeView::new_internal(g.clone(), v)))
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + 'graph,
        F: for<'a> Fn(&'a Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Edges {
        let graph = self.graph.clone();
        let base_graph = self.base_graph.clone();
        let node_op = self.op.clone();
        let edges = Arc::new(move || {
            let graph = graph.clone();
            let op = op.clone();
            node_op()
                .flat_map(move |node| op(&graph, node))
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
        I: Iterator<Item = VID> + Send + 'graph,
        F: for<'a> Fn(&'a Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::PathType {
        let old_op = self.op.clone();
        let graph = self.graph.clone();

        PathFromNode::new(self.base_graph.clone(), move || {
            let op = op.clone();
            let graph = graph.clone();
            old_op().flat_map(move |vv| op(&graph, vv)).into_dyn_boxed()
        })
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> IntoIterator
    for PathFromNode<'graph, G, GH>
{
    type Item = NodeView<G, GH>;
    type IntoIter = BoxedLIter<'graph, NodeView<G, GH>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
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
        let base_graph = self.base_graph.clone();
        PathFromNode {
            base_graph,
            graph: filtered_graph,
            op: self.op.clone(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::prelude::*;

    #[test]
    fn test_node_view_ops() {
        let g = Graph::new();

        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();

        let n = Vec::from_iter(g.node(1).unwrap().neighbours().id());
        assert_eq!(n, [2])
    }
}
