use crate::{
    core::entities::{edges::edge_ref::EdgeRef, VID},
    db::{
        api::{
            state::ops::{ArrowNodeOp, NodeOp},
            view::{
                filter_ops::Select,
                history::History,
                internal::{DynGraphArc, GraphView, InternalFilter, Static},
                BaseNodeViewOps, BoxedLIter, DynamicGraph, IntoDynBoxed, IntoDynamic,
                StaticGraphViewOps,
            },
        },
        graph::{
            create_node_type_filter,
            edges::{Edges, NestedEdges},
            node::NodeView,
            views::filter::{and_filtered_graph::AndFilteredGraph, CreateFilter},
        },
    },
    errors::GraphError,
    prelude::*,
};
use raphtory_storage::{core_ops::CoreGraphOps, graph::graph::GraphStorage};
use std::sync::Arc;

type GraphPathOp<'graph> =
    Arc<dyn Fn(DynGraphArc<'graph>, VID) -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>;

#[derive(Clone)]
pub struct PathFromGraph<'graph, G> {
    pub(crate) base_graph: G,
    pub(crate) select: DynGraphArc<'graph>,
    pub(crate) nodes: Arc<dyn Fn() -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>,
    pub(crate) op: GraphPathOp<'graph>,
}

impl<'graph, G: GraphViewOps<'graph>> PathFromGraph<'graph, G> {
    pub fn new(
        graph: G,
        nodes: Arc<dyn Fn() -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>,
        op: GraphPathOp<'graph>,
    ) -> Self {
        let base_graph = graph.clone();
        let select = Arc::new(graph) as DynGraphArc;
        PathFromGraph {
            base_graph,
            select,
            nodes,
            op,
        }
    }
}

impl<'graph, G: IntoDynamic> PathFromGraph<'graph, G> {
    pub fn into_dyn(self) -> PathFromGraph<'graph, DynamicGraph> {
        PathFromGraph {
            base_graph: self.base_graph.into_dynamic(),
            select: self.select,
            nodes: self.nodes,
            op: self.op,
        }
    }
}

impl<G: StaticGraphViewOps + IntoDynamic + Static> From<PathFromGraph<'static, G>>
    for PathFromGraph<'static, DynamicGraph>
{
    fn from(value: PathFromGraph<'static, G>) -> Self {
        PathFromGraph {
            base_graph: value.base_graph.into(),
            select: value.select.clone(),
            nodes: value.nodes.clone(),
            op: value.op.clone(),
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>> PathFromGraph<'graph, G> {
    pub fn new_filtered(
        base_graph: G,
        select: DynGraphArc<'graph>,
        nodes: Arc<dyn Fn() -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>,
        op: GraphPathOp<'graph>,
    ) -> Self {
        PathFromGraph {
            base_graph,
            select,
            nodes,
            op,
        }
    }

    fn base_iter(&self) -> BoxedLIter<'graph, VID> {
        (self.nodes)()
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (NodeView<'graph, G>, PathFromNode<'graph, G>)> + Send + 'graph {
        let base_graph = self.base_graph.clone();
        let select = self.select.clone();
        let op = self.op.clone();
        self.base_iter().map(move |v| {
            let op = op.clone();
            let node_op = Arc::new(move |graph| op(graph, v));
            (
                NodeView::new_internal(base_graph.clone(), v),
                PathFromNode::new_one_hop_filtered(base_graph.clone(), select.clone(), node_op),
            )
        })
    }

    pub fn iter_values(&self) -> impl Iterator<Item = PathFromNode<'graph, G>> + Send + 'graph {
        let base_graph = self.base_graph.clone();
        let select = self.select.clone();
        let op = self.op.clone();
        self.base_iter().map(move |v| {
            let op = op.clone();
            let node_op = Arc::new(move |graph| op(graph, v));
            PathFromNode::new_one_hop_filtered(base_graph.clone(), select.clone(), node_op)
        })
    }

    pub fn iter_refs(&self) -> impl Iterator<Item = BoxedLIter<'graph, VID>> + Send + 'graph {
        let op = self.op.clone();
        let select = self.select.clone();
        self.base_iter().map(move |vid| op(select.clone(), vid))
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
    ) -> PathFromGraph<'graph, G> {
        let node_types_filter =
            create_node_type_filter(self.base_graph.node_meta().node_type_meta(), node_types);

        let base_graph = self.base_graph.clone();
        let old_op = self.op.clone();

        PathFromGraph::new_filtered(
            self.base_graph.clone(),
            self.select.clone(),
            self.nodes.clone(),
            Arc::new(move |graph, vid| {
                let base_graph = base_graph.clone();
                let node_types_filter = node_types_filter.clone();
                old_op(graph, vid)
                    .filter(move |v| {
                        let node_type_id = base_graph.node_type_id(*v);
                        node_types_filter[node_type_id]
                    })
                    .into_dyn_boxed()
            }),
        )
    }

    pub fn collect(&self) -> Vec<Vec<NodeView<'graph, G>>> {
        self.iter_values().map(|path| path.collect()).collect()
    }

    pub fn combined_history(&self) -> History<'graph, Self> {
        History::new(self.clone())
    }
}

impl<'graph, G: GraphViewOps<'graph>> BaseNodeViewOps<'graph> for PathFromGraph<'graph, G> {
    type Graph = G;
    type ValueType<T: ArrowNodeOp + 'graph> =
        BoxedLIter<'graph, (NodeView<'graph, G>, BoxedLIter<'graph, T::Output>)>;
    type PropType = NodeView<'graph, G>;
    type PathType = PathFromGraph<'graph, G>;
    type Edges = NestedEdges<'graph, G>;

    fn graph(&self) -> &Self::Graph {
        &self.base_graph
    }

    fn map<F: ArrowNodeOp + Clone + 'graph>(&self, op: F) -> Self::ValueType<F>
    where
        <F as NodeOp>::Output: 'graph,
    {
        self.iter()
            .map(move |(node, path)| (node, path.map(op.clone())))
            .into_dyn_boxed()
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
        F: Fn(&GraphStorage, &DynGraphArc<'graph>, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Edges {
        let select = self.select.clone();
        let node_op = self.op.clone();
        let edges = Arc::new(move |graph: DynGraphArc<'graph>, node: VID| {
            let op = op.clone();
            node_op(select.clone(), node)
                .flat_map(move |node| op(graph.core_graph(), &graph, node))
                .into_dyn_boxed()
        });
        NestedEdges::new(self.base_graph.clone(), self.nodes.clone(), edges)
    }

    fn hop<
        I: Iterator<Item = VID> + Send + Sync + 'graph,
        F: Fn(&GraphStorage, &DynGraphArc<'graph>, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::PathType {
        let old_op = self.op.clone();
        let nodes = self.nodes.clone();
        let base_graph = self.base_graph.clone();
        let select = self.select.clone();
        PathFromGraph::new(
            self.base_graph.clone(),
            nodes,
            Arc::new(move |graph, v| {
                let op = op.clone();
                let base_graph = base_graph.clone();
                Box::new(
                    old_op(select.clone(), v)
                        .flat_map(move |vv| op(base_graph.core_graph(), &graph, vv)),
                )
            }),
        )
    }
}

impl<'graph, G: GraphViewOps<'graph>> IntoIterator for PathFromGraph<'graph, G> {
    type Item = (NodeView<'graph, G>, PathFromNode<'graph, G>);
    type IntoIter = BoxedLIter<'graph, Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        let base_graph = self.base_graph;
        let select = self.select.clone();
        let op = self.op;
        (self.nodes)()
            .map(move |node| {
                let op = op.clone();
                let node_op = Arc::new(move |graph| op(graph, node));
                (
                    NodeView::new_internal(base_graph.clone(), node),
                    PathFromNode::new_one_hop_filtered(base_graph.clone(), select.clone(), node_op),
                )
            })
            .into_dyn_boxed()
    }
}

impl<'graph, Current> InternalFilter<'graph> for PathFromGraph<'graph, Current>
where
    Current: GraphViewOps<'graph>,
{
    type Graph = Current;
    type Filtered<Next: GraphViewOps<'graph>> = PathFromGraph<'graph, Next>;

    fn base_graph(&self) -> &Self::Graph {
        &self.base_graph
    }

    fn apply_filter<Next: GraphViewOps<'graph>>(
        &self,
        filtered_graph: Next,
    ) -> Self::Filtered<Next> {
        PathFromGraph {
            base_graph: filtered_graph,
            select: self.select.clone(),
            nodes: self.nodes.clone(),
            op: self.op.clone(),
        }
    }
}

impl<'graph, G> Select<'graph> for PathFromGraph<'graph, G>
where
    G: GraphView + 'graph,
    Self: 'graph,
{
    type IterFiltered<Filter: CreateFilter + 'graph> = PathFromGraph<'graph, G>;

    fn select<F: CreateFilter + 'graph>(
        &self,
        filter: F,
    ) -> Result<PathFromGraph<'graph, G>, GraphError> {
        let filter_graph = filter.filter_graph_view(self.base_graph.clone())?;
        let filter = filter.create_node_filter(self.base_graph.clone(), filter_graph.clone())?;

        let select = Arc::new(AndFilteredGraph::new(
            self.base_graph.clone(),
            self.select.clone(),
            filter_graph,
        ));

        let op = self.op.clone();
        let op = Arc::new(move |graph: DynGraphArc<'graph>, node| {
            let filter = filter.clone();
            let storage = graph.core_graph().clone();
            op(graph, node)
                .filter(move |node| filter.apply(&storage, *node))
                .into_dyn_boxed()
        });
        Ok(PathFromGraph::new_filtered(
            self.base_graph.clone(),
            select,
            self.nodes.clone(),
            op,
        ))
    }
}

impl<G: StaticGraphViewOps + IntoDynamic + Static> From<PathFromNode<'static, G>>
    for PathFromNode<'static, DynamicGraph>
{
    fn from(value: PathFromNode<'static, G>) -> Self {
        PathFromNode {
            base_graph: value.base_graph.into(),
            select: value.select,
            op: value.op.clone(),
        }
    }
}

pub type NodePathOp<'graph> =
    Arc<dyn Fn(DynGraphArc<'graph>) -> BoxedLIter<'graph, VID> + Send + Sync + 'graph>;

#[derive(Clone)]
pub struct PathFromNode<'graph, G: 'graph> {
    pub(crate) base_graph: G,
    pub(crate) select: DynGraphArc<'graph>,
    pub(crate) op: NodePathOp<'graph>,
}

impl<'graph, G: IntoDynamic> PathFromNode<'graph, G> {
    pub fn into_dyn(self) -> PathFromNode<'graph, DynamicGraph> {
        PathFromNode {
            base_graph: self.base_graph.into_dynamic(),
            select: self.select.clone(),
            op: self.op,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>> PathFromNode<'graph, G> {
    pub(crate) fn new(graph: G, op: NodePathOp<'graph>) -> PathFromNode<'graph, G> {
        let base_graph = graph.clone();
        let select = Arc::new(graph) as DynGraphArc<'graph>;
        PathFromNode {
            base_graph,
            select,
            op,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>> PathFromNode<'graph, G> {
    pub(crate) fn new_one_hop_filtered(
        base_graph: G,
        select: DynGraphArc<'graph>,
        op: NodePathOp<'graph>,
    ) -> Self {
        Self {
            base_graph,
            select,
            op,
        }
    }

    pub fn iter_refs(&self) -> BoxedLIter<'graph, VID> {
        (self.op)(self.select.clone())
    }

    pub fn iter(&self) -> BoxedLIter<'graph, NodeView<'graph, G>> {
        let base_graph = self.base_graph.clone();
        let iter = self
            .iter_refs()
            .map(move |node| NodeView::new_internal(base_graph.clone(), node));
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
    ) -> PathFromNode<'graph, G> {
        let node_types_filter =
            create_node_type_filter(self.base_graph.node_meta().node_type_meta(), node_types);

        let base_graph = self.base_graph.clone();
        let old_op = self.op.clone();

        PathFromNode {
            base_graph: self.base_graph.clone(),
            select: self.select.clone(),
            op: Arc::new(move |graph| {
                let base_graph = base_graph.clone();
                let node_types_filter = node_types_filter.clone();
                old_op(graph)
                    .filter(move |v| {
                        let node_type_id = base_graph.node_type_id(*v);
                        node_types_filter[node_type_id]
                    })
                    .into_dyn_boxed()
            }),
        }
    }

    /// Collect all nodes into a list
    ///
    /// Returns:
    ///     list[NodeView]: the list of nodes
    pub fn collect(&self) -> Vec<NodeView<'graph, G>> {
        self.iter().collect()
    }

    pub fn combined_history(&self) -> History<'graph, Self> {
        History::new(self.clone())
    }
}

impl<'graph, G: GraphViewOps<'graph>> BaseNodeViewOps<'graph> for PathFromNode<'graph, G> {
    type Graph = G;
    type ValueType<T: ArrowNodeOp + 'graph> = BoxedLIter<'graph, T::Output>;
    type PropType = NodeView<'graph, G>;
    type PathType = PathFromNode<'graph, G>;
    type Edges = Edges<'graph, G>;

    fn graph(&self) -> &Self::Graph {
        &self.base_graph
    }

    fn map<F: ArrowNodeOp + 'graph>(&self, op: F) -> Self::ValueType<F>
    where
        <F as NodeOp>::Output: 'graph,
    {
        let storage = self.base_graph.core_graph().lock();
        Box::new(self.iter_refs().map(move |node| op.apply(&storage, node)))
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
        F: Fn(&GraphStorage, &DynGraphArc<'graph>, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Edges {
        let node_op = self.op.clone();
        let select = self.select.clone();
        let edges = Arc::new(move |graph: DynGraphArc<'graph>| {
            let op = op.clone();
            let graph = graph.clone();
            node_op(select.clone())
                .flat_map(move |node| op(graph.core_graph(), &graph, node))
                .into_dyn_boxed()
        });
        Edges {
            base_graph: self.base_graph.clone(),
            select: Arc::new(self.base_graph.clone()),
            edges,
        }
    }

    fn hop<
        I: Iterator<Item = VID> + Send + Sync + 'graph,
        F: Fn(&GraphStorage, &DynGraphArc<'graph>, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::PathType {
        let old_op = self.op.clone();
        let base_graph = Arc::new(self.base_graph.clone());
        let select = self.select.clone();
        PathFromNode::new(
            self.base_graph.clone(),
            Arc::new(move |graph| {
                let op = op.clone();
                let old_op = old_op.clone();
                let base_graph = base_graph.clone();
                old_op(select.clone())
                    .flat_map(move |vv| op(base_graph.core_graph(), &graph, vv))
                    .into_dyn_boxed()
            }),
        )
    }
}

impl<'graph, G: GraphViewOps<'graph>> IntoIterator for PathFromNode<'graph, G> {
    type Item = NodeView<'graph, G>;
    type IntoIter = BoxedLIter<'graph, NodeView<'graph, G>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'graph, Current> InternalFilter<'graph> for PathFromNode<'graph, Current>
where
    Current: GraphViewOps<'graph>,
{
    type Graph = Current;
    type Filtered<Next: GraphViewOps<'graph>> = PathFromNode<'graph, Next>;

    fn base_graph(&self) -> &Self::Graph {
        &self.base_graph
    }

    fn apply_filter<Next: GraphViewOps<'graph>>(
        &self,
        filtered_graph: Next,
    ) -> Self::Filtered<Next> {
        PathFromNode {
            base_graph: filtered_graph,
            select: self.select.clone(),
            op: self.op.clone(),
        }
    }
}

impl<'graph, G> Select<'graph> for PathFromNode<'graph, G>
where
    G: GraphViewOps<'graph>,
{
    type IterFiltered<Next: CreateFilter + 'graph> = PathFromNode<'graph, G>;

    fn select<F: CreateFilter + 'graph>(
        &self,
        filter: F,
    ) -> Result<PathFromNode<'graph, G>, GraphError> {
        let op = self.op.clone();
        let filter_graph = filter.filter_graph_view(self.base_graph.clone())?;
        let select = Arc::new(AndFilteredGraph::new(
            self.base_graph.clone(),
            self.select.clone(),
            filter_graph.clone(),
        ));
        let filter_op = filter.create_node_filter(self.base_graph.clone(), filter_graph)?;
        Ok(PathFromNode {
            base_graph: self.base_graph.clone(),
            select,
            op: Arc::new(move |graph| {
                let filter_op = filter_op.clone();
                let storage = graph.core_graph().clone();
                Box::new(op(graph).filter(move |node| filter_op.apply(&storage, *node)))
            }),
        })
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
