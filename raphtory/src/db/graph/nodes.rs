use crate::{
    core::entities::{edges::edge_ref::EdgeRef, nodes::node_ref::AsNodeRef, VID},
    db::{
        api::{
            state::{
                ops::{
                    filter::{FilterOp, NodeTypeFilter},
                    Const, NodeFilterOp, NodeOp,
                },
                Index, LazyNodeState,
            },
            view::{
                internal::{BaseFilter, FilterOps, IterFilter, NodeList, Static},
                BaseNodeViewOps, BoxedLIter, DynamicGraph, IntoDynBoxed, IntoDynamic,
            },
        },
        graph::{
            edges::NestedEdges, node::NodeView, path::PathFromGraph,
            views::filter::model::AndFilter,
        },
    },
    prelude::*,
};
use raphtory_api::inherit::Base;
use raphtory_storage::{
    core_ops::is_view_compatible,
    graph::{graph::GraphStorage, nodes::node_storage_ops::NodeStorageOps},
};
use rayon::iter::ParallelIterator;
use std::{
    collections::HashSet,
    fmt::{Debug, Formatter},
    hash::{BuildHasher, Hash},
    marker::PhantomData,
    sync::Arc,
};

#[derive(Clone)]
pub struct Nodes<'graph, G, F = Const<bool>> {
    pub(crate) base_graph: G,
    pub(crate) node_select: F,
    pub(crate) nodes: Option<Index<VID>>,
    _marker: PhantomData<&'graph ()>,
}

impl<
        'graph,
        G: GraphViewOps<'graph>,
        GH: NodeFilterOp + Clone + 'graph,
        V: AsNodeRef + Hash + Eq,
        S: BuildHasher,
    > PartialEq<HashSet<V, S>> for Nodes<'graph, G, GH>
{
    fn eq(&self, other: &HashSet<V, S>) -> bool {
        self.len() == other.len() && other.iter().all(|o| self.contains(o))
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: NodeFilterOp + Clone + 'graph, V: AsNodeRef>
    PartialEq<Vec<V>> for Nodes<'graph, G, GH>
{
    fn eq(&self, other: &Vec<V>) -> bool {
        self.iter_refs()
            .eq(other.iter().filter_map(|o| self.get(o).map(|n| n.node)))
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: NodeFilterOp + Clone + 'graph + Debug> Debug
    for Nodes<'graph, G, GH>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: NodeFilterOp + Clone + 'graph> PartialEq
    for Nodes<'graph, G, GH>
{
    fn eq(&self, other: &Self) -> bool {
        if is_view_compatible(&self.base_graph, &other.base_graph) {
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
            node_select: self.node_select,
            nodes: self.nodes,
            _marker: Default::default(),
        }
    }
}

// impl<G, GH: NodeFilterOp<DynamicGraph>> From<Nodes<'static, G, GH>>
//     for Nodes<'static, DynamicGraph, Arc<dyn NodeFilterOp<DynamicGraph>>>
// where
//     G: GraphViewOps<'graph> + IntoDynamic + Static,
// {
//     fn from(value: Nodes<'graph, G, GH>) -> Self {
//         Nodes {
//             base_graph: value.base_graph.into_dynamic(),
//             node_select: value.node_select.into_dynamic(),
//             nodes: value.nodes,
//             _marker: PhantomData,
//         }
//     }
// }

impl<'graph, G> Nodes<'graph, G>
where
    G: GraphViewOps<'graph> + Clone,
{
    pub fn new(graph: G) -> Self {
        Self {
            base_graph: graph.clone(),
            node_select: Const(true),
            nodes: None,
            _marker: PhantomData,
        }
    }
}

impl<'graph, G, GH> Nodes<'graph, G, GH>
where
    G: GraphViewOps<'graph> + 'graph,
    GH: NodeFilterOp + Clone + 'graph,
{
    pub fn new_filtered(base_graph: G, node_select: GH, nodes: Option<Index<VID>>) -> Self {
        Self {
            base_graph,
            node_select,
            nodes,
            _marker: PhantomData,
        }
    }

    pub fn node_list(&self) -> NodeList {
        match self.nodes.clone() {
            None => self.base_graph.node_list(),
            Some(elems) => NodeList::List { elems },
        }
    }

    pub(crate) fn par_iter_refs(&self) -> impl ParallelIterator<Item = VID> + 'graph {
        let g = self.base_graph.core_graph().lock();
        let view = self.base_graph.clone();
        let node_select = self.node_select.clone();
        self.node_list().into_par_iter().filter(move |&vid| {
            let node = g.core_node(vid);
            view.filter_node(node.as_ref())
                && node_select.apply(&NodeView::new_internal(&view, vid))
        })
    }

    pub fn indexed(&self, index: Index<VID>) -> Nodes<'graph, G, GH> {
        Nodes::new_filtered(
            self.base_graph.clone(),
            self.node_select.clone(),
            Some(index),
        )
    }

    #[inline]
    pub(crate) fn iter_refs(&self) -> impl Iterator<Item = VID> + Send + Sync + 'graph {
        let g = self.base_graph.core_graph().lock();
        let view = self.base_graph.clone();
        let node_select = self.node_select.clone();
        self.node_list().into_iter().filter(move |&vid| {
            let node = g.core_node(vid);
            view.filter_node(node.as_ref())
                && node_select.apply(&NodeView::new_internal(&view, vid))
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = NodeView<&G>> + use<'_, 'graph, G, GH> {
        self.iter_refs()
            .map(|v| NodeView::new_internal(&self.base_graph, v))
    }

    pub fn iter_owned(&self) -> BoxedLIter<'graph, NodeView<'graph, G>> {
        let base_graph = self.base_graph.clone();
        self.iter_refs()
            .map(move |v| NodeView::new_internal(base_graph.clone(), v))
            .into_dyn_boxed()
    }

    pub fn par_iter(&self) -> impl ParallelIterator<Item = NodeView<&G>> + use<'_, 'graph, G, GH> {
        self.par_iter_refs()
            .map(|v| NodeView::new_internal(&self.base_graph, v))
    }

    pub fn into_par_iter(self) -> impl ParallelIterator<Item = NodeView<'graph, G>> + 'graph {
        self.par_iter_refs()
            .map(move |n| NodeView::new_internal(self.base_graph.clone(), n))
    }

    /// Returns the number of nodes in the graph.
    #[inline]
    pub fn len(&self) -> usize {
        match self.nodes.as_ref() {
            None => {
                if self.is_list_filtered() {
                    self.par_iter_refs().count()
                } else {
                    self.base_graph.node_list().len()
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

    pub fn get<V: AsNodeRef>(&self, node: V) -> Option<NodeView<'graph, G>> {
        let vid = self.base_graph.internalise_node(node.as_node_ref())?;
        self.contains(vid)
            .then(|| NodeView::new_internal(self.base_graph.clone(), vid))
    }

    pub fn type_filter<I: IntoIterator<Item = V>, V: AsRef<str>>(
        &self,
        node_types: I,
    ) -> Nodes<'graph, G, AndFilter<GH, NodeTypeFilter>> {
        let node_types_filter = NodeTypeFilter::new(node_types, &self.base_graph);
        let node_select = self.node_select.clone().and(node_types_filter);
        Nodes {
            base_graph: self.base_graph.clone(),
            node_select,
            nodes: self.nodes.clone(),
            _marker: PhantomData,
        }
    }

    pub fn id_filter(
        &self,
        nodes: impl IntoIterator<Item = impl AsNodeRef>,
    ) -> Nodes<'graph, G, GH> {
        let index: Index<_> = nodes
            .into_iter()
            .filter_map(|n| self.base_graph.node(n).map(|n| n.node))
            .collect();
        self.indexed(index)
    }

    pub fn collect(&self) -> Vec<NodeView<'graph, G>> {
        self.iter_owned().collect()
    }

    pub fn get_metadata_id(&self, prop_name: &str) -> Option<usize> {
        self.base_graph.node_meta().get_prop_id(prop_name, true)
    }

    pub fn get_temporal_prop_id(&self, prop_name: &str) -> Option<usize> {
        self.base_graph.node_meta().get_prop_id(prop_name, false)
    }

    pub fn is_list_filtered(&self) -> bool {
        !self.base_graph.node_list_trusted() || self.node_select.is_filtered()
    }

    pub fn is_filtered(&self) -> bool {
        self.base_graph.filtered() || self.node_select.is_filtered()
    }

    pub fn contains<V: AsNodeRef>(&self, node: V) -> bool {
        (&self.graph())
            .node(node)
            .filter(|node| {
                self.nodes
                    .as_ref()
                    .map(|nodes| nodes.contains(&node.node))
                    .unwrap_or(true)
            })
            .is_some()
    }

    pub fn select<Filter: NodeFilterOp>(
        &self,
        filter: Filter,
    ) -> Nodes<'graph, G, AndFilter<GH, Filter>> {
        let node_select = self.node_select.clone().and(filter);
        Nodes {
            base_graph: self.base_graph.clone(),
            node_select,
            nodes: self.nodes.clone(),
            _marker: Default::default(),
        }
    }
}

impl<'graph, G, GH> BaseNodeViewOps<'graph> for Nodes<'graph, G, GH>
where
    G: GraphViewOps<'graph> + 'graph,
    GH: NodeFilterOp + Clone + 'graph,
{
    type Graph = G;
    type ValueType<T: NodeOp + 'graph> = LazyNodeState<'graph, T, G, GH>;
    type PropType = NodeView<'graph, G>;
    type PathType = PathFromGraph<'graph, G>;
    type Edges = NestedEdges<'graph, G>;

    fn graph(&self) -> &Self::Graph {
        &self.base_graph
    }

    fn map<F: NodeOp + 'graph>(&self, op: F) -> Self::ValueType<F> {
        LazyNodeState::new(op, self.clone())
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
        F: Fn(&GraphStorage, &Self::Graph, VID) -> I + Send + Sync + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Edges {
        let graph = self.base_graph.clone();
        let nodes = self.clone();
        let nodes = Arc::new(move || nodes.iter_refs().into_dyn_boxed());
        let edges = Arc::new(move |node: VID| {
            let cg = graph.core_graph();
            op(cg, &graph, node).into_dyn_boxed()
        });
        NestedEdges {
            base_graph: self.base_graph.clone(),
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
        let graph = self.base_graph.clone();
        let nodes = self.clone();
        let nodes = Arc::new(move || nodes.iter_refs().into_dyn_boxed());
        PathFromGraph::new(self.base_graph.clone(), nodes, move |v| {
            let cg = graph.core_graph();
            op(cg, &graph, v).into_dyn_boxed()
        })
    }
}

impl<'graph, Current, G> BaseFilter<'graph> for Nodes<'graph, Current, G>
where
    Current: GraphViewOps<'graph> + 'graph,
    G: NodeFilterOp + Clone + 'graph,
{
    type BaseGraph = Current;
    type Filtered<Next: GraphViewOps<'graph>> = Nodes<'graph, Next, G>;

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.base_graph
    }

    fn apply_filter<Next: GraphViewOps<'graph>>(
        &self,
        filtered_graph: Next,
    ) -> Self::Filtered<Next> {
        Nodes {
            base_graph: filtered_graph,
            node_select: self.node_select.clone(),
            nodes: self.nodes.clone(),
            _marker: PhantomData,
        }
    }
}

impl<'graph, G, GH> IntoIterator for Nodes<'graph, G, GH>
where
    G: GraphViewOps<'graph> + 'graph,
    GH: GraphViewOps<'graph> + 'graph,
{
    type Item = NodeView<'graph, G>;
    type IntoIter = BoxedLIter<'graph, Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter_owned())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        prelude::*,
        test_utils::{build_graph, build_graph_strat},
    };
    use proptest::{proptest, sample::subsequence};

    #[test]
    fn test_id_filter() {
        let graph = Graph::new();
        graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();

        assert_eq!(graph.nodes().id(), [0, 1]);
        assert_eq!(graph.nodes().id_filter([0]).len(), 1);
        assert_eq!(graph.nodes().id_filter([0]).id(), [0]);
        assert_eq!(graph.nodes().id_filter([0]).degree(), [1]);
    }

    #[test]
    fn test_indexed() {
        proptest!(|(graph in build_graph_strat(10, 10, false), nodes in subsequence((0..10).collect::<Vec<_>>(), 0..10))| {
            let graph = Graph::from(build_graph(&graph));
            let expected_node_ids = nodes.iter().copied().filter(|&id| graph.has_node(id)).collect::<Vec<_>>();
            let nodes = graph.nodes().id_filter(nodes);
            assert_eq!(nodes.id(), expected_node_ids);
        })
    }
}
