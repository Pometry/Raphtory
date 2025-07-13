use crate::{
    core::entities::{edges::edge_ref::EdgeRef, nodes::node_ref::AsNodeRef, VID},
    db::{
        api::{
            state::{Index, LazyNodeState, NodeOp},
            view::{
                internal::{BaseFilter, FilterOps, NodeList, Static},
                BaseNodeViewOps, BoxedLIter, DynamicGraph, ExplodedEdgePropertyFilterOps,
                IntoDynBoxed, IntoDynamic,
            },
        },
        graph::{create_node_type_filter, edges::NestedEdges, node::NodeView, path::PathFromGraph},
    },
    prelude::*,
};
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
        Nodes {
            base_graph: value.base_graph.into_dynamic(),
            graph: value.graph.into_dynamic(),
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
        Self {
            base_graph: graph.clone(),
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

    pub fn node_list(&self) -> NodeList {
        match self.nodes.clone() {
            None => self.graph.node_list(),
            Some(elems) => NodeList::List { elems },
        }
    }

    pub(crate) fn par_iter_refs(&self) -> impl ParallelIterator<Item = VID> + 'graph {
        let g = self.graph.core_graph().lock();
        let view = self.graph.clone();
        let node_types_filter = self.node_types_filter.clone();
        self.node_list().into_par_iter().filter(move |&vid| {
            let node = g.core_node(vid);
            node_types_filter
                .as_ref()
                .is_none_or(|type_filter| type_filter[node.node_type_id()])
                && view.filter_node(node.as_ref())
        })
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
        let view = self.graph.clone();
        self.node_list().into_iter().filter(move |&vid| {
            let node = g.core_node(vid);
            node_types_filter
                .as_ref()
                .is_none_or(|type_filter| type_filter[node.node_type_id()])
                && view.filter_node(node.as_ref())
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = NodeView<&G>> + use<'_, 'graph, G, GH> {
        self.iter_refs()
            .map(|v| NodeView::new_one_hop_filtered(&self.base_graph, v))
    }

    pub fn iter_owned(&self) -> BoxedLIter<'graph, NodeView<'graph, G>> {
        let base_graph = self.base_graph.clone();
        self.iter_refs()
            .map(move |v| NodeView::new_one_hop_filtered(base_graph.clone(), v))
            .into_dyn_boxed()
    }

    pub fn par_iter(&self) -> impl ParallelIterator<Item = NodeView<&G>> + use<'_, 'graph, G, GH> {
        self.par_iter_refs()
            .map(|v| NodeView::new_one_hop_filtered(&self.base_graph, v))
    }

    pub fn into_par_iter(self) -> impl ParallelIterator<Item = NodeView<'graph, G>> + 'graph {
        self.par_iter_refs()
            .map(move |n| NodeView::new_one_hop_filtered(self.base_graph.clone(), n))
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

    pub fn get<V: AsNodeRef>(&self, node: V) -> Option<NodeView<'graph, G>> {
        let vid = self.base_graph.internalise_node(node.as_node_ref())?;
        self.contains(vid)
            .then(|| NodeView::new_one_hop_filtered(self.base_graph.clone(), vid))
    }

    pub fn type_filter<I: IntoIterator<Item = V>, V: AsRef<str>>(
        &self,
        node_types: I,
    ) -> Nodes<'graph, G, GH> {
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

    pub fn id_filter(
        &self,
        nodes: impl IntoIterator<Item = impl AsNodeRef>,
    ) -> Nodes<'graph, G, GH> {
        let index: Index<_> = nodes
            .into_iter()
            .filter_map(|n| self.graph.node(n).map(|n| n.node))
            .collect();
        self.indexed(index)
    }

    pub fn collect(&self) -> Vec<NodeView<'graph, G>> {
        self.iter_owned().collect()
    }

    pub fn get_const_prop_id(&self, prop_name: &str) -> Option<usize> {
        self.graph.node_meta().get_prop_id(prop_name, true)
    }

    pub fn get_temporal_prop_id(&self, prop_name: &str) -> Option<usize> {
        self.graph.node_meta().get_prop_id(prop_name, false)
    }

    fn is_list_filtered(&self) -> bool {
        self.node_types_filter.is_some() || !self.graph.node_list_trusted()
    }

    pub fn is_filtered(&self) -> bool {
        self.node_types_filter.is_some() || self.graph.filtered()
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
    type Graph = G;
    type ValueType<T: NodeOp + 'graph> = LazyNodeState<'graph, T, G, GH>;
    type PropType = NodeView<'graph, G>;
    type PathType = PathFromGraph<'graph, G, G>;
    type Edges = NestedEdges<'graph, G, GH>;

    fn graph(&self) -> &Self::Graph {
        &self.base_graph
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
        let graph = self.base_graph.clone();
        let nodes = self.clone();
        let nodes = Arc::new(move || nodes.iter_refs().into_dyn_boxed());
        let edges = Arc::new(move |node: VID| {
            let cg = graph.core_graph();
            op(cg, &graph, node).into_dyn_boxed()
        });
        NestedEdges {
            base_graph: self.base_graph.clone(),
            graph: self.graph.clone(),
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
    G: GraphViewOps<'graph> + 'graph,
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
            graph: self.graph.clone(),
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
