use crate::{
    core::entities::{edges::edge_ref::EdgeRef, nodes::node_ref::AsNodeRef, VID},
    db::{
        api::{
            state::{
                ops::{
                    filter::{AndOp, NodeTypeFilterOp, NO_FILTER},
                    ArrowNodeOp, Const, DynNodeFilter, IntoDynNodeOp, NodeFilterOp,
                },
                Index, LazyNodeState,
            },
            view::{
                internal::{DynGraphArc, FilterOps, InternalFilter, InternalNodeSelect, NodeList},
                sort::{compare_node, NodeSortBy},
                BaseNodeViewOps, BoxedLIter, DynamicGraph, IntoDynBoxed, IntoDynamic,
            },
        },
        graph::{edges::NestedEdges, node::NodeView, path::PathFromGraph},
    },
    prelude::*,
};
use itertools::Itertools;
use raphtory_core::utils::iter::GenLockedIter;
use raphtory_storage::{core_ops::is_view_compatible, graph::graph::GraphStorage};
use rayon::iter::ParallelIterator;
use std::{
    cmp::Ordering,
    collections::HashSet,
    fmt::{Debug, Formatter},
    hash::{BuildHasher, Hash},
    marker::PhantomData,
    sync::Arc,
};
use storage::api::{node_type_index::NodeTypeIndexOps, nodes::NodeRefOps};

#[derive(Clone)]
pub struct Nodes<'graph, G, GH = G, F = Const<bool>> {
    pub(crate) base_graph: G,
    pub(crate) graph: GH,
    pub(crate) predicate: F,
    pub(crate) nodes: NodeList,
    _marker: PhantomData<&'graph ()>,
}

impl<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + Clone + 'graph,
        V: AsNodeRef + Hash + Eq,
        S: BuildHasher,
    > PartialEq<HashSet<V, S>> for Nodes<'graph, G, GH, F>
{
    fn eq(&self, other: &HashSet<V, S>) -> bool {
        self.len() == other.len() && other.iter().all(|o| self.contains(o))
    }
}

impl<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + Clone + 'graph,
        V: AsNodeRef,
    > PartialEq<Vec<V>> for Nodes<'graph, G, GH, F>
{
    fn eq(&self, other: &Vec<V>) -> bool {
        self.iter_refs()
            .eq(other.iter().filter_map(|o| self.get(o).map(|n| n.node)))
    }
}

impl<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + Clone + 'graph + Debug,
    > Debug for Nodes<'graph, G, GH, F>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + Clone + 'graph,
    > PartialEq for Nodes<'graph, G, GH, F>
{
    fn eq(&self, other: &Self) -> bool {
        if is_view_compatible(&self.graph, &other.graph) {
            // same storage, can use internal ids
            self.iter_refs().eq(other.iter_refs())
        } else {
            // different storage, use external ids
            self.id().iter_values().eq(other.id().iter_values())
        }
    }
}

pub trait IntoDynNodes {
    fn into_dyn(self) -> Nodes<'static, DynamicGraph, DynamicGraph, DynNodeFilter>;
}

impl<G: IntoDynamic, GH: IntoDynamic, F: NodeFilterOp + IntoDynNodeOp + 'static> IntoDynNodes
    for Nodes<'static, G, GH, F>
{
    fn into_dyn(self) -> Nodes<'static, DynamicGraph, DynamicGraph, DynNodeFilter> {
        Nodes {
            base_graph: self.base_graph.into_dynamic(),
            graph: self.graph.into_dynamic(),
            predicate: self.predicate.into_dynamic(),
            nodes: self.nodes,
            _marker: Default::default(),
        }
    }
}

impl<'graph, G> Nodes<'graph, G>
where
    G: GraphViewOps<'graph> + Clone,
{
    pub fn new(graph: G) -> Self {
        let base_graph = graph.clone();
        let node_index = base_graph.core_graph().node_state_index();
        Self {
            base_graph,
            graph,
            nodes: NodeList::List {
                elems: node_index.into(),
            },
            predicate: NO_FILTER,
            _marker: PhantomData,
        }
    }
}

impl<'graph, G, GH, F> Nodes<'graph, G, GH, F>
where
    G: GraphViewOps<'graph> + 'graph,
    GH: GraphViewOps<'graph> + 'graph,
    F: NodeFilterOp + Clone + 'graph,
{
    pub fn new_filtered(base_graph: G, graph: GH, predicate: F, nodes: impl Into<NodeList>) -> Self {
        Self {
            base_graph,
            graph,
            predicate,
            nodes,
            _marker: PhantomData,
        }
    }

    pub fn node_list(&self) -> NodeList {
        match &self.nodes {
            NodeList::All
            | NodeList::List {
                elems: Index::Full(_),
            } => self.base_graph.node_list(),
            nodes => nodes.clone(),
        }
    }

    pub(crate) fn par_iter_refs(
        &self,
        g: GraphStorage,
    ) -> impl ParallelIterator<Item = VID> + 'graph {
        let view = self.base_graph.clone();
        let node_select = self.predicate.clone();
        self.node_list().nodes_par_iter(&g).filter(move |&vid| {
            g.try_core_node(vid)
                .is_some_and(|node| view.filter_node(node.as_ref()) && node_select.apply(&g, vid))
        })
    }

    /// Reorder this collection by an ordered list of sort keys: members
    /// compare by the first key, ties break to the next. Returns a new
    /// collection backed by an explicit index in the sorted order.
    pub fn sorted(&self, sort_bys: &[NodeSortBy]) -> Nodes<'graph, G, GH, F> {
        let index: Index<VID> = self
            .iter()
            .sorted_by(|a, b| {
                sort_bys.iter().fold(Ordering::Equal, |current, sort_by| {
                    current.then_with(|| compare_node(a, b, sort_by))
                })
            })
            .map(|node_view| node_view.node)
            .collect();
        self.indexed(NodeList::List { elems: index })
    }

    fn indexed(&self, nodes: NodeList) -> Nodes<'graph, G, GH, F> {
        Nodes::new_filtered(
            self.base_graph.clone(),
            self.graph.clone(),
            self.predicate.clone(),
            nodes,
        )
    }

    fn locked_storage(&self) -> GraphStorage {
        self.base_graph.core_graph().lock()
    }

    #[inline]
    pub(crate) fn iter_refs(&self) -> impl Iterator<Item = VID> + Send + Sync + 'graph {
        let g = self.base_graph.core_graph().lock();
        self.iter_vids(g)
    }

    pub(crate) fn iter_vids(
        &self,
        g: GraphStorage,
    ) -> impl Iterator<Item = VID> + Send + Sync + 'graph {
        let view = self.base_graph.clone();
        let selector = self.predicate.clone();
        let node_list = self.node_list();
        GenLockedIter::from(
            (g, view, selector, node_list),
            |(g, view, selector, node_list)| {
                Box::new(node_list.clone().node_entries(g).filter_map(move |node| {
                    let node_ref = node.as_ref();
                    let vid = node_ref.vid();
                    (view.filter_node(node_ref) && selector.apply(g, vid)).then_some(vid)
                }))
            },
        )
    }

    #[inline]
    pub(crate) fn iter_refs_unlocked(&self) -> impl Iterator<Item = VID> + Send + Sync + 'graph {
        let g = self.base_graph.core_graph().clone();
        self.iter_vids(g)
    }

    pub fn iter(&self) -> impl Iterator<Item = NodeView<'_, &GH>> + use<'_, 'graph, G, GH, F> {
        self.iter_refs()
            .map(|v| NodeView::new_internal(&self.graph, v))
    }

    pub fn iter_unlocked(
        &self,
    ) -> impl Iterator<Item = NodeView<'_, &GH>> + use<'_, 'graph, G, GH, F> {
        self.iter_refs_unlocked()
            .map(|v| NodeView::new_internal(&self.graph, v))
    }

    pub fn iter_owned(&self) -> BoxedLIter<'graph, NodeView<'graph, GH>> {
        let graph = self.graph.clone();
        self.iter_refs()
            .map(move |v| NodeView::new_internal(graph.clone(), v))
            .into_dyn_boxed()
    }

    pub fn iter_owned_unlocked(&self) -> BoxedLIter<'graph, NodeView<'graph, GH>> {
        let g = self.graph.clone();
        self.iter_refs_unlocked()
            .map(move |v| NodeView::new_internal(g.clone(), v))
            .into_dyn_boxed()
    }

    pub fn par_iter(
        &self,
    ) -> impl ParallelIterator<Item = NodeView<'_, &GH>> + use<'_, 'graph, G, GH, F> {
        let g = self.base_graph.core_graph().lock();
        self.par_iter_refs(g)
            .map(|v| NodeView::new_internal(&self.graph, v))
    }

    pub fn into_par_iter(self) -> impl ParallelIterator<Item = NodeView<'graph, GH>> + 'graph {
        let g = self.locked_storage();
        self.par_iter_refs(g)
            .map(move |n| NodeView::new_internal(self.graph.clone(), n))
    }

    /// Returns the number of nodes in the graph.
    #[inline]
    pub fn len(&self) -> usize {
        if let NodeList::List {
            elems: Index::Sorted { keys, exact: true },
        } = &self.nodes
        {
            if self.base_graph.node_list_trusted() {
                return keys.len();
            }
        }
        if self.is_list_filtered() {
            let g = self.locked_storage();
            self.par_iter_refs(g).count()
        } else {
            match &self.nodes {
                NodeList::All
                | NodeList::List {
                    elems: Index::Full(_),
                } => self.base_graph.count_nodes(),
                NodeList::NodeTypeIdx { types } => self
                    .base_graph
                    .core_graph()
                    .node_type_index()
                    .node_type_entry(types)
                    .len(),
                NodeList::List { elems } => elems.len(),
            }
        }
    }

    /// Returns true if the graph contains no nodes.
    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    pub fn get<V: AsNodeRef>(&self, node: V) -> Option<NodeView<'graph, GH>> {
        let vid = self.graph.internalise_node(node.as_node_ref())?;
        self.contains(vid)
            .then(|| NodeView::new_internal(self.graph.clone(), vid))
    }

    pub fn type_filter<I: IntoIterator<Item = V>, V: AsRef<str>>(
        &self,
        node_types: I,
    ) -> Nodes<'graph, G, GH, AndOp<F, NodeTypeFilterOp>> {
        let node_types_filter = NodeTypeFilterOp::from_values(node_types, &self.graph);
        self.apply_iter_filter(node_types_filter)
    }

    pub fn id_filter(
        &self,
        nodes: impl IntoIterator<Item = impl AsNodeRef>,
    ) -> Nodes<'graph, G, GH, F> {
        let index: Index<_> = nodes
            .into_iter()
            .filter_map(|n| self.graph.node(n).map(|n| n.node))
            .collect();

        self.indexed(NodeList::List { elems: index })
    }

    /// Collect nodes into a vec
    pub fn collect(&self) -> Vec<NodeView<'graph, GH>> {
        self.iter_owned().collect()
    }

    pub fn get_metadata_id(&self, prop_name: &str) -> Option<usize> {
        self.graph.node_meta().get_prop_id(prop_name, true)
    }

    pub fn get_temporal_prop_id(&self, prop_name: &str) -> Option<usize> {
        self.graph.node_meta().get_prop_id(prop_name, false)
    }

    pub fn is_list_filtered(&self) -> bool {
        !self.base_graph.node_list_trusted()
            || self.predicate.is_domain_filtered(self.graph.core_graph())
    }

    pub fn is_filtered(&self) -> bool {
        self.graph.filtered() || self.predicate.is_filtered()
    }

    pub fn contains<V: AsNodeRef>(&self, node: V) -> bool {
        (&&self.base_graph)
            .node(node)
            .filter(|node| {
                self.nodes
                    .contains(&node.node, self.base_graph.core_graph())
                    && self
                        .predicate
                        .apply(self.base_graph.core_graph(), node.node)
            })
            .is_some()
    }
}

impl<'graph, G, GH, F> InternalNodeSelect<'graph> for Nodes<'graph, G, GH, F>
where
    G: GraphViewOps<'graph> + 'graph,
    GH: GraphViewOps<'graph> + 'graph,
    F: NodeFilterOp + 'graph,
{
    type IterGraph = GH;
    type IterFiltered<Filter: NodeFilterOp + 'graph> = Nodes<'graph, G, GH, AndOp<F, Filter>>;

    fn iter_graph(&self) -> &Self::IterGraph {
        &self.graph
    }

    fn apply_iter_filter<Filter: NodeFilterOp + 'graph>(
        &self,
        filter: Filter,
    ) -> Self::IterFiltered<Filter> {
        let storage = self.graph.core_graph();
        let domain = filter.domain(storage);
        let nodes = match domain {
            // `filter` is not reflected in the keys, so an exactness claim
            // from an earlier filter no longer covers the whole predicate
            NodeList::All if filter.is_filtered() => self.nodes.clone().into_inexact(),
            domain => self.nodes.intersection(&domain, storage),
        };
        let predicate = self.predicate.clone().and(filter);
        Nodes {
            base_graph: self.base_graph.clone(),
            graph: self.graph.clone(),
            predicate,
            nodes,
            _marker: Default::default(),
        }
    }
}

impl<'graph, G, GH, F> BaseNodeViewOps<'graph> for Nodes<'graph, G, GH, F>
where
    G: GraphViewOps<'graph> + 'graph,
    GH: GraphViewOps<'graph> + 'graph,
    F: NodeFilterOp + 'graph,
{
    type Graph = GH;
    type ValueType<T: ArrowNodeOp + 'graph> = LazyNodeState<'graph, T, G, GH, F>;
    type PropType = NodeView<'graph, G>;
    type PathType = PathFromGraph<'graph, GH>;
    type Edges = NestedEdges<'graph, GH>;

    fn graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn map<T: ArrowNodeOp + 'graph>(&self, op: T) -> Self::ValueType<T> {
        LazyNodeState::new(op, self.clone())
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
        T: Fn(&GraphStorage, &DynGraphArc<'graph>, VID) -> I + Send + Sync + 'graph,
    >(
        &self,
        op: T,
    ) -> Self::Edges {
        let nodes = self.clone();
        let nodes = Arc::new(move || nodes.iter_refs().into_dyn_boxed());
        let edges = Arc::new(move |graph: DynGraphArc<'graph>, node: VID| {
            let cg = graph.core_graph();
            op(cg, &graph, node).into_dyn_boxed()
        });
        NestedEdges::new(self.graph.clone(), nodes, edges)
    }

    fn hop<
        I: Iterator<Item = VID> + Send + Sync + 'graph,
        T: Fn(&GraphStorage, &DynGraphArc<'graph>, VID) -> I + Send + Sync + 'graph,
    >(
        &self,
        op: T,
    ) -> Self::PathType {
        let nodes = self.clone();
        let nodes = Arc::new(move || nodes.iter_refs().into_dyn_boxed());
        PathFromGraph::new(
            self.graph.clone(),
            nodes,
            Arc::new(move |graph, v| {
                let cg = graph.core_graph();
                op(cg, &graph, v).into_dyn_boxed()
            }),
        )
    }
}

impl<'graph, G, Current, F> InternalFilter<'graph> for Nodes<'graph, G, Current, F>
where
    G: GraphViewOps<'graph> + 'graph,
    Current: GraphViewOps<'graph> + 'graph,
    F: NodeFilterOp + Clone + 'graph,
{
    type Graph = Current;
    type Filtered<Next: GraphViewOps<'graph>> = Nodes<'graph, G, Next, F>;

    fn base_graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn apply_filter<Next: GraphViewOps<'graph>>(
        &self,
        filtered_graph: Next,
    ) -> Self::Filtered<Next> {
        Nodes {
            base_graph: self.base_graph.clone(),
            graph: filtered_graph,
            predicate: self.predicate.clone(),
            nodes: self.nodes.clone(),
            _marker: PhantomData,
        }
    }
}

impl<'graph, G, GH, F> IntoIterator for Nodes<'graph, G, GH, F>
where
    G: GraphViewOps<'graph> + 'graph,
    GH: GraphViewOps<'graph> + 'graph,
    F: NodeFilterOp + 'graph,
{
    type Item = NodeView<'graph, GH>;
    type IntoIter = BoxedLIter<'graph, Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter_owned())
    }
}

#[cfg(test)]
mod test {
    use crate::{
        db::api::{state::Index, view::internal::NodeList},
        prelude::*,
    };
    use itertools::Itertools;
    use rayon::prelude::*;
    use std::collections::BTreeSet;

    #[test]
    fn test_indexed_nodes_respect_index() {
        let graph = Graph::new();
        for n in 0..6u64 {
            graph.add_node(0, n, NO_PROPS, None, None).unwrap();
        }
        graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        graph.add_edge(0, 2, 3, NO_PROPS, None).unwrap();

        for graph in [&graph.subgraph([0, 1, 2, 3, 4]), &graph.subgraph([0, 1, 2])] {
            let nodes = graph.nodes();
            let all = nodes.iter().map(|n| n.id()).collect_vec();

            // Restrict to every other node of the view.
            let picked = nodes.iter().step_by(2).map(|n| n.node).collect_vec();
            let expected = nodes
                .iter()
                .step_by(2)
                .map(|n| n.id())
                .collect::<BTreeSet<_>>();
            assert!(picked.len() < all.len(), "index should be a strict subset");

            let indexed = nodes.indexed(NodeList::List {
                elems: Index::from_iter(picked),
            });
            assert_eq!(
                indexed.iter().map(|n| n.id()).collect::<BTreeSet<_>>(),
                expected
            );
            assert_eq!(
                indexed.par_iter().map(|n| n.id()).collect::<BTreeSet<_>>(),
                expected
            );
            assert_eq!(
                indexed
                    .collect()
                    .iter()
                    .map(|n| n.id())
                    .collect::<BTreeSet<_>>(),
                expected
            );
            assert_eq!(indexed.len(), expected.len());
        }
    }
}
