use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, VID},
    db::{
        api::{
            state::{NodeState, NodeStateOps},
            storage::graph::{nodes::node_storage_ops::NodeStorageOps, storage_ops::GraphStorage},
            view::{
                internal::{CoreGraphOps, NodeList, OneHopFilter},
                BoxedLIter, IntoDynBoxed,
            },
        },
        graph::{node::NodeView, nodes::Nodes},
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::Direction;
use rayon::prelude::*;
use std::{marker::PhantomData, ops::Deref, sync::Arc};

pub trait NodeOp: Send + Sync {
    type Output: Clone + Send + Sync;
    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output;
}

pub struct Degree<G> {
    pub(crate) graph: G,
    pub(crate) dir: Direction,
}

impl<'graph, G: GraphViewOps<'graph>> NodeOp for Degree<G> {
    type Output = usize;

    fn apply(&self, storage: &GraphStorage, node: VID) -> usize {
        storage.node_degree(node, self.dir, &self.graph)
    }
}

impl<'graph, G: GraphViewOps<'graph>> OneHopFilter<'graph> for Degree<G> {
    type BaseGraph = G;
    type FilteredGraph = G;
    type Filtered<GH: GraphViewOps<'graph> + 'graph> = Degree<GH>;

    fn current_filter(&self) -> &Self::FilteredGraph {
        &self.graph
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.graph
    }

    fn one_hop_filtered<GH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GH,
    ) -> Self::Filtered<GH> {
        Degree {
            graph: filtered_graph,
            dir: self.dir,
        }
    }
}

impl<V: Clone + Send + Sync> NodeOp for Arc<dyn NodeOp<Output = V>> {
    type Output = V;
    fn apply(&self, storage: &GraphStorage, node: VID) -> V {
        self.deref().apply(storage, node)
    }
}

pub struct LazyNodeState2<'graph, Op, G, GH = G> {
    nodes: Nodes<'graph, G, GH>,
    op: Op,
}

impl<'graph, Op: OneHopFilter<'graph>, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    OneHopFilter<'graph> for LazyNodeState2<'graph, Op, G, GH>
{
    type BaseGraph = G;
    type FilteredGraph = Op::FilteredGraph;
    type Filtered<GHH: GraphViewOps<'graph> + 'graph> =
        LazyNodeState2<'graph, Op::Filtered<GHH>, G, GH>;

    fn current_filter(&self) -> &Self::FilteredGraph {
        self.op.current_filter()
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        self.nodes.base_graph()
    }

    fn one_hop_filtered<GHH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GHH,
    ) -> Self::Filtered<GHH> {
        LazyNodeState2 {
            nodes: self.nodes.clone(),
            op: self.op.one_hop_filtered(filtered_graph),
        }
    }
}

impl<'graph, Op: NodeOp + 'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> IntoIterator
    for LazyNodeState2<'graph, Op, G, GH>
{
    type Item = Op::Output;
    type IntoIter = BoxedLIter<'graph, Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.into_values().into_dyn_boxed()
    }
}

impl<'graph, Op: NodeOp + 'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    NodeStateOps<'graph> for LazyNodeState2<'graph, Op, G, GH>
{
    type Graph = GH;
    type BaseGraph = G;
    type Value<'a>
        = Op::Output
    where
        'graph: 'a,
        Self: 'a;
    type OwnedValue = Op::Output;

    fn graph(&self) -> &Self::Graph {
        &self.nodes.graph
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.nodes.base_graph
    }

    fn values<'a>(&'a self) -> impl Iterator<Item = Self::Value<'a>> + 'a
    where
        'graph: 'a,
    {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .iter_refs()
            .map(move |vid| self.op.apply(&storage, vid))
    }

    fn par_values<'a>(&'a self) -> impl ParallelIterator<Item = Self::Value<'a>> + 'a
    where
        'graph: 'a,
    {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .par_iter_refs()
            .map(move |vid| self.op.apply(&storage, vid))
    }

    fn into_values(self) -> impl Iterator<Item = Self::OwnedValue> + 'graph {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .iter_refs()
            .map(move |vid| self.op.apply(&storage, vid))
    }

    fn into_par_values(self) -> impl ParallelIterator<Item = Self::OwnedValue> + 'graph {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .par_iter_refs()
            .map(move |vid| self.op.apply(&storage, vid))
    }

    fn iter<'a>(
        &'a self,
    ) -> impl Iterator<
        Item = (
            NodeView<&'a Self::BaseGraph, &'a Self::Graph>,
            Self::Value<'a>,
        ),
    > + 'a
    where
        'graph: 'a,
    {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .iter()
            .map(move |node| (node, self.op.apply(&storage, node.node)))
    }

    fn par_iter<'a>(
        &'a self,
    ) -> impl ParallelIterator<
        Item = (
            NodeView<&'a Self::BaseGraph, &'a Self::Graph>,
            Self::Value<'a>,
        ),
    >
    where
        'graph: 'a,
    {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .par_iter()
            .map(move |node| (node, self.op.apply(&storage, node.node)))
    }

    fn get_by_index(
        &self,
        index: usize,
    ) -> Option<(NodeView<&Self::BaseGraph, &Self::Graph>, Self::Value<'_>)> {
        if self.graph().nodes_filtered() {
            self.iter().nth(index)
        } else {
            let vid = match self.graph().node_list() {
                NodeList::All { num_nodes } => {
                    if index < num_nodes {
                        VID(index)
                    } else {
                        return None;
                    }
                }
                NodeList::List { nodes } => nodes.key(index)?,
            };
            let cg = self.graph().core_graph();
            Some((
                NodeView::new_one_hop_filtered(self.base_graph(), self.graph(), vid),
                self.op.apply(cg, vid),
            ))
        }
    }

    fn get_by_node<N: AsNodeRef>(&self, node: N) -> Option<Self::Value<'_>> {
        let node = (&self.graph()).node(node);
        node.map(|node| self.op.apply(self.graph().core_graph(), node.node))
    }

    fn len(&self) -> usize {
        self.nodes.len()
    }
}

#[derive(Clone)]
pub struct LazyNodeState<'graph, V, G, GH = G> {
    op: fn(&GraphStorage, &GH, VID) -> V,
    base_graph: G,
    graph: GH,
    node_types_filter: Option<Arc<[bool]>>,
    _marker: PhantomData<&'graph ()>,
}

impl<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        V: Clone + Send + Sync + 'graph,
    > LazyNodeState<'graph, V, G, GH>
{
    pub(crate) fn new(
        base_graph: G,
        graph: GH,
        node_types_filter: Option<Arc<[bool]>>,
        op: fn(&GraphStorage, &GH, VID) -> V,
    ) -> Self {
        Self {
            op,
            base_graph,
            graph,
            node_types_filter,
            _marker: Default::default(),
        }
    }

    fn apply(&self, cg: &GraphStorage, g: &GH, vid: VID) -> V {
        (self.op)(cg, g, vid)
    }

    pub fn compute(&self) -> NodeState<'graph, V, G, GH> {
        let cg = self.graph.core_graph().lock();
        if self.graph.nodes_filtered() || self.node_types_filter.is_some() {
            let keys: Vec<_> = cg
                .nodes_par(&self.graph, self.node_types_filter.as_ref())
                .collect();
            let mut values = Vec::with_capacity(keys.len());
            keys.par_iter()
                .map(|vid| self.apply(&cg, &self.graph, *vid))
                .collect_into_vec(&mut values);
            NodeState::new(
                self.base_graph.clone(),
                self.graph.clone(),
                values,
                Some(keys.into()),
            )
        } else {
            let n = cg.nodes().len();
            let mut values = Vec::with_capacity(n);
            (0..n)
                .into_par_iter()
                .map(|i| self.apply(&cg, &self.graph, VID(i)))
                .collect_into_vec(&mut values);
            NodeState::new(self.base_graph.clone(), self.graph.clone(), values, None)
        }
    }

    pub fn collect<C: FromParallelIterator<V>>(&self) -> C {
        self.par_values().collect()
    }

    pub fn collect_vec(&self) -> Vec<V> {
        self.collect()
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>, V: 'graph> IntoIterator
    for LazyNodeState<'graph, V, G, GH>
{
    type Item = V;
    type IntoIter = Box<dyn Iterator<Item = V> + Send + 'graph>;

    fn into_iter(self) -> Self::IntoIter {
        let cg = self.graph.core_graph().lock();
        let graph = self.graph;
        let op = self.op;
        cg.clone()
            .into_nodes_iter(graph.clone(), self.node_types_filter)
            .map(move |v| op(&cg, &graph, v))
            .into_dyn_boxed()
    }
}

impl<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        V: Clone + Send + Sync + 'graph,
    > NodeStateOps<'graph> for LazyNodeState<'graph, V, G, GH>
{
    type Graph = GH;
    type BaseGraph = G;
    type Value<'a>
        = V
    where
        'graph: 'a,
        Self: 'a;
    type OwnedValue = V;

    fn graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.base_graph
    }

    fn values<'a>(&'a self) -> impl Iterator<Item = Self::Value<'a>> + 'a
    where
        'graph: 'a,
    {
        let cg = self.graph.core_graph().lock();
        cg.clone()
            .into_nodes_iter(&self.graph, self.node_types_filter.clone())
            .map(move |vid| self.apply(&cg, &self.graph, vid))
    }

    fn par_values<'a>(&'a self) -> impl ParallelIterator<Item = Self::Value<'a>> + 'a
    where
        'graph: 'a,
    {
        let cg = self.graph.core_graph().lock();
        cg.clone()
            .into_nodes_par(&self.graph, self.node_types_filter.clone())
            .map(move |vid| self.apply(&cg, &self.graph, vid))
    }

    fn into_values(self) -> impl Iterator<Item = Self::OwnedValue> + 'graph {
        let cg = self.graph.core_graph().lock();
        let graph = self.graph.clone();
        let op = self.op;
        cg.clone()
            .into_nodes_iter(self.graph, self.node_types_filter)
            .map(move |n| op(&cg, &graph, n))
    }

    fn into_par_values(self) -> impl ParallelIterator<Item = Self::OwnedValue> + 'graph {
        let cg = self.graph.core_graph().lock();
        let graph = self.graph.clone();
        let op = self.op;
        cg.clone()
            .into_nodes_par(self.graph, self.node_types_filter)
            .map(move |n| op(&cg, &graph, n))
    }

    fn iter<'a>(
        &'a self,
    ) -> impl Iterator<
        Item = (
            NodeView<&'a Self::BaseGraph, &'a Self::Graph>,
            Self::Value<'a>,
        ),
    > + 'a
    where
        'graph: 'a,
    {
        let cg = self.graph.core_graph().lock();
        cg.clone()
            .into_nodes_iter(self.graph.clone(), self.node_types_filter.clone())
            .map(move |n| {
                (
                    NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, n),
                    (self.op)(&cg, &self.graph, n),
                )
            })
    }

    fn par_iter<'a>(
        &'a self,
    ) -> impl ParallelIterator<
        Item = (
            NodeView<&'a Self::BaseGraph, &'a Self::Graph>,
            Self::Value<'a>,
        ),
    >
    where
        'graph: 'a,
    {
        let cg = self.graph.core_graph().lock();
        cg.clone()
            .into_nodes_par(self.graph.clone(), self.node_types_filter.clone())
            .map(move |n| {
                (
                    NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, n),
                    (self.op)(&cg, &self.graph, n),
                )
            })
    }

    fn get_by_index(
        &self,
        index: usize,
    ) -> Option<(NodeView<&Self::BaseGraph, &Self::Graph>, Self::Value<'_>)> {
        if self.graph.nodes_filtered() {
            self.iter().nth(index)
        } else {
            let vid = match self.graph.node_list() {
                NodeList::All { num_nodes } => {
                    if index < num_nodes {
                        VID(index)
                    } else {
                        return None;
                    }
                }
                NodeList::List { nodes } => nodes.key(index)?,
            };
            let cg = self.graph.core_graph();
            Some((
                NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, vid),
                (self.op)(cg, &self.graph, vid),
            ))
        }
    }

    fn get_by_node<N: AsNodeRef>(&self, node: N) -> Option<Self::Value<'_>> {
        let vid = self.graph.internalise_node(node.as_node_ref())?;
        if !self.graph.has_node(vid) {
            return None;
        }
        if let Some(type_filter) = self.node_types_filter.as_ref() {
            let core_node_entry = &self.graph.core_node_entry(vid);
            if !type_filter[core_node_entry.node_type_id()] {
                return None;
            }
        }

        let cg = self.graph.core_graph();
        Some(self.apply(cg, &self.graph, vid))
    }

    fn len(&self) -> usize {
        self.graph.count_nodes()
    }
}

#[cfg(test)]
mod test {
    use crate::{
        db::api::{
            state::lazy_node_state::{Degree, LazyNodeState2, NodeOp},
            view::{internal::CoreGraphOps, IntoDynamic},
        },
        prelude::*,
    };
    use raphtory_api::core::{entities::VID, Direction};
    use std::sync::Arc;

    struct TestWrapper<Op: NodeOp>(Op);
    #[test]
    fn test_compile() {
        let g = Graph::new();
        g.add_edge(0, 0, 1, NO_PROPS, None).unwrap();

        let nodes = g.nodes();

        let node_state = LazyNodeState2 {
            nodes,
            op: Degree {
                graph: g.clone(),
                dir: Direction::BOTH,
            },
        };
        let node_state_window = node_state.after(1);

        let deg: Vec<_> = node_state.values().collect();
        let deg_w: Vec<_> = node_state_window.values().collect();

        let node_state_filter = node_state.valid_layers("bla");

        assert_eq!(deg, [1, 1]);
        assert_eq!(deg_w, [0, 0]);

        let g_dyn = g.clone().into_dynamic();

        let deg = Degree {
            graph: g_dyn,
            dir: Direction::BOTH,
        };
        let arc_deg: Arc<dyn NodeOp<Output = usize>> = Arc::new(deg);

        let node_state_dyn = LazyNodeState2 {
            nodes: g.nodes(),
            op: arc_deg.clone(),
        };

        let dyn_deg: Vec<_> = node_state_dyn.values().collect();
        assert_eq!(dyn_deg, [1, 1]);
        assert_eq!(arc_deg.apply(g.core_graph(), VID(0)), 1);

        let test_struct = TestWrapper(arc_deg);
    }
}
