use crate::{
    core::entities::VID,
    db::{
        api::state::{NodeState, NodeStateOps},
        graph::node::NodeView,
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::AsNodeRef;
use raphtory_memstorage::db::api::{list_ops::NodeList, storage::graph::GraphStorage};
use rayon::prelude::*;
use std::{marker::PhantomData, sync::Arc};

#[derive(Clone)]
pub struct LazyNodeState<'graph, V, G, GH = G> {
    op: Arc<dyn Fn(&GraphStorage, &GH, VID) -> V + Send + Sync + 'graph>,
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
        op: impl Fn(&GraphStorage, &GH, VID) -> V + Send + Sync + 'graph,
    ) -> Self {
        let op = Arc::new(op);
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
