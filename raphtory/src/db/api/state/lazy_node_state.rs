use crate::{
    core::entities::{
        nodes::node_ref::{AsNodeRef, NodeRef},
        VID,
    },
    db::{
        api::{
            state::{NodeState, NodeStateOps},
            storage::storage_ops::GraphStorage,
            view::{internal::NodeList, IntoDynBoxed},
        },
        graph::node::NodeView,
    },
    prelude::GraphViewOps,
};
use rayon::prelude::*;
use std::{marker::PhantomData, sync::Arc};

#[derive(Clone)]
pub struct LazyNodeState<'graph, V, G, GH = G> {
    op: Arc<dyn Fn(&GraphStorage, &GH, VID) -> V + Send + Sync + 'graph>,
    base_graph: G,
    graph: GH,
    nodes: NodeList,
    _marker: PhantomData<&'graph ()>,
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>, V: Send + Sync + 'graph>
    LazyNodeState<'graph, V, G, GH>
{
    pub(crate) fn new(
        base_graph: G,
        graph: GH,
        nodes: NodeList,
        op: impl Fn(&GraphStorage, &GH, VID) -> V + Send + Sync + 'graph,
    ) -> Self {
        let op = Arc::new(op);
        Self {
            op,
            base_graph,
            graph,
            nodes,
            _marker: Default::default(),
        }
    }

    fn apply(&self, cg: &GraphStorage, g: &GH, vid: VID) -> V {
        (self.op)(cg, g, vid)
    }

    pub fn compute(&self) -> NodeState<'graph, V, G, GH> {
        let cg = self.graph.core_graph();
        match &self.nodes {
            NodeList::All { .. } => {
                if self.graph.nodes_filtered() {
                    let keys: Vec<_> = cg.nodes_par(&self.graph).collect();
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
            NodeList::List { nodes } => {
                let cg = self.graph.core_graph();
                if self.graph.nodes_filtered() {
                    let keys: Vec<_> = nodes
                        .par_iter()
                        .filter(|&&v| {
                            self.graph
                                .filter_node(cg.nodes().node(v), self.graph.layer_ids())
                        })
                        .copied()
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
                    let mut values = Vec::with_capacity(nodes.len());
                    nodes
                        .par_iter()
                        .map(|vid| self.apply(&cg, &self.graph, *vid))
                        .collect_into_vec(&mut values);
                    NodeState::new(
                        self.base_graph.clone(),
                        self.graph.clone(),
                        values,
                        Some(nodes.clone()),
                    )
                }
            }
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>, V: 'graph> IntoIterator
    for LazyNodeState<'graph, V, G, GH>
{
    type Item = V;
    type IntoIter = Box<dyn Iterator<Item = V> + Send + 'graph>;

    fn into_iter(self) -> Self::IntoIter {
        let cg = self.graph.core_graph();
        self.nodes
            .into_iter()
            .filter_map(move |v| {
                self.graph
                    .filter_node(cg.node(v), self.graph.layer_ids())
                    .then(|| (self.op)(&cg, &self.graph, v))
            })
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
    type Value<'a> = V where 'graph: 'a, Self: 'a;
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
        let cg = self.graph.core_graph();
        self.nodes.iter().filter_map(move |n| {
            self.graph
                .filter_node(cg.nodes().node(n), self.graph.layer_ids())
                .then(|| (self.op)(&cg, &self.graph, n))
        })
    }

    fn par_values<'a>(&'a self) -> impl ParallelIterator<Item = Self::Value<'a>> + 'a
    where
        'graph: 'a,
    {
        let cg = self.graph.core_graph();
        self.nodes.par_iter().filter_map(move |n| {
            self.graph
                .filter_node(cg.nodes().node(n), self.graph.layer_ids())
                .then(|| (self.op)(&cg, &self.graph, n))
        })
    }

    fn into_values(self) -> impl Iterator<Item = Self::OwnedValue> + 'graph {
        let cg = self.graph.core_graph();
        self.nodes.into_iter().filter_map(move |n| {
            self.graph
                .filter_node(cg.nodes().node(n), self.graph.layer_ids())
                .then(|| (self.op)(&cg, &self.graph, n))
        })
    }

    fn into_par_values(self) -> impl ParallelIterator<Item = Self::OwnedValue> + 'graph {
        let cg = self.graph.core_graph();
        self.nodes.into_par_iter().filter_map(move |n| {
            self.graph
                .filter_node(cg.nodes().node(n), self.graph.layer_ids())
                .then(|| (self.op)(&cg, &self.graph, n))
        })
    }

    fn iter<'a>(
        &'a self,
    ) -> impl Iterator<
        Item = (
            NodeView<&'a Self::BaseGraph, &'a Self::Graph>,
            Self::Value<'_>,
        ),
    > + 'a
    where
        'graph: 'a,
    {
        let cg = self.graph.core_graph();
        self.nodes.iter().filter_map(move |n| {
            self.graph
                .filter_node(cg.nodes().node(n), self.graph.layer_ids())
                .then(|| {
                    (
                        NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, n),
                        (self.op)(&cg, &self.graph, n),
                    )
                })
        })
    }

    fn par_iter<'a>(
        &'a self,
    ) -> impl ParallelIterator<
        Item = (
            NodeView<&'a Self::BaseGraph, &'a Self::Graph>,
            Self::Value<'_>,
        ),
    >
    where
        'graph: 'a,
    {
        let cg = self.graph.core_graph();
        self.nodes.par_iter().filter_map(move |n| {
            self.graph
                .filter_node(cg.nodes().node(n), self.graph.layer_ids())
                .then(|| {
                    (
                        NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, n),
                        (self.op)(&cg, &self.graph, n),
                    )
                })
        })
    }

    fn get_by_index(
        &self,
        index: usize,
    ) -> Option<(NodeView<&Self::BaseGraph, &Self::Graph>, Self::Value<'_>)> {
        if self.graph.nodes_filtered() {
            self.iter().skip(index).next()
        } else {
            let vid = match &self.nodes {
                NodeList::All { num_nodes } => {
                    if index < *num_nodes {
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
                (self.op)(&cg, &self.graph, vid),
            ))
        }
    }

    fn get_by_node<N: AsNodeRef>(&self, node: N) -> Option<Self::Value<'_>> {
        let vid = self.graph.internalise_node(node.as_node_ref())?;
        match &self.nodes {
            NodeList::All { .. } => {}
            NodeList::List { nodes } => {
                if !nodes.contains(&vid) {
                    return None;
                }
            }
        }
        let cg = self.graph.core_graph();
        self.graph
            .filter_node(cg.nodes().node(vid), self.graph.layer_ids())
            .then(|| (self.op)(&cg, &self.graph, vid))
    }

    fn len(&self) -> usize {
        self.graph.count_nodes()
    }
}
