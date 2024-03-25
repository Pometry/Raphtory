use crate::{
    core::entities::{nodes::node_ref::NodeRef, VID},
    db::{
        api::{
            state::NodeStateOps,
            storage::locked::LockedGraph,
            view::{internal::NodeList, IntoDynBoxed},
        },
        graph::node::NodeView,
    },
    prelude::GraphViewOps,
};
use rayon::prelude::ParallelIterator;
use std::{marker::PhantomData, sync::Arc};

pub struct LazyNodeState<'graph, V, G, GH = G> {
    op: Arc<dyn Fn(&LockedGraph, &GH, VID) -> V + Send + Sync>,
    base_graph: G,
    graph: GH,
    nodes: NodeList,
    _marker: PhantomData<&'graph ()>,
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>, V: 'graph> IntoIterator
    for LazyNodeState<'graph, V, G, GH>
{
    type Item = (NodeView<G, GH>, V);
    type IntoIter = Box<dyn Iterator<Item = (NodeView<G, GH>, V)> + Send + 'graph>;

    fn into_iter(self) -> Self::IntoIter {
        let cg = self.graph.core_graph();
        self.nodes
            .into_iter()
            .filter_map(move |v| {
                self.graph
                    .filter_node(cg.nodes.get(v), self.graph.layer_ids())
                    .then(|| {
                        (
                            NodeView::new_one_hop_filtered(
                                self.base_graph.clone(),
                                self.graph.clone(),
                                v,
                            ),
                            (self.op)(&cg, &self.graph, v),
                        )
                    })
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
                .filter_node(cg.nodes.get(n), self.graph.layer_ids())
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
                .filter_node(cg.nodes.get(n), self.graph.layer_ids())
                .then(|| (self.op)(&cg, &self.graph, n))
        })
    }

    fn into_values(self) -> impl Iterator<Item = Self::OwnedValue> + 'graph {
        let cg = self.graph.core_graph();
        self.nodes.into_iter().filter_map(move |n| {
            self.graph
                .filter_node(cg.nodes.get(n), self.graph.layer_ids())
                .then(|| (self.op)(&cg, &self.graph, n))
        })
    }

    fn into_par_values(self) -> impl ParallelIterator<Item = Self::OwnedValue> + 'graph {
        let cg = self.graph.core_graph();
        self.nodes.into_par_iter().filter_map(move |n| {
            self.graph
                .filter_node(cg.nodes.get(n), self.graph.layer_ids())
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
                .filter_node(cg.nodes.get(n), self.graph.layer_ids())
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
                .filter_node(cg.nodes.get(n), self.graph.layer_ids())
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
        let vid = if self.graph.node_list_trusted() {
            match self.graph.node_list() {
                NodeList::All { num_nodes } => {
                    if index < num_nodes {
                        VID(index)
                    } else {
                        return None;
                    }
                }
                NodeList::List { nodes } => *nodes.get(index)?,
            }
        } else {
            let cg = self.graph.core_graph();
            self.graph
                .node_list()
                .into_iter()
                .filter(|vid| {
                    self.graph
                        .filter_node(cg.nodes.get(*vid), self.graph.layer_ids())
                })
                .skip(index)
                .next()?
        };
        let cg = self.graph.core_graph();
        Some((
            NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, vid),
            (self.op)(&cg, &self.graph, vid),
        ))
    }

    fn get_by_node<N: Into<NodeRef>>(
        &self,
        node: N,
    ) -> Option<(NodeView<&Self::BaseGraph, &Self::Graph>, Self::Value<'_>)> {
        let vid = self.graph.internalise_node(node.into())?;
        let cg = self.graph.core_graph();
        self.graph
            .filter_node(cg.nodes.get(vid), self.graph.layer_ids())
            .then(|| {
                (
                    NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, vid),
                    (self.op)(&cg, &self.graph, vid),
                )
            })
    }

    fn len(&self) -> usize {
        self.graph.count_nodes()
    }
}
