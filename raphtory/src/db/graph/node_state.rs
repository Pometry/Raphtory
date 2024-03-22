use crate::{
    core::entities::VID,
    db::{
        api::{
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

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>, V: Send + Sync + 'graph>
    LazyNodeState<'graph, V, G, GH>
{
    pub fn values(&self) -> impl Iterator<Item = V> + '_ {
        let cg = self.graph.core_graph();
        self.nodes.iter().filter_map(move |n| {
            self.graph
                .filter_node(cg.nodes.get(n), self.graph.layer_ids())
                .then(|| (self.op)(&cg, &self.graph, n))
        })
    }

    pub fn par_values(&self) -> impl ParallelIterator<Item = V> + '_ {
        let cg = self.graph.core_graph();
        self.nodes.par_iter().filter_map(move |n| {
            self.graph
                .filter_node(cg.nodes.get(n), self.graph.layer_ids())
                .then(|| (self.op)(&cg, &self.graph, n))
        })
    }

    pub fn into_values(self) -> impl Iterator<Item = V> + 'graph {
        let cg = self.graph.core_graph();
        self.nodes.into_iter().filter_map(move |n| {
            self.graph
                .filter_node(cg.nodes.get(n), self.graph.layer_ids())
                .then(|| (self.op)(&cg, &self.graph, n))
        })
    }

    pub fn into_par_values(self) -> impl ParallelIterator<Item = V> + 'graph {
        let cg = self.graph.core_graph();
        self.nodes.into_par_iter().filter_map(move |n| {
            self.graph
                .filter_node(cg.nodes.get(n), self.graph.layer_ids())
                .then(|| (self.op)(&cg, &self.graph, n))
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = (NodeView<&G, &GH>, V)> {
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

    pub fn par_iter(&self) -> impl ParallelIterator<Item = (NodeView<&G, &GH>, V)> {
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
