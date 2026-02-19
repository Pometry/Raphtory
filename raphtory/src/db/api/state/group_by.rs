use crate::{
    db::{
        api::state::{ops::Const, Index},
        graph::{nodes::Nodes, views::node_subgraph::NodeSubgraph},
    },
    prelude::{GraphViewOps, NodeStateOps},
};
use dashmap::DashMap;
use indexmap::IndexSet;
use raphtory_api::core::entities::VID;
use rayon::prelude::*;
use std::{fmt::Debug, hash::Hash, sync::Arc};

use super::node_state_ops::ToOwnedValue;

#[derive(Clone, Debug)]
pub struct NodeGroups<V, G> {
    pub groups: Arc<[(V, Index<VID>)]>,
    pub graph: G,
}

impl<'graph, V: Hash + Eq + Send + Sync + Clone, G: GraphViewOps<'graph>> NodeGroups<V, G> {
    pub(crate) fn new(values: impl ParallelIterator<Item = (VID, V)>, graph: G) -> Self {
        let groups: DashMap<V, IndexSet<VID, ahash::RandomState>, ahash::RandomState> =
            DashMap::default();
        values.for_each(|(node, v)| {
            groups.entry(v).or_default().insert(node);
        });

        let groups = groups
            .into_par_iter()
            .map(|(k, v)| (k, Index::new(v)))
            .collect();

        Self { groups, graph }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&V, Nodes<'graph, G>)> {
        self.groups.iter().map(|(v, nodes)| {
            (
                v,
                Nodes::new_filtered(
                    self.graph.clone(),
                    self.graph.clone(),
                    Const(true),
                    Some(nodes.clone()),
                ),
            )
        })
    }

    pub fn into_iter_groups(self) -> impl Iterator<Item = (V, Nodes<'graph, G>)>
    where
        V: Clone,
    {
        (0..self.len()).map(move |i| {
            let (v, nodes) = self.group(i).unwrap();
            (v.clone(), nodes)
        })
    }

    pub fn into_iter_subgraphs(self) -> impl Iterator<Item = (V, NodeSubgraph<G>)>
    where
        V: Clone,
    {
        (0..self.len()).map(move |i| {
            let (v, graph) = self.group_subgraph(i).unwrap();
            (v.clone(), graph)
        })
    }

    pub fn iter_subgraphs(&self) -> impl Iterator<Item = (&V, NodeSubgraph<G>)> {
        self.groups.iter().map(|(v, nodes)| {
            (
                v,
                NodeSubgraph {
                    graph: self.graph.clone(),
                    nodes: nodes.clone(),
                },
            )
        })
    }

    pub fn group(&self, index: usize) -> Option<(&V, Nodes<'graph, G>)> {
        self.groups.get(index).map(|(v, nodes)| {
            (
                v,
                Nodes::new_filtered(
                    self.graph.clone(),
                    self.graph.clone(),
                    Const(true),
                    Some(nodes.clone()),
                ),
            )
        })
    }

    pub fn group_subgraph(&self, index: usize) -> Option<(&V, NodeSubgraph<G>)> {
        self.groups.get(index).map(|(v, nodes)| {
            (
                v,
                NodeSubgraph {
                    graph: self.graph.clone(),
                    nodes: nodes.clone(),
                },
            )
        })
    }

    pub fn len(&self) -> usize {
        self.groups.len()
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }
}

pub trait NodeStateGroupBy<'a, 'graph: 'a>: NodeStateOps<'a, 'graph> {
    fn groups(&'a self) -> NodeGroups<Self::OwnedValue, Self::Graph>;
}

impl<'a, 'graph: 'a, S: NodeStateOps<'a, 'graph>> NodeStateGroupBy<'a, 'graph> for S
where
    S::OwnedValue: Hash + Eq + Debug,
{
    fn groups(&'a self) -> NodeGroups<Self::OwnedValue, Self::Graph> {
        self.group_by(|v| v.to_owned_value())
    }
}
