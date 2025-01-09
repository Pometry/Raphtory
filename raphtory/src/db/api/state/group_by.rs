use crate::{
    db::{
        api::state::Index,
        graph::{nodes::Nodes, views::node_subgraph::NodeSubgraph},
    },
    prelude::{GraphViewOps, NodeStateOps},
};
use raphtory_api::core::entities::VID;
use std::{collections::HashMap, hash::Hash, sync::Arc};

#[derive(Clone, Debug)]
pub struct NodeGroups<V, G> {
    groups: Arc<[(V, Index<VID>)]>,
    graph: G,
}

impl<'graph, V: Hash + Eq, G: GraphViewOps<'graph>> NodeGroups<V, G> {
    pub(crate) fn new(values: impl Iterator<Item = (VID, V)>, graph: G) -> Self {
        let mut groups: HashMap<V, Vec<VID>> = HashMap::new();
        for (node, v) in values {
            groups.entry(v).or_insert_with(Vec::new).push(node);
        }
        let groups = groups
            .into_iter()
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
                    Some(nodes.clone()),
                    None,
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
                    Some(nodes.clone()),
                    None,
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

pub trait NodeStateGroupBy<'graph>: NodeStateOps<'graph> {
    fn groups(&self) -> NodeGroups<Self::OwnedValue, Self::Graph>;
}

impl<'graph, S: NodeStateOps<'graph>> NodeStateGroupBy<'graph> for S
where
    S::OwnedValue: Hash + Eq,
{
    fn groups(&self) -> NodeGroups<Self::OwnedValue, Self::Graph> {
        self.group_by(|v| v.clone())
    }
}

#[cfg(test)]
mod tests {}
