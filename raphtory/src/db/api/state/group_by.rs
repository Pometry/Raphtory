use crate::{
    db::{
        api::state::Index,
        graph::{nodes::Nodes, views::node_subgraph::NodeSubgraph},
    },
    prelude::{GraphViewOps, NodeStateOps},
};
use dashmap::DashMap;
use raphtory_api::core::entities::VID;
use rayon::prelude::*;
use std::{hash::Hash, sync::Arc};

#[derive(Clone, Debug)]
pub struct NodeGroups<V, G> {
    groups: Arc<[(V, Index<VID>)]>,
    graph: G,
}

impl<'graph, V: Hash + Eq + Send + Sync + Clone, G: GraphViewOps<'graph>> NodeGroups<V, G> {
    pub(crate) fn new(values: impl ParallelIterator<Item = (VID, V)>, graph: G) -> Self {
        let groups: DashMap<V, Vec<VID>, ahash::RandomState> = DashMap::default();
        values.for_each(|(node, v)| {
            groups.entry(v).or_insert_with(Vec::new).push(node);
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
mod tests {
    use crate::{prelude::*, test_storage};
    use std::collections::HashMap;

    #[test]
    fn test() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.add_edge(0, 2, 3, NO_PROPS, None).unwrap();
        g.add_edge(0, 4, 5, NO_PROPS, None).unwrap();

        test_storage!(&g, |g| {
            let groups_from_lazy = g.nodes().out_degree().groups();
            let groups_from_eager = g.nodes().out_degree().compute().groups();

            let expected = HashMap::from([
                (0, vec![GID::U64(3), GID::U64(5)]),
                (1, vec![GID::U64(1), GID::U64(2), GID::U64(4)]),
            ]);

            assert_eq!(
                groups_from_lazy
                    .iter()
                    .map(|(v, nodes)| (*v, nodes.id().collect_vec()))
                    .collect::<HashMap<_, _>>(),
                expected
            );

            assert_eq!(
                groups_from_lazy
                    .clone()
                    .into_iter_groups()
                    .map(|(v, nodes)| (v, nodes.id().collect_vec()))
                    .collect::<HashMap<_, _>>(),
                expected
            );

            assert_eq!(
                groups_from_lazy
                    .iter_subgraphs()
                    .map(|(v, graph)| (*v, graph.nodes().id().collect_vec()))
                    .collect::<HashMap<_, _>>(),
                expected
            );

            assert_eq!(
                groups_from_lazy
                    .clone()
                    .into_iter_subgraphs()
                    .map(|(v, graph)| (v, graph.nodes().id().collect_vec()))
                    .collect::<HashMap<_, _>>(),
                expected
            );

            assert_eq!(
                groups_from_eager
                    .iter()
                    .map(|(v, nodes)| (*v, nodes.id().collect_vec()))
                    .collect::<HashMap<_, _>>(),
                expected
            );

            assert_eq!(groups_from_lazy.len(), expected.len());

            for (i, (v, nodes)) in groups_from_eager.iter().enumerate() {
                let (v2, nodes2) = groups_from_eager.group(i).unwrap();
                assert_eq!(v, v2);
                assert!(nodes.iter().eq(nodes2.iter()));
                let (v3, graph) = groups_from_eager.group_subgraph(i).unwrap();
                assert_eq!(v, v3);
                assert!(nodes.iter().eq(graph.nodes().iter()));
            }
        });
    }
}
