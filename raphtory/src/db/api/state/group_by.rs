use crate::{
    db::{api::state::Index, graph::nodes::Nodes},
    prelude::NodeStateOps,
};
use raphtory_api::core::entities::VID;
use rayon::prelude::*;
use std::{collections::HashMap, hash::Hash, sync::Arc};

#[derive(Clone, Debug)]
pub struct NodeGroups<V, G, GH = G> {
    groups: Arc<HashMap<V, Index<VID>>>,
    base_graph: G,
    graph: GH,
}

impl<V: Hash + Eq, G, GH> NodeGroups<V, G, GH> {
    pub(crate) fn new(values: impl Iterator<Item = (VID, V)>, base_graph: G, graph: GH) -> Self {
        let mut groups: HashMap<V, Vec<VID>> = HashMap::new();
        for (node, v) in values {
            groups.entry(v).or_insert_with(Vec::new).push(node);
        }
        let groups = Arc::new(groups.into_iter().map(|(k, v)| (k, v.into())).collect());
        Self {
            groups,
            base_graph,
            graph,
        }
    }

    fn groups(&self) -> impl Iterator<Item = (&V, Nodes<G, GH>)> {
        self.groups.iter().map(|(v, nodes)| {
            (
                v,
                Nodes::new_filtered(
                    self.base_graph.clone(),
                    self.graph.clone(),
                    Some(nodes.clone()),
                    None,
                ),
            )
        })
    }
}
pub trait NodeStateGroupBy<'graph>: NodeStateOps<'graph> {
    fn groups(&self) -> NodeGroups<Self::OwnedValue, Self::BaseGraph, Self::Graph>;
}

impl<'graph, S: NodeStateOps<'graph>> NodeStateGroupBy<'graph> for S
where
    S::OwnedValue: Hash + Eq,
{
    fn groups(&self) -> NodeGroups<Self::OwnedValue, Self::BaseGraph, Self::Graph> {
        self.group_by(|v| v.clone())
    }
}
