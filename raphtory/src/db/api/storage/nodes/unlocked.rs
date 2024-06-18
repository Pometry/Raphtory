use ouroboros::self_referencing;
use raphtory_api::core::{
    entities::{edges::edge_ref::EdgeRef, VID},
    Direction,
};
use rayon::prelude::*;

use crate::{
    core::{
        entities::{graph::tgraph::InternalGraph, nodes::node_store::NodeStore, LayerIds},
        storage::{ArcEntry, Entry},
    },
    db::api::storage::tprop_storage_ops::TPropOps,
};

impl<'a> Entry<'a, NodeStore> {
    pub fn into_edges_iter(
        self,
        layers: &'a LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + 'a {
        LockedEdgesRefIterBuilder {
            entry: self,
            iter_builder: |node| Box::new(node.edge_tuples(layers, dir)),
        }
        .build()
    }
}

#[self_referencing]
pub struct LockedEdgesRefIter<'a> {
    entry: Entry<'a, NodeStore>,
    #[borrows(entry)]
    #[covariant]
    iter: Box<dyn Iterator<Item = EdgeRef> + Send + 'this>,
}

impl<'a> Iterator for LockedEdgesRefIter<'a> {
    type Item = EdgeRef;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_iter_mut(|iter| iter.next())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct UnlockedNodes<'a>(pub &'a InternalGraph);

impl<'a> UnlockedNodes<'a> {
    pub fn len(self) -> usize {
        self.0.inner().storage.nodes.len()
    }

    pub fn node(&self, vid: VID) -> Entry<'a, NodeStore> {
        self.0.inner().storage.nodes.entry(vid)
    }

    pub fn iter(self) -> impl Iterator<Item = Entry<'a, NodeStore>> + 'a {
        let storage = &self.0.inner().storage.nodes;
        (0..storage.len()).map(VID).map(|vid| storage.entry(vid))
    }

    pub fn par_iter(self) -> impl ParallelIterator<Item = Entry<'a, NodeStore>> + 'a {
        let storage = &self.0.inner().storage.nodes;
        (0..storage.len())
            .into_par_iter()
            .map(VID)
            .map(|vid| storage.entry(vid))
    }
}

#[derive(Debug, Clone)]
pub struct UnlockedOwnedNode {
    g: InternalGraph,
    vid: VID,
}

impl UnlockedOwnedNode {
    pub fn new(g: InternalGraph, vid: VID) -> Self {
        Self { g, vid }
    }

    pub fn arc_node(&self) -> ArcEntry<NodeStore> {
        self.g.inner().storage.nodes.entry_arc(self.vid)
    }

    pub fn into_edges_iter(
        self,
        layers: LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> {
        self.arc_node().into_edges(&layers, dir)
    }
}
