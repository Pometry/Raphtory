use std::{borrow::Cow, ops::Deref};

use ouroboros::self_referencing;
use raphtory_api::core::{
    entities::{edges::edge_ref::EdgeRef, VID},
    storage::timeindex::TimeIndexEntry,
    Direction,
};
use rayon::prelude::*;

use crate::{
    core::{
        entities::{
            graph::tgraph::InternalGraph, nodes::node_store::NodeStore, properties::tprop::TProp,
            LayerIds,
        },
        storage::{locked_view::LockedView, ArcEntry, Entry},
    },
    db::api::{storage::tprop_storage_ops::TPropOps, view::internal::NodeAdditions},
    prelude::Prop,
};

use super::node_storage_ops::NodeStorageOps;

impl<'a> NodeStorageOps<'a> for Entry<'a, NodeStore> {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize {
        self.deref().degree(layers, dir)
    }

    fn additions(&self) -> NodeAdditions<'a> {
        todo!()
    }

    fn tprop(self, prop_id: usize) -> impl TPropOps<'a> {
        self.map(|e| e.temporal_property(prop_id).unwrap_or(&TProp::Empty))
    }

    fn edges_iter(
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

    fn node_type_id(&self) -> usize {
        self.deref().node_type
    }

    fn vid(&self) -> VID {
        self.deref().vid()
    }

    fn id(self) -> u64 {
        self.deref().id()
    }

    fn name(self) -> Option<Cow<'a, str>> {
        self.name.clone().map(Cow::Owned)
    }

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef> {
        self.deref().find_edge(dst, layer_ids)
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

impl<'a> TPropOps<'a> for LockedView<'a, TProp> {
    fn last_before(&self, t: i64) -> Option<(TimeIndexEntry, Prop)> {
        self.deref().last_before(t)
    }

    fn iter(self) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        LockedTIEPropIterBuilder {
            tprop: self,
            iter_builder: |tprop| Box::new(tprop.iter_inner()),
        }
        .build()
    }

    fn iter_window(
        self,
        r: std::ops::Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        LockedTIEPropIterBuilder {
            tprop: self,
            iter_builder: |tprop| Box::new(tprop.iter_window_inner(r)),
        }
        .build()
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        let deref = self.deref();
        deref.at(ti)
    }

    fn len(self) -> usize {
        self.deref().len()
    }
}

#[self_referencing]
pub struct LockedTIEPropIter<'a> {
    tprop: LockedView<'a, TProp>,
    #[borrows(tprop)]
    #[covariant]
    iter: Box<dyn Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'this>,
}

impl<'a> Iterator for LockedTIEPropIter<'a> {
    type Item = (TimeIndexEntry, Prop);

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
    pub fn node(&self) -> Entry<'_, NodeStore> {
        self.g.inner().storage.nodes.entry(self.vid)
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
