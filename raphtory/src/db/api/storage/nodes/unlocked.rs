use std::{borrow::Cow, ops::Deref};

use ouroboros::self_referencing;
use raphtory_api::core::{entities::{edges::edge_ref::EdgeRef, VID}, storage::timeindex::TimeIndexEntry, Direction};

use crate::{
    core::{
        entities::{nodes::node_store::NodeStore, properties::tprop::TProp, LayerIds},
        storage::{locked_view::LockedView, Entry},
    },
    db::api::{storage::tprop_storage_ops::TPropOps, view::internal::NodeAdditions}, prelude::Prop,
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
        LockedEdgesRefIterBuilder{
            entry: self,
            iter_builder: |node| { 
                Box::new(node.edge_tuples(layers, dir))
            },
        }.build()
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

    fn find_edge(
        self,
        dst: VID,
        layer_ids: &LayerIds,
    ) -> Option<EdgeRef> {
        self.deref().find_edge(dst, layer_ids)
    }
}

#[self_referencing]
pub struct LockedEdgesRefIter<'a> {
    entry: Entry<'a,NodeStore>,
    #[borrows(entry)]
    #[covariant]
    iter: Box<dyn Iterator<Item = EdgeRef> + Send + 'this>,
}

impl <'a> Iterator for LockedEdgesRefIter<'a> {
    type Item = EdgeRef;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_iter_mut(|iter| iter.next())
    }
}

impl <'a> TPropOps<'a> for LockedView<'a, TProp> {
    fn last_before(&self, t: i64) -> Option<(TimeIndexEntry, Prop)> {
        todo!()
    }

    fn iter(self) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        std::iter::empty()
    }

    fn iter_window(
        self,
        r: std::ops::Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        std::iter::empty()
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        todo!()
    }

    fn len(self) -> usize {
        todo!()
    }
}