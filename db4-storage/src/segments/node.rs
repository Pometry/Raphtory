use either::Either;
use parking_lot::lock_api::ArcRwLockReadGuard;
use raphtory_api::core::{
    Direction,
    entities::{
        EID, VID,
        properties::{meta::Meta, prop::Prop},
    },
};
use raphtory_core::{
    entities::{ELID, nodes::structure::adj::Adj},
    storage::timeindex::{AsTime, TimeIndexEntry},
};
use std::{
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicUsize, Ordering},
    },
};

use super::{HasRow, SegmentContainer};
use crate::{
    LocalPOS,
    api::nodes::{LockedNSSegment, NodeSegmentOps},
    error::StorageError,
    persist::strategy::PersistentStrategy,
    segments::node_entry::{MemNodeEntry, MemNodeRef},
};

#[derive(Debug, serde::Serialize)]
pub struct MemNodeSegment {
    segment_id: usize,
    max_page_len: usize,
    layers: Vec<SegmentContainer<AdjEntry>>,
}

impl<I: IntoIterator<Item = SegmentContainer<AdjEntry>>> From<I> for MemNodeSegment {
    fn from(inner: I) -> Self {
        let layers = inner.into_iter().collect::<Vec<_>>();
        assert!(
            !layers.is_empty(),
            "MemNodeSegment must have at least one layer"
        );
        let segment_id = layers[0].segment_id();
        let max_page_len = layers[0].max_page_len();
        Self {
            segment_id,
            max_page_len,
            layers,
        }
    }
}

#[derive(Debug, Default, serde::Serialize)]
pub struct AdjEntry {
    row: usize,
    adj: Adj,
}

impl AdjEntry {
    pub fn degree(&self, d: Direction) -> usize {
        self.adj.degree(d)
    }

    pub fn edges(&self, d: Direction) -> impl Iterator<Item = (VID, EID)> + '_ {
        match d {
            Direction::IN => Either::Left(self.adj.inb_iter()),
            Direction::OUT => Either::Right(self.adj.out_iter()),
            Direction::BOTH => panic!("AdjEntry::edges: BOTH direction is not supported"),
        }
    }
}

impl HasRow for AdjEntry {
    fn row(&self) -> usize {
        self.row
    }

    fn row_mut(&mut self) -> &mut usize {
        &mut self.row
    }
}

impl AsRef<[SegmentContainer<AdjEntry>]> for MemNodeSegment {
    fn as_ref(&self) -> &[SegmentContainer<AdjEntry>] {
        &self.layers
    }
}

impl AsMut<[SegmentContainer<AdjEntry>]> for MemNodeSegment {
    fn as_mut(&mut self) -> &mut [SegmentContainer<AdjEntry>] {
        &mut self.layers
    }
}

impl MemNodeSegment {
    pub fn segment_id(&self) -> usize {
        self.segment_id
    }

    pub fn swap_out_layers(&mut self) -> Vec<SegmentContainer<AdjEntry>> {
        let layers = self
            .layers
            .iter_mut()
            .map(|head_guard| {
                let mut old_head = SegmentContainer::new(
                    head_guard.segment_id(),
                    head_guard.max_page_len(),
                    head_guard.meta().clone(),
                );
                std::mem::swap(&mut *head_guard, &mut old_head);
                old_head
            })
            .collect::<Vec<_>>();
        layers
    }

    pub fn get_or_create_layer(&mut self, layer_id: usize) -> &mut SegmentContainer<AdjEntry> {
        if layer_id >= self.layers.len() {
            let max_page_len = self.layers[0].max_page_len();
            let segment_id = self.layers[0].segment_id();
            let meta = self.layers[0].meta().clone();
            self.layers.resize_with(layer_id + 1, || {
                SegmentContainer::new(segment_id, max_page_len, meta.clone())
            });
        }
        &mut self.layers[layer_id]
    }

    pub fn node_meta(&self) -> &Arc<Meta> {
        self.layers[0].meta()
    }

    pub fn get_layer(&self, layer_id: usize) -> Option<&SegmentContainer<AdjEntry>> {
        self.layers.get(layer_id)
    }

    pub fn degree(&self, n: LocalPOS, layer_id: usize, dir: Direction) -> usize {
        self.get_adj(n, layer_id).map_or(0, |adj| adj.degree(dir))
    }

    pub fn lsn(&self) -> u64 {
        self.layers.iter().map(|seg| seg.lsn()).min().unwrap_or(0)
    }

    pub fn to_vid(&self, pos: LocalPOS) -> VID {
        pos.as_vid(self.segment_id, self.max_page_len)
    }

    #[inline(always)]
    fn get_adj(&self, n: LocalPOS, layer_id: usize) -> Option<&Adj> {
        self.layers
            .get(layer_id)?
            .get(&n)
            .map(|AdjEntry { adj, .. }| adj)
    }

    pub fn has_node(&self, n: LocalPOS, layer_id: usize) -> bool {
        self.layers
            .get(layer_id)
            .is_some_and(|layer| layer.items().get(n.0).is_some_and(|x| *x))
    }

    pub fn get_out_edge(&self, n: LocalPOS, dst: VID, layer_id: usize) -> Option<EID> {
        self.get_adj(n, layer_id)
            .and_then(|adj| adj.get_edge(dst, Direction::OUT))
    }

    pub fn get_inb_edge(&self, n: LocalPOS, src: VID, layer_id: usize) -> Option<EID> {
        self.get_adj(n, layer_id)
            .and_then(|adj| adj.get_edge(src, Direction::IN))
    }

    pub fn out_edges(&self, n: LocalPOS, layer_id: usize) -> impl Iterator<Item = (VID, EID)> + '_ {
        self.get_adj(n, layer_id)
            .into_iter()
            .flat_map(|adj| adj.out_iter())
    }

    pub fn inb_edges(&self, n: LocalPOS, layer_id: usize) -> impl Iterator<Item = (VID, EID)> + '_ {
        self.get_adj(n, layer_id)
            .into_iter()
            .flat_map(|adj| adj.inb_iter())
    }

    pub fn new(segment_id: usize, max_page_len: usize, meta: Arc<Meta>) -> Self {
        Self {
            segment_id,
            max_page_len,
            layers: vec![SegmentContainer::new(segment_id, max_page_len, meta)],
        }
    }

    pub fn add_outbound_edge<T: AsTime>(
        &mut self,
        t: Option<T>,
        src_pos: LocalPOS,
        dst: impl Into<VID>,
        e_id: impl Into<ELID>,
        lsn: u64,
    ) -> (bool, usize) {
        let dst = dst.into();
        let e_id = e_id.into();
        let layer_id = e_id.layer();
        let layer = self.get_or_create_layer(layer_id);
        let est_size = layer.est_size();
        layer.set_lsn(lsn);

        let add_out = layer.reserve_local_row(src_pos).map_either(
            |row| {
                let new_mem_edge = row.adj.add_edge_out(dst, e_id.edge);
                (new_mem_edge, row.row())
            },
            |row| {
                row.adj.add_edge_out(dst, e_id.edge);
                (true, row.row())
            },
        );

        let is_new_edge = add_out.as_ref().either(
            |(is_new, _)| *is_new as usize,
            |(is_new, _)| *is_new as usize,
        );

        let new_entry = add_out.is_right();
        if let Some(t) = t {
            self.update_timestamp_inner(t, add_out.either(|(_, a)| a, |(_, a)| a), e_id);
        }
        let layer_est_size = self.layers[layer_id].est_size();
        let added_size =
            (layer_est_size - est_size) + (is_new_edge * std::mem::size_of::<(VID, VID)>());
        (new_entry, added_size)
    }

    pub fn add_inbound_edge(
        &mut self,
        t: Option<impl AsTime>,
        dst_pos: impl Into<LocalPOS>,
        src: impl Into<VID>,
        e_id: impl Into<ELID>,
        lsn: u64,
    ) -> (bool, usize) {
        let src = src.into();
        let e_id = e_id.into();
        let layer_id = e_id.layer();
        let dst_pos = dst_pos.into();

        let layer = self.get_or_create_layer(layer_id);
        let est_size = layer.est_size();
        layer.set_lsn(lsn);
        let add_in = layer.reserve_local_row(dst_pos).map_either(
            |row| {
                let new_mem_edge = row.adj.add_edge_into(src, e_id.edge);
                (new_mem_edge, row.row())
            },
            |row| {
                row.adj.add_edge_into(src, e_id.edge);
                (true, row.row())
            },
        );
        let is_new_edge = add_in.as_ref().either(
            |(is_new, _)| *is_new as usize,
            |(is_new, _)| *is_new as usize,
        );

        let new_entry = add_in.is_right();
        if let Some(t) = t {
            self.update_timestamp_inner(t, add_in.either(|(_, a)| a, |(_, a)| a), e_id);
        }
        let layer_est_size = self.layers[layer_id].est_size();
        let added_size =
            (layer_est_size - est_size) + (is_new_edge * std::mem::size_of::<(VID, VID)>());
        (new_entry, added_size)
    }

    fn update_timestamp_inner<T: AsTime>(&mut self, t: T, row: usize, e_id: ELID) {
        let mut prop_mut_entry = self.layers[e_id.layer()]
            .properties_mut()
            .get_mut_entry(row);
        let ts = TimeIndexEntry::new(t.t(), t.i());

        prop_mut_entry.addition_timestamp(ts, e_id);
    }

    pub fn update_timestamp<T: AsTime>(&mut self, t: T, node_pos: LocalPOS, e_id: ELID) -> usize {
        let (row, est_size) = {
            let segment_container = &mut self.layers[e_id.layer()];
            let est_size = segment_container.est_size();
            let row = segment_container
                .reserve_local_row(node_pos)
                .either(|a| a.row, |a| a.row);
            (row, est_size)
        };
        self.update_timestamp_inner(t, row, e_id);
        let layer_est_size = self.layers[e_id.layer()].est_size();
        let added_size = layer_est_size - est_size;
        added_size
    }

    pub fn add_props<T: AsTime>(
        &mut self,
        t: T,
        node_pos: LocalPOS,
        layer_id: usize,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) -> (bool, usize) {
        let layer = self.get_or_create_layer(layer_id);
        let est_size = layer.est_size();
        let row = layer.reserve_local_row(node_pos);
        let is_new = row.is_right();
        let row = row.either(|a| a.row, |a| a.row);
        let mut prop_mut_entry = layer.properties_mut().get_mut_entry(row);
        let ts = TimeIndexEntry::new(t.t(), t.i());
        prop_mut_entry.append_t_props(ts, props);
        let layer_est_size = layer.est_size();
        (is_new, layer_est_size - est_size)
    }

    pub fn check_metadata(
        &self,
        node_pos: LocalPOS,
        layer_id: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), StorageError> {
        if let Some(layer) = self.layers.get(layer_id) {
            layer.check_metadata(node_pos, props)?;
        }
        Ok(())
    }

    pub fn update_c_props(
        &mut self,
        node_pos: LocalPOS,
        layer_id: usize,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) -> (bool, usize) {
        let segment_container = &mut self.layers[layer_id];
        let est_size = segment_container.est_size();

        let row = segment_container
            .reserve_local_row(node_pos)
            .map_either(|a| a.row, |a| a.row);
        let is_new = row.is_right();
        let row = row.either(|a| a, |a| a);
        let mut prop_mut_entry = segment_container.properties_mut().get_mut_entry(row);
        prop_mut_entry.append_const_props(props);

        let layer_est_size = segment_container.est_size();
        let added_size = (layer_est_size - est_size) + 8; // random estimate for constant properties
        (is_new, added_size)
    }

    pub fn get_metadata(
        &self,
        node_pos: LocalPOS,
        layer_id: usize,
        prop_id: usize,
    ) -> Option<Prop> {
        let segment_container = &self.layers[layer_id];
        segment_container.c_prop(node_pos, prop_id)
    }

    pub fn latest(&self) -> Option<TimeIndexEntry> {
        Iterator::max(self.layers.iter().filter_map(|seg| seg.latest()))
    }

    pub fn earliest(&self) -> Option<TimeIndexEntry> {
        Iterator::min(self.layers.iter().filter_map(|seg| seg.earliest()))
    }

    pub fn t_len(&self) -> usize {
        self.layers.iter().map(|seg| seg.t_len()).sum()
    }

    pub fn node_ref(&self, pos: LocalPOS) -> MemNodeRef<'_> {
        MemNodeRef::new(pos, self)
    }
}

#[derive(Debug)]
pub struct NodeSegmentView<EXT> {
    inner: Arc<parking_lot::RwLock<MemNodeSegment>>,
    segment_id: usize,
    event_id: AtomicI64,
    est_size: AtomicUsize,
    _ext: EXT,
}

#[derive(Debug)]
pub struct ArcLockedSegmentView {
    inner: ArcRwLockReadGuard<parking_lot::RawRwLock, MemNodeSegment>,
}

impl LockedNSSegment for ArcLockedSegmentView {
    type EntryRef<'a> = MemNodeRef<'a>;

    fn entry_ref<'a>(&'a self, pos: impl Into<LocalPOS>) -> Self::EntryRef<'a> {
        let pos = pos.into();
        MemNodeRef::new(pos, &self.inner)
    }
}

impl<P: PersistentStrategy<NS = NodeSegmentView<P>>> NodeSegmentOps for NodeSegmentView<P> {
    type Extension = P;

    type Entry<'a> = MemNodeEntry<'a, parking_lot::RwLockReadGuard<'a, MemNodeSegment>>;

    type ArcLockedSegment = ArcLockedSegmentView;

    fn latest(&self) -> Option<TimeIndexEntry> {
        self.head().latest()
    }

    fn earliest(&self) -> Option<TimeIndexEntry> {
        self.head().latest()
    }

    fn t_len(&self) -> usize {
        self.head().t_len()
    }

    fn event_id(&self) -> i64 {
        self.event_id.load(Ordering::Relaxed)
    }

    fn increment_event_id(&self, i: i64) {
        self.event_id.fetch_add(i, Ordering::Relaxed);
    }

    fn decrement_event_id(&self) -> i64 {
        self.event_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |x| {
                if x > 0 { Some(x - 1) } else { None }
            })
            .unwrap_or_default()
    }

    fn load(
        _page_id: usize,
        _max_page_len: usize,
        _node_meta: Arc<Meta>,
        _edge_meta: Arc<Meta>,
        _path: impl AsRef<std::path::Path>,
        _ext: Self::Extension,
    ) -> Result<Self, StorageError>
    where
        Self: Sized,
    {
        todo!()
    }

    fn new(
        page_id: usize,
        max_page_len: usize,
        meta: Arc<Meta>,
        _edge_meta: Arc<Meta>,
        _path: impl AsRef<std::path::Path>,
        _ext: Self::Extension,
    ) -> Self {
        Self {
            inner: parking_lot::RwLock::new(MemNodeSegment::new(page_id, max_page_len, meta))
                .into(),
            segment_id: page_id,
            _ext,
            event_id: Default::default(),
            est_size: AtomicUsize::new(0),
        }
    }

    fn segment_id(&self) -> usize {
        self.segment_id
    }

    fn head(&self) -> parking_lot::RwLockReadGuard<'_, MemNodeSegment> {
        self.inner.read()
    }

    fn head_arc(&self) -> ArcRwLockReadGuard<parking_lot::RawRwLock, MemNodeSegment> {
        self.inner.read_arc_recursive()
    }

    fn head_mut(&self) -> parking_lot::RwLockWriteGuard<'_, MemNodeSegment> {
        self.inner.write()
    }

    fn notify_write(
        &self,
        _head_lock: impl DerefMut<Target = MemNodeSegment>,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    fn check_node(&self, _pos: LocalPOS, _layer_id: usize) -> bool {
        false
    }

    fn get_out_edge(
        &self,
        pos: LocalPOS,
        dst: impl Into<VID>,
        layer_id: usize,
        locked_head: impl Deref<Target = MemNodeSegment>,
    ) -> Option<EID> {
        locked_head.get_out_edge(pos, dst.into(), layer_id)
    }

    fn get_inb_edge(
        &self,
        pos: LocalPOS,
        src: impl Into<VID>,
        layer_id: usize,
        locked_head: impl Deref<Target = MemNodeSegment>,
    ) -> Option<EID> {
        locked_head.get_inb_edge(pos, src.into(), layer_id)
    }

    fn entry<'a>(&'a self, pos: impl Into<LocalPOS>) -> Self::Entry<'a> {
        let pos = pos.into();
        MemNodeEntry::new(pos, self.head())
    }

    fn locked(self: &Arc<Self>) -> Self::ArcLockedSegment {
        ArcLockedSegmentView {
            inner: self.inner.read_arc(),
        }
    }

    fn num_layers(&self) -> usize {
        self.head().layers.len()
    }

    fn layer_count(&self, layer_id: usize) -> usize {
        self.head()
            .get_layer(layer_id)
            .map_or(0, |layer| layer.len())
    }

    fn flush(&self) {}

    fn est_size(&self) -> usize {
        self.est_size.load(Ordering::Relaxed)
    }

    fn increment_est_size(&self, size: usize) -> usize {
        self.est_size.fetch_add(size, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod test {
    use std::{ops::Deref, sync::Arc};

    use raphtory_api::core::entities::properties::{
        meta::Meta,
        prop::{Prop, PropType},
    };
    use raphtory_core::entities::{EID, ELID, VID};
    use tempfile::tempdir;

    use crate::{
        LocalPOS,
        api::nodes::NodeSegmentOps,
        pages::{layer_counter::GraphStats, node_page::writer::NodeWriter},
        segments::node::NodeSegmentView,
    };

    #[test]
    fn est_size_changes() {
        let node_meta = Arc::new(Meta::default());
        let edge_meta = Arc::new(Meta::default());
        let path = tempdir().unwrap();
        let ext = ();
        let segment = NodeSegmentView::new(0, 10, node_meta.clone(), edge_meta, path.path(), ext);
        let stats = GraphStats::default();

        let mut writer = NodeWriter::new(&segment, &stats, segment.head_mut());

        let est_size1 = segment.est_size();
        assert_eq!(est_size1, 0);

        writer.add_outbound_edge(Some(1), LocalPOS(1), VID(3), EID(7).with_layer(0), 0);

        let est_size2 = segment.est_size();
        assert!(
            est_size2 > est_size1,
            "Estimated size should be greater than 0 after adding an edge"
        );

        writer.add_inbound_edge(Some(1), LocalPOS(2), VID(4), EID(8).with_layer(0), 0);

        let est_size3 = segment.est_size();
        assert!(
            est_size3 > est_size2,
            "Estimated size should increase after adding an inbound edge"
        );

        // no change when adding the same edge again

        writer.add_outbound_edge::<i64>(None, LocalPOS(1), VID(3), EID(7).with_layer(0), 0);
        let est_size4 = segment.est_size();
        assert_eq!(
            est_size4, est_size3,
            "Estimated size should not change when adding the same edge again"
        );

        // add constant properties

        let prop_id = node_meta
            .metadata_mapper()
            .get_or_create_and_validate("a", PropType::U64)
            .unwrap()
            .inner();

        writer.update_c_props(LocalPOS(1), 0, [(prop_id, Prop::U64(73))], 0);

        let est_size5 = segment.est_size();
        assert!(
            est_size5 > est_size4,
            "Estimated size should increase after adding constant properties"
        );

        writer.update_timestamp(17, LocalPOS(1), ELID::new(EID(0), 0), 0);

        let est_size6 = segment.est_size();
        assert!(
            est_size6 > est_size5,
            "Estimated size should increase after updating timestamp"
        );

        // add temporal properties
        let prop_id = node_meta
            .temporal_prop_mapper()
            .get_or_create_and_validate("b", PropType::F64)
            .unwrap()
            .inner();

        writer.add_props(42, LocalPOS(1), 0, [(prop_id, Prop::F64(4.13))], 0);

        let est_size7 = segment.est_size();
        assert!(
            est_size7 > est_size6,
            "Estimated size should increase after adding temporal properties"
        );

        writer.add_props(72, LocalPOS(1), 0, [(prop_id, Prop::F64(5.41))], 0);
        let est_size8 = segment.est_size();
        assert!(
            est_size8 > est_size7,
            "Estimated size should increase after adding another temporal property"
        );

        let actual_size = bincode::serialize(writer.mut_segment.deref())
            .unwrap()
            .len();
        println!("{actual_size} vs {est_size7}");
    }
}
