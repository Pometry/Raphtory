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
    sync::Arc,
};

use super::{HasRow, SegmentContainer};
use crate::{
    LocalPOS,
    api::nodes::{LockedNSSegment, NodeSegmentOps},
    error::DBV4Error,
    segments::node_entry::{MemNodeEntry, MemNodeRef},
};

#[derive(Debug)]
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

#[derive(Debug, Default)]
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

    pub fn get_layer(&self, layer_id: usize) -> Option<&SegmentContainer<AdjEntry>> {
        self.layers.get(layer_id)
    }

    pub fn degree(&self, n: LocalPOS, layer_id: usize, dir: Direction) -> usize {
        self.get_adj(n, layer_id)
            .map_or(0, |adj| adj.degree(dir))
    }

    pub fn lsn(&self) -> u64 {
        self.layers.iter().map(|seg| seg.lsn()).min().unwrap_or(0)
    }

    pub fn est_size(&self) -> usize {
        self.layers.iter().map(|seg| seg.est_size()).sum::<usize>()
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
            .is_some_and(|layer| layer.items().get(n.0).map_or(false, |v| *v))
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
    ) -> bool {
        let dst = dst.into();
        let e_id = e_id.into();
        let layer_id = e_id.layer();
        let layer = self.get_or_create_layer(layer_id);
        layer.set_lsn(lsn);

        let add_out = layer.reserve_local_row(src_pos).map_either(
            |row| {
                row.adj.add_edge_out(dst, e_id.edge);
                row.row()
            },
            |row| {
                row.adj.add_edge_out(dst, e_id.edge);
                row.row()
            },
        );

        let new_entry = add_out.is_right();
        if let Some(t) = t {
            self.update_timestamp_inner(t, add_out.either(|a| a, |a| a), e_id);
        }
        new_entry
    }

    pub fn add_inbound_edge(
        &mut self,
        t: Option<impl AsTime>,
        dst_pos: impl Into<LocalPOS>,
        src: impl Into<VID>,
        e_id: impl Into<ELID>,
        lsn: u64,
    ) -> bool {
        let src = src.into();
        let e_id = e_id.into();
        let layer_id = e_id.layer();
        let dst_pos = dst_pos.into();

        let layer = self.get_or_create_layer(layer_id);
        layer.set_lsn(lsn);
        let add_in = layer.reserve_local_row(dst_pos).map_either(
            |row| {
                row.adj.add_edge_into(src, e_id.edge);
                row.row()
            },
            |row| {
                row.adj.add_edge_into(src, e_id.edge);
                row.row()
            },
        );
        let new_entry = add_in.is_right();
        if let Some(t) = t {
            self.update_timestamp_inner(t, add_in.either(|a| a, |a| a), e_id);
        }
        new_entry
    }

    fn update_timestamp_inner<T: AsTime>(&mut self, t: T, row: usize, e_id: ELID) {
        let mut prop_mut_entry = self.layers[e_id.layer()]
            .properties_mut()
            .get_mut_entry(row);
        let ts = TimeIndexEntry::new(t.t(), t.i());

        prop_mut_entry.append_edge_ts(ts, e_id);
    }

    pub fn update_timestamp<T: AsTime>(&mut self, t: T, node_pos: LocalPOS, e_id: ELID) {
        let row = self.layers[e_id.layer()]
            .reserve_local_row(node_pos)
            .either(|a| a.row, |a| a.row);
        self.update_timestamp_inner(t, row, e_id);
    }

    pub fn add_props<T: AsTime>(
        &mut self,
        t: T,
        node_pos: LocalPOS,
        layer_id: usize,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) {
        let layer = self.get_or_create_layer(layer_id);
        let row = layer
            .reserve_local_row(node_pos)
            .either(|a| a.row, |a| a.row);
        let mut prop_mut_entry = self.layers[layer_id].properties_mut().get_mut_entry(row);
        let ts = TimeIndexEntry::new(t.t(), t.i());
        prop_mut_entry.append_t_props(ts, props);
    }

    pub fn update_c_props(
        &mut self,
        node_pos: LocalPOS,
        layer_id: usize,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) {
        let row = self.layers[layer_id]
            .reserve_local_row(node_pos)
            .either(|a| a.row, |a| a.row);
        let mut prop_mut_entry = self.layers[layer_id].properties_mut().get_mut_entry(row);
        prop_mut_entry.append_const_props(props);
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

    pub fn node_ref(&self, pos: LocalPOS) -> MemNodeRef {
        MemNodeRef::new(pos, self)
    }
}

#[derive(Debug)]
pub struct NodeSegmentView<EXT = ()> {
    inner: Arc<parking_lot::RwLock<MemNodeSegment>>,
    segment_id: usize,
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

impl NodeSegmentOps for NodeSegmentView {
    type Extension = ();

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

    fn load(
        _page_id: usize,
        _max_page_len: usize,
        _meta: Arc<Meta>,
        _path: impl AsRef<std::path::Path>,
        _ext: Self::Extension,
    ) -> Result<Self, DBV4Error>
    where
        Self: Sized,
    {
        todo!()
    }

    fn new(
        page_id: usize,
        max_page_len: usize,
        meta: Arc<Meta>,
        _path: impl AsRef<std::path::Path>,
        _ext: Self::Extension,
    ) -> Self {
        Self {
            inner: parking_lot::RwLock::new(MemNodeSegment::new(page_id, max_page_len, meta))
                .into(),
            segment_id: page_id,
            _ext: (),
        }
    }

    fn segment_id(&self) -> usize {
        self.segment_id
    }

    fn head(&self) -> parking_lot::RwLockReadGuard<MemNodeSegment> {
        self.inner.read()
    }

    fn head_arc(&self) -> ArcRwLockReadGuard<parking_lot::RawRwLock, MemNodeSegment> {
        self.inner.read_arc_recursive()
    }

    fn head_mut(&self) -> parking_lot::RwLockWriteGuard<MemNodeSegment> {
        self.inner.write()
    }

    fn notify_write(
        &self,
        _head_lock: impl DerefMut<Target = MemNodeSegment>,
    ) -> Result<(), DBV4Error> {
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

    fn layer_num_nodes(&self, layer_id: usize) -> usize {
        self.head()
            .get_layer(layer_id)
            .map_or(0, |layer| layer.len())
    }
}
