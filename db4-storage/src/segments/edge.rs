use std::{
    borrow::Borrow,
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{self, AtomicUsize},
    },
};

use crate::{
    LocalPOS,
    api::edges::{EdgeSegmentOps, LockedESegment},
    error::StorageError,
    persist::strategy::PersistentStrategy,
    properties::PropMutEntry,
    segments::edge_entry::MemEdgeRef,
    utils::Iter4,
};
use parking_lot::lock_api::ArcRwLockReadGuard;
use raphtory_api::core::entities::{
    VID,
    properties::{meta::Meta, prop::Prop},
};
use raphtory_core::{
    entities::{LayerIds, properties::props::MetadataError},
    storage::timeindex::{AsTime, TimeIndexEntry},
};
use rayon::prelude::*;

use super::{HasRow, SegmentContainer, edge_entry::MemEdgeEntry};

#[derive(Debug, Default)]
pub struct MemPageEntry {
    pub src: VID,
    pub dst: VID,
    pub row: usize,
}

impl HasRow for MemPageEntry {
    fn row(&self) -> usize {
        self.row
    }

    fn row_mut(&mut self) -> &mut usize {
        &mut self.row
    }
}

#[derive(Debug)]
pub struct MemEdgeSegment {
    layers: Vec<SegmentContainer<MemPageEntry>>,
    est_size: usize,
}

impl<I: IntoIterator<Item = SegmentContainer<MemPageEntry>>> From<I> for MemEdgeSegment {
    fn from(inner: I) -> Self {
        let layers: Vec<_> = inner.into_iter().collect();
        let est_size = layers.iter().map(|seg| seg.est_size()).sum();
        assert!(
            !layers.is_empty(),
            "MemEdgeSegment must have at least one layer"
        );
        Self { layers, est_size }
    }
}

impl AsRef<[SegmentContainer<MemPageEntry>]> for MemEdgeSegment {
    fn as_ref(&self) -> &[SegmentContainer<MemPageEntry>] {
        &self.layers
    }
}

impl AsMut<[SegmentContainer<MemPageEntry>]> for MemEdgeSegment {
    fn as_mut(&mut self) -> &mut [SegmentContainer<MemPageEntry>] {
        &mut self.layers
    }
}

impl MemEdgeSegment {
    pub fn new(segment_id: usize, max_page_len: usize, meta: Arc<Meta>) -> Self {
        Self {
            layers: vec![SegmentContainer::new(segment_id, max_page_len, meta)],
            est_size: 0,
        }
    }

    pub fn edge_meta(&self) -> &Arc<Meta> {
        self.layers[0].meta()
    }

    pub fn swap_out_layers(&mut self) -> Vec<SegmentContainer<MemPageEntry>> {
        let layers = self
            .as_mut()
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
        self.est_size = 0; // Reset estimated size after swapping out layers
        layers
    }

    pub fn get_or_create_layer(&mut self, layer_id: usize) -> &mut SegmentContainer<MemPageEntry> {
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

    pub fn get_layer(&self, layer_id: usize) -> Option<&SegmentContainer<MemPageEntry>> {
        self.layers.get(layer_id)
    }

    pub fn est_size(&self) -> usize {
        self.est_size
    }

    pub fn lsn(&self) -> u64 {
        self.layers.iter().map(|seg| seg.lsn()).min().unwrap_or(0)
    }

    pub fn max_page_len(&self) -> usize {
        self.layers[0].max_page_len()
    }

    pub fn get_edge(&self, edge_pos: impl Into<LocalPOS>, layer_id: usize) -> Option<(VID, VID)> {
        let edge_pos = edge_pos.into();
        self.layers
            .get(layer_id)?
            .get(&edge_pos)
            .map(|entry| (entry.src, entry.dst))
    }

    pub fn insert_edge_internal<T: AsTime>(
        &mut self,
        t: T,
        edge_pos: impl Into<LocalPOS>,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        layer_id: usize,
        props: impl IntoIterator<Item = (usize, Prop)>,
        lsn: u64,
    ) {
        let edge_pos = edge_pos.into();
        let src = src.into();
        let dst = dst.into();

        // Ensure we have enough layers
        self.ensure_layer(layer_id);
        let est_size = self.layers[layer_id].est_size();
        self.layers[layer_id].set_lsn(lsn);

        let local_row = self.reserve_local_row(edge_pos, src, dst, layer_id);

        let mut prop_entry: PropMutEntry<'_> = self.layers[layer_id]
            .properties_mut()
            .get_mut_entry(local_row);
        let ts = TimeIndexEntry::new(t.t(), t.i());
        prop_entry.append_t_props(ts, props);
        let layer_est_size = self.layers[layer_id].est_size();
        self.est_size += layer_est_size.saturating_sub(est_size);
    }

    pub fn delete_edge_internal<T: AsTime>(
        &mut self,
        t: T,
        edge_pos: impl Into<LocalPOS>,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        layer_id: usize,
        lsn: u64,
    ) {
        let edge_pos = edge_pos.into();
        let src = src.into();
        let dst = dst.into();
        let t = TimeIndexEntry::new(t.t(), t.i());

        // Ensure we have enough layers
        self.ensure_layer(layer_id);
        let est_size = self.layers[layer_id].est_size();
        self.layers[layer_id].set_lsn(lsn);

        let local_row = self.reserve_local_row(edge_pos, src, dst, layer_id);
        let props = self.layers[layer_id].properties_mut();
        props.get_mut_entry(local_row).deletion_timestamp(t, None);
        let layer_est_size = self.layers[layer_id].est_size();
        self.est_size += layer_est_size.saturating_sub(est_size);
    }

    pub fn insert_static_edge_internal(
        &mut self,
        edge_pos: LocalPOS,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        layer_id: usize,
        lsn: u64,
    ) {
        let src = src.into();
        let dst = dst.into();

        // Ensure we have enough layers
        self.ensure_layer(layer_id);
        self.layers[layer_id].set_lsn(lsn);
        let est_size = self.layers[layer_id].est_size();

        self.reserve_local_row(edge_pos, src, dst, layer_id);
        let layer_est_size = self.layers[layer_id].est_size();
        self.est_size += layer_est_size.saturating_sub(est_size);
    }

    fn ensure_layer(&mut self, layer_id: usize) {
        if layer_id >= self.layers.len() {
            // Get details from first layer to create consistent new layers
            if let Some(first_layer) = self.layers.first() {
                let segment_id = first_layer.segment_id();
                let max_page_len = first_layer.max_page_len();
                let meta = first_layer.meta().clone();

                // Extend with new layers
                while self.layers.len() <= layer_id {
                    self.layers.push(SegmentContainer::new(
                        segment_id,
                        max_page_len,
                        meta.clone(),
                    ));
                }
            }
        }
    }

    fn reserve_local_row(
        &mut self,
        edge_pos: LocalPOS,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        layer_id: usize,
    ) -> usize {
        let src = src.into();
        let dst = dst.into();

        // Ensure we have enough layers
        self.ensure_layer(layer_id);

        let row = self.layers[layer_id]
            .reserve_local_row(edge_pos)
            .map_either(
                |row| {
                    row.src = src;
                    row.dst = dst;
                    row.row()
                },
                |row| {
                    row.src = src;
                    row.dst = dst;
                    row.row()
                },
            );
        row.either(|a| a, |a| a)
    }

    pub fn check_const_properties(
        &self,
        edge_pos: LocalPOS,
        layer_id: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), StorageError> {
        if let Some(layer) = self.layers.get(layer_id) {
            layer.check_metadata(edge_pos, props)?;
        }
        Ok(())
    }

    pub fn update_const_properties(
        &mut self,
        edge_pos: impl Into<LocalPOS>,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        layer_id: usize,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) {
        let edge_pos = edge_pos.into();
        let src = src.into();
        let dst = dst.into();

        // Ensure we have enough layers
        self.ensure_layer(layer_id);
        let est_size = self.layers[layer_id].est_size();
        let local_row = self.reserve_local_row(edge_pos, src, dst, layer_id);
        let mut prop_entry: PropMutEntry<'_> = self.layers[layer_id]
            .properties_mut()
            .get_mut_entry(local_row);
        prop_entry.append_const_props(props);

        let layer_est_size = self.layers[layer_id].est_size() + 8;
        self.est_size += layer_est_size.saturating_sub(est_size);
    }

    pub fn contains_edge(&self, edge_pos: LocalPOS, layer_id: usize) -> bool {
        self.layers
            .get(layer_id)
            .and_then(|layer| layer.items().get::<usize>(edge_pos.0))
            .map(|b| *b)
            .unwrap_or_default()
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
}

// Update EdgeSegmentView implementation to use multiple layers
#[derive(Debug)]
pub struct EdgeSegmentView<EXT> {
    segment: Arc<parking_lot::RwLock<MemEdgeSegment>>,
    segment_id: usize,
    num_edges: AtomicUsize,
    _ext: EXT,
}

#[derive(Debug)]
pub struct ArcLockedSegmentView {
    inner: ArcRwLockReadGuard<parking_lot::RawRwLock, MemEdgeSegment>,
}

impl ArcLockedSegmentView {
    fn edge_iter_layer<'a>(
        &'a self,
        layer_id: usize,
    ) -> impl Iterator<Item = MemEdgeRef<'a>> + Send + Sync + 'a {
        self.inner
            .layers
            .get(layer_id)
            .into_iter()
            .flat_map(|layer| layer.items().iter_ones())
            .map(move |pos| MemEdgeRef::new(LocalPOS(pos), &self.inner))
    }

    fn edge_par_iter_layer<'a>(
        &'a self,
        layer_id: usize,
    ) -> impl ParallelIterator<Item = MemEdgeRef<'a>> + Send + Sync + 'a {
        self.inner
            .layers
            .get(layer_id)
            .into_par_iter()
            .flat_map(|layer| layer.items().iter_ones().par_bridge())
            .map(move |pos| MemEdgeRef::new(LocalPOS(pos), &self.inner))
    }
}

impl LockedESegment for ArcLockedSegmentView {
    type EntryRef<'a> = MemEdgeRef<'a>;

    fn entry_ref<'a>(&'a self, edge_pos: impl Into<LocalPOS>) -> Self::EntryRef<'a>
    where
        Self: 'a,
    {
        let edge_pos = edge_pos.into();
        MemEdgeRef::new(edge_pos, &self.inner)
    }

    fn edge_iter<'a, 'b: 'a>(
        &'a self,
        layer_ids: &'b LayerIds,
    ) -> impl Iterator<Item = Self::EntryRef<'a>> + Send + Sync + 'a {
        match layer_ids {
            LayerIds::None => Iter4::I(std::iter::empty()),
            LayerIds::All => Iter4::J(self.edge_iter_layer(0)),
            LayerIds::One(layer_id) => Iter4::K(self.edge_iter_layer(*layer_id)),
            LayerIds::Multiple(multiple) => Iter4::L(
                self.edge_iter_layer(0)
                    .filter(|pos| pos.has_layers(multiple)),
            ),
        }
    }

    fn edge_par_iter<'a, 'b: 'a>(
        &'a self,
        layer_ids: &'b LayerIds,
    ) -> impl ParallelIterator<Item = Self::EntryRef<'a>> + Send + Sync + 'a {
        match layer_ids {
            LayerIds::None => Iter4::I(rayon::iter::empty()),
            LayerIds::All => Iter4::J(self.edge_par_iter_layer(0)),
            LayerIds::One(layer_id) => Iter4::K(self.edge_par_iter_layer(*layer_id)),
            LayerIds::Multiple(multiple) => Iter4::L(
                self.edge_par_iter_layer(0)
                    .filter(|pos| pos.has_layers(multiple)),
            ),
        }
    }
}

impl<P: PersistentStrategy<ES = EdgeSegmentView<P>>> EdgeSegmentOps for EdgeSegmentView<P> {
    type Extension = P;

    type Entry<'a> = MemEdgeEntry<'a, parking_lot::RwLockReadGuard<'a, MemEdgeSegment>>;

    type ArcLockedSegment = ArcLockedSegmentView;

    fn latest(&self) -> Option<TimeIndexEntry> {
        self.head().latest()
    }

    fn earliest(&self) -> Option<TimeIndexEntry> {
        self.head().earliest()
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
        _path: impl AsRef<std::path::Path>,
        _ext: Self::Extension,
    ) -> Self {
        Self {
            segment: parking_lot::RwLock::new(MemEdgeSegment::new(page_id, max_page_len, meta))
                .into(),
            segment_id: page_id,
            num_edges: AtomicUsize::new(0),
            _ext,
        }
    }

    fn segment_id(&self) -> usize {
        self.segment_id
    }

    fn num_edges(&self) -> usize {
        self.num_edges.load(atomic::Ordering::Relaxed)
    }

    fn head(&self) -> parking_lot::RwLockReadGuard<'_, MemEdgeSegment> {
        self.segment.read_recursive()
    }

    fn head_arc(&self) -> ArcRwLockReadGuard<parking_lot::RawRwLock, MemEdgeSegment> {
        self.segment.read_arc_recursive()
    }

    fn head_mut(&self) -> parking_lot::RwLockWriteGuard<'_, MemEdgeSegment> {
        self.segment.write()
    }

    fn try_head_mut(&self) -> Option<parking_lot::RwLockWriteGuard<'_, MemEdgeSegment>> {
        self.segment.try_write()
    }

    fn notify_write(
        &self,
        _head_lock: impl DerefMut<Target = MemEdgeSegment>,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    fn increment_num_edges(&self) -> usize {
        self.num_edges.fetch_add(1, atomic::Ordering::Relaxed)
    }

    fn contains_edge(
        &self,
        edge_pos: LocalPOS,
        layer_id: usize,
        locked_head: impl Deref<Target = MemEdgeSegment>,
    ) -> bool {
        locked_head.contains_edge(edge_pos, layer_id)
    }

    fn get_edge(
        &self,
        edge_pos: LocalPOS,
        layer_id: usize,
        locked_head: impl Deref<Target = MemEdgeSegment>,
    ) -> Option<(VID, VID)> {
        locked_head.get_edge(edge_pos, layer_id)
    }

    fn entry<'a, LP: Into<LocalPOS>>(&'a self, edge_pos: LP) -> Self::Entry<'a> {
        let edge_pos = edge_pos.into();
        MemEdgeEntry::new(edge_pos, self.head())
    }

    fn layer_entry<'a, LP: Into<LocalPOS>>(
        &'a self,
        edge_pos: LP,
        layer_id: usize,
        locked_head: Option<parking_lot::RwLockReadGuard<'a, MemEdgeSegment>>,
    ) -> Option<Self::Entry<'a>> {
        let edge_pos = edge_pos.into();
        locked_head.and_then(|locked_head| {
            let layer = locked_head.as_ref().get(layer_id)?;
            let has_edge = layer.items().get(edge_pos.0).is_some_and(|item| *item);
            has_edge.then(|| MemEdgeEntry::new(edge_pos, locked_head))
        })
    }

    fn locked(self: &Arc<Self>) -> Self::ArcLockedSegment {
        ArcLockedSegmentView {
            inner: self.head_arc(),
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
}

#[cfg(test)]
mod test {
    use raphtory_api::core::entities::properties::prop::PropType;

    #[test]
    fn est_size_changes() {
        use super::*;
        use raphtory_api::core::entities::properties::meta::Meta;

        let meta = Arc::new(Meta::default());
        let mut segment = MemEdgeSegment::new(1, 100, meta.clone());

        assert_eq!(segment.est_size(), 0);

        segment.insert_edge_internal(
            TimeIndexEntry::new(1, 0),
            LocalPOS(0),
            1,
            2,
            0,
            vec![(0, Prop::from("test"))],
            1,
        );

        let est_size1 = segment.est_size();

        assert!(est_size1 > 0);

        segment.delete_edge_internal(TimeIndexEntry::new(2, 3), LocalPOS(0), 5, 3, 0, 0);

        let est_size2 = segment.est_size();

        assert!(
            est_size2 > est_size1,
            "Expected size to increase after deletion, but it did not."
        );

        // same edge insertion again to check size increase
        segment.insert_edge_internal(
            TimeIndexEntry::new(3, 0),
            LocalPOS(1),
            4,
            6,
            0,
            vec![(0, Prop::from("test2"))],
            1,
        );

        let est_size3 = segment.est_size();
        assert!(
            est_size3 > est_size2,
            "Expected size to increase after re-insertion, but it did not."
        );

        // Insert a static edge

        segment.insert_static_edge_internal(LocalPOS(1), 4, 6, 0, 1);

        let est_size4 = segment.est_size();
        assert_eq!(
            est_size4, est_size3,
            "Expected size to remain the same after static edge insertion, but it changed."
        );

        let prop_id = meta
            .metadata_mapper()
            .get_or_create_and_validate("a", PropType::U8)
            .unwrap()
            .inner();

        segment.update_const_properties(LocalPOS(1), 4, 6, 0, [(prop_id, Prop::U8(2))]);

        let est_size5 = segment.est_size();
        assert!(
            est_size5 > est_size4,
            "Expected size to increase after updating properties, but it did not."
        );

        // update const properties for the other edge, hard to predict size change
        // segment.update_const_properties(LocalPOS(0), 1, 2, 0, [(prop_id, Prop::U8(3))]);

        // let est_size6 = segment.est_size();
        // assert!(
        //     est_size6 > est_size5,
        //     "Expected size to increase after updating properties for the other edge, but it did not."
        // );
    }
}
