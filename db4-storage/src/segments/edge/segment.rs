use crate::{
    LocalPOS,
    api::edges::{EdgeSegmentOps, LockedESegment},
    error::StorageError,
    persist::strategy::PersistenceStrategy,
    properties::PropMutEntry,
    segments::{
        HasRow, SegmentContainer,
        edge::entry::{MemEdgeEntry, MemEdgeRef},
    },
    utils::Iter4,
    wal::LSN,
};
use parking_lot::lock_api::ArcRwLockReadGuard;
use raphtory_api::core::entities::{
    VID,
    properties::{meta::Meta, prop::Prop},
};
use raphtory_api_macros::box_on_debug_lifetime;
use raphtory_core::{
    entities::LayerIds,
    storage::timeindex::{AsTime, TimeIndexEntry},
};
use rayon::prelude::*;
use std::{
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{self, AtomicU32},
    },
};

#[derive(Debug, Default)]
pub struct EdgeEntry {
    pub src: VID,
    pub dst: VID,
    pub row: usize,
}

impl HasRow for EdgeEntry {
    fn row(&self) -> usize {
        self.row
    }

    fn row_mut(&mut self) -> &mut usize {
        &mut self.row
    }
}

#[derive(Debug)]
pub struct MemEdgeSegment {
    layers: Vec<SegmentContainer<EdgeEntry>>,
    est_size: usize,
    lsn: LSN,
}

impl<I: IntoIterator<Item = SegmentContainer<EdgeEntry>>> From<I> for MemEdgeSegment {
    fn from(inner: I) -> Self {
        let layers: Vec<_> = inner.into_iter().collect();
        let est_size = layers.iter().map(|seg| seg.est_size()).sum();
        assert!(
            !layers.is_empty(),
            "MemEdgeSegment must have at least one layer"
        );
        Self {
            layers,
            est_size,
            lsn: 0,
        }
    }
}

impl AsRef<[SegmentContainer<EdgeEntry>]> for MemEdgeSegment {
    fn as_ref(&self) -> &[SegmentContainer<EdgeEntry>] {
        &self.layers
    }
}

impl AsMut<[SegmentContainer<EdgeEntry>]> for MemEdgeSegment {
    fn as_mut(&mut self) -> &mut [SegmentContainer<EdgeEntry>] {
        &mut self.layers
    }
}

impl MemEdgeSegment {
    pub fn new(segment_id: usize, max_page_len: u32, meta: Arc<Meta>) -> Self {
        Self {
            layers: vec![SegmentContainer::new(segment_id, max_page_len, meta)],
            est_size: 0,
            lsn: 0,
        }
    }

    pub fn edge_meta(&self) -> &Arc<Meta> {
        self.layers[0].meta()
    }

    pub fn swap_out_layers(&mut self) -> Vec<SegmentContainer<EdgeEntry>> {
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

    pub fn get_or_create_layer(&mut self, layer_id: usize) -> &mut SegmentContainer<EdgeEntry> {
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

    pub fn get_layer(&self, layer_id: usize) -> Option<&SegmentContainer<EdgeEntry>> {
        self.layers.get(layer_id)
    }

    pub fn est_size(&self) -> usize {
        self.est_size
    }

    pub fn lsn(&self) -> u64 {
        self.lsn
    }

    pub fn set_lsn(&mut self, lsn: u64) {
        self.lsn = lsn;
    }

    /// Replaces this segment with an empty instance, returning the old segment
    /// with its data.
    ///
    /// The new segment will have the same number of layers as the original.
    pub fn take(&mut self) -> Self {
        let layers = self.layers.iter_mut().map(|layer| layer.take()).collect();

        Self {
            layers,
            est_size: 0,
            lsn: self.lsn,
        }
    }

    pub fn max_page_len(&self) -> u32 {
        self.layers[0].max_page_len()
    }

    pub fn get_edge(&self, edge_pos: LocalPOS, layer_id: usize) -> Option<(VID, VID)> {
        self.layers
            .get(layer_id)?
            .get(edge_pos)
            .map(|entry| (entry.src, entry.dst))
    }

    pub fn insert_edge_internal<T: AsTime>(
        &mut self,
        t: T,
        edge_pos: LocalPOS,
        src: VID,
        dst: VID,
        layer_id: usize,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) {
        // Ensure we have enough layers
        self.ensure_layer(layer_id);
        let est_size = self.layers[layer_id].est_size();

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
        edge_pos: LocalPOS,
        src: VID,
        dst: VID,
        layer_id: usize,
    ) {
        let t = TimeIndexEntry::new(t.t(), t.i());

        // Ensure we have enough layers
        self.ensure_layer(layer_id);
        let est_size = self.layers[layer_id].est_size();

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
    ) {
        let src = src.into();
        let dst = dst.into();

        // Ensure we have enough layers
        self.ensure_layer(layer_id);
        let est_size = self.layers[layer_id].est_size();

        self.reserve_local_row(edge_pos, src, dst, layer_id);
        let layer_est_size = self.layers[layer_id].est_size();
        self.est_size += layer_est_size.saturating_sub(est_size);
    }

    fn ensure_layer(&mut self, layer_id: usize) {
        if layer_id >= self.layers.len() {
            // Get details from first layer to create consistent new layers.
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

        let row = self.layers[layer_id].reserve_local_row(edge_pos).inner();
        row.src = src;
        row.dst = dst;
        row.row
    }

    pub fn check_metadata(
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
        edge_pos: LocalPOS,
        src: VID,
        dst: VID,
        layer_id: usize,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) {
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
            .filter(|layer| layer.has_item(edge_pos))
            .is_some()
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
    num_edges: AtomicU32,
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
            .flat_map(|layer| layer.filled_positions())
            .map(move |pos| MemEdgeRef::new(pos, &self.inner))
    }

    fn edge_par_iter_layer<'a>(
        &'a self,
        layer_id: usize,
    ) -> impl ParallelIterator<Item = MemEdgeRef<'a>> + 'a {
        self.inner
            .layers
            .get(layer_id)
            .into_par_iter()
            .flat_map(|layer| layer.filled_positions_par())
            .map(move |pos| MemEdgeRef::new(pos, &self.inner))
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

    #[box_on_debug_lifetime]
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
    ) -> impl ParallelIterator<Item = Self::EntryRef<'a>> + 'a {
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

impl<P: PersistenceStrategy<ES = EdgeSegmentView<P>>> EdgeSegmentOps for EdgeSegmentView<P> {
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
        _max_page_len: u32,
        _meta: Arc<Meta>,
        _path: impl AsRef<std::path::Path>,
        _ext: Self::Extension,
    ) -> Result<Self, StorageError>
    where
        Self: Sized,
    {
        Err(StorageError::GenericFailure(
            "load not supported".to_string(),
        ))
    }

    fn new(page_id: usize, meta: Arc<Meta>, _path: Option<PathBuf>, ext: Self::Extension) -> Self {
        let max_page_len = ext.persistence_config().max_edge_page_len;
        Self {
            segment: parking_lot::RwLock::new(MemEdgeSegment::new(page_id, max_page_len, meta))
                .into(),
            segment_id: page_id,
            num_edges: AtomicU32::new(0),
            _ext: ext,
        }
    }

    fn segment_id(&self) -> usize {
        self.segment_id
    }

    fn edges_counter(&self) -> &AtomicU32 {
        &self.num_edges
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

    fn increment_num_edges(&self) -> u32 {
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

    fn entry<'a>(&'a self, edge_pos: LocalPOS) -> Self::Entry<'a> {
        MemEdgeEntry::new(edge_pos, self.head())
    }

    fn layer_entry<'a>(
        &'a self,
        edge_pos: LocalPOS,
        layer_id: usize,
        locked_head: Option<parking_lot::RwLockReadGuard<'a, MemEdgeSegment>>,
    ) -> Option<Self::Entry<'a>> {
        locked_head.and_then(|locked_head| {
            let layer = locked_head.as_ref().get(layer_id)?;
            layer
                .has_item(edge_pos)
                .then(|| MemEdgeEntry::new(edge_pos, locked_head))
        })
    }

    fn locked(self: &Arc<Self>) -> Self::ArcLockedSegment {
        ArcLockedSegmentView {
            inner: self.head_arc(),
        }
    }

    fn vacuum(
        &self,
        _locked_head: impl DerefMut<Target = MemEdgeSegment>,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    fn num_layers(&self) -> usize {
        self.head().layers.len()
    }

    fn layer_count(&self, layer_id: usize) -> u32 {
        self.head()
            .get_layer(layer_id)
            .map_or(0, |layer| layer.len())
    }

    fn set_dirty(&self, _dirty: bool) {}

    fn immut_lsn(&self) -> LSN {
        panic!("immut_lsn not supported for EdgeSegmentView");
    }

    fn flush(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow_array::{Array, BooleanArray, StringArray};
    use raphtory_api::core::entities::properties::prop::PropType;
    use raphtory_core::{entities::EID, storage::timeindex::TimeIndexEntry};
    use super::*;
    use raphtory_api::core::entities::properties::meta::Meta;

    fn create_test_segment() -> MemEdgeSegment {
        let meta = Arc::new(Meta::default());
        MemEdgeSegment::new(1, 100, meta)
    }

    #[test]
    fn test_insert_edge_internal_baseline() {
        let mut segment = create_test_segment();

        // Insert a few edges using insert_edge_internal
        segment.insert_edge_internal(
            TimeIndexEntry::new(1, 0),
            LocalPOS(0),
            VID(1),
            VID(2),
            0,
            vec![(0, Prop::from("test1"))],
        );

        segment.insert_edge_internal(
            TimeIndexEntry::new(2, 1),
            LocalPOS(1),
            VID(3),
            VID(4),
            0,
            vec![(0, Prop::from("test2"))],
        );

        segment.insert_edge_internal(
            TimeIndexEntry::new(3, 2),
            LocalPOS(2),
            VID(5),
            VID(6),
            0,
            vec![(0, Prop::from("test3"))],
        );

        // Verify edges exist
        assert!(segment.contains_edge(LocalPOS(0), 0));
        assert!(segment.contains_edge(LocalPOS(1), 0));
        assert!(segment.contains_edge(LocalPOS(2), 0));

        // Verify edge data
        assert_eq!(segment.get_edge(LocalPOS(0), 0), Some((VID(1), VID(2))));
        assert_eq!(segment.get_edge(LocalPOS(1), 0), Some((VID(3), VID(4))));
        assert_eq!(segment.get_edge(LocalPOS(2), 0), Some((VID(5), VID(6))));

        // Verify time length increased
        assert_eq!(segment.t_len(), 3);
    }

    #[test]
    fn test_bulk_insert_edges_internal_basic() {
        let mut segment = create_test_segment();

        // Prepare bulk insert data
        let mask = BooleanArray::from(vec![true, true, true]);
        let times = vec![1i64, 2i64, 3i64];
        let eids = vec![EID(0), EID(1), EID(2)];
        let srcs = vec![VID(1), VID(3), VID(5)];
        let dsts = vec![VID(2), VID(4), VID(6)];
        let cols: Vec<Arc<dyn Array>> =
            vec![Arc::new(StringArray::from(vec!["test1", "test2", "test3"]))];
        let col_mapping = vec![0]; // property id 0

        // Bulk insert edges
        segment.bulk_insert_edges_internal(
            &mask,
            &times,
            0, // time_sec_index
            &eids,
            &srcs,
            &dsts,
            0, // layer_id
            &cols,
            &col_mapping,
        );

        // Verify edges exist
        assert!(segment.contains_edge(LocalPOS(0), 0));
        assert!(segment.contains_edge(LocalPOS(1), 0));
        assert!(segment.contains_edge(LocalPOS(2), 0));

        // Verify edge data
        assert_eq!(segment.get_edge(LocalPOS(0), 0), Some((VID(1), VID(2))));
        assert_eq!(segment.get_edge(LocalPOS(1), 0), Some((VID(3), VID(4))));
        assert_eq!(segment.get_edge(LocalPOS(2), 0), Some((VID(5), VID(6))));

        // Verify time length increased
        assert_eq!(segment.t_len(), 3);

        for (index, local_pos) in [LocalPOS(0), LocalPOS(1), LocalPOS(2)].iter().enumerate() {
            let actual = segment.layers[0]
                .t_prop(*local_pos, 0)
                .into_iter()
                .flat_map(|p| p.iter())
                .collect::<Vec<_>>();

            let i = local_pos.0 as i64;
            assert_eq!(
                actual,
                vec![(
                    TimeIndexEntry::new(i + 1, index),
                    Prop::str(format!("test{}", i + 1))
                )]
            );
        }
    }

    #[test]
    fn test_bulk_insert_with_mask() {
        let mut segment = create_test_segment();

        // Prepare bulk insert data with selective mask
        let mask = BooleanArray::from(vec![true, false, true, false]);
        let times = vec![1i64, 2i64, 3i64, 4i64];
        let eids = vec![EID(0), EID(1), EID(2), EID(3)];
        let srcs = vec![VID(1), VID(3), VID(5), VID(7)];
        let dsts = vec![VID(2), VID(4), VID(6), VID(8)];
        let cols: Vec<Arc<dyn Array>> = vec![Arc::new(StringArray::from(vec![
            "test1", "test2", "test3", "test4",
        ]))];
        let col_mapping = vec![0];

        // Bulk insert edges
        segment.bulk_insert_edges_internal(
            &mask,
            &times,
            0,
            &eids,
            &srcs,
            &dsts,
            0,
            &cols,
            &col_mapping,
        );

        // Only edges at positions 0 and 2 should exist (mask was true)
        assert!(segment.contains_edge(LocalPOS(0), 0));
        assert!(!segment.contains_edge(LocalPOS(1), 0));
        assert!(segment.contains_edge(LocalPOS(2), 0));
        assert!(!segment.contains_edge(LocalPOS(3), 0));

        // Verify correct edge data for existing edges
        assert_eq!(segment.get_edge(LocalPOS(0), 0), Some((VID(1), VID(2))));
        assert_eq!(segment.get_edge(LocalPOS(2), 0), Some((VID(5), VID(6))));

        // Only 2 edges should contribute to time length
        assert_eq!(segment.t_len(), 2);
    }

    #[test]
    fn test_bulk_vs_individual_equivalence() {
        let mut segment1 = create_test_segment();
        let mut segment2 = create_test_segment();

        // Individual insertions
        segment1.insert_edge_internal(
            TimeIndexEntry::new(1, 0),
            LocalPOS(0),
            VID(1),
            VID(2),
            0,
            vec![(0, Prop::from("test1"))],
        );
        segment1.insert_edge_internal(
            TimeIndexEntry::new(2, 1),
            LocalPOS(1),
            VID(3),
            VID(4),
            0,
            vec![(0, Prop::from("test2"))],
        );
        segment1.insert_edge_internal(
            TimeIndexEntry::new(3, 2),
            LocalPOS(2),
            VID(5),
            VID(6),
            0,
            vec![(0, Prop::from("test3"))],
        );

        // Equivalent bulk insertion
        let mask = BooleanArray::from(vec![true, true, true]);
        let times = vec![1i64, 2i64, 3i64];
        let eids = vec![EID(0), EID(1), EID(2)];
        let srcs = vec![VID(1), VID(3), VID(5)];
        let dsts = vec![VID(2), VID(4), VID(6)];
        let cols: Vec<Arc<dyn Array>> =
            vec![Arc::new(StringArray::from(vec!["test1", "test2", "test3"]))];
        let col_mapping = vec![0];

        segment2.bulk_insert_edges_internal(
            &mask,
            &times,
            0,
            &eids,
            &srcs,
            &dsts,
            0,
            &cols,
            &col_mapping,
        );

        // Both segments should have the same edges
        for pos in [LocalPOS(0), LocalPOS(1), LocalPOS(2)] {
            assert_eq!(
                segment1.contains_edge(pos, 0),
                segment2.contains_edge(pos, 0)
            );
            assert_eq!(segment1.get_edge(pos, 0), segment2.get_edge(pos, 0));
        }

        // Both should have same time length
        assert_eq!(segment1.t_len(), segment2.t_len());
    }

    #[test]
    fn test_interleaved_operations() {
        let mut segment = create_test_segment();

        // Start with individual insertion
        segment.insert_edge_internal(
            TimeIndexEntry::new(1, 0),
            LocalPOS(0),
            VID(1),
            VID(2),
            0,
            vec![(0, Prop::from("individual1"))],
        );

        // Bulk insert some edges
        let mask = BooleanArray::from(vec![true, true]);
        let times = vec![2i64, 3i64];
        let eids = vec![EID(1), EID(2)];
        let srcs = vec![VID(3), VID(5)];
        let dsts = vec![VID(4), VID(6)];
        let cols: Vec<Arc<dyn Array>> = vec![Arc::new(StringArray::from(vec!["bulk1", "bulk2"]))];
        let col_mapping = vec![0];

        segment.bulk_insert_edges_internal(
            &mask,
            &times,
            1, // time_sec_index continues from previous
            &eids,
            &srcs,
            &dsts,
            0,
            &cols,
            &col_mapping,
        );

        // Insert another individual edge
        segment.insert_edge_internal(
            TimeIndexEntry::new(4, 3),
            LocalPOS(3),
            VID(7),
            VID(8),
            0,
            vec![(0, Prop::from("individual2"))],
        );

        // Another bulk insert
        let mask2 = BooleanArray::from(vec![true, false, true]);
        let times2 = vec![5i64, 6i64, 7i64];
        let eids2 = vec![EID(4), EID(5), EID(6)];
        let srcs2 = vec![VID(9), VID(11), VID(13)];
        let dsts2 = vec![VID(10), VID(12), VID(14)];
        let cols2: Vec<Arc<dyn Array>> =
            vec![Arc::new(StringArray::from(vec!["bulk3", "bulk4", "bulk5"]))];

        segment.bulk_insert_edges_internal(
            &mask2,
            &times2,
            4, // time_sec_index continues
            &eids2,
            &srcs2,
            &dsts2,
            0,
            &cols2,
            &col_mapping,
        );

        // Verify all edges exist correctly
        assert!(segment.contains_edge(LocalPOS(0), 0)); // individual1
        assert!(segment.contains_edge(LocalPOS(1), 0)); // bulk1
        assert!(segment.contains_edge(LocalPOS(2), 0)); // bulk2
        assert!(segment.contains_edge(LocalPOS(3), 0)); // individual2
        assert!(segment.contains_edge(LocalPOS(4), 0)); // bulk3
        assert!(!segment.contains_edge(LocalPOS(5), 0)); // masked out
        assert!(segment.contains_edge(LocalPOS(6), 0)); // bulk5

        // Verify edge data
        assert_eq!(segment.get_edge(LocalPOS(0), 0), Some((VID(1), VID(2))));
        assert_eq!(segment.get_edge(LocalPOS(1), 0), Some((VID(3), VID(4))));
        assert_eq!(segment.get_edge(LocalPOS(2), 0), Some((VID(5), VID(6))));
        assert_eq!(segment.get_edge(LocalPOS(3), 0), Some((VID(7), VID(8))));
        assert_eq!(segment.get_edge(LocalPOS(4), 0), Some((VID(9), VID(10))));
        assert_eq!(segment.get_edge(LocalPOS(6), 0), Some((VID(13), VID(14))));

        // Total time length should be 6 (4 individual + 2 from first bulk + 2 from second bulk)
        assert_eq!(segment.t_len(), 6);
    }

    #[test]
    fn test_bulk_insert_multiple_layers() {
        let mut segment = create_test_segment();

        // Insert into layer 0
        let mask = BooleanArray::from(vec![true, true]);
        let times = vec![1i64, 2i64];
        let eids = vec![EID(0), EID(1)];
        let srcs = vec![VID(1), VID(3)];
        let dsts = vec![VID(2), VID(4)];
        let cols: Vec<Arc<dyn Array>> =
            vec![Arc::new(StringArray::from(vec!["layer0_1", "layer0_2"]))];
        let col_mapping = vec![0];

        segment.bulk_insert_edges_internal(
            &mask,
            &times,
            0,
            &eids,
            &srcs,
            &dsts,
            0, // layer 0
            &cols,
            &col_mapping,
        );

        // Insert into layer 1
        let mask2 = BooleanArray::from(vec![true]);
        let times2 = vec![3i64];
        let eids2 = vec![EID(0)]; // same eid, different layer
        let srcs2 = vec![VID(5)];
        let dsts2 = vec![VID(6)];
        let cols2: Vec<Arc<dyn Array>> = vec![Arc::new(StringArray::from(vec!["layer1_1"]))];

        segment.bulk_insert_edges_internal(
            &mask2,
            &times2,
            2,
            &eids2,
            &srcs2,
            &dsts2,
            1, // layer 1
            &cols2,
            &col_mapping,
        );

        // Verify edges in both layers
        assert!(segment.contains_edge(LocalPOS(0), 0));
        assert!(segment.contains_edge(LocalPOS(1), 0));
        assert!(segment.contains_edge(LocalPOS(0), 1));
        assert!(!segment.contains_edge(LocalPOS(1), 1));

        // Verify correct layer data
        assert_eq!(segment.get_edge(LocalPOS(0), 0), Some((VID(1), VID(2))));
        assert_eq!(segment.get_edge(LocalPOS(1), 0), Some((VID(3), VID(4))));
        assert_eq!(segment.get_edge(LocalPOS(0), 1), Some((VID(5), VID(6))));
    }

    #[test]
    fn est_size_changes() {
        let meta = Arc::new(Meta::default());
        let mut segment = MemEdgeSegment::new(1, 100, meta.clone());

        assert_eq!(segment.est_size(), 0);

        segment.insert_edge_internal(
            TimeIndexEntry::new(1, 0),
            LocalPOS(0),
            VID(1),
            VID(2),
            0,
            vec![(0, Prop::from("test"))],
        );

        let est_size1 = segment.est_size();

        assert!(est_size1 > 0);

        segment.delete_edge_internal(TimeIndexEntry::new(2, 3), LocalPOS(0), VID(5), VID(3), 0);

        let est_size2 = segment.est_size();

        assert!(
            est_size2 > est_size1,
            "Expected size to increase after deletion, but it did not."
        );

        // same edge insertion again to check size increase
        segment.insert_edge_internal(
            TimeIndexEntry::new(3, 0),
            LocalPOS(1),
            VID(4),
            VID(6),
            0,
            vec![(0, Prop::from("test2"))],
        );

        let est_size3 = segment.est_size();
        assert!(
            est_size3 > est_size2,
            "Expected size to increase after re-insertion, but it did not."
        );

        // Insert a static edge

        segment.insert_static_edge_internal(LocalPOS(1), 4, 6, 0);

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

        segment.update_const_properties(LocalPOS(1), VID(4), VID(6), 0, [(prop_id, Prop::U8(2))]);

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
