use crate::{
    LocalPOS,
    api::edges::EdgeSegmentOps,
    error::StorageError,
    pages::{
        edge_page::{bulk_writer::BulkEdgeWriter, writer::EdgeWriter},
        layer_counter::GraphStats,
        resolve_pos,
    },
    persist::strategy::PersistenceStrategy,
    segments::edge::segment::MemEdgeSegment,
};
use parking_lot::RwLockWriteGuard;
use raphtory_api::core::entities::LayerId;
use raphtory_core::entities::{EID, ELID};
use rayon::prelude::*;
use std::{
    ops::{Deref, DerefMut},
    path::Path,
};

#[derive(Debug)]
pub struct LockedEdgeSegment<'a, ES> {
    id: usize,
    max_page_len: u32,
    segment: &'a ES,
    num_edges: &'a GraphStats,
    head: RwLockWriteGuard<'a, MemEdgeSegment>,
    /// a bulk writer was used, so property time cells may be unsorted (see `finish_bulk_load`)
    bulk_used: bool,
}

impl<'a, ES: EdgeSegmentOps> LockedEdgeSegment<'a, ES> {
    pub fn new(
        id: usize,
        max_page_len: u32,
        segment: &'a ES,
        num_edges: &'a GraphStats,
        head: RwLockWriteGuard<'a, MemEdgeSegment>,
    ) -> Self {
        Self {
            id,
            max_page_len,
            segment,
            num_edges,
            head,
            bulk_used: false,
        }
    }

    #[inline(always)]
    pub fn writer(&mut self) -> EdgeWriter<'_, &mut MemEdgeSegment, ES> {
        EdgeWriter::new(self.num_edges, self.segment, self.head.deref_mut())
    }

    /// Whether the edge at `pos` is in layer `layer_id`, in memory or on disk.
    pub fn has_edge(&self, pos: LocalPOS, layer_id: LayerId) -> bool {
        self.segment.has_edge(pos, layer_id, self.head.deref())
    }

    #[inline(always)]
    pub fn bulk_writer(&mut self) -> BulkEdgeWriter<'_, &mut MemEdgeSegment, ES> {
        self.bulk_used = true;
        EdgeWriter::new(self.num_edges, self.segment, self.head.deref_mut()).into()
    }

    #[inline(always)]
    pub fn id(&self) -> usize {
        self.id
    }

    #[inline(always)]
    pub fn resolve_pos(&self, edge_id: EID) -> Option<LocalPOS> {
        let (page, pos) = resolve_pos(edge_id, self.max_page_len);

        if page == self.id { Some(pos) } else { None }
    }

    pub fn ensure_layer(&mut self, layer_id: LayerId) {
        self.head.get_or_create_layer(layer_id);
    }

    pub fn segment(&self) -> &ES {
        self.segment
    }

    pub fn flush(&mut self) -> Result<(), StorageError> {
        let head = self.head.deref_mut();
        self.segment.flush_with_head(head)
    }
}

impl<ES> LockedEdgeSegment<'_, ES> {
    /// Restores sorted property time cells after bulk writes. Flushing sorts the head before
    /// freezing it, so only the head needs sorting. Runs on drop if not called.
    pub fn finish_bulk_load(&mut self) {
        if std::mem::take(&mut self.bulk_used) {
            self.head.sort_times_from_props();
        }
    }
}

impl<ES> Drop for LockedEdgeSegment<'_, ES> {
    fn drop(&mut self) {
        self.finish_bulk_load();
    }
}

#[derive(Debug)]
pub struct WriteLockedEdgeSegments<'a, ES> {
    segments: Vec<LockedEdgeSegment<'a, ES>>,
}

impl<'a, ES> WriteLockedEdgeSegments<'a, ES> {
    /// Takes every held segment lock out, leaving none.
    pub fn take_all(&mut self) -> Vec<LockedEdgeSegment<'a, ES>> {
        std::mem::take(&mut self.segments)
    }
}

impl<ES> Default for WriteLockedEdgeSegments<'_, ES> {
    fn default() -> Self {
        Self {
            segments: Vec::new(),
        }
    }
}

impl<'a, EXT: PersistenceStrategy<ES = ES>, ES: EdgeSegmentOps<Extension = EXT>>
    WriteLockedEdgeSegments<'a, ES>
{
    pub fn new(segments: Vec<LockedEdgeSegment<'a, ES>>) -> Self {
        Self { segments }
    }

    #[inline]
    pub fn get_mut(&mut self, segment_id: usize) -> Option<&mut LockedEdgeSegment<'a, ES>> {
        self.segments.get_mut(segment_id)
    }

    pub fn par_iter_mut(&mut self) -> rayon::slice::IterMut<'_, LockedEdgeSegment<'a, ES>> {
        self.segments.par_iter_mut()
    }

    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, LockedEdgeSegment<'a, ES>> {
        self.segments.iter_mut()
    }

    pub fn into_par_iter(self) -> impl ParallelIterator<Item = LockedEdgeSegment<'a, ES>> + 'a {
        self.segments.into_par_iter()
    }

    pub fn ensure_layer(&mut self, layer_id: LayerId) {
        for segment in &mut self.segments {
            segment.ensure_layer(layer_id);
        }
    }

    pub fn exists(&self, elid: ELID) -> bool {
        let max_page_len = if !self.segments.is_empty() {
            self.segments[0].max_page_len
        } else {
            return false;
        };

        let (segment_id, pos) = resolve_pos(elid.eid(), max_page_len);

        self.segments.get(segment_id).is_some_and(|locked_segment| {
            let head = locked_segment.head.deref();

            locked_segment.segment.has_edge(pos, elid.layer(), head)
        })
    }

    pub fn vacuum(&mut self) -> Result<(), StorageError> {
        self.segments
            .par_iter_mut()
            .try_for_each(|locked_segment| {
                let LockedEdgeSegment { segment, head, .. } = locked_segment;
                segment.vacuum(head.deref_mut())
            })?;

        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), StorageError> {
        self.segments
            .par_iter_mut()
            .try_for_each(|locked_segment| locked_segment.flush())
    }

    /// [`LockedEdgeSegment::finish_bulk_load`] on every segment, in parallel.
    pub fn finish_bulk_load(&mut self) {
        self.segments
            .par_iter_mut()
            .for_each(|locked_segment| locked_segment.finish_bulk_load());
    }

    pub fn len(&self) -> usize {
        self.segments.len()
    }

    /// Append the page for the next segment; pages are indexed by segment id.
    pub fn push(&mut self, segment: LockedEdgeSegment<'a, ES>) {
        assert_eq!(segment.id(), self.segments.len());
        self.segments.push(segment);
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn copy_to(&self, dst: &Path) -> Result<(), StorageError> {
        std::fs::create_dir_all(dst)?;

        self.segments.par_iter().try_for_each(|locked_segment| {
            let segment_dst = dst.join(locked_segment.id().to_string());

            locked_segment.segment().copy_to(&segment_dst)
        })?;

        Ok(())
    }
}
