use crate::{
    LocalPOS,
    api::nodes::NodeSegmentOps,
    error::StorageError,
    pages::{
        layer_counter::GraphStats,
        node_page::{bulk_writer::BulkNodeWriter, writer::NodeWriter},
        resolve_pos,
    },
    persist::strategy::PersistenceStrategy,
    segments::node::segment::MemNodeSegment,
};
use parking_lot::RwLockWriteGuard;
use raphtory_api::core::entities::{LayerId, properties::prop::Prop};
use raphtory_core::entities::VID;
use rayon::prelude::*;
use std::{ops::DerefMut, path::Path};

#[derive(Debug)]
pub struct LockedNodeSegment<'a, NS> {
    id: usize,
    max_page_len: u32,
    layer_counter: &'a GraphStats,
    segment: &'a NS,
    head: RwLockWriteGuard<'a, MemNodeSegment>,
    /// a bulk writer was used, so addition cells may be unsorted (see `finish_bulk_load`)
    bulk_used: bool,
    /// `NodeSegmentOps::wait_for_flush`, captured here so `Drop` needs no trait bound
    wait_for_flush: fn(&NS),
}

impl<'a, NS: NodeSegmentOps> LockedNodeSegment<'a, NS> {
    pub fn new(
        id: usize,
        layer_counter: &'a GraphStats,
        max_page_len: u32,
        segment: &'a NS,
        head: RwLockWriteGuard<'a, MemNodeSegment>,
    ) -> Self {
        Self {
            id,
            layer_counter,
            max_page_len,
            segment,
            head,
            bulk_used: false,
            wait_for_flush: NS::wait_for_flush,
        }
    }

    /// Restores sorted addition cells after bulk writes: waits for any flush that may still
    /// hold an unsorted frozen head, then sorts the head. Runs on drop if not called.
    pub fn segment(&self) -> &NS {
        self.segment
    }

    #[inline(always)]
    pub fn writer(&mut self) -> NodeWriter<'_, &mut MemNodeSegment, NS> {
        NodeWriter::new(self.segment, self.layer_counter, self.head.deref_mut())
    }

    #[inline(always)]
    pub fn bulk_writer(&mut self) -> BulkNodeWriter<'_, &mut MemNodeSegment, NS> {
        self.bulk_used = true;
        NodeWriter::new(self.segment, self.layer_counter, self.head.deref_mut()).into()
    }

    pub fn head(&mut self) -> &mut MemNodeSegment {
        self.head.deref_mut()
    }

    pub fn vacuum(&mut self) {
        let _ = self.segment.vacuum(self.head.deref_mut());
    }

    pub fn flush(&mut self) -> Result<(), StorageError> {
        let head = self.head.deref_mut();
        self.segment.flush_with_head(head)
    }

    #[inline(always)]
    pub fn segment_id(&self) -> usize {
        self.id
    }

    #[inline(always)]
    pub fn resolve_pos(&self, node_id: VID) -> Option<LocalPOS> {
        let (page, pos) = resolve_pos(node_id, self.max_page_len);

        if page == self.id { Some(pos) } else { None }
    }

    pub fn ensure_layer(&mut self, layer_id: LayerId) {
        self.head.get_or_create_layer(layer_id);
        self.layer_counter.get(layer_id);
    }

    pub fn get_metadata(&self, pos: LocalPOS, layer_id: LayerId, prop_id: usize) -> Option<Prop> {
        self.head
            .get_metadata(pos, layer_id, prop_id)
            .or_else(|| self.segment.get_metadata_immut(pos, layer_id, prop_id))
    }
}

impl<NS> LockedNodeSegment<'_, NS> {
    /// Restores sorted addition cells after bulk writes: waits for any flush that may still
    /// hold an unsorted frozen head, then sorts the head. Runs on drop if not called.
    pub fn finish_bulk_load(&mut self) {
        if std::mem::take(&mut self.bulk_used) {
            (self.wait_for_flush)(self.segment);
            self.head.sort_additions();
        }
    }
}

impl<NS> Drop for LockedNodeSegment<'_, NS> {
    fn drop(&mut self) {
        self.finish_bulk_load();
    }
}

pub struct WriteLockedNodeSegments<'a, NS> {
    segments: Vec<LockedNodeSegment<'a, NS>>,
}

impl<NS> Default for WriteLockedNodeSegments<'_, NS> {
    fn default() -> Self {
        Self {
            segments: Vec::new(),
        }
    }
}

impl<'a, EXT: PersistenceStrategy<NS = NS>, NS: NodeSegmentOps<Extension = EXT>>
    WriteLockedNodeSegments<'a, NS>
{
    pub fn new(segments: Vec<LockedNodeSegment<'a, NS>>) -> Self {
        Self { segments }
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    /// Read node metadata through the held write locks. Use this instead of the graph's
    /// read path while the pages are held, which would deadlock on the segment lock.
    ///
    /// Returns `None` if the node's segment is not held.
    pub fn node_metadata(&self, vid: VID, layer_id: LayerId, prop_id: usize) -> Option<Prop> {
        let max_page_len = self.segments.first()?.max_page_len;
        let (segment_id, pos) = resolve_pos(vid, max_page_len);
        self.segments
            .get(segment_id)?
            .get_metadata(pos, layer_id, prop_id)
    }

    /// Append the page for the next segment; pages are indexed by segment id.
    pub fn push(&mut self, page: LockedNodeSegment<'a, NS>) {
        assert_eq!(page.segment_id(), self.segments.len());
        self.segments.push(page);
    }

    #[inline]
    pub fn get_mut(&mut self, segment_id: usize) -> Option<&mut LockedNodeSegment<'a, NS>> {
        self.segments.get_mut(segment_id)
    }

    pub fn par_iter_mut(&mut self) -> rayon::slice::IterMut<'_, LockedNodeSegment<'a, NS>> {
        self.segments.par_iter_mut()
    }

    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, LockedNodeSegment<'a, NS>> {
        self.segments.iter_mut()
    }

    pub fn into_par_iter(self) -> impl ParallelIterator<Item = LockedNodeSegment<'a, NS>> + 'a {
        self.segments.into_par_iter()
    }

    pub fn ensure_layer(&mut self, layer_id: LayerId) {
        for segment in &mut self.segments {
            segment.ensure_layer(layer_id);
        }
    }

    pub fn vacuum(&mut self) -> Result<(), StorageError> {
        self.segments
            .par_iter_mut()
            .try_for_each(|locked_segment| {
                let LockedNodeSegment { segment, head, .. } = locked_segment;
                segment.vacuum(head.deref_mut())
            })?;

        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), StorageError> {
        self.segments
            .par_iter_mut()
            .try_for_each(|locked_segment| locked_segment.flush())
    }

    /// [`LockedNodeSegment::finish_bulk_load`] on every segment, in parallel.
    pub fn finish_bulk_load(&mut self) {
        self.segments
            .par_iter_mut()
            .for_each(|locked_segment| locked_segment.finish_bulk_load());
    }

    pub fn copy_to(&self, dst: &Path) -> Result<(), StorageError> {
        std::fs::create_dir_all(dst)?;

        self.segments.par_iter().try_for_each(|locked_segment| {
            let segment_dst = dst.join(locked_segment.segment_id().to_string());

            locked_segment.segment().copy_to(&segment_dst)
        })
    }
}
