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
use raphtory_api::core::entities::LayerId;
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
        }
    }

    pub fn segment(&self) -> &NS {
        self.segment
    }

    #[inline(always)]
    pub fn writer(&mut self) -> NodeWriter<'_, &mut MemNodeSegment, NS> {
        NodeWriter::new(self.segment, self.layer_counter, self.head.deref_mut())
    }

    #[inline(always)]
    pub fn bulk_writer(&mut self) -> BulkNodeWriter<'_, &mut MemNodeSegment, NS> {
        NodeWriter::new(self.segment, self.layer_counter, self.head.deref_mut()).into()
    }

    pub fn head(&mut self) -> &mut MemNodeSegment {
        self.head.deref_mut()
    }

    pub fn vacuum(&mut self) {
        let _ = self.segment.vacuum(self.head.deref_mut());
    }

    pub fn flush(&mut self) -> Result<(), StorageError> {
        let head_lock = self.head.deref_mut();
        self.segment.flush_locked(head_lock)
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

    pub fn len(&self) -> usize {
        self.segments.len()
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

    pub fn copy_to(&self, dst: &Path) -> Result<(), StorageError> {
        std::fs::create_dir_all(dst)?;

        self.segments.par_iter().try_for_each(|locked_segment| {
            locked_segment
                .segment()
                .copy_to(&dst.join(locked_segment.segment_id().to_string()))
        })?;

        // TODO: Copy node type index

        Ok(())
    }
}
