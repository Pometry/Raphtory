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
        }
    }

    #[inline(always)]
    pub fn writer(&mut self) -> EdgeWriter<'_, &mut MemEdgeSegment, ES> {
        EdgeWriter::new(self.num_edges, self.segment, self.head.deref_mut())
    }

    #[inline(always)]
    pub fn bulk_writer(&mut self) -> BulkEdgeWriter<'_, &mut MemEdgeSegment, ES> {
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
}

#[derive(Debug)]
pub struct WriteLockedEdgeSegments<'a, ES> {
    segments: Vec<LockedEdgeSegment<'a, ES>>,
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
            let locked_head = locked_segment.head.deref();
            locked_segment
                .segment
                .has_edge(pos, elid.layer(), locked_head)
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
            .try_for_each(|locked_segment| {
                let LockedEdgeSegment { segment, head, .. } = locked_segment;
                segment.flush(head.deref_mut())
            })?;

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.segments.len()
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn copy_to(&self, dst: &Path) -> Result<(), StorageError> {
        std::fs::create_dir_all(dst)?;

        self.segments.par_iter().try_for_each(|locked_segment| {
            locked_segment
                .segment()
                .copy_to(&dst.join(locked_segment.id().to_string()))
        })?;

        Ok(())
    }
}
