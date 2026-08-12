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
use parking_lot::{RawRwLock, lock_api::ArcRwLockWriteGuard};
use raphtory_api::core::entities::LayerId;
use raphtory_core::entities::VID;
use rayon::prelude::*;
use std::{ops::DerefMut, path::Path, sync::Arc};

#[derive(Debug)]
pub struct LockedNodePage<NS> {
    segment_id: usize,
    max_page_len: u32,
    layer_counter: Arc<GraphStats>,
    page: Arc<NS>,
    lock: ArcRwLockWriteGuard<RawRwLock, MemNodeSegment>,
}

impl<NS: NodeSegmentOps> LockedNodePage<NS> {
    pub fn new(
        segment_id: usize,
        layer_counter: Arc<GraphStats>,
        max_page_len: u32,
        page: Arc<NS>,
        lock: ArcRwLockWriteGuard<RawRwLock, MemNodeSegment>,
    ) -> Self {
        Self {
            segment_id,
            layer_counter,
            max_page_len,
            page,
            lock,
        }
    }

    pub fn segment(&self) -> &NS {
        self.page.as_ref()
    }

    #[inline(always)]
    pub fn writer(&mut self) -> NodeWriter<'_, &mut MemNodeSegment, NS> {
        NodeWriter::new(
            self.page.as_ref(),
            self.layer_counter.as_ref(),
            self.lock.deref_mut(),
        )
    }

    #[inline(always)]
    pub fn bulk_writer(&mut self) -> BulkNodeWriter<'_, &mut MemNodeSegment, NS> {
        NodeWriter::new(
            self.page.as_ref(),
            self.layer_counter.as_ref(),
            self.lock.deref_mut(),
        )
        .into()
    }

    pub fn head(&mut self) -> &mut MemNodeSegment {
        self.lock.deref_mut()
    }

    pub fn vacuum(&mut self) {
        let _ = self.page.vacuum(self.lock.deref_mut());
    }

    #[inline(always)]
    pub fn segment_id(&self) -> usize {
        self.segment_id
    }

    #[inline(always)]
    pub fn resolve_pos(&self, node_id: VID) -> Option<LocalPOS> {
        let (page, pos) = resolve_pos(node_id, self.max_page_len);

        if page == self.segment_id {
            Some(pos)
        } else {
            None
        }
    }

    pub fn ensure_layer(&mut self, layer_id: LayerId) {
        self.lock.get_or_create_layer(layer_id);
        self.layer_counter.get(layer_id);
    }
}

pub struct WriteLockedNodePages<NS> {
    writers: Vec<LockedNodePage<NS>>,
}

impl<NS> Default for WriteLockedNodePages<NS> {
    fn default() -> Self {
        Self {
            writers: Vec::new(),
        }
    }
}

impl<EXT: PersistenceStrategy<NS = NS>, NS: NodeSegmentOps<Extension = EXT>>
    WriteLockedNodePages<NS>
{
    pub fn new(writers: Vec<LockedNodePage<NS>>) -> Self {
        Self { writers }
    }

    pub fn len(&self) -> usize {
        self.writers.len()
    }

    #[inline]
    pub fn get_mut(&mut self, segment_id: usize) -> Option<&mut LockedNodePage<NS>> {
        self.writers.get_mut(segment_id)
    }

    pub fn par_iter_mut(&mut self) -> rayon::slice::IterMut<'_, LockedNodePage<NS>> {
        self.writers.par_iter_mut()
    }

    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, LockedNodePage<NS>> {
        self.writers.iter_mut()
    }

    pub fn into_par_iter(self) -> impl ParallelIterator<Item = LockedNodePage<NS>> {
        self.writers.into_par_iter()
    }

    pub fn ensure_layer(&mut self, layer_id: LayerId) {
        for writer in &mut self.writers {
            writer.ensure_layer(layer_id);
        }
    }

    pub fn vacuum(&mut self) -> Result<(), StorageError> {
        self.writers.par_iter_mut().try_for_each(|writer| {
            let LockedNodePage { page, lock, .. } = writer;
            page.vacuum(lock.deref_mut())
        })?;

        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), StorageError> {
        self.writers.par_iter_mut().try_for_each(|writer| {
            let LockedNodePage { page, lock, .. } = writer;
            page.flush(lock.deref_mut())
        })?;

        Ok(())
    }

    pub fn copy_to(&self, dst: &Path) -> Result<(), StorageError> {
        std::fs::create_dir_all(dst)?;

        for writer in &self.writers {
            let segment_dst = dst.join(writer.segment_id().to_string());
            std::fs::create_dir_all(&segment_dst)?;
            writer.segment().copy_to(&segment_dst)?;
        }

        Ok(())
    }
}
