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
use std::ops::DerefMut;

#[derive(Debug)]
pub struct LockedNodePage<'a, NS> {
    segment_id: usize,
    max_page_len: u32,
    layer_counter: &'a GraphStats,
    page: &'a NS,
    lock: RwLockWriteGuard<'a, MemNodeSegment>,
}

impl<'a, NS: NodeSegmentOps> LockedNodePage<'a, NS> {
    pub fn new(
        segment_id: usize,
        layer_counter: &'a GraphStats,
        max_page_len: u32,
        page: &'a NS,
        lock: RwLockWriteGuard<'a, MemNodeSegment>,
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
        self.page
    }

    #[inline(always)]
    pub fn writer(&mut self) -> NodeWriter<'_, &mut MemNodeSegment, NS> {
        NodeWriter::new(self.page, self.layer_counter, self.lock.deref_mut())
    }

    #[inline(always)]
    pub fn bulk_writer(&mut self) -> BulkNodeWriter<'_, &mut MemNodeSegment, NS> {
        NodeWriter::new(self.page, self.layer_counter, self.lock.deref_mut()).into()
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

    pub fn get_metadata(&self, pos: LocalPOS, layer_id: LayerId, prop_id: usize) -> Option<Prop> {
        self.lock
            .get_metadata(pos, layer_id, prop_id)
            .or_else(|| self.page.get_metadata_immut(pos, layer_id, prop_id))
    }
}

pub struct WriteLockedNodePages<'a, NS> {
    writers: Vec<LockedNodePage<'a, NS>>,
}

impl<NS> Default for WriteLockedNodePages<'_, NS> {
    fn default() -> Self {
        Self {
            writers: Vec::new(),
        }
    }
}

impl<'a, EXT: PersistenceStrategy<NS = NS>, NS: NodeSegmentOps<Extension = EXT>>
    WriteLockedNodePages<'a, NS>
{
    pub fn new(writers: Vec<LockedNodePage<'a, NS>>) -> Self {
        Self { writers }
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.writers.len()
    }

    /// Read node metadata through the held write locks. Use this instead of the graph's
    /// read path while the pages are held, which would deadlock on the segment lock.
    ///
    /// Returns `None` if the node's segment is not held.
    pub fn node_metadata(&self, vid: VID, layer_id: LayerId, prop_id: usize) -> Option<Prop> {
        let max_page_len = self.writers.first()?.max_page_len;
        let (segment_id, pos) = resolve_pos(vid, max_page_len);
        self.writers
            .get(segment_id)?
            .get_metadata(pos, layer_id, prop_id)
    }

    /// Append the page for the next segment; pages are indexed by segment id.
    pub fn push(&mut self, page: LockedNodePage<'a, NS>) {
        assert_eq!(page.segment_id(), self.writers.len());
        self.writers.push(page);
    }

    #[inline]
    pub fn get_mut(&mut self, segment_id: usize) -> Option<&mut LockedNodePage<'a, NS>> {
        self.writers.get_mut(segment_id)
    }

    pub fn par_iter_mut(&mut self) -> rayon::slice::IterMut<'_, LockedNodePage<'a, NS>> {
        self.writers.par_iter_mut()
    }

    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, LockedNodePage<'a, NS>> {
        self.writers.iter_mut()
    }

    pub fn into_par_iter(self) -> impl ParallelIterator<Item = LockedNodePage<'a, NS>> + 'a {
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
}
