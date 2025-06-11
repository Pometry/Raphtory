use crate::{
    LocalPOS, NodeSegmentOps,
    pages::{node_page::writer::NodeWriter, resolve_pos},
    segments::node::MemNodeSegment,
};
use parking_lot::RwLockWriteGuard;
use raphtory_core::entities::VID;
use rayon::prelude::*;
use std::{ops::DerefMut, sync::atomic::AtomicUsize};

pub struct LockedNodePage<'a, NS> {
    page_id: usize,
    max_page_len: usize,
    num_nodes: &'a AtomicUsize,
    page: &'a NS,
    lock: RwLockWriteGuard<'a, MemNodeSegment>,
}

impl<'a, EXT, NS: NodeSegmentOps<Extension = EXT>> LockedNodePage<'a, NS> {
    pub fn new(
        page_id: usize,
        num_nodes: &'a AtomicUsize,
        max_page_len: usize,
        page: &'a NS,
        lock: RwLockWriteGuard<'a, MemNodeSegment>,
    ) -> Self {
        Self {
            page_id,
            num_nodes,
            max_page_len,
            page,
            lock,
        }
    }

    #[inline(always)]
    pub fn writer(&mut self) -> NodeWriter<'_, &mut MemNodeSegment, NS> {
        NodeWriter::new(self.page, self.num_nodes, self.lock.deref_mut())
    }

    #[inline(always)]
    pub fn page_id(&self) -> usize {
        self.page_id
    }

    #[inline(always)]
    pub fn resolve_pos(&self, node_id: VID) -> Option<LocalPOS> {
        let (page, pos) = resolve_pos(node_id, self.max_page_len);
        if page == self.page_id {
            Some(pos)
        } else {
            None
        }
    }
}
pub struct WriteLockedNodePages<'a, NS> {
    writers: Vec<LockedNodePage<'a, NS>>,
}

impl<'a, EXT, NS: NodeSegmentOps<Extension = EXT>> WriteLockedNodePages<'a, NS> {
    pub fn new(writers: Vec<LockedNodePage<'a, NS>>) -> Self {
        Self { writers }
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
}
