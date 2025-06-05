use std::{ops::DerefMut, sync::atomic::AtomicUsize};

use crate::{
    EdgeSegmentOps,
    pages::{edge_page::writer::EdgeWriter, resolve_pos},
    segments::edge::MemEdgeSegment,
};
use db4_common::LocalPOS;
use parking_lot::RwLockWriteGuard;
use raphtory::core::entities::EID;
use rayon::prelude::*;

pub struct LockedEdgePage<'a, ES> {
    page_id: usize,
    max_page_len: usize,
    page: &'a ES,
    num_edges: &'a AtomicUsize,
    lock: RwLockWriteGuard<'a, MemEdgeSegment>,
}

impl<'a, EXT, ES: EdgeSegmentOps<Extension = EXT>> LockedEdgePage<'a, ES> {
    pub fn new(
        page_id: usize,
        max_page_len: usize,
        page: &'a ES,
        num_edges: &'a AtomicUsize,
        lock: RwLockWriteGuard<'a, MemEdgeSegment>,
    ) -> Self {
        Self {
            page_id,
            max_page_len,
            page,
            num_edges,
            lock,
        }
    }

    #[inline(always)]
    pub fn writer(&mut self) -> EdgeWriter<'_, &mut MemEdgeSegment, ES> {
        EdgeWriter::new(self.num_edges, self.page, self.lock.deref_mut())
    }

    #[inline(always)]
    pub fn page_id(&self) -> usize {
        self.page_id
    }

    #[inline(always)]
    pub fn resolve_pos(&self, edge_id: EID) -> Option<LocalPOS> {
        let (page, pos) = resolve_pos(edge_id, self.max_page_len);
        if page == self.page_id {
            Some(pos)
        } else {
            None
        }
    }
}
pub struct WriteLockedEdgePages<'a, ES> {
    writers: Vec<LockedEdgePage<'a, ES>>,
}

impl<'a, EXT, ES: EdgeSegmentOps<Extension = EXT>> WriteLockedEdgePages<'a, ES> {
    pub fn new(writers: Vec<LockedEdgePage<'a, ES>>) -> Self {
        Self { writers }
    }

    pub fn par_iter_mut(&mut self) -> rayon::slice::IterMut<'_, LockedEdgePage<'a, ES>> {
        self.writers.par_iter_mut()
    }

    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, LockedEdgePage<'a, ES>> {
        self.writers.iter_mut()
    }

    pub fn into_par_iter(self) -> impl ParallelIterator<Item = LockedEdgePage<'a, ES>> + 'a {
        self.writers.into_par_iter()
    }
}
