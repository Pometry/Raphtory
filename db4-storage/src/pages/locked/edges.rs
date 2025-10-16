use std::ops::{Deref, DerefMut};

use crate::{
    LocalPOS,
    api::edges::EdgeSegmentOps,
    pages::{edge_page::writer::EdgeWriter, layer_counter::GraphStats, resolve_pos},
    segments::edge::MemEdgeSegment,
};
use parking_lot::RwLockWriteGuard;
use raphtory_core::entities::{EID, ELID};
use rayon::prelude::*;

#[derive(Debug)]
pub struct LockedEdgePage<'a, ES> {
    page_id: usize,
    max_page_len: u32,
    page: &'a ES,
    num_edges: &'a GraphStats,
    lock: RwLockWriteGuard<'a, MemEdgeSegment>,
}

impl<'a, EXT, ES: EdgeSegmentOps<Extension = EXT>> LockedEdgePage<'a, ES> {
    pub fn new(
        page_id: usize,
        max_page_len: u32,
        page: &'a ES,
        num_edges: &'a GraphStats,
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

    pub fn ensure_layer(&mut self, layer_id: usize) {
        self.lock.get_or_create_layer(layer_id);
    }
}
#[derive(Debug)]
pub struct WriteLockedEdgePages<'a, ES> {
    writers: Vec<LockedEdgePage<'a, ES>>,
}

impl<ES> Default for WriteLockedEdgePages<'_, ES> {
    fn default() -> Self {
        Self {
            writers: Vec::new(),
        }
    }
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

    pub fn ensure_layer(&mut self, layer_id: usize) {
        for writer in &mut self.writers {
            writer.ensure_layer(layer_id);
        }
    }

    pub fn exists(&self, elid: ELID) -> bool {
        let max_page_len = if !self.writers.is_empty() {
            self.writers[0].max_page_len
        } else {
            return false;
        };
        let (page_id, pos) = resolve_pos(elid.edge, max_page_len);
        self.writers
            .get(page_id)
            .and_then(|page| {
                let locked_head = page.lock.deref();
                page.page.get_edge(pos, elid.layer(), locked_head)
            })
            .is_some()
    }
}
