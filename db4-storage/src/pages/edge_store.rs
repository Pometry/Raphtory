use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{self, AtomicUsize},
    },
};

use super::{edge_page::writer::EdgeWriter, resolve_pos};
use crate::{
    EdgeSegmentOps, LocalPOS, ReadLockedES, error::DBV4Error, segments::edge::MemEdgeSegment,
};
use parking_lot::{RwLock, RwLockWriteGuard};
use raphtory_api::core::entities::{EID, VID, properties::meta::Meta};
use raphtory_core::storage::timeindex::TimeIndexEntry;

const N: usize = 32;

#[derive(Debug)]
pub struct EdgeStorageInner<ES, EXT> {
    pages: boxcar::Vec<Arc<ES>>,
    num_edges: AtomicUsize,
    free_pages: Box<[RwLock<usize>; N]>,
    edges_path: PathBuf,
    max_page_len: usize,
    prop_meta: Arc<Meta>,
    ext: EXT,
}

#[derive(Debug)]
pub struct ReadLockedEdgeStorage<ES, EXT> {
    storage: Arc<EdgeStorageInner<ES, EXT>>,
    locked_pages: Box<[ReadLockedES<ES>]>,
}

impl<ES: EdgeSegmentOps<Extension = EXT>, EXT: Clone> EdgeStorageInner<ES, EXT> {
    pub fn locked(self: &Arc<Self>) -> ReadLockedEdgeStorage<ES, EXT> {
        let locked_pages = self
            .pages
            .iter()
            .map(|(_, segment)| segment.locked())
            .collect::<Box<_>>();
        ReadLockedEdgeStorage {
            storage: self.clone(),
            locked_pages,
        }
    }

    pub fn layer(
        edges_path: impl AsRef<Path>,
        max_page_len: usize,
        meta: &Arc<Meta>,
        ext: EXT,
    ) -> Self {
        let free_pages = (0..N).map(RwLock::new).collect::<Box<[_]>>();
        Self {
            pages: boxcar::Vec::new(),
            num_edges: AtomicUsize::new(0),
            free_pages: free_pages.try_into().unwrap(),
            edges_path: edges_path.as_ref().to_path_buf(),
            max_page_len,
            prop_meta: meta.clone(),
            ext,
        }
    }

    pub fn new(edges_path: impl AsRef<Path>, max_page_len: usize, ext: EXT) -> Self {
        let free_pages = (0..N).map(RwLock::new).collect::<Box<[_]>>();
        Self {
            pages: boxcar::Vec::new(),
            num_edges: AtomicUsize::new(0),
            free_pages: free_pages.try_into().unwrap(),
            edges_path: edges_path.as_ref().to_path_buf(),
            max_page_len,
            prop_meta: Arc::new(Meta::new()),
            ext,
        }
    }

    pub fn pages(&self) -> &boxcar::Vec<Arc<ES>> {
        &self.pages
    }

    pub fn edges_path(&self) -> &Path {
        &self.edges_path
    }

    pub fn earliest(&self) -> Option<TimeIndexEntry> {
        Iterator::min(self.pages.iter().filter_map(|(_, page)| page.earliest()))
        // see : https://github.com/rust-lang/rust-analyzer/issues/10653
    }

    pub fn latest(&self) -> Option<TimeIndexEntry> {
        Iterator::max(self.pages.iter().filter_map(|(_, page)| page.latest()))
    }

    pub fn t_len(&self) -> usize {
        self.pages.iter().map(|(_, page)| page.t_len()).sum()
    }

    pub fn prop_meta(&self) -> &Arc<Meta> {
        &self.prop_meta
    }

    #[inline(always)]
    pub fn resolve_pos(&self, e_id: EID) -> (usize, LocalPOS) {
        resolve_pos(e_id, self.max_page_len)
    }

    pub fn load(
        edges_path: impl AsRef<Path>,
        max_page_len: usize,
        ext: EXT,
    ) -> Result<Self, DBV4Error> {
        let edges_path = edges_path.as_ref();

        let meta = Arc::new(Meta::new());
        if !edges_path.exists() {
            return Ok(Self::new(edges_path, max_page_len, ext.clone()));
        }
        let mut pages = std::fs::read_dir(edges_path)?
            .filter(|entry| {
                entry
                    .as_ref()
                    .ok()
                    .and_then(|entry| entry.file_type().ok().map(|ft| ft.is_dir()))
                    .unwrap_or_default()
            })
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let page_id = entry
                    .path()
                    .file_stem()
                    .and_then(|name| name.to_str().and_then(|name| name.parse::<usize>().ok()))?;
                let page = ES::load(page_id, max_page_len, meta.clone(), edges_path, ext.clone())
                    .map(|page| (page_id, page));
                Some(page)
            })
            .collect::<Result<HashMap<_, _>, _>>()?;

        if pages.is_empty() {
            return Err(DBV4Error::EmptyGraphDir(edges_path.to_path_buf()));
        }

        let max_page = Iterator::max(pages.keys().copied()).unwrap();

        let pages: boxcar::Vec<Arc<ES>> = (0..=max_page)
            .map(|page_id| {
                let np = pages.remove(&page_id).unwrap_or_else(|| {
                    ES::new(page_id, max_page_len, meta.clone(), edges_path, ext.clone())
                });
                Arc::new(np)
            })
            .collect::<boxcar::Vec<_>>();

        let first_page = pages.iter().next().unwrap().1;
        let first_p_id = first_page.segment_id();

        if first_p_id != 0 {
            return Err(DBV4Error::GenericFailure(format!(
                "First page id is not 0 in {:?}",
                edges_path
            )));
        }

        let mut free_pages = pages
            .iter()
            .filter_map(|(_, page)| {
                let len = page.num_edges();
                if len < max_page_len {
                    Some(RwLock::new(page.segment_id()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let mut next_free_page = free_pages
            .last()
            .map(|page| *(page.read()))
            .map(|last| last + 1)
            .unwrap_or_else(|| pages.count());
        free_pages.resize_with(N, || {
            let lock = RwLock::new(next_free_page);
            next_free_page += 1;
            lock
        });

        let num_edges = pages.iter().map(|(_, page)| page.num_edges()).sum();

        Ok(Self {
            pages,
            edges_path: edges_path.to_path_buf(),
            max_page_len,
            num_edges: AtomicUsize::new(num_edges),
            free_pages: free_pages.try_into().unwrap(),
            prop_meta: meta,
            ext,
        })
    }

    pub fn grow(&self, size: usize) {
        self.get_or_create_segment(size - 1);
    }

    pub fn push_new_page(&self) -> usize {
        let segment_id = self.pages.push_with(|segment_id| {
            Arc::new(ES::new(
                segment_id,
                self.max_page_len,
                self.prop_meta.clone(),
                self.edges_path.clone(),
                self.ext.clone(),
            ))
        });

        while self.pages.get(segment_id).is_none() {
            // wait
        }
        segment_id
    }

    pub fn get_or_create_segment(&self, segment_id: usize) -> &Arc<ES> {
        if let Some(segment) = self.pages.get(segment_id) {
            return segment;
        }
        let count = self.pages.count();
        if count >= segment_id + 1 {
            // something has allocated the segment, wait for it to be added
            loop {
                if let Some(segment) = self.pages.get(segment_id) {
                    return segment;
                } else {
                    // wait for the segment to be created
                    std::thread::yield_now();
                }
            }
        } else {
            // we need to create the segment
            self.pages.reserve(segment_id + 1 - count);

            loop {
                let new_segment_id = self.pages.push_with(|segment_id| {
                    Arc::new(ES::new(
                        segment_id,
                        self.max_page_len,
                        self.prop_meta.clone(),
                        self.edges_path.clone(),
                        self.ext.clone(),
                    ))
                });

                if new_segment_id >= segment_id {
                    loop {
                        if let Some(segment) = self.pages.get(segment_id) {
                            return segment;
                        } else {
                            // wait for the segment to be created
                            std::thread::yield_now();
                        }
                    }
                }
            }
        }
    }

    pub fn max_page_len(&self) -> usize {
        self.max_page_len
    }

    // pub fn locked<'a>(&'a self) -> WriteLockedEdgePages<'a, ES> {
    //     WriteLockedEdgePages::new(
    //         self.pages
    //             .iter()
    //             .map(|(page_id, page)| {
    //                 LockedEdgePage::new(
    //                     page_id,
    //                     self.max_page_len,
    //                     page.as_ref(),
    //                     &self.num_edges,
    //                     page.head_mut(),
    //                 )
    //             })
    //             .collect(),
    //     )
    // }

    pub fn get_edge(&self, e_id: EID) -> Option<(VID, VID)> {
        let (chunk, local_edge) = resolve_pos(e_id, self.max_page_len);
        let page = self.pages.get(chunk)?;
        page.get_edge(local_edge, page.head())
    }

    pub fn edge(&self, e_id: impl Into<EID>) -> ES::Entry<'_> {
        let e_id = e_id.into();
        let (page_id, local_edge) = resolve_pos(e_id, self.max_page_len);
        let page = self
            .pages
            .get(page_id)
            .expect("Internal error: page not found");
        page.entry(local_edge)
    }

    pub fn num_edges(&self) -> usize {
        self.num_edges.load(atomic::Ordering::Relaxed)
    }

    pub fn get_writer<'a>(
        &'a self,
        e_id: EID,
    ) -> EdgeWriter<'a, RwLockWriteGuard<'a, MemEdgeSegment>, ES> {
        let (chunk, _) = resolve_pos(e_id, self.max_page_len);
        let page = self.get_or_create_segment(chunk);
        EdgeWriter::new(&self.num_edges, page, page.head_mut())
    }

    pub fn try_get_writer<'a>(
        &'a self,
        e_id: EID,
    ) -> Result<EdgeWriter<'a, RwLockWriteGuard<'a, MemEdgeSegment>, ES>, DBV4Error> {
        let (segment_id, _) = resolve_pos(e_id, self.max_page_len);
        let page = self.get_or_create_segment(segment_id);
        let writer = page.head_mut();
        Ok(EdgeWriter::new(&self.num_edges, page, writer))
    }

    pub fn get_free_writer<'a>(
        &'a self,
    ) -> EdgeWriter<'a, RwLockWriteGuard<'a, MemEdgeSegment>, ES> {
        // optimistic first try to get a free page 3 times
        let num_edges = self.num_edges();
        let slot_idx = num_edges % N;
        let maybe_free_page = self.free_pages[slot_idx..]
            .iter()
            .cycle()
            .take(3)
            .filter_map(|lock| lock.try_read())
            .filter_map(|page_id| {
                let page = self.pages.get(*page_id)?;
                let guard = page.try_head_mut()?;
                if page.num_edges() < self.max_page_len {
                    Some((page, guard))
                } else {
                    None
                }
            })
            .next();

        if let Some((edge_page, writer)) = maybe_free_page {
            EdgeWriter::new(&self.num_edges, edge_page, writer)
        } else {
            // not lucky, go wait on your slot
            loop {
                let mut slot = self.free_pages[slot_idx].write();
                match self.pages.get(*slot).map(|page| (page, page.head_mut())) {
                    Some((edge_page, writer)) if edge_page.num_edges() < self.max_page_len => {
                        return EdgeWriter::new(&self.num_edges, edge_page, writer);
                    }
                    _ => {
                        *slot = self.push_new_page();
                    }
                }
            }
        }
    }
}
