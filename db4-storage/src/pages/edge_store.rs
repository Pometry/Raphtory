use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use super::{edge_page::writer::EdgeWriter, resolve_pos};
use crate::{
    LocalPOS,
    api::edges::{EdgeRefOps, EdgeSegmentOps, LockedESegment},
    error::StorageError,
    pages::{
        layer_counter::GraphStats,
        locked::edges::{LockedEdgePage, WriteLockedEdgePages},
    },
    persist::strategy::Config,
    segments::edge::MemEdgeSegment,
};
use parking_lot::{RwLock, RwLockWriteGuard};
use raphtory_api::core::entities::{EID, VID, properties::meta::Meta};
use raphtory_core::{
    entities::{ELID, LayerIds},
    storage::timeindex::{AsTime, TimeIndexEntry, TimeIndexOps},
};
use rayon::prelude::*;

const N: usize = 32;

#[derive(Debug)]
pub struct EdgeStorageInner<ES, EXT> {
    segments: boxcar::Vec<Arc<ES>>,
    layer_counter: Arc<GraphStats>,
    free_pages: Box<[RwLock<usize>; N]>,
    edges_path: Option<PathBuf>,
    prop_meta: Arc<Meta>,
    ext: EXT,
}

#[derive(Debug)]
pub struct ReadLockedEdgeStorage<ES: EdgeSegmentOps<Extension = EXT>, EXT> {
    storage: Arc<EdgeStorageInner<ES, EXT>>,
    locked_pages: Box<[ES::ArcLockedSegment]>,
}

impl<ES: EdgeSegmentOps<Extension = EXT>, EXT: Config> ReadLockedEdgeStorage<ES, EXT> {
    pub fn storage(&self) -> &EdgeStorageInner<ES, EXT> {
        &self.storage
    }

    pub fn edge_ref(
        &self,
        e_id: impl Into<EID>,
    ) -> <<ES as EdgeSegmentOps>::ArcLockedSegment as LockedESegment>::EntryRef<'_> {
        let e_id = e_id.into();
        let (page_id, pos) = self.storage.resolve_pos(e_id);
        let locked_page = &self.locked_pages[page_id];
        locked_page.entry_ref(pos)
    }

    pub fn iter<'a, 'b: 'a>(
        &'a self,
        layer_ids: &'b LayerIds,
    ) -> impl Iterator<
        Item = <<ES as EdgeSegmentOps>::ArcLockedSegment as LockedESegment>::EntryRef<'a>,
    > + 'a {
        self.locked_pages
            .iter()
            .flat_map(move |page| page.edge_iter(layer_ids))
    }

    pub fn par_iter<'a, 'b: 'a>(
        &'a self,
        layer_ids: &'b LayerIds,
    ) -> impl ParallelIterator<
        Item = <<ES as EdgeSegmentOps>::ArcLockedSegment as LockedESegment>::EntryRef<'a>,
    > + 'a {
        self.locked_pages
            .par_iter()
            .flat_map(move |page| page.edge_par_iter(layer_ids))
    }

    /// Returns an iterator over the segments of the edge store, where each segment is
    /// a tuple of the segment index and an iterator over the entries in that segment.
    pub fn segmented_par_iter(
        &self,
    ) -> impl ParallelIterator<Item = (usize, impl Iterator<Item = EID>)> + '_ {
        self.locked_pages
            .par_iter()
            .enumerate()
            .map(move |(segment_id, page)| {
                (
                    segment_id,
                    page.edge_iter(&LayerIds::All).map(|e| e.edge_id()),
                )
            })
    }
}

impl<ES: EdgeSegmentOps<Extension = EXT>, EXT: Config> EdgeStorageInner<ES, EXT> {
    pub fn locked(self: &Arc<Self>) -> ReadLockedEdgeStorage<ES, EXT> {
        let locked_pages = self
            .segments
            .iter()
            .map(|(_, segment)| segment.locked())
            .collect::<Box<_>>();
        ReadLockedEdgeStorage {
            storage: self.clone(),
            locked_pages,
        }
    }

    pub fn edge_meta(&self) -> &Arc<Meta> {
        &self.prop_meta
    }

    pub fn stats(&self) -> &Arc<GraphStats> {
        &self.layer_counter
    }

    pub fn new_with_meta(edges_path: Option<PathBuf>, edge_meta: Arc<Meta>, ext: EXT) -> Self {
        let free_pages = (0..N).map(RwLock::new).collect::<Box<[_]>>();
        Self {
            segments: boxcar::Vec::new(),
            layer_counter: GraphStats::new().into(),
            free_pages: free_pages.try_into().unwrap(),
            edges_path,
            prop_meta: edge_meta,
            ext,
        }
    }

    pub fn new(edges_path: Option<PathBuf>, ext: EXT) -> Self {
        Self::new_with_meta(edges_path, Meta::new_for_edges().into(), ext)
    }

    pub fn pages(&self) -> &boxcar::Vec<Arc<ES>> {
        &self.segments
    }

    pub fn edges_path(&self) -> Option<&Path> {
        self.edges_path.as_ref().map(|path| path.as_path())
    }

    pub fn earliest(&self) -> Option<TimeIndexEntry> {
        Iterator::min(self.segments.iter().filter_map(|(_, page)| page.earliest()))
        // see : https://github.com/rust-lang/rust-analyzer/issues/10653
    }

    pub fn latest(&self) -> Option<TimeIndexEntry> {
        Iterator::max(self.segments.iter().filter_map(|(_, page)| page.latest()))
    }

    pub fn t_len(&self) -> usize {
        self.segments.iter().map(|(_, page)| page.t_len()).sum()
    }

    pub fn prop_meta(&self) -> &Arc<Meta> {
        &self.prop_meta
    }

    #[inline(always)]
    pub fn resolve_pos(&self, e_id: EID) -> (usize, LocalPOS) {
        resolve_pos(e_id, self.max_page_len())
    }

    pub fn load(edges_path: impl AsRef<Path>, ext: EXT) -> Result<Self, StorageError> {
        let edges_path = edges_path.as_ref();
        let max_page_len = ext.max_edge_page_len();

        let meta = Arc::new(Meta::new_for_edges());
        if !edges_path.exists() {
            return Ok(Self::new(Some(edges_path.to_path_buf()), ext.clone()));
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
            return Err(StorageError::EmptyGraphDir(edges_path.to_path_buf()));
        }

        let max_page = Iterator::max(pages.keys().copied()).unwrap();

        let pages: boxcar::Vec<Arc<ES>> = (0..=max_page)
            .map(|page_id| {
                let np = pages.remove(&page_id).unwrap_or_else(|| {
                    ES::new(
                        page_id,
                        meta.clone(),
                        Some(edges_path.to_path_buf()),
                        ext.clone(),
                    )
                });
                Arc::new(np)
            })
            .collect::<boxcar::Vec<_>>();

        let first_page = pages.iter().next().unwrap().1;
        let first_p_id = first_page.segment_id();

        if first_p_id != 0 {
            return Err(StorageError::GenericFailure(format!(
                "First page id is not 0 in {edges_path:?}"
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

        let mut layer_counts = vec![];

        for (_, page) in pages.iter() {
            for layer_id in 0..page.num_layers() {
                let count = page.layer_count(layer_id) as usize;
                if layer_counts.len() <= layer_id {
                    layer_counts.resize(layer_id + 1, 0);
                }
                layer_counts[layer_id] += count;
            }
        }

        let earliest = pages
            .iter()
            .filter_map(|(_, page)| page.earliest().filter(|t| t.t() != i64::MAX))
            .map(|t| t.t())
            .min()
            .unwrap_or(i64::MAX);

        let latest = pages
            .iter()
            .filter_map(|(_, page)| page.latest().filter(|t| t.t() != i64::MIN))
            .map(|t| t.t())
            .max()
            .unwrap_or(i64::MIN);

        let stats = GraphStats::load(layer_counts, earliest, latest);

        Ok(Self {
            segments: pages,
            edges_path: Some(edges_path.to_path_buf()),
            layer_counter: stats.into(),
            free_pages: free_pages.try_into().unwrap(),
            prop_meta: meta,
            ext,
        })
    }

    pub fn grow(&self, size: usize) {
        self.get_or_create_segment(size - 1);
    }

    pub fn push_new_page(&self) -> usize {
        let segment_id = self.segments.push_with(|segment_id| {
            Arc::new(ES::new(
                segment_id,
                self.prop_meta.clone(),
                self.edges_path.clone(),
                self.ext.clone(),
            ))
        });

        while self.segments.get(segment_id).is_none() {
            // wait
        }
        segment_id
    }

    pub fn get_or_create_segment(&self, segment_id: usize) -> &Arc<ES> {
        if let Some(segment) = self.segments.get(segment_id) {
            return segment;
        }
        let count = self.segments.count();
        if count > segment_id {
            // something has allocated the segment, wait for it to be added
            loop {
                if let Some(segment) = self.segments.get(segment_id) {
                    return segment;
                } else {
                    // wait for the segment to be created
                    std::thread::yield_now();
                }
            }
        } else {
            // we need to create the segment
            self.segments.reserve(segment_id + 1 - count);

            loop {
                let new_segment_id = self.segments.push_with(|segment_id| {
                    Arc::new(ES::new(
                        segment_id,
                        self.prop_meta.clone(),
                        self.edges_path.clone(),
                        self.ext.clone(),
                    ))
                });

                if new_segment_id >= segment_id {
                    loop {
                        if let Some(segment) = self.segments.get(segment_id) {
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

    pub fn max_page_len(&self) -> u32 {
        self.ext.max_edge_page_len()
    }

    pub fn write_locked<'a>(&'a self) -> WriteLockedEdgePages<'a, ES> {
        WriteLockedEdgePages::new(
            self.segments
                .iter()
                .map(|(page_id, page)| {
                    LockedEdgePage::new(
                        page_id,
                        self.max_page_len(),
                        page.as_ref(),
                        &self.layer_counter,
                        page.head_mut(),
                    )
                })
                .collect(),
        )
    }

    /// Retrieve the segment for an edge given its EID
    pub fn get_edge_segment(&self, eid: EID) -> Option<&Arc<ES>> {
        let (segment_id, _) = resolve_pos(eid, self.max_page_len());
        self.segments.get(segment_id)
    }

    pub fn get_edge(&self, e_id: ELID) -> Option<(VID, VID)> {
        let layer = e_id.layer();
        let e_id = e_id.edge;
        let (segment_id, local_edge) = resolve_pos(e_id, self.max_page_len());
        let segment = self.segments.get(segment_id)?;
        segment.get_edge(local_edge, layer, segment.head())
    }

    pub fn edge(&self, e_id: impl Into<EID>) -> ES::Entry<'_> {
        let e_id = e_id.into();
        let (segment_id, local_edge) = resolve_pos(e_id, self.max_page_len());
        let segment = self.segments.get(segment_id).unwrap_or_else(|| {
            panic!(
                "{e_id:?} Not found in seg: {segment_id}, pos: {local_edge:?}, num_segments: {}",
                self.segments.count()
            )
        });
        segment.entry(local_edge)
    }

    pub fn num_edges(&self) -> usize {
        self.layer_counter.get(0)
    }

    pub fn num_edges_layer(&self, layer_id: usize) -> usize {
        self.layer_counter.get(layer_id)
    }

    pub fn get_writer<'a>(
        &'a self,
        e_id: EID,
    ) -> EdgeWriter<'a, RwLockWriteGuard<'a, MemEdgeSegment>, ES> {
        let (chunk, _) = resolve_pos(e_id, self.max_page_len());
        let page = self.get_or_create_segment(chunk);
        EdgeWriter::new(&self.layer_counter, page, page.head_mut())
    }

    pub fn try_get_writer<'a>(
        &'a self,
        e_id: EID,
    ) -> Result<EdgeWriter<'a, RwLockWriteGuard<'a, MemEdgeSegment>, ES>, StorageError> {
        let (segment_id, _) = resolve_pos(e_id, self.max_page_len());
        let page = self.get_or_create_segment(segment_id);
        let writer = page.head_mut();
        Ok(EdgeWriter::new(&self.layer_counter, page, writer))
    }

    pub fn get_free_writer<'a>(
        &'a self,
    ) -> EdgeWriter<'a, RwLockWriteGuard<'a, MemEdgeSegment>, ES> {
        // optimistic first try to get a free page 3 times
        let num_edges = self.num_edges();
        let slot_idx = num_edges as usize % N;
        let maybe_free_page = self.free_pages[slot_idx..]
            .iter()
            .cycle()
            .take(3)
            .filter_map(|lock| lock.try_read())
            .filter_map(|page_id| {
                let page = self.segments.get(*page_id)?;
                let guard = page.try_head_mut()?;
                if page.num_edges() < self.max_page_len() {
                    Some((page, guard))
                } else {
                    None
                }
            })
            .next();

        if let Some((edge_page, writer)) = maybe_free_page {
            EdgeWriter::new(&self.layer_counter, edge_page, writer)
        } else {
            // not lucky, go wait on your slot
            loop {
                let mut slot = self.free_pages[slot_idx].write();
                match self.segments.get(*slot).map(|page| (page, page.head_mut())) {
                    Some((edge_page, writer)) if edge_page.num_edges() < self.max_page_len() => {
                        return EdgeWriter::new(&self.layer_counter, edge_page, writer);
                    }
                    _ => {
                        *slot = self.push_new_page();
                    }
                }
            }
        }
    }

    pub fn par_iter(&self, layer: usize) -> impl ParallelIterator<Item = ES::Entry<'_>> + '_ {
        (0..self.segments.count())
            .into_par_iter()
            .filter_map(move |page_id| self.segments.get(page_id))
            .flat_map(move |page| {
                (0..page.num_edges())
                    .into_par_iter()
                    .map(LocalPOS)
                    .filter_map(move |local_edge| {
                        page.layer_entry(local_edge, layer, Some(page.head()))
                    })
            })
    }

    pub fn iter(&self, layer: usize) -> impl Iterator<Item = ES::Entry<'_>> + '_ {
        (0..self.segments.count())
            .filter_map(move |page_id| self.segments.get(page_id))
            .flat_map(move |page| {
                (0..page.num_edges()).filter_map(move |local_edge| {
                    page.layer_entry(LocalPOS(local_edge), layer, Some(page.head()))
                })
            })
    }

    /// Returns an iterator over the segments of the edge store, where each segment is
    /// a tuple of the segment index and an iterator over the entries in that segment.
    pub fn segmented_par_iter(
        &self,
    ) -> impl ParallelIterator<Item = (usize, impl Iterator<Item = EID>)> + '_ {
        let max_page_len = self.max_page_len();
        (0..self.segments.count())
            .into_par_iter()
            .filter_map(move |segment_id| {
                self.segments.get(segment_id).map(move |page| {
                    (
                        segment_id,
                        (0..page.num_edges()).map(move |edge_pos| {
                            LocalPOS(edge_pos).as_eid(segment_id, max_page_len)
                        }),
                    )
                })
            })
    }
}
