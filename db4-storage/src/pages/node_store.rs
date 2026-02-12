use super::{node_page::writer::NodeWriter, resolve_pos};
use crate::{
    LocalPOS,
    api::nodes::{LockedNSSegment, NodeSegmentOps},
    error::StorageError,
    pages::{
        SegmentCounts,
        layer_counter::GraphStats,
        locked::nodes::{LockedNodePage, WriteLockedNodePages},
        row_group_par_iter,
    },
    persist::{config::ConfigOps, strategy::PersistenceStrategy},
    segments::node::segment::MemNodeSegment,
};
use parking_lot::{RwLock, RwLockWriteGuard};
use raphtory_api::core::entities::{GidType, properties::meta::Meta};
use raphtory_core::{
    entities::{EID, VID},
    storage::timeindex::AsTime,
};
use rayon::prelude::*;
use std::{
    collections::HashMap,
    ops::Deref,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicU32},
};

// graph // (nodes|edges) // graph segments // layers // chunks
pub const N: usize = 32;

#[derive(Debug)]
pub struct NodeStorageInner<NS, EXT> {
    segments: boxcar::Vec<Arc<NS>>,
    stats: Arc<GraphStats>,
    free_segments: Box<[RwLock<usize>; N]>,
    nodes_path: Option<PathBuf>,
    node_meta: Arc<Meta>,
    edge_meta: Arc<Meta>,
    ext: EXT,
}

#[derive(Debug)]
pub struct ReadLockedNodeStorage<NS: NodeSegmentOps<Extension = EXT>, EXT> {
    storage: Arc<NodeStorageInner<NS, EXT>>,
    locked_segments: Box<[NS::ArcLockedSegment]>,
}

impl<NS: NodeSegmentOps<Extension = EXT>, EXT: PersistenceStrategy> ReadLockedNodeStorage<NS, EXT> {
    pub fn node_ref(
        &self,
        node: impl Into<VID>,
    ) -> <<NS as NodeSegmentOps>::ArcLockedSegment as LockedNSSegment>::EntryRef<'_> {
        let (segment_id, pos) = self.storage.resolve_pos(node);
        let locked_segment = &self.locked_segments[segment_id];
        locked_segment.entry_ref(pos)
    }

    pub fn try_node_ref(
        &self,
        node: VID,
    ) -> Option<<<NS as NodeSegmentOps>::ArcLockedSegment as LockedNSSegment>::EntryRef<'_>> {
        let (segment_id, pos) = self.storage.resolve_pos(node);
        let locked_segment = &self.locked_segments.get(segment_id)?;
        if pos.0 < locked_segment.num_nodes() {
            Some(locked_segment.entry_ref(pos))
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.storage.num_nodes()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<
        Item = <<NS as NodeSegmentOps>::ArcLockedSegment as LockedNSSegment>::EntryRef<'_>,
    > + '_ {
        self.locked_segments
            .iter()
            .flat_map(move |segment| segment.iter_entries())
    }

    pub fn par_iter(
        &self,
    ) -> impl rayon::iter::ParallelIterator<
        Item = <<NS as NodeSegmentOps>::ArcLockedSegment as LockedNSSegment>::EntryRef<'_>,
    > + '_ {
        self.locked_segments
            .par_iter()
            .flat_map(move |segment| segment.par_iter_entries())
    }

    pub fn row_groups_par_iter(
        &self,
    ) -> impl IndexedParallelIterator<Item = (usize, impl Iterator<Item = VID> + '_)> {
        let max_actual_seg_len = self
            .locked_segments
            .iter()
            .map(|seg| seg.num_nodes())
            .max()
            .unwrap_or(0);
        row_group_par_iter(
            self.storage.max_segment_len() as usize,
            self.locked_segments.len(),
            self.storage.max_segment_len(),
            max_actual_seg_len,
        )
        .map(|(s_id, iter)| (s_id, iter.filter(|vid| self.has_vid(*vid))))
    }

    fn has_vid(&self, vid: VID) -> bool {
        let (segment_id, pos) = self.storage.resolve_pos(vid);
        segment_id < self.locked_segments.len()
            && pos.0 < self.locked_segments[segment_id].num_nodes()
    }
}

impl<NS: Send + Sync, EXT: PersistenceStrategy> NodeStorageInner<NS, EXT> {
    pub fn prop_meta(&self) -> &Arc<Meta> {
        &self.node_meta
    }

    pub fn num_layers(&self) -> usize {
        self.stats.len()
    }

    pub fn num_nodes(&self) -> usize {
        self.stats.get(0)
    }

    // FIXME: this should be called by the high level APIs on layer filter
    pub fn layer_num_nodes(&self, layer_id: usize) -> usize {
        self.stats.get(layer_id)
    }

    pub fn stats(&self) -> &Arc<GraphStats> {
        &self.stats
    }

    pub fn segments(&self) -> &boxcar::Vec<Arc<NS>> {
        &self.segments
    }

    fn segments_par_iter(&self) -> impl ParallelIterator<Item = &NS> {
        let len = self.segments.count();
        (0..len)
            .into_par_iter()
            .filter_map(|idx| self.segments.get(idx).map(|seg| seg.deref()))
    }

    pub fn nodes_path(&self) -> Option<&Path> {
        self.nodes_path.as_deref()
    }

    /// Return the position of the chunk and the position within the chunk
    pub fn resolve_pos(&self, i: impl Into<VID>) -> (usize, LocalPOS) {
        resolve_pos(i.into(), self.max_segment_len())
    }

    pub fn max_segment_len(&self) -> u32 {
        self.ext.config().max_node_page_len()
    }
}

impl<NS: NodeSegmentOps<Extension = EXT>, EXT: PersistenceStrategy> NodeStorageInner<NS, EXT> {
    pub fn new_with_meta(
        nodes_path: Option<PathBuf>,
        node_meta: Arc<Meta>,
        edge_meta: Arc<Meta>,
        ext: EXT,
    ) -> Self {
        let free_segments = (0..N).map(RwLock::new).collect::<Box<[_]>>();
        let empty = Self {
            segments: boxcar::Vec::new(),
            stats: GraphStats::new().into(),
            free_segments: free_segments.try_into().unwrap(),
            nodes_path,
            node_meta,
            edge_meta,
            ext,
        };
        let layer_mapper = empty.node_meta.layer_meta();
        let prop_mapper = empty.node_meta.temporal_prop_mapper();
        let metadata_mapper = empty.node_meta.metadata_mapper();
        if layer_mapper.num_fields() > 0
            || prop_mapper.num_fields() > 0
            || metadata_mapper.num_fields() > 0
        {
            let segment = empty.get_or_create_segment(0);
            let mut head = segment.head_mut();
            if prop_mapper.num_fields() > 0 {
                head.get_or_create_layer(0)
                    .properties_mut()
                    .set_has_properties()
            }
            segment.set_dirty(true);
        }
        empty
    }

    pub fn locked(self: &Arc<Self>) -> ReadLockedNodeStorage<NS, EXT> {
        let locked_segments = self
            .segments
            .iter()
            .map(|(_, segment)| segment.locked())
            .collect::<Box<_>>();
        ReadLockedNodeStorage {
            storage: self.clone(),
            locked_segments,
        }
    }

    pub fn write_locked<'a>(&'a self) -> WriteLockedNodePages<'a, NS> {
        WriteLockedNodePages::new(
            self.segments
                .iter()
                .map(|(page_id, page)| {
                    LockedNodePage::new(
                        page_id,
                        &self.stats,
                        self.max_segment_len(),
                        page.as_ref(),
                        page.head_mut(),
                    )
                })
                .collect(),
        )
    }

    pub fn reserve_vid(&self, row: usize) -> VID {
        let (seg, pos) = self.reserve_free_pos(row);
        pos.as_vid(seg, self.max_segment_len())
    }

    pub fn reserve_free_pos(&self, row: usize) -> (usize, LocalPOS) {
        let slot_idx = row % N;
        let maybe_free_page = {
            let page_id = *self.free_segments[slot_idx].read_recursive();
            let page = self.segments.get(page_id);

            page.and_then(|page| {
                self.reserve_segment_row(page)
                    .map(|pos| (page.segment_id(), LocalPOS(pos)))
            })
        };

        if let Some(reserved_pos) = maybe_free_page {
            reserved_pos
        } else {
            // not lucky, go wait on your slot
            let mut slot = self.free_segments[slot_idx].write();
            loop {
                if let Some(page) = self.segments.get(*slot)
                    && let Some(pos) = self.reserve_segment_row(page)
                {
                    return (page.segment_id(), LocalPOS(pos));
                }
                *slot = self.push_new_segment();
            }
        }
    }

    pub fn reserve_and_lock_segment(
        &self,
        row: usize,
        required_space: u32,
    ) -> (
        LocalPOS,
        NodeWriter<'_, RwLockWriteGuard<'_, MemNodeSegment>, NS>,
    ) {
        let slot_idx = row % N;
        let mut page_id = *self.free_segments[slot_idx].read_recursive();
        let mut writer = self.writer(page_id);
        match self.reserve_segment_rows(writer.page, required_space) {
            None => {
                // segment is full, we need to create a new one
                let mut slot = self.free_segments[slot_idx].write();
                if *slot == page_id {
                    // page_id is unchanged, no other thread created a new segment before we got the lock
                    page_id = self.push_new_segment();
                }
                loop {
                    writer = self.writer(page_id);
                    match self.reserve_segment_rows(writer.page, required_space) {
                        None => page_id = self.push_new_segment(),
                        Some(local_pos) => {
                            *slot = page_id;
                            return (LocalPOS(local_pos), writer);
                        }
                    }
                }
            }
            Some(local_pos) => (LocalPOS(local_pos), writer),
        }
    }

    fn reserve_segment_row(&self, segment: &NS) -> Option<u32> {
        // TODO: if this becomes a hotspot, we can switch to a fetch_add followed by a fetch_min
        // this means when we read the counter we need to clamp it to max_page_len so the iterators don't break
        increment_and_clamp(segment.nodes_counter(), 1, self.max_segment_len())
    }

    fn reserve_segment_rows(&self, segment: &NS, rows: u32) -> Option<u32> {
        increment_and_clamp(segment.nodes_counter(), rows, self.max_segment_len())
    }

    fn push_new_segment(&self) -> usize {
        let segment_id = self.segments.push_with(|segment_id| {
            Arc::new(NS::new(
                segment_id,
                self.node_meta.clone(),
                self.edge_meta.clone(),
                self.nodes_path.clone(),
                self.ext.clone(),
            ))
        });

        while self.segments.get(segment_id).is_none() {
            std::thread::yield_now();
        }

        segment_id
    }

    pub fn node<'a>(&'a self, node: impl Into<VID>) -> NS::Entry<'a> {
        let (page_id, pos) = self.resolve_pos(node);
        let node_page = self
            .segments
            .get(page_id)
            .expect("Internal error: page not found");
        node_page.entry(pos)
    }

    pub fn try_node(&self, node: VID) -> Option<NS::Entry<'_>> {
        let (page_id, pos) = self.resolve_pos(node);
        let node_page = self.segments.get(page_id)?;
        if pos.0 < node_page.num_nodes() {
            Some(node_page.entry(pos))
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn writer<'a>(
        &'a self,
        segment_id: usize,
    ) -> NodeWriter<'a, RwLockWriteGuard<'a, MemNodeSegment>, NS> {
        let segment = self.get_or_create_segment(segment_id);
        let head = segment.head_mut();
        NodeWriter::new(segment, &self.stats, head)
    }

    pub fn try_writer<'a>(
        &'a self,
        segment_id: usize,
    ) -> Option<NodeWriter<'a, RwLockWriteGuard<'a, MemNodeSegment>, NS>> {
        let segment = &self.segments[segment_id];
        let head = segment.try_head_mut()?;
        Some(NodeWriter::new(segment, &self.stats, head))
    }

    pub fn id_type(&self) -> Option<GidType> {
        self.node_meta
            .metadata_mapper()
            .d_types()
            .first()
            .and_then(|dtype| GidType::from_prop_type(dtype))
    }

    pub fn load(
        nodes_path: impl AsRef<Path>,
        edge_meta: Arc<Meta>,
        ext: EXT,
    ) -> Result<Self, StorageError> {
        let nodes_path = nodes_path.as_ref();
        let max_page_len = ext.config().max_node_page_len();
        let node_meta = Arc::new(Meta::new_for_nodes());

        if !nodes_path.exists() {
            return Ok(Self::new_with_meta(
                Some(nodes_path.to_path_buf()),
                node_meta,
                edge_meta,
                ext.clone(),
            ));
        }

        let mut pages = std::fs::read_dir(nodes_path)?
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
                let page = NS::load(
                    page_id,
                    node_meta.clone(),
                    edge_meta.clone(),
                    nodes_path,
                    ext.clone(),
                )
                .map(|page| (page_id, page));
                Some(page)
            })
            .collect::<Result<HashMap<_, _>, _>>()?;

        if pages.is_empty() {
            return Err(StorageError::EmptyGraphDir(nodes_path.to_path_buf()));
        }

        let max_page = Iterator::max(pages.keys().copied()).unwrap();

        let pages = (0..=max_page)
            .map(|page_id| {
                let np = pages.remove(&page_id).unwrap_or_else(|| {
                    NS::new(
                        page_id,
                        node_meta.clone(),
                        edge_meta.clone(),
                        Some(nodes_path.to_path_buf()),
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
                "First page id is not 0 in {nodes_path:?}"
            )));
        }

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

        let mut free_pages = pages
            .iter()
            .filter_map(|(_, page)| {
                let len = page.num_nodes();
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

        let stats = GraphStats::load(layer_counts, earliest, latest);

        Ok(Self {
            segments: pages,
            free_segments: free_pages.try_into().unwrap(),
            nodes_path: Some(nodes_path.to_path_buf()),
            stats: stats.into(),
            node_meta,
            edge_meta,
            ext,
        })
    }

    pub fn get_edge(&self, src: VID, dst: VID, layer_id: usize) -> Option<EID> {
        let (src_chunk, src_pos) = self.resolve_pos(src);
        if src_chunk >= self.segments.count() {
            return None;
        }
        let src_page = &self.segments[src_chunk];
        src_page.get_out_edge(src_pos, dst, layer_id, src_page.head())
    }

    pub fn grow(&self, new_len: usize) {
        self.get_or_create_segment(new_len - 1);
    }

    pub fn get_or_create_segment(&self, segment_id: usize) -> &Arc<NS> {
        if let Some(segment) = self.segments.get(segment_id) {
            return segment;
        }

        let count = self.segments.count();

        if count > segment_id {
            // Something has allocated the segment, wait for it to be added.
            loop {
                if let Some(segment) = self.segments.get(segment_id) {
                    return segment;
                } else {
                    // Wait for the segment to be created.
                    std::thread::yield_now();
                }
            }
        } else {
            // we need to create the segment.
            self.segments.reserve(segment_id + 1 - count);

            loop {
                let new_segment_id = self.segments.push_with(|segment_id| {
                    Arc::new(NS::new(
                        segment_id,
                        self.node_meta.clone(),
                        self.edge_meta.clone(),
                        self.nodes_path.clone(),
                        self.ext.clone(),
                    ))
                });

                if new_segment_id >= segment_id {
                    loop {
                        if let Some(segment) = self.segments.get(segment_id) {
                            return segment;
                        } else {
                            // Wait for the segment to be created.
                            std::thread::yield_now();
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn segment_counts(&self) -> SegmentCounts<VID> {
        SegmentCounts::new(
            self.max_segment_len(),
            self.segments().iter().map(|(_, seg)| seg.num_nodes()),
        )
    }

    pub(crate) fn flush(&self) -> Result<(), StorageError> {
        self.segments_par_iter().try_for_each(|seg| seg.flush())
    }
}

pub fn increment_and_clamp(
    counter: &AtomicU32,
    increment: u32,
    max_segment_len: u32,
) -> Option<u32> {
    counter
        .fetch_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |current| {
                let updated = current + increment;
                if updated <= max_segment_len {
                    Some(updated)
                } else {
                    None
                }
            },
        )
        .ok()
}
