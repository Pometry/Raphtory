use super::{node_page::writer::NodeWriter, resolve_pos};
use crate::{
    LocalPOS,
    api::nodes::{LockedNSSegment, NodeSegmentOps},
    error::StorageError,
    pages::{
        layer_counter::GraphStats,
        locked::nodes::{LockedNodePage, WriteLockedNodePages},
    },
    segments::node::MemNodeSegment,
};
use parking_lot::RwLockWriteGuard;
use raphtory_api::core::entities::properties::meta::Meta;
use raphtory_core::entities::{EID, VID};
use rayon::prelude::*;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

// graph // (nodes|edges) // graph segments // layers // chunks

#[derive(Debug)]
pub struct NodeStorageInner<NS, EXT> {
    pages: boxcar::Vec<Arc<NS>>,
    stats: Arc<GraphStats>,
    nodes_path: PathBuf,
    max_page_len: usize,
    node_meta: Arc<Meta>,
    edge_meta: Arc<Meta>,
    ext: EXT,
}

#[derive(Debug)]
pub struct ReadLockedNodeStorage<NS: NodeSegmentOps<Extension = EXT>, EXT> {
    storage: Arc<NodeStorageInner<NS, EXT>>,
    locked_segments: Box<[NS::ArcLockedSegment]>,
}

impl<NS: NodeSegmentOps<Extension = EXT>, EXT: Send + Sync + Clone> ReadLockedNodeStorage<NS, EXT> {
    pub fn node_ref(
        &self,
        node: impl Into<VID>,
    ) -> <<NS as NodeSegmentOps>::ArcLockedSegment as LockedNSSegment>::EntryRef<'_> {
        let (page_id, pos) = self.storage.resolve_pos(node);
        let locked_page = &self.locked_segments[page_id];
        locked_page.entry_ref(pos)
    }

    pub fn len(&self) -> usize {
        self.storage.num_nodes()
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<
        Item = <<NS as NodeSegmentOps>::ArcLockedSegment as LockedNSSegment>::EntryRef<'_>,
    > + '_ {
        (0..self.len()).map(move |i| {
            let vid = VID(i);
            self.node_ref(vid)
        })
    }

    pub fn par_iter(
        &self,
    ) -> impl rayon::iter::ParallelIterator<
        Item = <<NS as NodeSegmentOps>::ArcLockedSegment as LockedNSSegment>::EntryRef<'_>,
    > + '_ {
        (0..self.len()).into_par_iter().map(move |i| {
            let vid = VID(i);
            self.node_ref(vid)
        })
    }
}

impl<NS: NodeSegmentOps<Extension = EXT>, EXT: Clone> NodeStorageInner<NS, EXT> {
    pub fn locked(self: &Arc<Self>) -> ReadLockedNodeStorage<NS, EXT> {
        let locked_segments = self
            .pages
            .iter()
            .map(|(_, segment)| segment.locked())
            .collect::<Box<_>>();
        ReadLockedNodeStorage {
            storage: self.clone(),
            locked_segments,
        }
    }

    pub fn new_with_meta(
        nodes_path: impl AsRef<Path>,
        max_page_len: usize,
        node_meta: Arc<Meta>,
        edge_meta: Arc<Meta>,
        ext: EXT,
    ) -> Self {
        Self {
            pages: boxcar::Vec::new(),
            stats: GraphStats::new().into(),
            nodes_path: nodes_path.as_ref().to_path_buf(),
            max_page_len,
            node_meta,
            edge_meta,
            ext,
        }
    }

    pub fn node_meta(&self) -> &Arc<Meta> {
        &self.node_meta
    }

    pub fn write_locked<'a>(&'a self) -> WriteLockedNodePages<'a, NS> {
        WriteLockedNodePages::new(
            self.pages
                .iter()
                .map(|(page_id, page)| {
                    LockedNodePage::new(
                        page_id,
                        &self.stats,
                        self.max_page_len,
                        page.as_ref(),
                        page.head_mut(),
                    )
                })
                .collect(),
        )
    }

    pub fn num_layers(&self) -> usize {
        self.stats.len()
    }

    pub fn node<'a>(&'a self, node: impl Into<VID>) -> NS::Entry<'a> {
        let (page_id, pos) = self.resolve_pos(node);
        let node_page = self
            .pages
            .get(page_id)
            .expect("Internal error: page not found");
        node_page.entry(pos)
    }

    pub fn prop_meta(&self) -> &Arc<Meta> {
        &self.node_meta
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

    pub fn num_nodes(&self) -> usize {
        self.stats.get(0)
    }

    pub fn layer_num_nodes(&self, layer_id: usize) -> usize {
        self.stats.get(layer_id)
    }

    pub fn stats(&self) -> &Arc<GraphStats> {
        &self.stats
    }

    pub fn segments(&self) -> &boxcar::Vec<Arc<NS>> {
        &self.pages
    }

    pub fn nodes_path(&self) -> &Path {
        &self.nodes_path
    }

    pub fn load(
        nodes_path: impl AsRef<Path>,
        max_page_len: usize,
        edge_meta: Arc<Meta>,
        ext: EXT,
    ) -> Result<Self, StorageError> {
        let nodes_path = nodes_path.as_ref();

        let node_meta = Arc::new(Meta::new());
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
                    max_page_len,
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
                        max_page_len,
                        node_meta.clone(),
                        edge_meta.clone(),
                        nodes_path,
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
                let count = page.layer_count(layer_id);
                if layer_counts.len() <= layer_id {
                    layer_counts.resize(layer_id + 1, 0);
                }
                layer_counts[layer_id] += count;
            }
        }

        Ok(Self {
            pages,
            nodes_path: nodes_path.to_path_buf(),
            max_page_len,
            stats: GraphStats::from(layer_counts).into(),
            node_meta,
            edge_meta,
            ext,
        })
    }

    /// Return the position of the chunk and the position within the chunk
    pub fn resolve_pos(&self, i: impl Into<VID>) -> (usize, LocalPOS) {
        resolve_pos(i.into(), self.max_page_len)
    }

    pub fn get_edge(&self, src: VID, dst: VID, layer_id: usize) -> Option<EID> {
        let (src_chunk, src_pos) = self.resolve_pos(src);
        if src_chunk >= self.pages.count() {
            return None;
        }
        let src_page = &self.pages[src_chunk];
        src_page.get_out_edge(src_pos, dst, layer_id, src_page.head())
    }

    pub fn grow(&self, new_len: usize) {
        self.get_or_create_segment(new_len - 1);
    }

    pub fn get_or_create_segment(&self, segment_id: usize) -> &Arc<NS> {
        if let Some(segment) = self.pages.get(segment_id) {
            return segment;
        }
        let count = self.pages.count();
        if count > segment_id {
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
                    Arc::new(NS::new(
                        segment_id,
                        self.max_page_len,
                        self.node_meta.clone(),
                        self.edge_meta.clone(),
                        self.nodes_path.clone(),
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
}
