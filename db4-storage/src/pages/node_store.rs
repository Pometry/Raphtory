use super::{node_page::writer::NodeWriter, resolve_pos};
use crate::{
    LocalPOS, NodeSegmentOps, ReadLockedNS, error::DBV4Error, pages::layer_counter::LayerCounter,
    segments::node::MemNodeSegment,
};
use parking_lot::RwLockWriteGuard;
use raphtory_api::core::entities::properties::meta::Meta;
use raphtory_core::entities::{EID, VID};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

// graph // (nodes|edges) // graph segments // layers // chunks

#[derive(Debug)]
pub struct NodeStorageInner<NS, EXT> {
    pages: boxcar::Vec<Arc<NS>>,
    layer_counter: LayerCounter,
    nodes_path: PathBuf,
    max_page_len: usize,
    prop_meta: Arc<Meta>,
    ext: EXT,
}

#[derive(Debug)]
pub struct ReadLockedNodeStorage<NS, EXT> {
    storage: Arc<NodeStorageInner<NS, EXT>>,
    locked_pages: Box<[ReadLockedNS<NS>]>,
}

impl<NS: NodeSegmentOps<Extension = EXT>, EXT: Clone> NodeStorageInner<NS, EXT> {
    pub fn locked(self: &Arc<Self>) -> ReadLockedNodeStorage<NS, EXT> {
        let locked_pages = self
            .pages
            .iter()
            .map(|(_, segment)| segment.locked())
            .collect::<Box<_>>();
        ReadLockedNodeStorage {
            storage: self.clone(),
            locked_pages,
        }
    }

    pub fn new(nodes_path: impl AsRef<Path>, max_page_len: usize, ext: EXT) -> Self {
        Self {
            pages: boxcar::Vec::new(),
            layer_counter: LayerCounter::new(),
            nodes_path: nodes_path.as_ref().to_path_buf(),
            max_page_len,
            prop_meta: Arc::new(Meta::new()),
            ext,
        }
    }

    // pub fn locked<'a>(&'a self) -> WriteLockedNodePages<'a, NS> {
    //     WriteLockedNodePages::new(
    //         self.pages
    //             .iter()
    //             .map(|(page_id, page)| {
    //                 LockedNodePage::new(
    //                     page_id,
    //                     &self.num_nodes,
    //                     self.max_page_len,
    //                     page.as_ref(),
    //                     page.head_mut(),
    //                 )
    //             })
    //             .collect(),
    //     )
    // }

    pub fn node<'a>(&'a self, node: impl Into<VID>) -> NS::Entry<'a> {
        let (page_id, pos) = self.resolve_pos(node);
        let node_page = self
            .pages
            .get(page_id)
            .expect("Internal error: page not found");
        node_page.entry(pos)
    }

    pub fn prop_meta(&self) -> &Arc<Meta> {
        &self.prop_meta
    }

    #[inline(always)]
    pub fn writer<'a>(
        &'a self,
        segment_id: usize,
    ) -> NodeWriter<'a, RwLockWriteGuard<'a, MemNodeSegment>, NS> {
        let segment = self.get_or_create_segment(segment_id);
        let head = segment.head_mut();
        NodeWriter::new(segment, &self.layer_counter, head)
    }

    pub fn num_nodes(&self) -> usize {
        self.layer_counter.get(0)
    }

    pub fn layer_num_nodes(&self, layer_id: usize) -> usize {
        self.layer_counter.get(layer_id)
    }

    pub fn pages(&self) -> &boxcar::Vec<Arc<NS>> {
        &self.pages
    }

    pub fn nodes_path(&self) -> &Path {
        &self.nodes_path
    }

    pub fn load(
        nodes_path: impl AsRef<Path>,
        max_page_len: usize,
        ext: EXT,
    ) -> Result<Self, DBV4Error> {
        let nodes_path = nodes_path.as_ref();

        let meta = Arc::new(Meta::new());
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
                let page = NS::load(page_id, max_page_len, meta.clone(), nodes_path, ext.clone())
                    .map(|page| (page_id, page));
                Some(page)
            })
            .collect::<Result<HashMap<_, _>, _>>()?;

        if pages.is_empty() {
            return Err(DBV4Error::EmptyGraphDir(nodes_path.to_path_buf()));
        }

        let max_page = Iterator::max(pages.keys().copied()).unwrap();

        let pages = (0..=max_page)
            .map(|page_id| {
                let np = pages.remove(&page_id).unwrap_or_else(|| {
                    NS::new(page_id, max_page_len, meta.clone(), nodes_path, ext.clone())
                });
                Arc::new(np)
            })
            .collect::<boxcar::Vec<_>>();

        let first_page = pages.iter().next().unwrap().1;
        let first_p_id = first_page.segment_id();

        if first_p_id != 0 {
            return Err(DBV4Error::GenericFailure(format!(
                "First page id is not 0 in {:?}",
                nodes_path
            )));
        }

        let mut layer_counts = vec![];

        for (_, page) in pages.iter() {
            for layer_id in 0..page.num_layers() {
                let count = page.layer_num_nodes(layer_id);
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
            layer_counter: LayerCounter::from(layer_counts),
            prop_meta: meta,
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
                    Arc::new(NS::new(
                        segment_id,
                        self.max_page_len,
                        self.prop_meta.clone(),
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
