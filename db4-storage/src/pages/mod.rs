use crate::{
    EID, LocalPOS, VID,
    api::{edges::EdgeSegmentOps, graph_props::GraphPropSegmentOps, nodes::NodeSegmentOps},
    error::StorageError,
    pages::{edge_store::ReadLockedEdgeStorage, node_store::ReadLockedNodeStorage},
    persist::{
        config::ConfigOps,
        control_file::{ControlFileOps, DBState},
        strategy::PersistenceStrategy,
    },
    segments::{edge::segment::MemEdgeSegment, node::segment::MemNodeSegment},
    state::StateIndex,
    wal::{GraphWalOps, WalOps},
};
use edge_page::writer::EdgeWriter;
use edge_store::EdgeStorageInner;
use graph_prop_store::GraphPropStorageInner;
use node_page::writer::NodeWriter;
use node_store::NodeStorageInner;
use parking_lot::RwLockWriteGuard;
use raphtory_api::core::entities::properties::meta::Meta;
use rayon::prelude::*;
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{self, AtomicUsize},
    },
};
use tinyvec::TinyVec;

pub mod edge_page;
pub mod edge_store;
pub mod graph_prop_page;
pub mod graph_prop_store;
pub mod layer_counter;
pub mod locked;
pub mod node_page;
pub mod node_store;
pub mod session;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

// graph // (node/edges) // segment // layer_ids (0, 1, 2, ...) // actual graphy bits

#[derive(Debug)]
pub struct GraphStore<
    NS: NodeSegmentOps<Extension = EXT>,
    ES: EdgeSegmentOps<Extension = EXT>,
    GS: GraphPropSegmentOps<Extension = EXT>,
    EXT: PersistenceStrategy<NS = NS, ES = ES, GS = GS>,
> {
    nodes: Arc<NodeStorageInner<NS, EXT>>,
    edges: Arc<EdgeStorageInner<ES, EXT>>,
    graph_props: Arc<GraphPropStorageInner<GS, EXT>>,
    graph_dir: Option<PathBuf>,
    event_id: AtomicUsize,
    ext: EXT,
}

impl<
    NS: NodeSegmentOps<Extension = EXT>,
    ES: EdgeSegmentOps<Extension = EXT>,
    GS: GraphPropSegmentOps<Extension = EXT>,
    EXT: PersistenceStrategy<NS = NS, ES = ES, GS = GS>,
> GraphStore<NS, ES, GS, EXT>
{
    pub fn flush(&self) -> Result<(), StorageError> {
        let node_types = self.nodes.prop_meta().get_all_node_types();
        let config = self.ext.config().with_node_types(node_types);

        if let Some(graph_dir) = self.graph_dir.as_ref() {
            config.save_to_dir(graph_dir)?;
        }

        self.nodes.flush()?;
        self.edges.flush()?;
        self.graph_props.flush()?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct ReadLockedGraphStore<
    NS: NodeSegmentOps<Extension = EXT>,
    ES: EdgeSegmentOps<Extension = EXT>,
    GS: GraphPropSegmentOps<Extension = EXT>,
    EXT: PersistenceStrategy<NS = NS, ES = ES, GS = GS>,
> {
    pub nodes: Arc<ReadLockedNodeStorage<NS, EXT>>,
    pub edges: Arc<ReadLockedEdgeStorage<ES, EXT>>,
    pub graph: Arc<GraphStore<NS, ES, GS, EXT>>,
}

impl<
    NS: NodeSegmentOps<Extension = EXT>,
    ES: EdgeSegmentOps<Extension = EXT>,
    GS: GraphPropSegmentOps<Extension = EXT>,
    EXT: PersistenceStrategy<NS = NS, ES = ES, GS = GS>,
> GraphStore<NS, ES, GS, EXT>
{
    pub fn new(graph_dir: Option<&Path>, ext: EXT) -> Self {
        let node_meta = Meta::new_for_nodes();
        let edge_meta = Meta::new_for_edges();
        let graph_props_meta = Meta::new_for_graph_props();

        Self::new_with_meta(graph_dir, node_meta, edge_meta, graph_props_meta, ext)
    }

    pub fn new_with_meta(
        graph_dir: Option<&Path>,
        node_meta: Meta,
        edge_meta: Meta,
        graph_props_meta: Meta,
        ext: EXT,
    ) -> Self {
        let nodes_path = graph_dir.map(|graph_dir| graph_dir.join("nodes"));
        let edges_path = graph_dir.map(|graph_dir| graph_dir.join("edges"));
        let graph_props_path = graph_dir.map(|graph_dir| graph_dir.join("graph_props"));

        let node_meta = Arc::new(node_meta);
        let edge_meta = Arc::new(edge_meta);
        let graph_props_meta = Arc::new(graph_props_meta);

        let node_storage = Arc::new(NodeStorageInner::new_with_meta(
            nodes_path,
            node_meta,
            edge_meta.clone(),
            ext.clone(),
        ));
        let edge_storage = Arc::new(EdgeStorageInner::new_with_meta(
            edges_path,
            edge_meta,
            ext.clone(),
        ));
        let graph_prop_storage = Arc::new(GraphPropStorageInner::new_with_meta(
            graph_props_path.as_deref(),
            graph_props_meta,
            ext.clone(),
        ));

        if let Some(graph_dir) = graph_dir {
            ext.config()
                .save_to_dir(graph_dir)
                .expect("Failed to write config to disk");
        }

        Self {
            nodes: node_storage,
            edges: edge_storage,
            graph_props: graph_prop_storage,
            event_id: AtomicUsize::new(0),
            graph_dir: graph_dir.map(|p| p.to_path_buf()),
            ext,
        }
    }

    pub fn load(graph_dir: impl AsRef<Path>, ext: EXT) -> Result<Self, StorageError> {
        let nodes_path = graph_dir.as_ref().join("nodes");
        let edges_path = graph_dir.as_ref().join("edges");
        let graph_props_path = graph_dir.as_ref().join("graph_props");

        let edge_storage = Arc::new(EdgeStorageInner::load(edges_path, ext.clone())?);
        let edge_meta = edge_storage.edge_meta().clone();
        let node_storage: Arc<NodeStorageInner<NS, EXT>> = Arc::new(NodeStorageInner::load(
            nodes_path,
            edge_meta.clone(),
            ext.clone(),
        )?);
        let node_meta = node_storage.prop_meta();

        // Load graph temporal properties and metadata.
        let graph_prop_storage =
            Arc::new(GraphPropStorageInner::load(graph_props_path, ext.clone())?);

        for node_type in ext.config().node_types().iter() {
            node_meta.get_or_create_node_type_id(node_type);
        }

        let t_len = edge_meta
            .all_layer_iter()
            .map(|(layer_id, _)| edge_storage.t_len(layer_id.0))
            .sum::<usize>();

        Ok(Self {
            nodes: node_storage,
            edges: edge_storage,
            graph_props: graph_prop_storage,
            event_id: AtomicUsize::new(t_len),
            graph_dir: Some(graph_dir.as_ref().to_path_buf()),
            ext,
        })
    }

    pub fn read_locked(self: &Arc<Self>) -> ReadLockedGraphStore<NS, ES, GS, EXT> {
        let nodes = self.nodes.locked().into();
        let edges = self.edges.locked().into();

        ReadLockedGraphStore {
            nodes,
            edges,
            graph: self.clone(),
        }
    }

    pub fn extension(&self) -> &EXT {
        &self.ext
    }

    pub fn nodes(&self) -> &Arc<NodeStorageInner<NS, EXT>> {
        &self.nodes
    }

    pub fn edges(&self) -> &Arc<EdgeStorageInner<ES, EXT>> {
        &self.edges
    }

    pub fn graph_props(&self) -> &Arc<GraphPropStorageInner<GS, EXT>> {
        &self.graph_props
    }

    pub fn edge_meta(&self) -> &Meta {
        self.edges.edge_meta()
    }

    pub fn node_meta(&self) -> &Meta {
        self.nodes.prop_meta()
    }

    pub fn graph_props_meta(&self) -> &Meta {
        self.graph_props.meta()
    }

    pub fn earliest(&self) -> i64 {
        self.nodes
            .stats()
            .earliest()
            .min(self.edges.stats().earliest())
    }

    pub fn latest(&self) -> i64 {
        self.nodes.stats().latest().max(self.edges.stats().latest())
    }

    pub fn node_segment_counts(&self) -> SegmentCounts<VID> {
        self.nodes.segment_counts()
    }

    pub fn edge_segment_counts(&self) -> SegmentCounts<EID> {
        self.edges.segment_counts()
    }

    pub fn read_event_id(&self) -> usize {
        self.event_id.load(atomic::Ordering::Relaxed)
    }

    pub fn set_event_id(&self, event_id: usize) {
        self.event_id.store(event_id, atomic::Ordering::Relaxed);
    }

    pub fn next_event_id(&self) -> usize {
        self.event_id.fetch_add(1, atomic::Ordering::Relaxed)
    }

    pub fn reserve_event_ids(&self, num_ids: usize) -> usize {
        self.event_id.fetch_add(num_ids, atomic::Ordering::Relaxed)
    }

    pub fn set_max_event_id(&self, value: usize) -> usize {
        self.event_id.fetch_max(value, atomic::Ordering::Relaxed)
    }

    pub fn node_writer(
        &self,
        node_segment: usize,
    ) -> NodeWriter<'_, RwLockWriteGuard<'_, MemNodeSegment>, NS> {
        self.nodes().writer(node_segment)
    }

    pub fn edge_writer(
        &self,
        eid: EID,
    ) -> EdgeWriter<'_, RwLockWriteGuard<'_, MemEdgeSegment>, ES> {
        self.edges().get_writer(eid)
    }

    pub fn get_free_writer(&self) -> EdgeWriter<'_, RwLockWriteGuard<'_, MemEdgeSegment>, ES> {
        self.edges().get_free_writer()
    }

    pub fn vacuum(self: &Arc<Self>) -> Result<(), StorageError> {
        let mut locked_nodes = self.nodes.write_locked();
        let mut locked_edges = self.edges.write_locked();

        locked_nodes.vacuum()?;
        locked_edges.vacuum()?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct SegmentCounts<I> {
    max_seg_len: u32,
    counts: TinyVec<[u32; 32]>, // this might come to be a problem
    _marker: std::marker::PhantomData<I>,
}

impl<I: From<usize> + Into<usize>> SegmentCounts<I> {
    pub fn new(max_seg_len: u32, counts: impl IntoIterator<Item = u32>) -> Self {
        let counts: TinyVec<[u32; 32]> = counts.into_iter().collect();

        Self {
            max_seg_len,
            counts,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn into_iter(self) -> impl Iterator<Item = I> {
        let max_seg_len = self.max_seg_len as usize;
        self.counts.into_iter().enumerate().flat_map(move |(i, c)| {
            let g_pos = i * max_seg_len as usize;
            (0..c).map(move |offset| I::from(g_pos + offset as usize))
        })
    }

    pub fn into_index(self) -> StateIndex<I> {
        StateIndex::from(self)
    }

    pub fn counts(&self) -> &[u32] {
        &self.counts
    }

    pub(crate) fn max_seg_len(&self) -> u32 {
        self.max_seg_len
    }
}
impl<I: From<usize> + Send> SegmentCounts<I> {
    pub fn into_par_iter(self) -> impl ParallelIterator<Item = I> {
        let max_seg_len = self.max_seg_len as usize;
        (0..self.counts.len()).into_par_iter().flat_map(move |i| {
            let c = self.counts[i];
            let g_pos = i * max_seg_len;
            (0..c)
                .into_par_iter()
                .map(move |offset| I::from(g_pos + offset as usize))
        })
    }
}

impl<
    NS: NodeSegmentOps<Extension = EXT>,
    ES: EdgeSegmentOps<Extension = EXT>,
    GS: GraphPropSegmentOps<Extension = EXT>,
    EXT: PersistenceStrategy<NS = NS, ES = ES, GS = GS>,
> Drop for GraphStore<NS, ES, GS, EXT>
{
    fn drop(&mut self) {
        let wal = self.ext.wal();
        let control_file = self.ext.control_file();

        match self.flush() {
            Ok(_) => {
                // Log a checkpoint record in the WAL, indicating that the DB was shutdown
                // with all the segments flushed to disk.
                // On startup, recovery is skipped since there are no pending writes to replay.
                let checkpoint_lsn = match wal.log_shutdown_checkpoint() {
                    Ok(lsn) => lsn,
                    Err(err) => {
                        eprintln!("Failed to log shutdown checkpoint in drop: {err}");
                        return;
                    }
                };

                // Flush up to the end of the WAL stream.
                let flush_lsn = wal.position();

                if let Err(err) = wal.flush(flush_lsn) {
                    eprintln!("Failed to flush checkpoint record in drop: {err}");
                    return;
                }

                // Record the checkpoint and shutdown state and write control file to disk.
                control_file.set_checkpoint(checkpoint_lsn);
                control_file.set_db_state(DBState::Shutdown);

                if let Err(err) = control_file.save() {
                    eprintln!("Failed to save control file in drop: {err}");
                    return;
                }
            }
            Err(err) => {
                eprintln!("Failed to flush storage in drop: {err}")
            }
        }
    }
}

#[inline(always)]
pub fn resolve_pos<I: Copy + Into<usize>>(i: I, max_page_len: u32) -> (usize, LocalPOS) {
    let i = i.into();
    let seg = i / max_page_len as usize;
    let pos = i % max_page_len as usize;
    (seg, LocalPOS(pos as u32))
}

pub fn row_group_par_iter<I: From<usize>>(
    chunk_size: usize,
    num_segments: usize,
    max_seg_len: u32,
    max_actual_seg_len: u32,
) -> impl IndexedParallelIterator<Item = (usize, impl Iterator<Item = I>)> {
    let (num_chunks, chunk_size) = if num_segments != 0 {
        let chunk_size = (chunk_size / num_segments).max(1);
        let num_chunks = (max_seg_len as usize + chunk_size - 1) / chunk_size;
        (num_chunks, chunk_size)
    } else {
        (0, 0)
    };

    (0..num_chunks).into_par_iter().map(move |chunk_id| {
        let start = chunk_id * chunk_size;
        let end = ((chunk_id + 1) * chunk_size).min(max_actual_seg_len as usize);

        let iter = (start..end).flat_map(move |x| {
            (0..num_segments).map(move |seg| I::from(seg * max_seg_len as usize + x))
        });

        (chunk_id, iter)
    })
}

#[cfg(test)]
mod test {
    use rayon::iter::ParallelIterator;

    #[test]
    fn test_interleave() {
        let chunk_size = 3;
        let num_segments = 3;
        let max_seg_len = 4;

        let actual = super::row_group_par_iter(chunk_size, num_segments, max_seg_len, max_seg_len)
            .map(|(c, items)| (c, items.collect::<Vec<_>>()))
            .collect::<Vec<_>>();

        let expected = vec![
            (0, vec![0, 4, 8]),
            (1, vec![1, 5, 9]),
            (2, vec![2, 6, 10]),
            (3, vec![3, 7, 11]),
        ];

        assert_eq!(actual, expected);
    }
}
