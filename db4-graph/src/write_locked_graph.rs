use crate::TemporalGraph;
use raphtory_core::entities::{EID, VID};
use std::sync::Arc;
use storage::{
    api::{edges::EdgeSegmentOps, graph_props::GraphPropSegmentOps, nodes::NodeSegmentOps},
    pages::{
        layer_counter::GraphStats,
        locked::{
            edges::WriteLockedEdgePages, graph_props::WriteLockedGraphPropPages,
            nodes::WriteLockedNodePages,
        },
    },
    persist::strategy::PersistenceStrategy,
    ES, GS, NS,
};

/// Holds write locks across all segments in the graph for fast bulk ingestion.
pub struct WriteLockedGraph<'a, EXT>
where
    EXT: PersistenceStrategy<NS = NS<EXT>, ES = ES<EXT>, GS = GS<EXT>>,
    NS<EXT>: NodeSegmentOps<Extension = EXT>,
    ES<EXT>: EdgeSegmentOps<Extension = EXT>,
    GS<EXT>: GraphPropSegmentOps<Extension = EXT>,
{
    pub nodes: WriteLockedNodePages<'a, NS<EXT>>,
    pub edges: WriteLockedEdgePages<'a, ES<EXT>>,
    pub graph_props: WriteLockedGraphPropPages<'a, GS<EXT>>,
    pub graph: &'a TemporalGraph<EXT>,
}

impl<'a, EXT> WriteLockedGraph<'a, EXT>
where
    EXT: PersistenceStrategy<NS = NS<EXT>, ES = ES<EXT>, GS = GS<EXT>>,
    NS<EXT>: NodeSegmentOps<Extension = EXT>,
    ES<EXT>: EdgeSegmentOps<Extension = EXT>,
    GS<EXT>: GraphPropSegmentOps<Extension = EXT>,
{
    pub fn new(graph: &'a TemporalGraph<EXT>) -> Self {
        WriteLockedGraph {
            nodes: graph.storage.nodes().write_locked(),
            edges: graph.storage.edges().write_locked(),
            graph_props: graph.storage.graph_props().write_locked(),
            graph,
        }
    }

    pub fn graph(&self) -> &TemporalGraph<EXT> {
        self.graph
    }

    pub fn resize_segments_to_vid(&mut self, vid: VID) {
        let (segment_id, _) = self.graph.storage.nodes().resolve_pos(vid);
        self.graph.storage().nodes().grow(segment_id + 1);
        std::mem::take(&mut self.nodes);
        self.nodes = self.graph.storage.nodes().write_locked();
    }

    pub fn resize_segments_to_eid(&mut self, eid: EID) {
        let (segment_id, _) = self.graph.storage.edges().resolve_pos(eid);
        self.graph.storage().edges().grow(segment_id + 1);
        std::mem::take(&mut self.edges);
        self.edges = self.graph.storage.edges().write_locked();
    }

    pub fn edge_stats(&self) -> &Arc<GraphStats> {
        self.graph.storage().edges().stats()
    }

    pub fn node_stats(&self) -> &Arc<GraphStats> {
        self.graph.storage().nodes().stats()
    }

    /// Flush dirty in-memory segments to disk using the existing segment write locks.
    pub fn flush(&mut self) -> Result<(), StorageError> {
        self.graph.storage.save_config()?;

        self.graph.gid_resolver.flush()?;
        self.nodes.flush()?;
        self.edges.flush()?;
        self.graph_props.flush()?;

        self.graph.storage.refresh_metadata()
    }

    /// Copy graph data to a new directory.
    ///
    /// Assumes `dst` is created and graph has been flushed to disk.
    pub fn copy_to(&self, dst: impl AsRef<Path>) -> Result<(), StorageError> {
        let dst = GraphDir::from(dst.as_ref());

        let config = self.graph.extension().config();
        config.save_to_dir(dst.path())?;

        self.graph.gid_resolver.copy_to(dst.gid_resolver_dir())?;
        self.nodes.copy_to(&dst.nodes_dir())?;
        self.edges.copy_to(&dst.edges_dir())?;
        self.graph_props.copy_to(&dst.graph_props_dir())?;

        // All segments have been flushed, mark checkpoint event in the WAL and control file.
        let wal = self.graph.extension().wal();
        let redo_lsn = None; // Nothing to redo since all segments have been flushed.
        let checkpoint_lsn = wal.log_checkpoint(redo_lsn)?;
        wal.flush(checkpoint_lsn)?;

        let control_file = self.graph.extension().control_file();
        control_file.set_checkpoint(checkpoint_lsn);
        control_file.save()?;
        control_file.copy_to(dst.path())?;

        // After checkpointing, copy over the latest WAL file to the destination.
        wal.copy_tail_to(&dst.wal_dir())?;

        Ok(())
    }
}
