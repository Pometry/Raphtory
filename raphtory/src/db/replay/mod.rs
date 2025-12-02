use db4_graph::TemporalGraph;
use raphtory_api::core::{
    entities::{properties::prop::Prop, EID, GID, VID},
    storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
};
use storage::{
    api::edges::EdgeSegmentOps,
    error::StorageError,
    wal::{GraphReplayer, TransactionID, LSN},
    Extension,
};

/// Wrapper struct for implementing `GraphReplayer` for a `TemporalGraph`.
/// This is needed to workaround Rust's orphan rule since both `GraphReplayer`
/// and `TemporalGraph` are foreign to this crate.
#[derive(Debug)]
pub struct ReplayGraph {
    graph: TemporalGraph<Extension>,
}

impl ReplayGraph {
    pub fn new(graph: TemporalGraph<Extension>) -> Self {
        Self { graph }
    }
}

impl GraphReplayer for ReplayGraph {
    fn replay_add_edge<PN: AsRef<str>>(
        &self,
        lsn: LSN,
        transaction_id: TransactionID,
        t: TimeIndexEntry,
        src_name: GID,
        src_id: VID,
        dst_name: GID,
        dst_id: VID,
        eid: EID,
        layer_name: Option<&str>,
        layer_id: usize,
        props: &[MaybeNew<(PN, usize, Prop)>],
    ) -> Result<(), StorageError> {
        let edge_segment = self.graph.storage().edges().get_edge_segment(eid);

        match edge_segment {
            Some(edge_segment) => {
                edge_segment.head().lsn();
            }
            _ => {}
        }

        // TODO: Check max lsn on disk to see if replay is needed.

        Ok(())
    }
}
