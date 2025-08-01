use db4_graph::TemporalGraph;
use raphtory_api::core::{
    entities::{properties::prop::Prop, EID, GID, VID},
    storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
};
use raphtory_storage::mutation::addition_ops::{EdgeWriteLock, InternalAdditionOps};
use storage::{
    api::edges::EdgeSegmentOps,
    error::DBV4Error,
    wal::{GraphReplayer, TransactionID, LSN},
    Extension,
};

/// Wrapper struct for implementing GraphReplayer for a TemporalGraph.
/// This is needed to workaround Rust's orphan rule since both ReplayGraph and TemporalGraph
/// are foreign to this crate.
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
    fn replay_begin_transaction(
        &self,
        lsn: LSN,
        transaction_id: TransactionID,
    ) -> Result<(), DBV4Error> {
        Ok(())
    }

    fn replay_end_transaction(
        &self,
        lsn: LSN,
        transaction_id: TransactionID,
    ) -> Result<(), DBV4Error> {
        Ok(())
    }

    fn replay_add_static_edge(
        &self,
        lsn: LSN,
        transaction_id: TransactionID,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
    ) -> Result<(), DBV4Error> {
        Ok(())
    }

    fn replay_add_edge(
        &self,
        lsn: LSN,
        transaction_id: TransactionID,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        eid: EID,
        layer_id: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), DBV4Error> {
        let edge_segment = self.graph.storage().edges().get_edge_segment(eid);

        match edge_segment {
            Some(edge_segment) => {
                edge_segment.head().lsn();
            }
            _ => {}
        }

        Ok(())
    }

    fn replay_node_id(
        &self,
        lsn: LSN,
        transaction_id: TransactionID,
        gid: GID,
        vid: VID,
    ) -> Result<(), DBV4Error> {
        Ok(())
    }

    fn replay_const_prop_ids<PN: AsRef<str>>(
        &self,
        lsn: LSN,
        transaction_id: TransactionID,
        props: &[MaybeNew<(PN, usize, Prop)>],
    ) -> Result<(), DBV4Error> {
        Ok(())
    }

    fn replay_temporal_prop_ids<PN: AsRef<str>>(
        &self,
        lsn: LSN,
        transaction_id: TransactionID,
        props: &[MaybeNew<(PN, usize, Prop)>],
    ) -> Result<(), DBV4Error> {
        Ok(())
    }

    fn replay_layer_id(
        &self,
        lsn: LSN,
        transaction_id: TransactionID,
        name: &str,
        id: usize,
    ) -> Result<(), DBV4Error> {
        Ok(())
    }
}
