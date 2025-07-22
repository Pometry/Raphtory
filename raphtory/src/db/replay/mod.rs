use db4_graph::TemporalGraph;
use raphtory_storage::mutation::addition_ops::InternalAdditionOps;
use storage::{error::DBV4Error, wal::{GraphReplayer, TransactionID}, Extension};
use raphtory_api::core::{
    entities::{properties::prop::Prop, GID, EID, VID},
    storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
};

/// Wrapper struct for replaying wal entries.
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
    fn replay_begin_txn(&self, txn_id: TransactionID) -> Result<(), DBV4Error> {
        Ok(())
    }

    fn replay_end_txn(&self, txn_id: TransactionID) -> Result<(), DBV4Error> {
        Ok(())
    }

    fn replay_add_edge(
        &self,
        txn_id: TransactionID,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer_id: usize,
        t_props: &[(usize, Prop)],
        c_props: &[(usize, Prop)],
    ) -> Result<(), DBV4Error> {
        Ok(())
    }

    fn replay_node_id(&self, txn_id: TransactionID, gid: GID, vid: VID) -> Result<(), DBV4Error> {
        Ok(())
    }

    fn replay_edge_id(
        &self,
        txn_id: TransactionID,
        src: VID,
        dst: VID,
        eid: EID,
        layer_id: usize,
    ) -> Result<(), DBV4Error> {
        Ok(())
    }

    fn replay_const_prop_ids<PN: AsRef<str>>(
        &self,
        txn_id: TransactionID,
        props: &[MaybeNew<(PN, usize, Prop)>],
    ) -> Result<(), DBV4Error> {
        Ok(())
    }

    fn replay_temporal_prop_ids<PN: AsRef<str>>(
        &self,
        txn_id: TransactionID,
        props: &[MaybeNew<(PN, usize, Prop)>],
    ) -> Result<(), DBV4Error> {
        Ok(())
    }

    fn replay_layer_id(&self, txn_id: TransactionID, name: &str, id: usize) -> Result<(), DBV4Error> {
        Ok(())
    }
}
