use std::path::Path;

use raphtory_api::core::{entities::properties::prop::Prop, storage::dict_mapper::MaybeNew};
use raphtory_core::{
    entities::{EID, GID, VID},
    storage::timeindex::TimeIndexEntry,
};

use crate::wal::no_wal::NoWal;
use crate::{
    error::DBV4Error,
    wal::{GraphWal, GraphReplayer, TransactionID, LSN},
};

impl GraphWal for NoWal {
    type ReplayEntry = ();

    fn log_begin_transaction(&self, _transaction_id: TransactionID) -> Result<LSN, DBV4Error> {
        Ok(0)
    }

    fn log_end_transaction(&self, _transaction_id: TransactionID) -> Result<LSN, DBV4Error> {
        Ok(0)
    }

    fn log_add_static_edge(
        &self,
        _transaction_id: TransactionID,
        _t: TimeIndexEntry,
        _src: VID,
        _dst: VID,
    ) -> Result<LSN, DBV4Error> {
        Ok(0)
    }

    fn log_add_edge(
        &self,
        _transaction_id: TransactionID,
        _t: TimeIndexEntry,
        _src: VID,
        _dst: VID,
        _eid: EID,
        _layer_id: usize,
        _props: &[(usize, Prop)],
    ) -> Result<LSN, DBV4Error> {
        Ok(0)
    }

    fn log_node_id(&self, _transaction_id: TransactionID, _gid: GID, _vid: VID) -> Result<LSN, DBV4Error> {
        Ok(0)
    }

    fn log_edge_id(
        &self,
        _transaction_id: TransactionID,
        _src: VID,
        _dst: VID,
        _eid: EID,
        _layer_id: usize,
    ) -> Result<LSN, DBV4Error> {
        Ok(0)
    }

    fn log_const_prop_ids<PN: AsRef<str>>(
        &self,
        _transaction_id: TransactionID,
        _props: &[MaybeNew<(PN, usize, Prop)>],
    ) -> Result<LSN, DBV4Error> {
        Ok(0)
    }

    fn log_temporal_prop_ids<PN: AsRef<str>>(
        &self,
        _transaction_id: TransactionID,
        _props: &[MaybeNew<(PN, usize, Prop)>],
    ) -> Result<LSN, DBV4Error> {
        Ok(0)
    }

    fn log_layer_id(
        &self,
        _transaction_id: TransactionID,
        _name: &str,
        _id: usize,
    ) -> Result<LSN, DBV4Error> {
        Ok(0)
    }

    fn log_checkpoint(&self, _lsn: LSN) -> Result<LSN, DBV4Error> {
        Ok(0)
    }

    fn replay_iter(_dir: impl AsRef<Path>) -> impl Iterator<Item = Result<(LSN, ()), DBV4Error>> {
        std::iter::once(Ok((0, ())))
    }

    fn replay_to_graph<G: GraphReplayer>(
        _dir: impl AsRef<Path>,
        _graph: &mut G,
    ) -> Result<(), DBV4Error> {
        todo!()
    }
}
