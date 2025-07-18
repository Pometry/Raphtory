use std::path::Path;

use raphtory_api::core::{entities::properties::prop::Prop, storage::dict_mapper::MaybeNew};
use raphtory_core::{
    entities::{EID, GID, VID},
    storage::timeindex::TimeIndexEntry,
};

use crate::error::DBV4Error;
use crate::wal::{LSN, TransactionID, GraphWalOps};
use crate::wal::no_wal::NoWal;

impl GraphWalOps for NoWal {
    type Entry = ();

    fn log_begin_txn(&self, _txn_id: TransactionID) -> Result<LSN, DBV4Error> {
        Ok(0)
    }

    fn log_end_txn(&self, _txn_id: TransactionID) -> Result<LSN, DBV4Error> {
        Ok(0)
    }

    fn log_add_edge(
        &self,
        _txn_id: TransactionID,
        _t: TimeIndexEntry,
        _src: VID,
        _dst: VID,
        _layer_id: usize,
        _t_props: &[(usize, Prop)],
        _c_props: &[(usize, Prop)],
    ) -> Result<LSN, DBV4Error> {
        Ok(0)
    }

    fn log_node_id(&self, _txn_id: TransactionID, _gid: GID, _vid: VID) -> Result<LSN, DBV4Error> {
        Ok(0)
    }

    fn log_edge_id(&self, _txn_id: TransactionID, _src: VID, _dst: VID, _eid: EID) -> Result<LSN, DBV4Error> {
        Ok(0)
    }

    fn log_const_prop_ids<PN: AsRef<str>>(
        &self,
        _txn_id: TransactionID,
        _props: &[MaybeNew<(PN, usize, Prop)>],
    ) -> Result<LSN, DBV4Error> {
        Ok(0)
    }

    fn log_temporal_prop_ids<PN: AsRef<str>>(
        &self,
        _txn_id: TransactionID,
        _props: &[MaybeNew<(PN, usize, Prop)>],
    ) -> Result<LSN, DBV4Error> {
        Ok(0)
    }

    fn log_layer_id(&self, _txn_id: TransactionID, _name: &str, _id: usize) -> Result<LSN, DBV4Error> {
        Ok(0)
    }

    fn log_checkpoint(&self, _lsn: LSN) -> Result<LSN, DBV4Error> {
        Ok(0)
    }

    fn recover(_dir: impl AsRef<Path>) -> impl Iterator<Item = Result<(LSN, ()), DBV4Error>> {
        std::iter::once(Ok((0, ())))
    }
}
