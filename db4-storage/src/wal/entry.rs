use raphtory_api::core::{entities::properties::prop::Prop, storage::dict_mapper::MaybeNew};
use raphtory_core::{
    entities::{EID, GID, VID},
    storage::timeindex::TimeIndexEntry,
};
use serde::{Deserialize, Serialize};

use crate::error::DBV4Error;
use crate::wal::{LSN, TransactionID, WalEntryBuilder};

#[derive(Debug, Serialize, Deserialize)]
pub struct NoWalEntry;

impl<'a> WalEntryBuilder<'a> for NoWalEntry {
    fn begin_txn(_txn_id: TransactionID) -> Self {
        NoWalEntry
    }

    fn commit_txn(_txn_id: TransactionID) -> Self {
        NoWalEntry
    }

    fn add_edge(
        _txn_id: TransactionID,
        _t: TimeIndexEntry,
        _src: VID,
        _dst: VID,
        _layer_id: usize,
        _t_props: &'a [(usize, Prop)],
        _c_props: &'a [(usize, Prop)],
    ) -> Self {
        NoWalEntry
    }

    fn add_node_id(_txn_id: TransactionID, _gid: GID, _vid: VID) -> Self {
        NoWalEntry
    }

    fn add_edge_id(_txn_id: TransactionID, _src: VID, _dst: VID, _eid: EID) -> Self {
        NoWalEntry
    }

    fn add_new_const_prop_ids<PN: AsRef<str>>(
        _txn_id: TransactionID,
        _props: &'a [MaybeNew<(PN, usize, Prop)>],
    ) -> Self {
        NoWalEntry
    }

    fn add_new_temporal_prop_ids<PN: AsRef<str>>(
        _txn_id: TransactionID,
        _props: &'a [MaybeNew<(PN, usize, Prop)>],
    ) -> Self {
        NoWalEntry
    }

    fn add_layer_id(_txn_id: TransactionID, _name: &'a str, _id: usize) -> Self {
        NoWalEntry
    }

    fn checkpoint(_lsn: LSN) -> Self {
        NoWalEntry
    }

    fn to_bytes(&self) -> Result<Vec<u8>, DBV4Error> {
        Ok(vec![])
    }

    fn from_bytes(_bytes: &[u8]) -> Result<Self, DBV4Error> {
        Ok(NoWalEntry)
    }
}
