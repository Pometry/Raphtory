use raphtory_core::{
    entities::{VID, EID, GID},
    storage::timeindex::TimeIndexEntry,
};
use raphtory_api::core::{entities::properties::prop::Prop, storage::dict_mapper::MaybeNew};
use serde::{Serialize, Deserialize};
use std::borrow::Cow;

use crate::error::DBV4Error;
use crate::wal::{LSN, TransactionID, WalEntryBuilder};

#[derive(Debug, Serialize, Deserialize)]
pub struct EmptyWalEntry;

impl<'a> WalEntryBuilder<'a> for EmptyWalEntry {
    fn begin_txn(_txn_id: TransactionID) -> Self {
        EmptyWalEntry
    }

    fn commit_txn(_txn_id: TransactionID) -> Self {
        EmptyWalEntry
    }

    fn add_edge(
        _t: TimeIndexEntry,
        _src: VID,
        _dst: VID,
        _eid: EID,
        _layer_id: usize,
        _t_props: Cow<'a, [(usize, Prop)]>,
        _c_props: Cow<'a, [(usize, Prop)]>,
    ) -> Self {
        EmptyWalEntry
    }

    fn add_node_id(_gid: GID, _vid: VID) -> Self {
        EmptyWalEntry
    }

    fn add_new_const_prop_ids<PN: AsRef<str>>(_props: &'a [MaybeNew<(PN, usize, Prop)>]) -> Self {
        EmptyWalEntry
    }

    fn add_new_temporal_prop_ids<PN: AsRef<str>>(_props: &'a [MaybeNew<(PN, usize, Prop)>]) -> Self {
        EmptyWalEntry
    }

    fn add_layer_id(_name: String, _id: usize) -> Self {
        EmptyWalEntry
    }

    fn checkpoint(_lsn: LSN) -> Self {
        EmptyWalEntry
    }

    fn to_bytes(&self) -> Result<Vec<u8>, DBV4Error> {
        Ok(vec![])
    }

    fn from_bytes(_bytes: &[u8]) -> Result<Self, DBV4Error> {
        Ok(EmptyWalEntry)
    }
}
