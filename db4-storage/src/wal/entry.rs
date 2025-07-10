use raphtory_core::{
    entities::{VID, EID, GID},
    storage::timeindex::TimeIndexEntry,
};
use raphtory_api::core::entities::properties::prop::Prop;
use serde::{Serialize, Deserialize};
use std::borrow::Cow;

use crate::error::DBV4Error;
use crate::wal::{LSN, WalEntryBuilder};

#[derive(Debug, Serialize, Deserialize)]
pub struct EmptyWalEntry;

impl<'a> WalEntryBuilder<'a> for EmptyWalEntry {
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

    fn add_const_prop_ids(_props: Cow<'a, [(Cow<'a, str>, usize)]>) -> Self {
        EmptyWalEntry
    }

    fn add_temporal_prop_ids(_props: Cow<'a, [(Cow<'a, str>, usize)]>) -> Self {
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
