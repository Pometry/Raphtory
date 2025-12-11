use std::path::Path;

use raphtory_api::core::entities::properties::prop::Prop;
use raphtory_core::{
    entities::{EID, GID, VID},
    storage::timeindex::TimeIndexEntry,
};

use crate::{
    error::StorageError,
    wal::{GraphReplay, GraphWal, LSN, TransactionID, no_wal::NoWal},
};

impl GraphWal for NoWal {
    type ReplayEntry = ();

    fn log_add_edge(
        &self,
        _transaction_id: TransactionID,
        _t: TimeIndexEntry,
        _src_name: GID,
        _src_id: VID,
        _dst_name: GID,
        _dst_id: VID,
        _eid: EID,
        _layer_name: Option<&str>,
        _layer_id: usize,
        _props: Vec<(&str, usize, Prop)>,
    ) -> Result<LSN, StorageError> {
        Ok(0)
    }

    fn log_checkpoint(&self, _lsn: LSN) -> Result<LSN, StorageError> {
        Ok(0)
    }

    fn replay_iter(
        _dir: impl AsRef<Path>,
    ) -> impl Iterator<Item = Result<(LSN, ()), StorageError>> {
        std::iter::once(Ok((0, ())))
    }

    fn replay_to_graph<G: GraphReplay>(
        _dir: impl AsRef<Path>,
        _graph: &G,
    ) -> Result<(), StorageError> {
        todo!()
    }
}
