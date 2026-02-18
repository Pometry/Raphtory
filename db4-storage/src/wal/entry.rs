use raphtory_api::core::entities::{GidRef, properties::prop::Prop};
use raphtory_core::{
    entities::{EID, VID},
    storage::timeindex::EventTime,
};

use crate::{
    error::StorageError,
    wal::{GraphReplay, GraphWalOps, LSN, TransactionID, no_wal::NoWal},
};

impl GraphWalOps for NoWal {
    type ReplayEntry = ();

    fn log_add_edge(
        &self,
        _transaction_id: TransactionID,
        _t: EventTime,
        _src_name: Option<GidRef<'_>>,
        _src_id: VID,
        _dst_name: Option<GidRef<'_>>,
        _dst_id: VID,
        _eid: EID,
        _layer_name: Option<&str>,
        _layer_id: usize,
        _props: Vec<(&str, usize, Prop)>,
    ) -> Result<LSN, StorageError> {
        Ok(0)
    }

    fn log_add_node(
        &self,
        _transaction_id: TransactionID,
        _t: EventTime,
        _node_name: Option<GidRef<'_>>,
        _node_id: VID,
        _node_type_and_id: Option<(&str, usize)>,
        _props: Vec<(&str, usize, Prop)>,
        _layer_id: usize,
    ) -> Result<LSN, StorageError> {
        Ok(0)
    }

    fn log_checkpoint(&self, _lsn: LSN) -> Result<LSN, StorageError> {
        Ok(0)
    }

    fn replay_iter(&self) -> impl Iterator<Item = Result<(LSN, ()), StorageError>> {
        std::iter::empty()
    }

    fn replay_to_graph<G: GraphReplay>(&self, _graph: &mut G) -> Result<(), StorageError> {
        panic!("NoWAL does not support replay")
    }
}
