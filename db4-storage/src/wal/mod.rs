use crate::error::StorageError;
use raphtory_api::core::entities::{GidRef, LayerId, properties::prop::Prop};
use raphtory_core::{
    entities::{EID, GID, VID},
    storage::timeindex::EventTime,
};
use std::path::Path;

pub mod entry;
pub mod no_wal;

pub type LSN = u64;
pub type TransactionID = u64;

/// Core Wal methods.
pub trait WalOps {
    /// Appends data to the WAL and returns the assigned LSN.
    fn append(&self, data: &[u8]) -> Result<LSN, StorageError>;

    /// Flushes in-memory WAL entries up to the given LSN to disk.
    /// Returns immediately if the given LSN is already flushed to disk.
    fn flush(&self, lsn: LSN) -> Result<(), StorageError>;

    /// Reads the WAL record at the given LSN.
    /// Returns `Ok(None)` if there is no record at that LSN.
    fn read(&self, lsn: LSN) -> Result<Option<ReplayRecord>, StorageError>;

    /// Returns an iterator over the entries in the wal, starting from the given LSN.
    /// If `start` is `None`, replay begins at the first record in the WAL stream.
    fn replay(
        &self,
        start: Option<LSN>,
    ) -> impl Iterator<Item = Result<ReplayRecord, StorageError>>;

    /// Returns the current position in the WAL stream.
    fn position(&self) -> LSN;

    /// Sets the position in the WAL stream.
    fn set_position(&self, lsn: LSN) -> Result<(), StorageError>;

    /// Copies the latest WAL log file from `src` to `dst`.
    ///
    /// Used after a checkpoint is completed, since recovery now only needs the
    /// WAL file containing the checkpoint. Older WAL files are omitted from the copy.
    fn copy_tail_to(src: &Path, dst: &Path) -> Result<(), StorageError>;
}

#[derive(Debug)]
pub struct ReplayRecord {
    lsn: LSN,

    data: Vec<u8>,

    /// LSN immediately after this record in the WAL stream.
    next_lsn: LSN,
}

impl ReplayRecord {
    pub fn new(lsn: LSN, data: Vec<u8>, next_lsn: LSN) -> Self {
        Self {
            lsn,
            data,
            next_lsn,
        }
    }

    pub fn lsn(&self) -> LSN {
        self.lsn
    }

    /// Returns the LSN immediately following this record in the WAL stream.
    pub fn next_lsn(&self) -> LSN {
        self.next_lsn
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

// Raphtory-specific logging & replay methods.
pub trait GraphWalOps {
    /// ReplayEntry represents the type of the wal entry returned during replay.
    type ReplayEntry;

    fn log_add_edge(
        &self,
        transaction_id: TransactionID,
        t: EventTime,
        src_name: Option<GidRef<'_>>,
        src_id: VID,
        dst_name: Option<GidRef<'_>>,
        dst_id: VID,
        eid: EID,
        props: Vec<(&str, usize, Prop)>,
        layer_name: Option<&str>,
        layer_id: LayerId,
    ) -> Result<LSN, StorageError>;

    fn log_add_edge_metadata(
        &self,
        transaction_id: TransactionID,
        eid: EID,
        layer_id: LayerId,
        props: Vec<(&str, usize, Prop)>,
    ) -> Result<LSN, StorageError>;

    fn log_delete_edge(
        &self,
        transaction_id: TransactionID,
        t: EventTime,
        src_name: Option<GidRef<'_>>,
        src_id: VID,
        dst_name: Option<GidRef<'_>>,
        dst_id: VID,
        eid: EID,
        layer_name: Option<&str>,
        layer_id: LayerId,
    ) -> Result<LSN, StorageError>;

    fn log_add_node(
        &self,
        transaction_id: TransactionID,
        t: EventTime,
        node_name: Option<GidRef<'_>>,
        node_id: VID,
        node_type_and_id: Option<(&str, usize)>,
        props: Vec<(&str, usize, Prop)>,
        layer_name: Option<&str>,
        layer_id: LayerId,
    ) -> Result<LSN, StorageError>;

    fn log_add_node_metadata(
        &self,
        transaction_id: TransactionID,
        vid: VID,
        props: Vec<(&str, usize, Prop)>,
    ) -> Result<LSN, StorageError>;

    fn log_set_node_type(
        &self,
        transaction_id: TransactionID,
        vid: VID,
        node_type: &str,
        node_type_id: usize,
    ) -> Result<LSN, StorageError>;

    fn log_add_graph_props(
        &self,
        transaction_id: TransactionID,
        t: EventTime,
        props: Vec<(&str, usize, Prop)>,
    ) -> Result<LSN, StorageError>;

    fn log_add_graph_metadata(
        &self,
        transaction_id: TransactionID,
        props: Vec<(&str, usize, Prop)>,
    ) -> Result<LSN, StorageError>;

    /// Logs a checkpoint indicating that entries up to `redo` are reflected on disk.
    /// Set `redo` to `None` to indicate all entries up to this checkpoint are on disk.
    ///
    /// Returns the LSN of the checkpoint entry.
    fn log_checkpoint(&self, redo: Option<LSN>) -> Result<LSN, StorageError>;

    /// Logs a shutdown checkpoint indicating a clean shutdown with all WAL entries
    /// applied to storage.
    ///
    /// Returns the LSN of the shutdown checkpoint entry.
    fn log_shutdown_checkpoint(&self) -> Result<LSN, StorageError>;

    /// Reads the WAL entry at `lsn` and validates it is a checkpoint.
    ///
    /// Returns the checkpoint redo LSN, or `None` if there is nothing to redo
    /// up to this checkpoint.
    fn read_checkpoint(&self, lsn: LSN) -> Result<Option<LSN>, StorageError>;

    /// Reads the WAL entry at `lsn` and validates it is a shutdown checkpoint.
    ///
    /// Returns the LSN immediately after this entry, denoting the end of the WAL stream.
    fn read_shutdown_checkpoint(&self, lsn: LSN) -> Result<LSN, StorageError>;

    /// Replays all entries in the WAL to the given graph, starting from `start`.
    /// If `start` is `None`, replay begins at the first entry in the WAL stream.
    /// Returns the LSN immediately after the last entry in the WAL stream on success.
    fn replay_to_graph<G: GraphReplay>(
        &self,
        graph: &mut G,
        start: Option<LSN>,
    ) -> Result<LSN, StorageError>;
}

/// Trait for defining callbacks for replaying from wal.
pub trait GraphReplay {
    fn replay_add_edge(
        &mut self,
        lsn: LSN,
        transaction_id: TransactionID,
        t: EventTime,
        src_name: Option<GID>,
        src_id: VID,
        dst_name: Option<GID>,
        dst_id: VID,
        eid: EID,
        props: Vec<(String, usize, Prop)>,
        layer_name: Option<String>,
        layer_id: LayerId,
    ) -> Result<(), StorageError>;

    fn replay_add_edge_metadata(
        &mut self,
        lsn: LSN,
        transaction_id: TransactionID,
        eid: EID,
        layer_id: LayerId,
        props: Vec<(String, usize, Prop)>,
    ) -> Result<(), StorageError>;

    fn replay_delete_edge(
        &mut self,
        lsn: LSN,
        transaction_id: TransactionID,
        t: EventTime,
        src_name: Option<GID>,
        src_id: VID,
        dst_name: Option<GID>,
        dst_id: VID,
        eid: EID,
        layer_name: Option<String>,
        layer_id: LayerId,
    ) -> Result<(), StorageError>;

    fn replay_add_node(
        &mut self,
        lsn: LSN,
        transaction_id: TransactionID,
        t: EventTime,
        node_name: Option<GID>,
        node_id: VID,
        node_type_and_id: Option<(String, usize)>,
        props: Vec<(String, usize, Prop)>,
        layer_name: Option<String>,
        layer_id: LayerId,
    ) -> Result<(), StorageError>;

    fn replay_add_node_metadata(
        &mut self,
        lsn: LSN,
        transaction_id: TransactionID,
        vid: VID,
        props: Vec<(String, usize, Prop)>,
    ) -> Result<(), StorageError>;

    fn replay_set_node_type(
        &mut self,
        lsn: LSN,
        transaction_id: TransactionID,
        vid: VID,
        node_type: String,
        node_type_id: usize,
    ) -> Result<(), StorageError>;

    fn replay_add_graph_props(
        &mut self,
        lsn: LSN,
        transaction_id: TransactionID,
        t: EventTime,
        props: Vec<(String, usize, Prop)>,
    ) -> Result<(), StorageError>;

    fn replay_add_graph_metadata(
        &mut self,
        lsn: LSN,
        transaction_id: TransactionID,
        props: Vec<(String, usize, Prop)>,
    ) -> Result<(), StorageError>;
}
