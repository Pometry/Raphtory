use crate::error::StorageError;
use raphtory_api::core::entities::{GidRef, properties::prop::Prop};
use raphtory_core::{
    entities::{EID, GID, VID},
    storage::timeindex::EventTime,
};

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

    /// Returns an iterator over the entries in the wal.
    fn replay(&self) -> impl Iterator<Item = Result<ReplayRecord, StorageError>>;

    /// Returns the LSN that will be assigned to the next appended record.
    fn next_lsn(&self) -> LSN;

    /// Sets the next LSN to be assigned to a record.
    fn set_next_lsn(&self, lsn: LSN);
}

#[derive(Debug)]
pub struct ReplayRecord {
    lsn: LSN,

    data: Vec<u8>,

    /// The raw bytes of the WAL entry stored on disk, including CRC data.
    raw_bytes: Vec<u8>,
}

impl ReplayRecord {
    pub fn new(lsn: LSN, data: Vec<u8>, raw_bytes: Vec<u8>) -> Self {
        Self {
            lsn,
            data,
            raw_bytes,
        }
    }

    pub fn lsn(&self) -> LSN {
        self.lsn
    }

    /// Returns the LSN immediately following this record in the WAL stream.
    pub fn next_lsn(&self) -> LSN {
        self.lsn + self.raw_bytes.len() as LSN
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn raw_bytes(&self) -> &[u8] {
        &self.raw_bytes
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
        layer_name: Option<&str>,
        layer_id: usize,
        props: Vec<(&str, usize, Prop)>,
    ) -> Result<LSN, StorageError>;

    fn log_add_edge_metadata(
        &self,
        transaction_id: TransactionID,
        eid: EID,
        layer_id: usize,
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
        layer_id: usize,
    ) -> Result<LSN, StorageError>;

    fn log_add_node(
        &self,
        transaction_id: TransactionID,
        t: EventTime,
        node_name: Option<GidRef<'_>>,
        node_id: VID,
        node_type_and_id: Option<(&str, usize)>,
        props: Vec<(&str, usize, Prop)>,
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

    /// Logs a checkpoint indicating that all LSN < `redo` are persisted.
    /// On recovery, replay will start from `redo` in the WAL stream.
    /// Set `is_shutdown` to true on a clean shutdown to differentiate from periodic checkpoints.
    fn log_checkpoint(&self, redo: LSN, is_shutdown: bool) -> Result<LSN, StorageError>;

    /// Reads and decodes the WAL entry at the given LSN and validates that it is a checkpoint.
    /// Returns the checkpoint redo LSN, denoting where replay should start from.
    fn read_checkpoint(&self, lsn: LSN) -> Result<LSN, StorageError>;

    /// Reads and decodes the WAL entry at the given LSN and validates that it is a shutdown checkpoint.
    /// Returns the LSN immediately after this record which marks the end of the WAL stream.
    fn read_shutdown_checkpoint(&self, lsn: LSN) -> Result<LSN, StorageError>;

    /// Returns an iterator over the entries in the wal.
    fn replay_iter(&self) -> impl Iterator<Item = Result<(LSN, Self::ReplayEntry), StorageError>>;

    /// Replays and applies all the entries in the wal to the given graph.
    fn replay_to_graph<G: GraphReplay>(&self, graph: &mut G) -> Result<(), StorageError>;
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
        layer_name: Option<String>,
        layer_id: usize,
        props: Vec<(String, usize, Prop)>,
    ) -> Result<(), StorageError>;

    fn replay_add_edge_metadata(
        &mut self,
        lsn: LSN,
        transaction_id: TransactionID,
        eid: EID,
        layer_id: usize,
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
        layer_id: usize,
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
