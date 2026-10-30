//! `load_edges_from_df2`: the pipelined bulk edge loader (ADR-0002).
//!
//! Readers take chunks in input order under the in-flight budget, resolve GIDs and group rows
//! by the owner of each row's source node. Segment owners are the only writers to their
//! segments: an owner finds or creates every edge whose source node it owns, allocating new
//! edges in edge segments it owns, and sends the inbound half to the destination node's owner.
//! The load coordinator (the calling thread) holds the graph's write lock, locks new node
//! segments as resolution creates them and decides who owns them.
//!
//! Not yet supported, and so left to `load_edges_from_df`: properties and metadata, deletions,
//! pre-resolved VIDs, and graphs that already have edges (whose edge segments would need
//! owners other than the edges' source node owners).

use crate::{
    arrow_loader::{
        dataframe::{DFChunk, DFView, SecondaryIndexCol, TimeCol},
        df_loaders::{edges::store_node_ids, secondary_index_at},
        layer_col::lift_layer_col,
    },
    db::api::view::StaticGraphViewOps,
    errors::{into_graph_err, GraphError, LoadError},
    prelude::*,
};
use crossbeam_channel::{bounded, select, unbounded, Receiver, Sender};
use parking_lot::{Mutex, RwLock};
use raphtory_api::core::{
    entities::{properties::prop::PropRef, LayerId, EID},
    storage::timeindex::EventTime,
};
use raphtory_core::entities::{GidRef, VID};
use std::{
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use db4_graph::TemporalGraph;
use storage::{
    api::{edges::EdgeSegmentOps, nodes::NodeSegmentOps},
    pages::{
        edge_store::EdgeStorageInner,
        locked::{edges::LockedEdgeSegment, nodes::LockedNodeSegment},
        node_store::NodeStorageInner,
        resolve_pos,
    },
    resolver::GIDResolverOps,
    Extension, ES, NS,
};

use super::edges::ColumnNames;

type NodeSeg = NS<Extension>;
type EdgeSeg = ES<Extension>;
type NodeStore = NodeStorageInner<NodeSeg, Extension>;
type EdgeStore = EdgeStorageInner<EdgeSeg, Extension>;

/// How long to wait for a segment lock; nothing else should hold one during a bulk load.
const LOCK_TIMEOUT: Duration = Duration::from_secs(60);

/// An owner opens a writer on its dirty segments this often (in messages), so segments that
/// receive no rows still get flushed under memory pressure.
const FLUSH_CHECK_EVERY: usize = 64;

struct Config {
    readers: usize,
    owners: usize,
    in_flight: usize,
    stats: bool,
}

impl Config {
    fn from_env() -> Self {
        let var = |name: &str| std::env::var(name).ok().and_then(|v| v.parse::<usize>().ok());
        let cores = std::thread::available_parallelism().map_or(8, |n| n.get());
        let readers = var("RAPHTORY_LOAD2_READERS").unwrap_or(4).max(1);
        let owners = var("RAPHTORY_LOAD2_OWNERS")
            .unwrap_or(cores.saturating_sub(readers))
            .max(1);
        Self {
            readers,
            owners,
            in_flight: var("RAPHTORY_LOAD2_IN_FLIGHT").unwrap_or(2 * owners).max(1),
            stats: std::env::var_os("RAPHTORY_LOAD2_STATS").is_some(),
        }
    }
}

/// The rows per chunk when the caller doesn't choose: big enough that resolution's fixed cost
/// per call stays small, small enough to keep the pipeline busy.
pub const DEFAULT_CHUNK_ROWS: usize = 256 * 1024;

/// A run of input rows taken as one piece, in input order: either chunks already decoded, or
/// row groups of a parquet file that the reader taking the unit decodes.
pub struct InputUnit {
    pub rows: usize,
    pub decode: Box<dyn FnOnce() -> Result<Vec<DFChunk>, GraphError> + Send>,
}

impl InputUnit {
    pub fn decoded(chunk: DFChunk) -> Self {
        Self {
            rows: chunk.len(),
            decode: Box::new(move || Ok(vec![chunk])),
        }
    }
}

/// Whether a load of edges goes through `load_edges_from_df2`: `RAPHTORY_LOAD_V2` is set and
/// the load is one it supports so far.
pub fn use_load2(resolve_nodes: bool, delete: bool, graph: &impl StaticGraphViewOps) -> bool {
    std::env::var_os("RAPHTORY_LOAD_V2").is_some()
        && resolve_nodes
        && !delete
        && graph.count_edges() == 0
}

/// One chunk's columns, shared by every message about it. Dropping the last reference returns
/// the chunk's slot in the in-flight budget.
struct ChunkWork {
    df: DFChunk,
    src_index: usize,
    dst_index: usize,
    src: Vec<VID>,
    dst: Vec<VID>,
    time: TimeCol,
    secondary_index: SecondaryIndexCol,
    layer: Vec<usize>,
    _slot: Slot,
}

impl ChunkWork {
    fn event_time(&self, row: usize) -> EventTime {
        EventTime(self.time[row], secondary_index_at(&self.secondary_index, row))
    }
}

/// A slot in the in-flight budget, returned when dropped.
struct Slot(Sender<()>);

impl Drop for Slot {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

/// The inbound half of a row, for the owner of its destination node.
struct InboundRow {
    src: VID,
    dst: VID,
    eid: EID,
    t: EventTime,
    layer: usize,
    /// the edge was already in this layer
    in_layer: bool,
    /// the edge already existed in the static graph
    exists: bool,
}

enum OwnerMsg<'a> {
    NodeSegment(LockedNodeSegment<'a, NodeSeg>),
    /// Nodes created by resolution, to store GIDs for; rows index the chunk's src column
    /// followed by its dst column.
    NewNodes(Arc<ChunkWork>, Vec<(u32, VID)>),
    /// Rows whose source node this owner owns.
    Outbound(Arc<ChunkWork>, Vec<u32>),
    Inbound(Arc<ChunkWork>, Vec<InboundRow>),
    Shutdown,
}

enum Request {
    /// Lock and assign every node segment below `upto`; replies with the owner of each segment.
    EnsureNodeSegments {
        upto: usize,
        reply: Sender<Result<Arc<[u16]>, GraphError>>,
    },
}

#[derive(Default)]
struct ReaderStats {
    chunks: u64,
    rows: u64,
    /// waiting for a slot in the in-flight budget
    budget_wait: Duration,
    /// taking the next unit from the input (under its lock)
    source: Duration,
    decode: Duration,
    resolve: Duration,
    /// everything else: validation, layers, grouping, sending
    other: Duration,
}

#[derive(Default)]
struct OwnerStats {
    busy: Duration,
    idle: Duration,
    finish: Duration,
    outbound_rows: u64,
    inbound_rows: u64,
}

/// Loads the edges of `df_view`; see the module docs.
pub fn load_edges_from_df2<G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>> + Send>,
    column_names: ColumnNames,
    layer: Option<&str>,
    graph: &G,
) -> Result<(), GraphError> {
    if df_view.is_empty() {
        return Ok(());
    }
    let units = df_view
        .chunks
        .map(|chunk| chunk.map(InputUnit::decoded));
    load_edges_from_units(df_view.names, units, column_names, layer, graph)
}

/// Loads the edges of `units`, whose chunks have the columns `names`; see the module docs.
pub fn load_edges_from_units<G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps>(
    names: Vec<String>,
    units: impl Iterator<Item = Result<InputUnit, GraphError>> + Send,
    column_names: ColumnNames,
    layer: Option<&str>,
    graph: &G,
) -> Result<(), GraphError> {
    let config = Config::from_env();
    let load_start = Instant::now();
    let df_view = DFView::new(names, (), None);

    let src_index = df_view.get_index(column_names.src)?;
    let dst_index = df_view.get_index(column_names.dst)?;
    let time_index = df_view.get_index(column_names.time)?;
    let secondary_index_index = column_names
        .secondary_index
        .map(|col| df_view.get_index(col))
        .transpose()?;
    let layer_index = column_names
        .layer_col
        .map(|name| df_view.get_index(name))
        .transpose()?;

    let mut write_locked_graph = graph.write_lock().map_err(into_graph_err)?;
    let tg = write_locked_graph.graph;
    let node_store: &NodeStore = tg.storage().nodes();
    let edge_store: &EdgeStore = tg.storage().edges();
    let max_node_len = node_store.max_segment_len();
    let max_edge_len = edge_store.max_page_len();

    let initial_nodes = write_locked_graph.nodes.take_all();
    // held, unused, until the end: new edges go into segments the owners create
    let initial_edges = write_locked_graph.edges.take_all();
    if initial_edges.iter().any(|s| s.segment().num_edges() > 0) {
        return Err(GraphError::LoadFailure(
            "load_edges_from_df2 only loads into a graph without edges".to_string(),
        ));
    }

    let (owner_txs, owner_rxs): (Vec<_>, Vec<_>) =
        (0..config.owners).map(|_| unbounded()).unzip();

    // initial node segments are spread round-robin
    let mut node_owner = Vec::with_capacity(initial_nodes.len());
    let mut segments_owned = vec![0; config.owners];
    for segment in initial_nodes {
        let owner = segment.segment_id() % config.owners;
        node_owner.push(owner as u16);
        segments_owned[owner] += 1;
        let _ = owner_txs[owner].send(OwnerMsg::NodeSegment(segment));
    }
    let node_owner_map = RwLock::new(Arc::<[u16]>::from(node_owner));
    let rows_routed: Vec<AtomicU64> = (0..config.owners).map(|_| AtomicU64::new(0)).collect();

    let (slot_tx, slot_rx) = bounded(config.in_flight);
    for _ in 0..config.in_flight {
        let _ = slot_tx.send(());
    }
    let (req_tx, req_rx) = unbounded::<Request>();
    let (done_tx, done_rx) = unbounded::<()>();

    let source = Mutex::new(units);
    let first_error: Mutex<Option<GraphError>> = Mutex::new(None);
    let abort = AtomicBool::new(false);

    let (reader_stats, owner_stats) = std::thread::scope(|s| {
        let owners: Vec<_> = owner_rxs
            .into_iter()
            .enumerate()
            .map(|(id, rx)| {
                let owner = Owner {
                    id,
                    nodes: Vec::new(),
                    edges: Vec::new(),
                    open_edge_segment: None,
                    edge_store,
                    max_node_len,
                    max_edge_len,
                    peers: owner_txs.clone(),
                    node_owner: &node_owner_map,
                    stats: OwnerStats::default(),
                };
                let (first_error, abort) = (&first_error, &abort);
                s.spawn(move || {
                    match std::panic::catch_unwind(AssertUnwindSafe(|| owner.run(&rx))) {
                        Ok(stats) => stats,
                        Err(panic) => {
                            abort.store(true, Ordering::Relaxed);
                            first_error.lock().get_or_insert(GraphError::LoadFailure(format!(
                                "segment owner {id} panicked: {}",
                                panic_message(panic.as_ref())
                            )));
                            // Messages left queued here hold their chunks' slots in the
                            // in-flight budget: keep dropping them so the coordinator, which
                            // waits for every slot, isn't left waiting forever.
                            while let Ok(msg) = rx.recv() {
                                if matches!(msg, OwnerMsg::Shutdown) {
                                    break;
                                }
                            }
                            OwnerStats::default()
                        }
                    }
                })
            })
            .collect();

        let readers: Vec<_> = (0..config.readers)
            .map(|_| {
                let ctx = ReaderCtx {
                    graph,
                    tg,
                    node_store,
                    source: &source,
                    slots: (&slot_tx, &slot_rx),
                    requests: req_tx.clone(),
                    owners: &owner_txs,
                    rows_routed: &rows_routed,
                    abort: &abort,
                    src_index,
                    dst_index,
                    time_index,
                    secondary_index_index,
                    layer_index,
                    layer,
                    max_node_len,
                };
                let done = done_tx.clone();
                let first_error = &first_error;
                let abort = &abort;
                s.spawn(move || {
                    let _done = DoneOnDrop(done);
                    let mut stats = ReaderStats::default();
                    if let Err(err) = ctx.run(&mut stats) {
                        abort.store(true, Ordering::Relaxed);
                        first_error.lock().get_or_insert(err);
                    }
                    stats
                })
            })
            .collect();
        drop(req_tx);
        drop(done_tx);

        // the coordinator: assign new node segments until every reader is done
        let mut coordinator = Coordinator {
            node_store,
            owners: &owner_txs,
            rows_routed: &rows_routed,
            segments_owned,
            node_owner: &node_owner_map,
        };
        let mut readers_done = 0;
        while readers_done < config.readers {
            select! {
                recv(req_rx) -> request => {
                    if let Ok(Request::EnsureNodeSegments { upto, reply }) = request {
                        let _ = reply.send(coordinator.ensure_node_segments(upto));
                    }
                }
                recv(done_rx) -> _ => readers_done += 1,
            }
        }
        // every slot coming back means every chunk has been fully written
        for _ in 0..config.in_flight {
            let _ = slot_rx.recv();
        }
        for tx in &owner_txs {
            let _ = tx.send(OwnerMsg::Shutdown);
        }

        let reader_stats: Vec<_> = readers
            .into_iter()
            .map(|h| h.join().expect("bulk load reader panicked"))
            .collect();
        let owner_stats: Vec<_> = owners
            .into_iter()
            .map(|h| h.join().unwrap_or_default())
            .collect();
        (reader_stats, owner_stats)
    });
    drop(initial_edges);

    if config.stats {
        print_stats(&config, load_start.elapsed(), &reader_stats, &owner_stats);
    }
    match first_error.into_inner() {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}

/// Tells the coordinator a reader finished, even if it panicked.
struct DoneOnDrop(Sender<()>);

impl Drop for DoneOnDrop {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

struct Coordinator<'s, 'a> {
    node_store: &'a NodeStore,
    owners: &'s [Sender<OwnerMsg<'a>>],
    rows_routed: &'s [AtomicU64],
    /// node segments held by each owner
    segments_owned: Vec<usize>,
    node_owner: &'s RwLock<Arc<[u16]>>,
}

impl Coordinator<'_, '_> {
    fn ensure_node_segments(&mut self, upto: usize) -> Result<Arc<[u16]>, GraphError> {
        let current = self.node_owner.read().clone();
        if upto <= current.len() {
            return Ok(current);
        }
        let mut owners = current.to_vec();
        for segment_id in current.len()..upto {
            let segment = self
                .node_store
                .lock_segment(segment_id, LOCK_TIMEOUT)
                .map_err(into_graph_err)?;
            // Every segment gets a share of new nodes for the rest of the load, so balance by
            // segments held; rows routed only breaks ties (they are all 0 when the first
            // chunk creates a batch of segments at once).
            let owner = (0..self.owners.len())
                .min_by_key(|&o| (self.segments_owned[o], self.rows_routed[o].load(Ordering::Relaxed)))
                .unwrap_or(0);
            // sent before any row for the segment, so it arrives first
            let _ = self.owners[owner].send(OwnerMsg::NodeSegment(segment));
            self.segments_owned[owner] += 1;
            owners.push(owner as u16);
        }
        let owners = Arc::<[u16]>::from(owners);
        *self.node_owner.write() = owners.clone();
        Ok(owners)
    }
}

struct ReaderCtx<'s, 'a, G, I> {
    graph: &'a G,
    tg: &'a TemporalGraph<Extension>,
    node_store: &'a NodeStore,
    source: &'s Mutex<I>,
    slots: (&'s Sender<()>, &'s Receiver<()>),
    requests: Sender<Request>,
    owners: &'s [Sender<OwnerMsg<'a>>],
    rows_routed: &'s [AtomicU64],
    abort: &'s AtomicBool,
    src_index: usize,
    dst_index: usize,
    time_index: usize,
    secondary_index_index: Option<usize>,
    layer_index: Option<usize>,
    layer: Option<&'s str>,
    max_node_len: u32,
}

impl<G, I> ReaderCtx<'_, '_, G, I>
where
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
    I: Iterator<Item = Result<InputUnit, GraphError>> + Send,
{
    fn run(&self, stats: &mut ReaderStats) -> Result<(), GraphError> {
        let mut scratch = ReaderScratch::default();
        while !self.abort.load(Ordering::Relaxed) {
            // take the next unit and its event ids together, so ids follow input order
            let source_start = Instant::now();
            let next = {
                let mut source = self.source.lock();
                match source.next() {
                    None => None,
                    Some(unit) => {
                        let unit = unit?;
                        let event_ids = self
                            .secondary_index_index
                            .is_none()
                            .then(|| self.tg.storage().reserve_event_ids(unit.rows));
                        Some((unit, event_ids))
                    }
                }
            };
            stats.source += source_start.elapsed();
            let Some((unit, event_ids)) = next else {
                break;
            };

            let decode_start = Instant::now();
            let chunks = (unit.decode)()?;
            stats.decode += decode_start.elapsed();

            let mut offset = 0;
            for df in chunks {
                let secondary_index = match (self.secondary_index_index, event_ids) {
                    (Some(index), _) => {
                        let col = df.secondary_index_col(index)?;
                        self.tg.storage().set_max_event_id(col.max());
                        col
                    }
                    (None, Some(start)) => SecondaryIndexCol::new_from_range(
                        start + offset,
                        start + offset + df.len(),
                    ),
                    (None, None) => unreachable!("event ids are reserved without an index column"),
                };
                offset += df.len();

                let wait = Instant::now();
                if self.slots.1.recv().is_err() {
                    return Ok(());
                }
                let slot = Slot(self.slots.0.clone());
                stats.budget_wait += wait.elapsed();
                self.route_chunk(df, secondary_index, slot, &mut scratch, stats)?;
            }
            if offset != unit.rows {
                return Err(GraphError::LoadFailure(format!(
                    "an input unit decoded to {offset} rows, not the {} it reserved event ids for",
                    unit.rows
                )));
            }
        }
        Ok(())
    }

    /// Resolves one chunk's nodes and sends its rows to the owners of their source nodes.
    fn route_chunk(
        &self,
        df: DFChunk,
        secondary_index: SecondaryIndexCol,
        slot: Slot,
        scratch: &mut ReaderScratch,
        stats: &mut ReaderStats,
    ) -> Result<(), GraphError> {
        let (tg, node_store) = (self.tg, self.node_store);
        let ReaderScratch {
            vids,
            new_nodes,
            node_owner,
        } = scratch;
        let other_start = Instant::now();
        let n = df.len();
        let src_col = df.node_col(self.src_index)?;
        let dst_col = df.node_col(self.dst_index)?;
        src_col.validate(self.graph, LoadError::InvalidSrcError)?;
        dst_col.validate(self.graph, LoadError::InvalidDstError)?;
        let time = df.time_col(self.time_index)?;
        let (layer, _) = lift_layer_col(self.layer, self.layer_index, &df)?
            .resolve_layer(None, self.graph, false)?;
        let layer = layer.into_owned();
        let mut other = other_start.elapsed();

        let resolve_start = Instant::now();
        let alloc = |hint: usize, count: u32| {
            let (segment, pos, got) = node_store.reserve_free_range(hint, count);
            (pos.as_vid(segment, self.max_node_len), got)
        };
        tg.gid_resolver
            .resolve_batch(
                &[df.chunk[self.src_index].as_ref(), df.chunk[self.dst_index].as_ref()],
                &alloc,
                vids,
                new_nodes,
            )
            .map_err(into_graph_err)?;
        stats.resolve += resolve_start.elapsed();

        let other_start = Instant::now();
        // Resolution may have created node segments that have no owner yet. Take the highest
        // segment this chunk uses, not the store's segment count: resolution allocates from
        // several threads, so a later segment can finish being created before an earlier one,
        // and the count only covers segments up to the first that is still being created.
        let segments = vids
            .iter()
            .map(|vid| resolve_pos(vid.index(), self.max_node_len).0 + 1)
            .max()
            .unwrap_or(0);
        if segments > node_owner.len() {
            let (reply_tx, reply_rx) = bounded(1);
            let _ = self.requests.send(Request::EnsureNodeSegments {
                upto: segments,
                reply: reply_tx,
            });
            *node_owner = reply_rx.recv().map_err(|_| {
                GraphError::LoadFailure("bulk load coordinator stopped".to_string())
            })??;
        }
        let owner_of = |vid: VID| {
            let (segment, _) = resolve_pos(vid.index(), self.max_node_len);
            node_owner[segment] as usize
        };

        let mut outbound = vec![Vec::new(); self.owners.len()];
        for (row, &src) in vids[..n].iter().enumerate() {
            outbound[owner_of(src)].push(row as u32);
        }
        let mut created = vec![Vec::new(); self.owners.len()];
        for &(row, vid) in new_nodes.iter() {
            created[owner_of(vid)].push((row as u32, vid));
        }

        let work = Arc::new(ChunkWork {
            src: vids[..n].to_vec(),
            dst: vids[n..].to_vec(),
            df,
            src_index: self.src_index,
            dst_index: self.dst_index,
            time,
            secondary_index,
            layer,
            _slot: slot,
        });
        for (owner, rows) in created.into_iter().enumerate() {
            if !rows.is_empty() {
                let _ = self.owners[owner].send(OwnerMsg::NewNodes(work.clone(), rows));
            }
        }
        for (owner, rows) in outbound.into_iter().enumerate() {
            if !rows.is_empty() {
                self.rows_routed[owner].fetch_add(rows.len() as u64, Ordering::Relaxed);
                let _ = self.owners[owner].send(OwnerMsg::Outbound(work.clone(), rows));
            }
        }
        other += other_start.elapsed();
        stats.other += other;
        stats.chunks += 1;
        stats.rows += n as u64;
        Ok(())
    }
}

/// A reader's buffers, reused across chunks.
#[derive(Default)]
struct ReaderScratch {
    vids: Vec<VID>,
    new_nodes: Vec<(usize, VID)>,
    /// the coordinator's latest owner of each node segment
    node_owner: Arc<[u16]>,
}

struct Owner<'s, 'a> {
    id: usize,
    /// by segment id
    nodes: Vec<Option<LockedNodeSegment<'a, NodeSeg>>>,
    edges: Vec<Option<LockedEdgeSegment<'a, EdgeSeg>>>,
    /// where this owner allocates new edges
    open_edge_segment: Option<usize>,
    edge_store: &'a EdgeStore,
    max_node_len: u32,
    max_edge_len: u32,
    peers: Vec<Sender<OwnerMsg<'a>>>,
    node_owner: &'s RwLock<Arc<[u16]>>,
    stats: OwnerStats,
}

fn slot_mut<T>(slots: &mut Vec<Option<T>>, index: usize) -> &mut Option<T> {
    if slots.len() <= index {
        slots.resize_with(index + 1, || None);
    }
    &mut slots[index]
}

/// Groups `items` by `key`, keeping their order within each group.
fn group_by_key<T>(items: impl IntoIterator<Item = T>, key: impl Fn(&T) -> usize) -> Vec<(usize, Vec<T>)> {
    let mut groups: Vec<(usize, Vec<T>)> = Vec::new();
    for item in items {
        let k = key(&item);
        match groups.iter_mut().find(|(g, _)| *g == k) {
            Some((_, group)) => group.push(item),
            None => groups.push((k, vec![item])),
        }
    }
    groups
}

impl<'a> Owner<'_, 'a> {
    fn run(mut self, rx: &Receiver<OwnerMsg<'a>>) -> OwnerStats {
        let mut since_flush_check = 0;
        loop {
            let wait = Instant::now();
            let Ok(msg) = rx.recv() else { break };
            self.stats.idle += wait.elapsed();

            let busy = Instant::now();
            match msg {
                OwnerMsg::NodeSegment(segment) => {
                    let id = segment.segment_id();
                    *slot_mut(&mut self.nodes, id) = Some(segment);
                }
                OwnerMsg::NewNodes(work, rows) => self.store_new_nodes(&work, &rows),
                OwnerMsg::Outbound(work, rows) => self.outbound(&work, &rows),
                // `_work` keeps the chunk's slot taken until its inbound rows are written
                OwnerMsg::Inbound(_work, rows) => self.inbound(&rows),
                OwnerMsg::Shutdown => break,
            }
            since_flush_check += 1;
            if since_flush_check >= FLUSH_CHECK_EVERY {
                self.check_flush();
                since_flush_check = 0;
            }
            self.stats.busy += busy.elapsed();
        }

        // dropping the node segments sorts their additions (see `finish_bulk_load`)
        let finish = Instant::now();
        self.check_flush();
        self.nodes.clear();
        self.edges.clear();
        self.stats.finish = finish.elapsed();
        self.stats
    }

    /// Opens a writer on every dirty segment, which flushes it if memory is short.
    fn check_flush(&mut self) {
        for segment in self.nodes.iter_mut().flatten() {
            if segment.segment().is_dirty() {
                let _writer = segment.writer();
            }
        }
        for segment in self.edges.iter_mut().flatten() {
            if segment.segment().is_dirty() {
                let _writer = segment.writer();
            }
        }
    }

    fn node_segment(&mut self, vid: VID) -> &mut LockedNodeSegment<'a, NodeSeg> {
        let (segment, _) = resolve_pos(vid.index(), self.max_node_len);
        self.nodes
            .get_mut(segment)
            .and_then(Option::as_mut)
            .expect("rows were routed to an owner that doesn't hold their node segment")
    }

    fn store_new_nodes(&mut self, work: &ChunkWork, rows: &[(u32, VID)]) {
        let (Ok(src_col), Ok(dst_col)) = (work.df.node_col(work.src_index), work.df.node_col(work.dst_index)) else {
            return;
        };
        let n = work.src.len();
        let gid = |row: u32| -> GidRef<'_> {
            let row = row as usize;
            let gid = if row < n { src_col.get(row) } else { dst_col.get(row - n) };
            gid.expect("new node row out of range")
        };
        let max_node_len = self.max_node_len;
        for (_, group) in group_by_key(rows.iter().copied(), |&(_, vid)| {
            resolve_pos(vid.index(), max_node_len).0
        }) {
            let segment = self.node_segment(group[0].1);
            let gids: Vec<(GidRef<'_>, VID)> = group.iter().map(|&(row, vid)| (gid(row), vid)).collect();
            store_node_ids(&gids, segment);
        }
    }

    /// A new EID in an edge segment this owner holds, opening a new segment when it is full.
    fn new_eid(
        edges: &mut Vec<Option<LockedEdgeSegment<'a, EdgeSeg>>>,
        open: &mut Option<usize>,
        edge_store: &'a EdgeStore,
        max_edge_len: u32,
    ) -> EID {
        loop {
            if let Some(segment_id) = *open {
                if let Some(pos) = edge_store.reserve_pos_in(segment_id) {
                    return pos.as_eid(segment_id, max_edge_len);
                }
            }
            let segment_id = edge_store.push_new_segment();
            let segment = edge_store
                .lock_segment(segment_id, LOCK_TIMEOUT)
                .expect("a new edge segment is locked by nobody else");
            *slot_mut(edges, segment_id) = Some(segment);
            *open = Some(segment_id);
        }
    }

    fn outbound(&mut self, work: &Arc<ChunkWork>, rows: &[u32]) {
        let node_owner = self.node_owner.read().clone();
        let max_node_len = self.max_node_len;
        let max_edge_len = self.max_edge_len;
        let mut inbound: Vec<Vec<InboundRow>> = (0..self.peers.len()).map(|_| Vec::new()).collect();
        let mut edge_rows: Vec<(u32, EID, bool)> = Vec::with_capacity(rows.len());

        let Owner {
            nodes,
            edges,
            open_edge_segment,
            edge_store,
            ..
        } = self;
        for (segment_id, group) in group_by_key(rows.iter().copied(), |&row| {
            resolve_pos(work.src[row as usize].index(), max_node_len).0
        }) {
            let segment = nodes[segment_id]
                .as_mut()
                .expect("rows were routed to an owner that doesn't hold their node segment");
            let mut writer = segment.bulk_writer();
            for row in group {
                let r = row as usize;
                let (src, dst, layer) = (work.src[r], work.dst[r], work.layer[r]);
                let t = work.event_time(r);
                let Some(src_pos) = writer.resolve_pos(src) else {
                    continue;
                };

                // find the edge in the static graph, or create it
                let (eid, exists) = match writer.get_out_edge(src_pos, dst, LayerId(0)) {
                    Some(eid) => (eid, true),
                    None => {
                        let eid = Self::new_eid(edges, open_edge_segment, edge_store, max_edge_len);
                        writer.add_static_outbound_edge(src_pos, dst, eid);
                        (eid, false)
                    }
                };
                let elid = eid.with_layer(LayerId(layer));
                let in_layer = exists && {
                    let (edge_segment, edge_pos) = resolve_pos(eid, max_edge_len);
                    edges
                        .get(edge_segment)
                        .and_then(Option::as_ref)
                        .is_some_and(|s| s.has_edge(edge_pos, LayerId(layer)))
                        || writer.get_out_edge(src_pos, dst, LayerId(layer)).is_some()
                };
                if in_layer {
                    writer.update_timestamp(t, src_pos, elid);
                } else {
                    writer.add_outbound_edge(Some(t), src_pos, dst, elid);
                }

                edge_rows.push((row, eid, exists));
                let (dst_segment, _) = resolve_pos(dst.index(), max_node_len);
                inbound[node_owner[dst_segment] as usize].push(InboundRow {
                    src,
                    dst,
                    eid,
                    t,
                    layer,
                    in_layer,
                    exists,
                });
            }
        }

        // register the edges and their times in this owner's edge segments
        for (segment_id, group) in
            group_by_key(edge_rows, |&(_, eid, _)| resolve_pos(eid, max_edge_len).0)
        {
            let segment = edges[segment_id]
                .as_mut()
                .expect("an edge was created in a segment its owner doesn't hold");
            let mut writer = segment.bulk_writer();
            for (row, eid, exists) in group {
                let r = row as usize;
                if let Some(pos) = writer.resolve_pos(eid) {
                    writer.bulk_add_edge(
                        work.event_time(r),
                        pos,
                        work.src[r],
                        work.dst[r],
                        exists,
                        LayerId(work.layer[r]),
                        std::iter::empty::<(usize, PropRef)>(),
                        std::iter::empty::<(usize, PropRef)>(),
                    );
                }
            }
        }
        self.stats.outbound_rows += rows.len() as u64;

        for (owner, rows) in inbound.into_iter().enumerate() {
            if rows.is_empty() {
                continue;
            }
            if owner == self.id {
                self.inbound(&rows);
            } else {
                let _ = self.peers[owner].send(OwnerMsg::Inbound(work.clone(), rows));
            }
        }
    }

    fn inbound(&mut self, rows: &[InboundRow]) {
        let max_node_len = self.max_node_len;
        for (segment_id, group) in group_by_key(rows.iter(), |row| {
            resolve_pos(row.dst.index(), max_node_len).0
        }) {
            let segment = self.nodes[segment_id]
                .as_mut()
                .expect("rows were routed to an owner that doesn't hold their node segment");
            let mut writer = segment.bulk_writer();
            for row in group {
                let Some(dst_pos) = writer.resolve_pos(row.dst) else {
                    continue;
                };
                if !row.exists {
                    writer.add_static_inbound_edge(dst_pos, row.src, row.eid);
                }
                let elid = row.eid.with_layer(LayerId(row.layer));
                if row.src != row.dst {
                    if row.in_layer {
                        writer.update_timestamp(row.t, dst_pos, elid);
                    } else {
                        writer.add_inbound_edge(Some(row.t), dst_pos, row.src, elid);
                    }
                } else if !row.in_layer {
                    // self-loop: the outbound side recorded the time
                    writer.add_inbound_edge::<i64>(None, dst_pos, row.src, elid);
                }
            }
        }
        self.stats.inbound_rows += rows.len() as u64;
    }
}

fn print_stats(config: &Config, wall: Duration, readers: &[ReaderStats], owners: &[OwnerStats]) {
    let secs = |d: Duration| d.as_secs_f64();
    let rows: u64 = readers.iter().map(|r| r.rows).sum();
    eprintln!(
        "LOAD2 {} rows in {:.1}s ({:.2}M rows/s), {} readers, {} owners, {} chunks in flight",
        rows,
        secs(wall),
        rows as f64 / secs(wall) / 1e6,
        config.readers,
        config.owners,
        config.in_flight
    );
    for (i, r) in readers.iter().enumerate() {
        eprintln!(
            "LOAD2 reader {i}: {} chunks, budget wait {:.1}s, source {:.1}s, decode {:.1}s, resolve {:.1}s, other {:.1}s",
            r.chunks,
            secs(r.budget_wait),
            secs(r.source),
            secs(r.decode),
            secs(r.resolve),
            secs(r.other)
        );
    }
    for (i, o) in owners.iter().enumerate() {
        eprintln!(
            "LOAD2 owner {i}: busy {:.1}s, idle {:.1}s, finish {:.1}s, {} outbound rows, {} inbound rows",
            secs(o.busy),
            secs(o.idle),
            secs(o.finish),
            o.outbound_rows,
            o.inbound_rows
        );
    }
}
