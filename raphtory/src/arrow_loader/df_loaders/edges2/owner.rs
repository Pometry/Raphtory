use crate::arrow_loader::df_loaders::edges::store_node_ids;
use crossbeam_channel::Receiver;
use raphtory_api::core::entities::{properties::meta::STATIC_GRAPH_LAYER_ID, EID};
use raphtory_core::entities::{GidRef, VID};
use std::{ops::ControlFlow, panic::AssertUnwindSafe, sync::Arc};
use storage::{
    pages::node_page::bulk_writer::BulkNodeWriter, segments::node::segment::MemNodeSegment,
    Extension, NS,
};

use super::{
    chunk::{Chunk, EdgeRow, NewNode},
    columns::SharedMetadata,
    failure::Failure,
    grouping::{bucket_by, group_by},
    pool::OwnerPool,
    segments::{EdgeSegments, NodeSegmentLock, NodeSegments},
    stats::{OwnerPhase, OwnerStats},
};

type NodeWriter<'w> = BulkNodeWriter<'w, &'w mut MemNodeSegment, NS<Extension>>;

/// An owner looks for dirty segments to flush this often, in messages, so segments that stop
/// receiving rows are still flushed when memory runs short.
const FLUSH_CHECK_EVERY: usize = 64;

pub(super) enum OwnerMsg<'a> {
    TakeNodeSegment(NodeSegmentLock<'a>),
    StoreNewNodes(Arc<Chunk>, Vec<NewNode>),
    /// Rows whose source node this owner's segments hold.
    WriteOutbound(Arc<Chunk>, Vec<u32>),
    /// Edges written on their source's side, to add to their destination's.
    WriteInbound(Arc<Chunk>, Vec<WrittenEdge>),
    Shutdown,
}

pub(super) struct WrittenEdge {
    row: u32,
    eid: EID,
    already: AlreadyInGraph,
}

#[derive(Clone, Copy)]
struct AlreadyInGraph {
    edge: bool,
    in_layer: bool,
}

/// The only writer of the node segments it is given and of the edge segments it creates: every
/// edge is created by the owner of its source node.
pub(super) struct Owner<'s, 'a> {
    nodes: NodeSegments<'a>,
    edges: EdgeSegments<'a>,
    pool: &'s OwnerPool<'a>,
    shared_metadata: &'s SharedMetadata,
}

impl<'s, 'a> Owner<'s, 'a> {
    pub fn new(
        nodes: NodeSegments<'a>,
        edges: EdgeSegments<'a>,
        pool: &'s OwnerPool<'a>,
        shared_metadata: &'s SharedMetadata,
    ) -> Self {
        Self {
            nodes,
            edges,
            pool,
            shared_metadata,
        }
    }

    /// Writes what arrives until shut down. A panic is recorded as the load's failure, and the
    /// rest of the mailbox is dropped unread: queued messages hold their chunks' budget slots,
    /// which the load waits for before shutting owners down.
    pub fn run(self, mailbox: &Receiver<OwnerMsg<'a>>, failure: &Failure, id: usize) -> OwnerStats {
        match std::panic::catch_unwind(AssertUnwindSafe(|| self.serve(mailbox))) {
            Ok(stats) => stats,
            Err(panic) => {
                failure.record_panic(&format!("segment owner {id}"), panic.as_ref());
                discard_until_shutdown(mailbox);
                OwnerStats::default()
            }
        }
    }

    fn serve(mut self, mailbox: &Receiver<OwnerMsg<'a>>) -> OwnerStats {
        let mut stats = OwnerStats::default();
        let mut until_flush_check = FLUSH_CHECK_EVERY;
        while let Ok(message) = stats.time(OwnerPhase::Idle, || mailbox.recv()) {
            stats.count(&message);
            let next = stats.time(OwnerPhase::Busy, || self.handle(message));
            if next.is_break() {
                break;
            }
            until_flush_check -= 1;
            if until_flush_check == 0 {
                stats.time(OwnerPhase::Busy, || self.flush_if_memory_is_short());
                until_flush_check = FLUSH_CHECK_EVERY;
            }
        }
        stats.time(OwnerPhase::Finishing, || self.finish());
        stats
    }

    fn handle(&mut self, message: OwnerMsg<'a>) -> ControlFlow<()> {
        match message {
            OwnerMsg::TakeNodeSegment(segment) => self.nodes.take(segment),
            OwnerMsg::StoreNewNodes(chunk, nodes) => self.store_new_nodes(&chunk, &nodes),
            OwnerMsg::WriteOutbound(chunk, rows) => self.write_outbound(&chunk, &rows),
            OwnerMsg::WriteInbound(chunk, edges) => self.write_inbound(&chunk, &edges),
            OwnerMsg::Shutdown => return ControlFlow::Break(()),
        }
        ControlFlow::Continue(())
    }

    fn store_new_nodes(&mut self, chunk: &Chunk, new_nodes: &[NewNode]) {
        let ids = chunk
            .rows
            .node_ids()
            .expect("a chunk's endpoint columns were read when it was routed");
        for (segment, nodes) in group_by(new_nodes, |node| self.nodes.segment_of(node.vid)) {
            let gids: Vec<(GidRef<'_>, VID)> =
                nodes.iter().map(|node| (ids.of(node), node.vid)).collect();
            store_node_ids(&gids, self.nodes.get_mut(segment));
        }
    }

    fn write_outbound(&mut self, chunk: &Arc<Chunk>, rows: &[u32]) {
        let written = self.add_out_edges(chunk, rows);
        self.add_to_edge_segments(chunk, &written);
        self.send_inbound(chunk, written);
    }

    fn add_out_edges(&mut self, chunk: &Chunk, rows: &[u32]) -> Vec<WrittenEdge> {
        let mut written = Vec::with_capacity(rows.len());
        for (segment, rows) in group_by(rows.iter().copied(), |&row| {
            self.nodes.segment_of(chunk.edge(row).src)
        }) {
            let mut writer = self.nodes.get_mut(segment).bulk_writer();
            for row in rows {
                if let Some((eid, already)) =
                    add_out_edge(&mut writer, &mut self.edges, chunk.edge(row))
                {
                    written.push(WrittenEdge { row, eid, already });
                }
            }
        }
        written
    }

    fn add_to_edge_segments(&mut self, chunk: &Chunk, written: &[WrittenEdge]) {
        for (segment, edges) in group_by(written, |edge| self.edges.segment_of(edge.eid)) {
            let mut writer = self.edges.get_mut(segment).bulk_writer();
            for written in edges {
                let Some(pos) = writer.resolve_pos(written.eid) else {
                    continue;
                };
                let edge = chunk.edge(written.row);
                writer.bulk_add_edge(
                    edge.time,
                    pos,
                    edge.src,
                    edge.dst,
                    written.already.edge,
                    edge.layer,
                    chunk
                        .metadata(written.row)
                        .chain(self.shared_metadata.iter()),
                    chunk.temporal_properties(written.row),
                );
            }
        }
    }

    fn write_inbound(&mut self, chunk: &Chunk, written: &[WrittenEdge]) {
        for (segment, edges) in group_by(written, |edge| {
            self.nodes.segment_of(chunk.edge(edge.row).dst)
        }) {
            let mut writer = self.nodes.get_mut(segment).bulk_writer();
            for written in edges {
                add_in_edge(
                    &mut writer,
                    chunk.edge(written.row),
                    written.eid,
                    written.already,
                );
            }
        }
    }

    fn send_inbound(&self, chunk: &Arc<Chunk>, written: Vec<WrittenEdge>) {
        let owners = self.pool.segment_owners.latest();
        let by_owner = bucket_by(written, self.pool.len(), |edge| {
            owners.of_segment(self.nodes.segment_of(chunk.edge(edge.row).dst))
        });
        self.pool.send_buckets(by_owner, |edges| {
            OwnerMsg::WriteInbound(chunk.clone(), edges)
        });
    }

    fn flush_if_memory_is_short(&mut self) {
        self.nodes.flush_if_memory_is_short();
        self.edges.flush_if_memory_is_short();
    }

    /// Flushes what memory requires, then releases the segments, which sorts what the bulk
    /// writers appended unsorted (see `finish_bulk_load`).
    fn finish(mut self) {
        self.flush_if_memory_is_short();
    }
}

/// Adds an edge update to its source node, creating the edge if the graph doesn't have it yet.
fn add_out_edge(
    writer: &mut NodeWriter<'_>,
    edges: &mut EdgeSegments,
    edge: EdgeRow,
) -> Option<(EID, AlreadyInGraph)> {
    let src = writer.resolve_pos(edge.src)?;
    let (eid, edge_existed) = match writer.get_out_edge(src, edge.dst, STATIC_GRAPH_LAYER_ID) {
        Some(eid) => (eid, true),
        None => {
            let eid = edges.new_edge_id();
            writer.add_static_outbound_edge(src, edge.dst, eid);
            (eid, false)
        }
    };
    let in_layer = edge_existed
        && (edges.has_edge_in_layer(eid, edge.layer)
            || writer.get_out_edge(src, edge.dst, edge.layer).is_some());

    let layered = eid.with_layer(edge.layer);
    if in_layer {
        writer.update_timestamp(edge.time, src, layered);
    } else {
        writer.add_outbound_edge(Some(edge.time), src, edge.dst, layered);
    }
    Some((
        eid,
        AlreadyInGraph {
            edge: edge_existed,
            in_layer,
        },
    ))
}

/// Adds an edge update to its destination node. A self-loop's time is already on its source
/// side, which is the same node.
fn add_in_edge(writer: &mut NodeWriter<'_>, edge: EdgeRow, eid: EID, already: AlreadyInGraph) {
    let Some(dst) = writer.resolve_pos(edge.dst) else {
        return;
    };
    if !already.edge {
        writer.add_static_inbound_edge(dst, edge.src, eid);
    }
    let layered = eid.with_layer(edge.layer);
    let self_loop = edge.src == edge.dst;
    match (self_loop, already.in_layer) {
        (false, true) => writer.update_timestamp(edge.time, dst, layered),
        (false, false) => writer.add_inbound_edge(Some(edge.time), dst, edge.src, layered),
        (true, false) => writer.add_inbound_edge::<i64>(None, dst, edge.src, layered),
        (true, true) => {}
    }
}

fn discard_until_shutdown(mailbox: &Receiver<OwnerMsg>) {
    while let Ok(message) = mailbox.recv() {
        if matches!(message, OwnerMsg::Shutdown) {
            break;
        }
    }
}
