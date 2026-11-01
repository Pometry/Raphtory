use crate::{
    arrow_loader::{
        dataframe::{DFChunk, SecondaryIndexCol, TimeCol},
        layer_col::lift_layer_col,
    },
    db::api::view::StaticGraphViewOps,
    errors::{into_graph_err, GraphError, LoadError},
    prelude::*,
};
use db4_graph::TemporalGraph;
use raphtory_core::entities::VID;
use std::{panic::AssertUnwindSafe, sync::Arc};
use storage::{resolver::GIDResolverOps, Extension};

use super::{
    budget::{InFlightBudget, Slot},
    chunk::{Chunk, Endpoint, Endpoints, EventTimes, NewNode, RowProperties, Rows},
    columns::{EdgeColumns, EdgeSchema},
    coordinator::CoordinatorHandle,
    failure::Failure,
    grouping::bucket_by,
    input::{Input, InputUnit},
    owner::OwnerMsg,
    pool::OwnerPool,
    segments::{SegmentOwners, SegmentSizes},
    stats::{ReaderPhase, ReaderStats},
};

/// Takes units from the input, decodes them, and turns each chunk into messages for the owners
/// of its rows' nodes.
pub(super) struct Reader<'s, 'a, G, I> {
    pub input: &'s Input<'a, I>,
    pub budget: &'s InFlightBudget,
    pub preparer: ChunkPreparer<'s, 'a, G>,
    pub resolver: NodeResolver<'a>,
    pub router: Router<'s, 'a>,
}

/// Reads a chunk's columns into the form owners write: validated endpoints, times, layers and
/// properties.
pub(super) struct ChunkPreparer<'s, 'a, G> {
    pub graph: &'a G,
    pub schema: &'s EdgeSchema<'s>,
}

/// Maps a chunk's node ids to VIDs, creating the nodes the graph doesn't have yet.
pub(super) struct NodeResolver<'a> {
    pub graph: &'a TemporalGraph<Extension>,
    pub columns: EdgeColumns,
}

/// Sends a chunk's rows to the owners of their nodes, asking the coordinator for owners of
/// segments it hasn't heard of yet.
pub(super) struct Router<'s, 'a> {
    pub owners: &'s OwnerPool<'a>,
    pub coordinator: CoordinatorHandle,
    pub known_owners: SegmentOwners,
    pub sizes: SegmentSizes,
}

struct PreparedColumns {
    time: TimeCol,
    layers: Vec<usize>,
    properties: RowProperties,
}

/// A chunk's endpoints resolved: the source of every row, then the destination of every row.
#[derive(Default)]
struct ResolvedNodes {
    vids: Vec<VID>,
    created: Vec<(usize, VID)>,
}

impl<G, I> Reader<'_, '_, G, I>
where
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
    I: Iterator<Item = Result<InputUnit, GraphError>> + Send,
{
    /// Reads until the input runs out or the load fails. Dropping the reader on the way out
    /// tells the coordinator it is done, even after a panic.
    pub fn run(mut self, failure: &Failure) -> ReaderStats {
        let mut stats = ReaderStats::default();
        let read = std::panic::catch_unwind(AssertUnwindSafe(|| {
            self.read_until_done(failure, &mut stats)
        }));
        match read {
            Ok(Ok(())) => {}
            Ok(Err(error)) => failure.record(error),
            Err(panic) => failure.record_panic("a bulk load reader", panic.as_ref()),
        }
        stats
    }

    fn read_until_done(
        &mut self,
        failure: &Failure,
        stats: &mut ReaderStats,
    ) -> Result<(), GraphError> {
        let mut resolved = ResolvedNodes::default();
        while !failure.has_happened() {
            let Some(next) = stats.time(ReaderPhase::TakingInput, || self.input.next()) else {
                return Ok(());
            };
            let (unit, mut event_ids) = next?;
            let chunks = stats.time(ReaderPhase::Decoding, unit.decode)?;
            for data in chunks {
                let event_ids = event_ids.for_next_chunk(&data)?;
                let slot = stats.time(ReaderPhase::WaitingForBudget, || self.budget.take_slot());
                self.read_chunk(data, event_ids, slot, &mut resolved, stats)?;
            }
            event_ids.ensure_all_used()?;
        }
        Ok(())
    }

    fn read_chunk(
        &mut self,
        data: DFChunk,
        event_ids: SecondaryIndexCol,
        slot: Slot,
        resolved: &mut ResolvedNodes,
        stats: &mut ReaderStats,
    ) -> Result<(), GraphError> {
        let rows = data.len();
        let prepared = stats.time(ReaderPhase::Preparing, || self.preparer.prepare(&data))?;
        stats.time(ReaderPhase::Resolving, || {
            self.resolver.resolve(&data, resolved)
        })?;
        stats.time(ReaderPhase::Routing, || {
            let chunk = Chunk {
                rows: Rows {
                    data,
                    columns: self.preparer.schema.columns,
                },
                endpoints: resolved.endpoints(rows),
                times: EventTimes {
                    time: prepared.time,
                    event_ids,
                },
                layers: prepared.layers,
                properties: prepared.properties,
                _slot: slot,
            };
            self.router.route(Arc::new(chunk), resolved.new_nodes(rows))
        })?;
        stats.count_chunk(rows);
        Ok(())
    }
}

impl<G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps> ChunkPreparer<'_, '_, G> {
    fn prepare(&self, data: &DFChunk) -> Result<PreparedColumns, GraphError> {
        let EdgeSchema {
            columns,
            layer,
            properties,
        } = self.schema;
        data.node_col(columns.src)?
            .validate(self.graph, LoadError::InvalidSrcError)?;
        data.node_col(columns.dst)?
            .validate(self.graph, LoadError::InvalidDstError)?;
        let (layers, distinct_layers) =
            lift_layer_col(*layer, columns.layer, data)?.resolve_layer(None, self.graph, false)?;
        let layers = layers.into_owned();
        let properties = properties.read(data, self.graph, &layers, &distinct_layers)?;
        Ok(PreparedColumns {
            time: data.time_col(columns.time)?,
            layers,
            properties,
        })
    }
}

impl NodeResolver<'_> {
    fn resolve(&self, data: &DFChunk, into: &mut ResolvedNodes) -> Result<(), GraphError> {
        let nodes = self.graph.storage().nodes();
        let allocate = |hint: usize, count: u32| {
            let (segment, pos, allocated) = nodes.reserve_free_range(hint, count);
            (pos.as_vid(segment, nodes.max_segment_len()), allocated)
        };
        let endpoints = [
            data.chunk[self.columns.src].as_ref(),
            data.chunk[self.columns.dst].as_ref(),
        ];
        self.graph
            .gid_resolver
            .resolve_batch(&endpoints, &allocate, &mut into.vids, &mut into.created)
            .map_err(into_graph_err)
    }
}

impl Router<'_, '_> {
    fn route(&mut self, chunk: Arc<Chunk>, new_nodes: Vec<NewNode>) -> Result<(), GraphError> {
        self.learn_owners_covering(&chunk)?;
        let owners = &self.known_owners;
        let owner_of = |vid: VID| owners.of_segment(self.sizes.node_segment(vid));

        let new_nodes = bucket_by(new_nodes, self.owners.len(), |node| owner_of(node.vid));
        self.owners.send_buckets(new_nodes, |nodes| {
            OwnerMsg::StoreNewNodes(chunk.clone(), nodes)
        });

        let rows = 0..chunk.len() as u32;
        let rows = bucket_by(rows, self.owners.len(), |&row| {
            owner_of(chunk.edge(row).src)
        });
        for (owner, rows) in rows.iter().enumerate() {
            self.owners.workload.add_rows(owner, rows.len());
        }
        self.owners
            .send_buckets(rows, |rows| OwnerMsg::WriteOutbound(chunk.clone(), rows));
        Ok(())
    }

    /// Resolution may have created node segments with no owner yet. The highest segment the
    /// chunk uses decides, not the store's segment count: segments are created from several
    /// threads, and the count stops at the first one still being created.
    fn learn_owners_covering(&mut self, chunk: &Chunk) -> Result<(), GraphError> {
        let endpoints = chunk.endpoints.src.iter().chain(&chunk.endpoints.dst);
        let segments = endpoints
            .map(|&vid| self.sizes.node_segment(vid) + 1)
            .max()
            .unwrap_or(0);
        if !self.known_owners.covers(segments) {
            self.known_owners = self.coordinator.owners_covering(segments)?;
        }
        Ok(())
    }
}

impl ResolvedNodes {
    fn endpoints(&self, rows: usize) -> Endpoints {
        let (src, dst) = self.vids.split_at(rows);
        Endpoints {
            src: src.to_vec(),
            dst: dst.to_vec(),
        }
    }

    fn new_nodes(&self, rows: usize) -> Vec<NewNode> {
        self.created
            .iter()
            .map(|&(index, vid)| {
                let (endpoint, row) = if index < rows {
                    (Endpoint::Src, index)
                } else {
                    (Endpoint::Dst, index - rows)
                };
                NewNode {
                    endpoint,
                    row: row as u32,
                    vid,
                }
            })
            .collect()
    }
}
