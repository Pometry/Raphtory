//! `load_edges_from_df2`: the pipelined bulk edge loader (ADR-0002).
//!
//! Readers take chunks of input in order, resolve their nodes and send each row to the owner of
//! its source node. Owners are the only writers of their segments: an owner creates the edges
//! whose source it owns, in edge segments of its own, then sends the inbound half of each edge
//! to the owner of its destination. The coordinator (the calling thread) holds the graph's write
//! lock and gives each new node segment to the least busy owner.
//!
//! Left to `load_edges_from_df` for now: deletions, pre-resolved VIDs, and graphs that already
//! have edges, whose edge segments would need owners other than their source nodes' owners.

mod budget;
mod chunk;
mod columns;
mod config;
mod coordinator;
mod failure;
mod grouping;
mod input;
mod owner;
mod pool;
mod reader;
mod segments;
mod stats;

pub use input::{InputUnit, DEFAULT_CHUNK_ROWS};

use crate::{
    arrow_loader::dataframe::{DFChunk, DFView},
    db::api::{storage::storage::PersistenceStrategy, view::StaticGraphViewOps},
    errors::{into_graph_err, GraphError},
    prelude::*,
};
use crossbeam_channel::Sender;
use db4_graph::TemporalGraph;
use std::{collections::HashMap, thread::ScopedJoinHandle, time::Instant};
use storage::{api::edges::EdgeSegmentOps, Extension};

use self::{
    budget::InFlightBudget,
    columns::{EdgeColumns, EdgeProperties, EdgeSchema, Header},
    config::Config,
    coordinator::{request_channel, Coordinator, CoordinatorHandle, Request},
    failure::Failure,
    input::Input,
    owner::Owner,
    pool::OwnerPool,
    reader::{ChunkPreparer, NodeResolver, Reader, Router},
    segments::{
        EdgeSegmentLock, EdgeSegments, NodeSegmentLock, NodeSegments, SegmentOwners, SegmentSizes,
    },
    stats::{LoadStats, MemorySampler},
};

use super::edges::ColumnNames;

/// Whether a load of edges goes through `load_edges_from_df2`: `RAPHTORY_LOAD_V2` is set and
/// the load is one it supports.
pub fn use_load2(resolve_nodes: bool, delete: bool, graph: &impl StaticGraphViewOps) -> bool {
    std::env::var_os("RAPHTORY_LOAD_V2").is_some()
        && resolve_nodes
        && !delete
        && graph.count_edges() == 0
}

pub fn load_edges_from_df2<G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps>(
    df_view: DFView<impl Iterator<Item = Result<DFChunk, GraphError>> + Send>,
    column_names: ColumnNames,
    properties: &[&str],
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    graph: &G,
) -> Result<(), GraphError> {
    if df_view.is_empty() {
        return Ok(());
    }
    let units = df_view.chunks.map(|chunk| chunk.map(InputUnit::decoded));
    load_edges_from_units(
        df_view.names,
        units,
        column_names,
        properties,
        metadata,
        shared_metadata,
        layer,
        graph,
    )
}

/// Loads the edges of `units`, whose chunks have the columns `names`.
#[allow(clippy::too_many_arguments)]
pub fn load_edges_from_units<G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps>(
    names: Vec<String>,
    units: impl Iterator<Item = Result<InputUnit, GraphError>> + Send,
    column_names: ColumnNames,
    properties: &[&str],
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    graph: &G,
) -> Result<(), GraphError> {
    let config = Config::from_env();
    let started = Instant::now();
    let header = Header::new(names, (), None);
    let schema = EdgeSchema {
        columns: EdgeColumns::find(&header, column_names)?,
        layer,
        properties: EdgeProperties::find(&header, graph, properties, metadata, shared_metadata)?,
    };

    let mut locked = graph.write_lock().map_err(into_graph_err)?;
    let node_segments = locked.nodes.take_all();
    let _edge_segments_kept_from_others = take_empty_edge_segments(locked.edges.take_all())?;

    let load = Load {
        config: &config,
        graph,
        storage: locked.graph,
        schema: &schema,
    };
    let (failure, stats) = load.run(units, node_segments, started);

    if config.print_stats {
        stats.print(&config, started.elapsed());
    }
    failure.into_result()
}

/// New edges go into segments the owners create, so the graph's own edge segments must be empty.
fn take_empty_edge_segments(
    segments: Vec<EdgeSegmentLock<'_>>,
) -> Result<Vec<EdgeSegmentLock<'_>>, GraphError> {
    if segments
        .iter()
        .any(|segment| segment.segment().num_edges() > 0)
    {
        return Err(GraphError::LoadFailure(
            "load_edges_from_df2 only loads into a graph without edges".to_string(),
        ));
    }
    Ok(segments)
}

struct Load<'s, 'a, G> {
    config: &'s Config,
    graph: &'a G,
    storage: &'a TemporalGraph<Extension>,
    schema: &'s EdgeSchema<'s>,
}

impl<'s, 'a, G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps> Load<'s, 'a, G> {
    /// Runs readers and owners until every unit is written, then shuts the owners down. Owners
    /// never block on sending; readers block only on the in-flight budget and the coordinator,
    /// and the coordinator answers until the last reader is done, so nothing waits in a cycle.
    fn run<I>(
        &self,
        units: I,
        node_segments: Vec<NodeSegmentLock<'a>>,
        started: Instant,
    ) -> (Failure, LoadStats)
    where
        I: Iterator<Item = Result<InputUnit, GraphError>> + Send,
    {
        let input = Input::new(units, self.storage, self.schema.columns.event_id);
        let budget = InFlightBudget::new(self.config.chunks_in_flight);
        let (owners, mailboxes) = OwnerPool::new(self.config.owners);
        let (requests, incoming_requests) = request_channel();
        let failure = Failure::default();

        let stats = std::thread::scope(|scope| {
            let _sampler = self.config.print_stats.then(|| {
                MemorySampler::start(
                    scope,
                    started,
                    self.storage.extension().memory_tracker().clone(),
                )
            });
            let coordinator = Coordinator::new(self.storage.storage().nodes(), &owners);
            coordinator.hand_out(node_segments);

            let owner_threads: Vec<_> = mailboxes
                .into_iter()
                .enumerate()
                .map(|(id, mailbox)| {
                    let owner = self.owner(&owners);
                    let failure = &failure;
                    scope.spawn(move || owner.run(&mailbox, failure, id))
                })
                .collect();
            let reader_threads: Vec<_> = (0..self.config.readers)
                .map(|_| {
                    let reader = self.reader(&input, &budget, &owners, &requests);
                    let failure = &failure;
                    scope.spawn(move || reader.run(failure))
                })
                .collect();
            drop(requests);

            coordinator.serve(incoming_requests, reader_threads.len());
            budget.wait_until_all_returned();
            owners.shut_down();
            LoadStats {
                readers: join_all(reader_threads),
                owners: join_all(owner_threads),
            }
        });
        (failure, stats)
    }

    fn owner<'p>(&'p self, owners: &'p OwnerPool<'a>) -> Owner<'p, 'a> {
        let sizes = SegmentSizes::of(self.storage);
        Owner::new(
            NodeSegments::new(sizes),
            EdgeSegments::new(self.storage.storage().edges(), sizes),
            owners,
            self.schema.properties.shared(),
        )
    }

    fn reader<'p, I>(
        &'p self,
        input: &'p Input<'a, I>,
        budget: &'p InFlightBudget,
        owners: &'p OwnerPool<'a>,
        requests: &Sender<Request>,
    ) -> Reader<'p, 'a, G, I> {
        let sizes = SegmentSizes::of(self.storage);
        Reader {
            input,
            budget,
            preparer: ChunkPreparer {
                graph: self.graph,
                schema: self.schema,
            },
            resolver: NodeResolver {
                graph: self.storage,
                columns: self.schema.columns,
                new_segments: requests.clone(),
            },
            router: Router {
                owners,
                coordinator: CoordinatorHandle::new(requests.clone()),
                known_owners: SegmentOwners::default(),
                sizes,
            },
        }
    }
}

fn join_all<T: Default>(threads: Vec<ScopedJoinHandle<'_, T>>) -> Vec<T> {
    threads
        .into_iter()
        .map(|thread| thread.join().unwrap_or_default())
        .collect()
}
