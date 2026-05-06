#[cfg(feature = "io")]
use crate::serialise::GraphPaths;
use crate::{
    arrow_loader::{
        dataframe::{DFChunk, DFView},
        df_loaders::{
            edge_props::load_edges_from_df as load_edge_props_from_df,
            edges::{load_edges_from_df, ColumnNames},
            load_edge_deletions_from_df, load_graph_props_from_df,
            nodes::{load_node_props_from_df, load_nodes_from_df},
        },
        LOAD_POOL,
    },
    core::entities::{nodes::node_ref::AsNodeRef, LayerIds, VID},
    db::{
        api::{
            properties::{Metadata, Properties},
            state::ops::filter::NodeTypeFilterOp,
            view::{internal::*, *},
        },
        graph::{
            edge::EdgeView,
            edges::Edges,
            node::NodeView,
            nodes::Nodes,
            views::{
                cached_view::CachedView, filter::node_filtered_graph::NodeFilteredGraph,
                node_subgraph::NodeSubgraph, valid_graph::ValidGraph,
            },
        },
    },
    errors::GraphError,
    parquet_encoder::{
        encode_edge_cprop, encode_edge_deletions, encode_edge_tprop, encode_graph_cprop,
        encode_graph_tprop, encode_nodes_cprop, encode_nodes_tprop, RecordBatchSink, DST_GID_COL,
        DST_VID_COL, EDGE_COL_ID, ENCODE_POOL, LAYER_COL, LAYER_ID_COL, NODE_GID_COL, NODE_VID_COL,
        SECONDARY_INDEX_COL, SRC_GID_COL, SRC_VID_COL, TIME_COL, TYPE_COL, TYPE_ID_COL,
    },
    prelude::*,
};
use ahash::HashSet;
use arrow::array::RecordBatch;
use db4_graph::TemporalGraph;
use itertools::Itertools;
use raphtory_api::core::{
    entities::properties::meta::{Meta, PropMapper},
    storage::{arc_str::ArcStr, timeindex::EventTime},
    Direction,
};
use raphtory_core::utils::iter::GenLockedIter;
use raphtory_storage::{
    graph::{
        edges::edge_storage_ops::EdgeStorageOps, graph::GraphStorage,
        nodes::node_storage_ops::NodeStorageOps,
    },
    mutation::addition_ops::SessionAdditionOps,
};
use rayon::prelude::*;
use rustc_hash::FxHashSet;
use std::{path::Path, sync::Arc};
use storage::{persist::strategy::PersistenceStrategy, Config, Extension};

#[cfg(feature = "search")]
use crate::{
    db::graph::views::filter::model::TryAsCompositeFilter,
    search::{fallback_filter_edges, fallback_filter_exploded_edges, fallback_filter_nodes},
};

/// This trait GraphViewOps defines operations for accessing
/// information about a graph. The trait has associated types
/// that are used to define the type of the nodes, edges
/// and the corresponding iterators.
pub trait GraphViewOps<'graph>: BoxableGraphView + Sized + Clone + 'graph {
    /// Return an iterator over all edges in the graph.
    fn edges(&self) -> Edges<'graph, Self>;

    /// Return an unlocked iterator over all edges in the graph.
    fn edges_unlocked(&self) -> Edges<'graph, Self>;

    /// Return a View of the nodes in the Graph
    fn nodes(&self) -> Nodes<'graph, Self>;

    /// Materializes the view into a new graph.
    /// If a path is provided, it will be used to store the new graph
    /// (assuming the storage feature is enabled). Inherits config from the graph.
    ///
    /// Arguments:
    ///     path: Option<&Path>: An optional path used to store the new graph.
    ///
    /// Returns:
    ///     MaterializedGraph: Returns a new materialized graph.
    #[cfg(feature = "io")]
    fn materialize_at(
        &self,
        path: &(impl GraphPaths + ?Sized),
    ) -> Result<MaterializedGraph, GraphError> {
        self.materialize_at_with_config(path, self.core_graph().extension().config().clone())
    }

    /// Materializes the view into a new graph.
    /// If a path is provided, it will be used to store the new graph
    /// (assuming the storage feature is enabled). Sets a new config.
    ///
    /// # Arguments
    ///     path: The path for the new graph.
    ///     config: The new config.
    #[cfg(feature = "io")]
    fn materialize_at_with_config(
        &self,
        path: &(impl GraphPaths + ?Sized),
        config: Config,
    ) -> Result<MaterializedGraph, GraphError>;

    fn materialize(&self) -> Result<MaterializedGraph, GraphError>;

    fn subgraph<I: IntoIterator<Item = V>, V: AsNodeRef>(&self, nodes: I) -> NodeSubgraph<Self>;

    fn cache_view(&self) -> CachedView<Self>;

    fn valid(&self) -> ValidGraph<Self>;

    fn subgraph_node_types<I: IntoIterator<Item = V>, V: AsRef<str>>(
        &self,
        nodes_types: I,
    ) -> NodeFilteredGraph<Self, NodeTypeFilterOp>;

    fn exclude_nodes<I: IntoIterator<Item = V>, V: AsNodeRef>(
        &self,
        nodes: I,
    ) -> NodeSubgraph<Self>;

    /// Return all the layer ids in the graph
    fn unique_layers(&self) -> BoxedIter<ArcStr>;

    /// Get the `EventTime` of the earliest activity in the graph.
    fn earliest_time(&self) -> Option<EventTime>;

    /// Get the `EventTime` of the latest activity in the graph.
    fn latest_time(&self) -> Option<EventTime>;

    /// Return the number of nodes in the graph.
    fn count_nodes(&self) -> usize;

    /// Check if the graph is empty.
    fn is_empty(&self) -> bool {
        self.count_nodes() == 0
    }

    /// Return the number of edges in the graph.
    fn count_edges(&self) -> usize;

    // Return the number of temporal edges in the graph.
    fn count_temporal_edges(&self) -> usize;

    /// Check if the graph contains a node `v`.
    fn has_node<T: AsNodeRef>(&self, v: T) -> bool;

    /// Check if the graph contains an edge given a pair of nodes `(src, dst)`.
    fn has_edge<T: AsNodeRef>(&self, src: T, dst: T) -> bool;

    /// Get a node `v`.
    fn node<T: AsNodeRef>(&self, v: T) -> Option<NodeView<'graph, Self>>;

    /// Get an edge `(src, dst)`.
    fn edge<T: AsNodeRef>(&self, src: T, dst: T) -> Option<EdgeView<Self>>;

    /// Get all property values of this graph.
    ///
    /// Returns:
    ///
    /// A view of the properties of the graph
    fn properties(&self) -> Properties<Self>;

    /// Get a view of the metadat for this graph
    fn metadata(&self) -> Metadata<'graph, Self>;
}

#[cfg(feature = "search")]
pub trait SearchableGraphOps: Sized {
    fn get_index_spec(&self) -> Result<IndexSpec, GraphError>;

    fn search_nodes<F: TryAsCompositeFilter>(
        &self,
        filter: F,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<'static, Self>>, GraphError>;

    fn search_edges<F: TryAsCompositeFilter>(
        &self,
        filter: F,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<Self>>, GraphError>;

    fn search_exploded_edges<F: TryAsCompositeFilter>(
        &self,
        filter: F,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<Self>>, GraphError>;

    fn is_indexed(&self) -> bool;
}

fn edges_inner<'graph, G: GraphView + 'graph>(g: &G, locked: bool) -> Edges<'graph, G> {
    let graph = g.clone();
    let edges: Arc<dyn Fn() -> BoxedLIter<'graph, EdgeRef> + Send + Sync + 'graph> = match graph
        .node_list()
    {
        NodeList::All { .. } => Arc::new(move || {
            let layer_ids = graph.layer_ids().clone();
            let graph = graph.clone();
            let gs = if locked {
                graph.core_graph().lock()
            } else {
                graph.core_graph().clone()
            };
            GenLockedIter::from((gs, layer_ids, graph), move |(gs, layer_ids, graph)| {
                let edges = gs.edges();
                let iter = edges.iter(layer_ids);
                if graph.filtered() {
                    iter.filter_map(|e| graph.filter_edge(e.as_ref()).then(|| e.out_ref()))
                        .into_dyn_boxed()
                } else {
                    iter.map(|e| e.out_ref()).into_dyn_boxed()
                }
            })
            .into_dyn_boxed()
        }),
        NodeList::List { elems } => Arc::new(move || {
            let cg = if locked {
                graph.core_graph().lock()
            } else {
                graph.core_graph().clone()
            };
            let graph = graph.clone();
            elems
                .clone()
                .into_iter()
                .flat_map(move |node| node_edges(cg.clone(), graph.clone(), node, Direction::OUT))
                .into_dyn_boxed()
        }),
    };
    Edges {
        base_graph: g.clone(),
        edges,
    }
}

fn df_view_from_record_batch(
    batch: RecordBatch,
) -> DFView<impl Iterator<Item = Result<DFChunk, GraphError>> + Send> {
    let (schema, columns, num_rows) = batch.into_parts();
    let field_names = schema
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect();

    DFView::new(
        field_names,
        std::iter::once(Ok(DFChunk::new(columns))),
        Some(num_rows),
    )
}

fn df_columns_except(names: &[String], exclude: &[&str]) -> Vec<String> {
    names
        .iter()
        .map(|name| name.as_str())
        .filter(|name| !exclude.contains(name))
        .map(str::to_string)
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum RecordBatchKind {
    EdgesT,
    EdgesC,
    EdgesD,
    NodesT,
    NodesC,
    GraphT,
    GraphC,
}

#[derive(Debug, Clone)]
pub(crate) struct RecordBatchMessage {
    batch: RecordBatch,
    kind: RecordBatchKind,
}

impl RecordBatchMessage {
    pub(crate) fn kind(&self) -> RecordBatchKind {
        self.kind
    }

    pub(crate) fn into_batch(self) -> RecordBatch {
        self.batch
    }
}

/// This RecordBatchSink allows for RecordBatches to be sent from multithreaded contexts (such as the parquet serializer)
/// to be consumed by the receiver.
#[derive(Debug, Clone)]
pub(crate) struct ChannelRecordBatchSink {
    tx: crossbeam_channel::Sender<RecordBatchMessage>,
    kind: RecordBatchKind,
}

impl ChannelRecordBatchSink {
    pub(crate) fn new(
        tx: crossbeam_channel::Sender<RecordBatchMessage>,
        kind: RecordBatchKind,
    ) -> Self {
        Self { tx, kind }
    }
}

impl RecordBatchSink for ChannelRecordBatchSink {
    fn send_batch(&mut self, batch: RecordBatch) -> Result<(), GraphError> {
        // sinks propagate their record batch kind to their messages
        let record_batch_message = RecordBatchMessage {
            batch,
            kind: self.kind,
        };

        self.tx
            .send(record_batch_message)
            .map_err(|e| GraphError::IOErrorMsg(format!("RecordBatch receiver was dropped: {e}")))
    }

    fn finish(self) -> Result<(), GraphError> {
        // implicitly drops self, so the transmitter (tx) is dropped as well
        Ok(())
    }
}

pub fn materialize_impl(
    graph: &impl GraphView,
    path: Option<&Path>,
    config: Config,
) -> Result<MaterializedGraph, GraphError> {
    let mut node_meta = Meta::new_for_nodes();
    let mut edge_meta = Meta::new_for_edges();
    let mut graph_props_meta = Meta::new_for_graph_props();

    // Preserve property mappers from the source graph so that
    // windowed views expose the same prop mappings even for keys with no
    // values inside the window.
    node_meta.set_metadata_mapper(graph.node_meta().metadata_mapper().deep_clone());
    node_meta.set_temporal_prop_mapper(graph.node_meta().temporal_prop_mapper().deep_clone());
    edge_meta.set_metadata_mapper(graph.edge_meta().metadata_mapper().deep_clone());
    edge_meta.set_temporal_prop_mapper(graph.edge_meta().temporal_prop_mapper().deep_clone());
    graph_props_meta.set_metadata_mapper(graph.graph_props_meta().metadata_mapper().deep_clone());
    graph_props_meta
        .set_temporal_prop_mapper(graph.graph_props_meta().temporal_prop_mapper().deep_clone());

    let base_layer_meta = graph.edge_meta().layer_meta();
    let layer_meta = edge_meta.layer_meta();
    // NOTE: layers must be set in layer_meta before the TemporalGraph is initialized to
    // make sure empty layers are created.
    match graph.layer_ids() {
        LayerIds::None => {}
        LayerIds::All => {
            for name in base_layer_meta.keys().iter() {
                layer_meta.get_or_create_id(name);
            }
        }
        LayerIds::One(id) => {
            layer_meta.get_or_create_id(&base_layer_meta.get_name(id.0));
        }
        LayerIds::Multiple(ids) => {
            let all_layers = base_layer_meta.all_keys();
            for id in ids {
                layer_meta.get_or_create_id(&all_layers[id.0]);
            }
        }
    }
    node_meta.set_layer_mapper(layer_meta.deep_clone());

    let ext = Extension::new(config, path)?;
    let temporal_graph = TemporalGraph::new_with_meta(
        path.map(|p| p.into()),
        node_meta,
        edge_meta,
        graph_props_meta,
        ext,
    )?;

    if let Some(earliest) = graph.earliest_time() {
        temporal_graph.update_time(earliest);
    }

    if let Some(latest) = graph.latest_time() {
        temporal_graph.update_time(latest);
    }

    temporal_graph
        .storage()
        .set_event_id(graph.core_graph().lock().read_event_id());

    let graph_storage = GraphStorage::from(Arc::new(temporal_graph));
    let materialized = graph.new_base_graph(graph_storage);

    let stream_capacity = 10;
    let (tx, rx) = crossbeam_channel::bounded::<RecordBatchMessage>(stream_capacity);

    let mut scope_result = Ok(());
    // Use std::thread::scope rather than rayon::scope so the producer runs on its own OS thread.
    // With rayon::scope on a single-thread pool, the main thread blocking on rx.recv() would starve the spawned producer.
    std::thread::scope(|scope| {
        let producer_tx = tx.clone();
        let producer_handle = scope.spawn(move || {
            let make_sink_factory = |kind| {
                let tx = producer_tx.clone();
                move |_, _, _| Ok(ChannelRecordBatchSink::new(tx.clone(), kind))
            };

            // EdgesD must run before EdgesC: edges that exist only via
            // deletions (e.g. in a windowed persistent graph) aren't
            // produced by EdgesT, so the deletion pass is what
            // materializes them. The edge-metadata loader then expects
            // every layer-edge it sees to already exist.
            // NodesC must run before NodesT as well.
            ENCODE_POOL.install(|| -> Result<(), GraphError> {
                encode_nodes_cprop(graph, make_sink_factory(RecordBatchKind::NodesC))?;
                encode_nodes_tprop(graph, make_sink_factory(RecordBatchKind::NodesT))?;
                encode_edge_tprop(graph, make_sink_factory(RecordBatchKind::EdgesT))?;
                encode_edge_deletions(graph, make_sink_factory(RecordBatchKind::EdgesD))?;
                encode_edge_cprop(graph, make_sink_factory(RecordBatchKind::EdgesC))?;
                encode_graph_tprop(graph, make_sink_factory(RecordBatchKind::GraphT))?;
                encode_graph_cprop(graph, make_sink_factory(RecordBatchKind::GraphC))?;
                Ok(())
            })
        });

        drop(tx);

        let consumer_result = loop {
            let message = match rx.recv() {
                Ok(message) => message,
                Err(_) => break Ok(()), // error here means the channel is empty and disconnected
            };

            let kind = message.kind();
            let df_view = df_view_from_record_batch(message.into_batch());

            let result = match kind {
                RecordBatchKind::NodesC => {
                    let node_c_props = df_columns_except(
                        &df_view.names,
                        &[NODE_ID_COL, NODE_VID_COL, TYPE_COL, TYPE_ID_COL],
                    );
                    let node_c_props_refs =
                        node_c_props.iter().map(String::as_str).collect::<Vec<_>>();

                    LOAD_POOL.install(|| {
                        load_node_props_from_df(
                            df_view,
                            NODE_ID_COL,
                            None,
                            Some(TYPE_COL),
                            None,
                            None,
                            &node_c_props_refs,
                            None,
                            &materialized,
                            true,
                            None,
                            None,
                        )
                    })
                }
                RecordBatchKind::NodesT => {
                    let node_t_props = df_columns_except(
                        &df_view.names,
                        &[
                            NODE_ID_COL,
                            NODE_VID_COL,
                            TYPE_COL,
                            TIME_COL,
                            SECONDARY_INDEX_COL,
                            LAYER_COL,
                        ],
                    );
                    let node_t_props_refs =
                        node_t_props.iter().map(String::as_str).collect::<Vec<_>>();

                    load_nodes_from_df(
                        df_view,
                        TIME_COL,
                        Some(SECONDARY_INDEX_COL),
                        NODE_ID_COL,
                        &node_t_props_refs,
                        &[],
                        None,
                        None,
                        Some(TYPE_COL),
                        &materialized,
                        true,
                        None,
                        Some(LAYER_COL),
                    )
                }
                RecordBatchKind::EdgesT => {
                    let edge_t_props = df_columns_except(
                        &df_view.names,
                        &[
                            TIME_COL,
                            SECONDARY_INDEX_COL,
                            SRC_COL_VID,
                            SRC_COL_ID,
                            DST_COL_VID,
                            DST_COL_ID,
                            EDGE_COL_ID,
                            LAYER_COL,
                            LAYER_ID_COL,
                        ],
                    );
                    let edge_t_props_refs =
                        edge_t_props.iter().map(String::as_str).collect::<Vec<_>>();

                    LOAD_POOL.install(|| {
                        load_edges_from_df(
                            df_view,
                            ColumnNames::new(
                                TIME_COL,
                                Some(SECONDARY_INDEX_COL),
                                SRC_COL_ID,
                                DST_COL_ID,
                                Some(LAYER_COL),
                            ),
                            true,
                            &edge_t_props_refs,
                            &[],
                            None,
                            None,
                            &materialized,
                            false,
                        )
                    })
                }
                RecordBatchKind::EdgesC => {
                    let edge_c_props = df_columns_except(
                        &df_view.names,
                        &[
                            SRC_COL_VID,
                            SRC_COL_ID,
                            DST_COL_VID,
                            DST_COL_ID,
                            EDGE_COL_ID,
                            LAYER_COL,
                        ],
                    );
                    let edge_c_props_refs =
                        edge_c_props.iter().map(String::as_str).collect::<Vec<_>>();

                    LOAD_POOL.install(|| {
                        load_edge_props_from_df(
                            df_view,
                            ColumnNames::new("", None, SRC_COL_ID, DST_COL_ID, Some(LAYER_COL)),
                            true,
                            &edge_c_props_refs,
                            None,
                            None,
                            &materialized,
                        )
                    })
                }
                RecordBatchKind::EdgesD => LOAD_POOL.install(|| {
                    load_edge_deletions_from_df(
                        df_view,
                        ColumnNames::new(
                            TIME_COL,
                            Some(SECONDARY_INDEX_COL),
                            SRC_COL_ID,
                            DST_COL_ID,
                            Some(LAYER_COL),
                        ),
                        true,
                        None,
                        &materialized,
                    )
                }),
                RecordBatchKind::GraphT => {
                    let graph_t_props =
                        df_columns_except(&df_view.names, &[TIME_COL, SECONDARY_INDEX_COL]);
                    let graph_t_props_refs =
                        graph_t_props.iter().map(String::as_str).collect::<Vec<_>>();

                    LOAD_POOL.install(|| {
                        load_graph_props_from_df(
                            df_view,
                            TIME_COL,
                            Some(SECONDARY_INDEX_COL),
                            Some(&graph_t_props_refs),
                            None,
                            &materialized,
                        )
                    })
                }
                RecordBatchKind::GraphC => {
                    let graph_c_props = df_columns_except(&df_view.names, &[TIME_COL]);
                    let graph_c_props_refs =
                        graph_c_props.iter().map(String::as_str).collect::<Vec<_>>();

                    LOAD_POOL.install(|| {
                        load_graph_props_from_df(
                            df_view,
                            TIME_COL,
                            None,
                            None,
                            Some(&graph_c_props_refs),
                            &materialized,
                        )
                    })
                }
            };

            if let Err(err) = result {
                break Err(err);
            }
        };

        drop(rx);

        let producer_result = producer_handle.join().unwrap_or_else(|_| {
            Err(GraphError::IOErrorMsg(
                "record batch producer scope exited without reporting a result".to_string(),
            ))
        });

        scope_result = consumer_result.and(producer_result);
    }); // std::thread::scope
    scope_result?;

    Ok(materialized)
}

impl<'graph, G: GraphView + 'graph> GraphViewOps<'graph> for G {
    fn edges(&self) -> Edges<'graph, Self> {
        edges_inner(self, true)
    }

    fn edges_unlocked(&self) -> Edges<'graph, Self> {
        edges_inner(self, false)
    }

    fn nodes(&self) -> Nodes<'graph, Self> {
        let graph = self.clone();
        Nodes::new(graph)
    }

    fn materialize(&self) -> Result<MaterializedGraph, GraphError> {
        materialize_impl(self, None, self.core_graph().extension().config().clone())
    }

    #[cfg(feature = "io")]
    fn materialize_at_with_config(
        &self,
        path: &(impl GraphPaths + ?Sized),
        config: Config,
    ) -> Result<MaterializedGraph, GraphError> {
        if Extension::disk_storage_enabled() {
            path.init()?;
            let graph_path = path.graph_path()?;
            let graph = materialize_impl(self, Some(graph_path.as_ref()), config)?;
            path.write_metadata(&graph)?;
            Ok(graph)
        } else {
            Err(GraphError::DiskGraphNotEnabled)
        }
    }

    fn subgraph<I: IntoIterator<Item = V>, V: AsNodeRef>(&self, nodes: I) -> NodeSubgraph<G> {
        NodeSubgraph::new(self.clone(), nodes)
    }

    fn cache_view(&self) -> CachedView<G> {
        CachedView::new(self.clone())
    }

    fn valid(&self) -> ValidGraph<Self> {
        ValidGraph::new(self.clone())
    }

    fn subgraph_node_types<I: IntoIterator<Item = V>, V: AsRef<str>>(
        &self,
        node_types: I,
    ) -> NodeFilteredGraph<Self, NodeTypeFilterOp> {
        NodeFilteredGraph::new(
            self.clone(),
            NodeTypeFilterOp::new_from_values(node_types, self),
        )
    }

    fn exclude_nodes<I: IntoIterator<Item = V>, V: AsNodeRef>(&self, nodes: I) -> NodeSubgraph<G> {
        let _layer_ids = self.layer_ids();

        let nodes_to_exclude: FxHashSet<VID> = nodes
            .into_iter()
            .flat_map(|v| (&self).node(v).map(|v| v.node))
            .collect();

        let nodes_to_include = self
            .nodes()
            .into_iter()
            .filter(|node| !nodes_to_exclude.contains(&node.node))
            .map(|node| node.node);

        NodeSubgraph::new(self.clone(), nodes_to_include)
    }

    /// Return all the layer ids in the graph
    fn unique_layers(&self) -> BoxedIter<ArcStr> {
        self.get_layer_names_from_ids(self.layer_ids())
    }

    /// Get the `EventTime` of the earliest activity in the graph.
    #[inline]
    fn earliest_time(&self) -> Option<EventTime> {
        if self.layer_ids().is_all() && !self.filtered() {
            self.earliest_time_global().map(EventTime::start)
        } else {
            self.properties()
                .temporal()
                .values()
                .flat_map(|prop| prop.history().earliest_time())
                .min()
                .into_iter()
                .chain(
                    self.nodes()
                        .earliest_time()
                        .par_iter_values()
                        .flatten()
                        .min(),
                )
                .min()
        }
    }

    /// Get the `EventTime` of the latest activity in the graph.
    #[inline]
    fn latest_time(&self) -> Option<EventTime> {
        if self.layer_ids().is_all() && !self.filtered() {
            self.latest_time_global().map(EventTime::end)
        } else {
            self.properties()
                .temporal()
                .values()
                .flat_map(|prop| prop.history().latest_time())
                .max()
                .into_iter()
                .chain(self.nodes().latest_time().par_iter_values().flatten().max())
                .max()
        }
    }

    #[inline]
    fn count_nodes(&self) -> usize {
        (&self).nodes().len()
    }

    #[inline]
    fn count_edges(&self) -> usize {
        if self.filtered() {
            let edges = self.core_edges();
            edges
                .as_ref()
                .par_iter(self.layer_ids())
                .filter(|e| self.filter_edge(e.as_ref()))
                .count()
        } else {
            self.unfiltered_num_edges(self.layer_ids())
        }
    }

    fn count_temporal_edges(&self) -> usize {
        let core_edges = self.core_edges();
        let layer_ids = self.layer_ids();
        let edge_time_semantics = self.edge_time_semantics();
        if self.filtered() {
            core_edges
                .as_ref()
                .par_iter(layer_ids)
                .filter(|e| self.filter_edge(e.as_ref()))
                .map(move |edge| edge_time_semantics.edge_exploded_count(edge.as_ref(), self))
                .sum()
        } else {
            core_edges
                .as_ref()
                .par_iter(layer_ids)
                .map(move |edge| edge_time_semantics.edge_exploded_count(edge.as_ref(), self))
                .sum()
        }
    }

    #[inline]
    fn has_node<T: AsNodeRef>(&self, v: T) -> bool {
        if let Some(node_id) = self.internalise_node(v.as_node_ref()) {
            if self.filtered() {
                let node = self.core_node(node_id);
                self.filter_node(node.as_ref())
            } else {
                true
            }
        } else {
            false
        }
    }

    #[inline]
    fn has_edge<T: AsNodeRef>(&self, src: T, dst: T) -> bool {
        (&self).edge(src, dst).is_some()
    }

    fn node<T: AsNodeRef>(&self, v: T) -> Option<NodeView<'graph, Self>> {
        let v = v.as_node_ref();
        let vid = self.internalise_node(v)?;
        if self.filtered() {
            let core_node = self.core_node(vid);
            if !self.filter_node(core_node.as_ref()) {
                return None;
            }
        }
        Some(NodeView::new_internal(self.clone(), vid))
    }

    fn edge<T: AsNodeRef>(&self, src: T, dst: T) -> Option<EdgeView<Self>> {
        let layer_ids = self.layer_ids();
        let src = self.internalise_node(src.as_node_ref())?;
        let dst = self.internalise_node(dst.as_node_ref())?;
        let src_node = self.core_node(src);
        let edge_ref = src_node.find_edge(dst, layer_ids)?;
        match self.filter_state() {
            FilterState::Neither => {}
            FilterState::Both | FilterState::BothIndependent | FilterState::Edges => {
                let edge = self.core_edge(edge_ref.pid());
                if !self.filter_edge(edge.as_ref()) {
                    return None;
                }
            }
            FilterState::Nodes => {
                if !self.filter_node(src_node.as_ref()) {
                    return None;
                }
                let dst_node = self.core_node(dst);
                if !self.filter_node(dst_node.as_ref()) {
                    return None;
                }
            }
        }
        Some(EdgeView::new(self.clone(), edge_ref))
    }

    fn properties(&self) -> Properties<Self> {
        Properties::new(self.clone())
    }

    fn metadata(&self) -> Metadata<'graph, Self> {
        Metadata::new(self.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct IndexSpec {
    pub(crate) node_metadata: HashSet<usize>,
    pub(crate) node_properties: HashSet<usize>,
    pub(crate) edge_metadata: HashSet<usize>,
    pub(crate) edge_properties: HashSet<usize>,
}

/// (Experimental) IndexSpec data structure.
impl IndexSpec {
    pub fn diff(existing: &IndexSpec, requested: &IndexSpec) -> Option<IndexSpec> {
        fn diff_props(existing: &HashSet<usize>, requested: &HashSet<usize>) -> HashSet<usize> {
            requested.difference(existing).copied().collect()
        }

        let node_metadata = diff_props(&existing.node_metadata, &requested.node_metadata);
        let node_properties = diff_props(&existing.node_properties, &requested.node_properties);
        let edge_metadata = diff_props(&existing.edge_metadata, &requested.edge_metadata);
        let edge_properties = diff_props(&existing.edge_properties, &requested.edge_properties);

        if node_metadata.is_empty()
            && node_properties.is_empty()
            && edge_metadata.is_empty()
            && edge_properties.is_empty()
        {
            None
        } else {
            Some(IndexSpec {
                node_metadata,
                node_properties,
                edge_metadata,
                edge_properties,
            })
        }
    }

    pub fn union(existing: &IndexSpec, other: &IndexSpec) -> IndexSpec {
        fn union_props(a: &HashSet<usize>, b: &HashSet<usize>) -> HashSet<usize> {
            a.union(b).copied().collect()
        }

        IndexSpec {
            node_metadata: union_props(&existing.node_metadata, &other.node_metadata),
            node_properties: union_props(&existing.node_properties, &other.node_properties),
            edge_metadata: union_props(&existing.edge_metadata, &other.edge_metadata),
            edge_properties: union_props(&existing.edge_properties, &other.edge_properties),
        }
    }

    pub fn props<G: BoxableGraphView + Sized + Clone + 'static>(
        &self,
        graph: &G,
    ) -> ResolvedIndexSpec {
        let extract_names = |props: &HashSet<usize>, meta: &PropMapper| {
            let mut names: Vec<String> = props
                .iter()
                .map(|prop_id| meta.get_name(*prop_id).to_string())
                .collect();
            names.sort();
            names
        };

        ResolvedIndexSpec {
            node_metadata: extract_names(&self.node_metadata, graph.node_meta().metadata_mapper()),
            node_properties: extract_names(
                &self.node_properties,
                graph.node_meta().temporal_prop_mapper(),
            ),
            edge_metadata: extract_names(&self.edge_metadata, graph.edge_meta().metadata_mapper()),
            edge_properties: extract_names(
                &self.edge_properties,
                graph.edge_meta().temporal_prop_mapper(),
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ResolvedIndexSpec {
    pub node_metadata: Vec<String>,
    pub node_properties: Vec<String>,
    pub edge_metadata: Vec<String>,
    pub edge_properties: Vec<String>,
}

impl ResolvedIndexSpec {
    pub fn to_vec(&self) -> Vec<Vec<String>> {
        vec![
            self.node_metadata.clone(),
            self.node_properties.clone(),
            self.edge_metadata.clone(),
            self.edge_properties.clone(),
        ]
    }
}

#[derive(Clone)]
/// (Experimental) Creates a IndexSpec data structure containing the specified properties and metadata.
pub struct IndexSpecBuilder<G: BoxableGraphView + Sized + Clone + 'static> {
    pub graph: G,
    node_metadata: Option<HashSet<usize>>,
    node_properties: Option<HashSet<usize>>,
    edge_metadata: Option<HashSet<usize>>,
    edge_properties: Option<HashSet<usize>>,
}

impl<G: BoxableGraphView + Sized + Clone + 'static> IndexSpecBuilder<G> {
    /// Create a new IndexSpecBuilder.
    ///
    /// Arguments:
    ///     graph:
    pub fn new(graph: G) -> Self {
        Self {
            graph,
            node_metadata: None,
            node_properties: None,
            edge_metadata: None,
            edge_properties: None,
        }
    }

    /// Include all node properties and metadata in the IndexSpec.
    ///
    /// Returns:
    ///     IndexSpecBuilder:
    pub fn with_all_node_properties_and_metadata(mut self) -> Self {
        self.node_metadata = Some(Self::extract_props(
            self.graph.node_meta().metadata_mapper(),
        ));
        self.node_properties = Some(Self::extract_props(
            self.graph.node_meta().temporal_prop_mapper(),
        ));
        self
    }

    /// Include all node metadata in the IndexSpec.
    ///
    /// Returns:
    ///     IndexSpecBuilder:
    pub fn with_all_node_metadata(mut self) -> Self {
        self.node_metadata = Some(Self::extract_props(
            self.graph.node_meta().metadata_mapper(),
        ));
        self
    }

    /// Include all node properties in the IndexSpec.
    ///
    /// Returns:
    ///     IndexSpecBuilder:
    pub fn with_all_node_properties(mut self) -> Self {
        self.node_properties = Some(Self::extract_props(
            self.graph.node_meta().temporal_prop_mapper(),
        ));
        self
    }

    /// Include the specified node metadata in the IndexSpec.
    ///
    /// Specified node metadata is gathered using the extract_named_props function.
    ///
    /// Returns:
    ///     IndexSpecBuilder:
    pub fn with_node_metadata<S: AsRef<str>>(
        mut self,
        props: impl IntoIterator<Item = S>,
    ) -> Result<Self, GraphError> {
        self.node_metadata = Some(Self::extract_named_props(
            self.graph.node_meta().metadata_mapper(),
            props,
        )?);
        Ok(self)
    }

    /// Include the specified node properties in the IndexSpec.
    ///
    /// Specified node properties is gathered using the extract_named_props function.
    ///
    /// Returns:
    ///     IndexSpecBuilder:
    pub fn with_node_properties<S: AsRef<str>>(
        mut self,
        props: impl IntoIterator<Item = S>,
    ) -> Result<Self, GraphError> {
        self.node_properties = Some(Self::extract_named_props(
            self.graph.node_meta().temporal_prop_mapper(),
            props,
        )?);
        Ok(self)
    }

    /// Include all edge properties and metadata in the IndexSpec.
    ///
    /// Returns:
    ///     IndexSpecBuilder:
    pub fn with_all_edge_properties_and_metadata(mut self) -> Self {
        self.edge_metadata = Some(Self::extract_props(
            self.graph.edge_meta().metadata_mapper(),
        ));
        self.edge_properties = Some(Self::extract_props(
            self.graph.edge_meta().temporal_prop_mapper(),
        ));
        self
    }

    /// Include all edge metadata in the IndexSpec.
    ///
    /// Returns:
    ///     IndexSpecBuilder:
    pub fn with_all_edge_metadata(mut self) -> Self {
        self.edge_metadata = Some(Self::extract_props(
            self.graph.edge_meta().metadata_mapper(),
        ));
        self
    }

    /// Include all edge properties in the IndexSpec.
    ///
    /// Returns:
    ///     IndexSpecBuilder:
    pub fn with_all_edge_properties(mut self) -> Self {
        self.edge_properties = Some(Self::extract_props(
            self.graph.edge_meta().temporal_prop_mapper(),
        ));
        self
    }

    /// Include the specified edge metadata in the IndexSpec.
    ///
    /// Specified edge metadata is gathered using the extract_named_props function.
    ///
    /// Returns:
    ///     IndexSpecBuilder:
    pub fn with_edge_metadata<S: AsRef<str>>(
        mut self,
        props: impl IntoIterator<Item = S>,
    ) -> Result<Self, GraphError> {
        self.edge_metadata = Some(Self::extract_named_props(
            self.graph.edge_meta().metadata_mapper(),
            props,
        )?);
        Ok(self)
    }

    /// Include the specified edge properties in the IndexSpec.
    ///
    /// Specified edge properties is gathered using the extract_named_props function.
    ///
    /// Returns:
    ///     IndexSpecBuilder:
    pub fn with_edge_properties<S: AsRef<str>>(
        mut self,
        props: impl IntoIterator<Item = S>,
    ) -> Result<Self, GraphError> {
        self.edge_properties = Some(Self::extract_named_props(
            self.graph.edge_meta().temporal_prop_mapper(),
            props,
        )?);
        Ok(self)
    }

    /// Extract properties or metadata.
    fn extract_props(meta: &PropMapper) -> HashSet<usize> {
        meta.ids().collect()
    }

    /// Extract specified named properties or metadata.
    fn extract_named_props<S: AsRef<str>>(
        meta: &PropMapper,
        keys: impl IntoIterator<Item = S>,
    ) -> Result<HashSet<usize>, GraphError> {
        keys.into_iter()
            .map(|k| {
                let s = k.as_ref();
                let id = meta
                    .get_id(s)
                    .ok_or_else(|| GraphError::PropertyMissingError(s.to_string()))?;
                Ok(id)
            })
            .collect()
    }

    /// Create a new IndexSpec.
    ///
    /// Returns:
    ///     IndexSpec:
    pub fn build(self) -> IndexSpec {
        IndexSpec {
            node_metadata: self.node_metadata.unwrap_or_default(),
            node_properties: self.node_properties.unwrap_or_default(),
            edge_metadata: self.edge_metadata.unwrap_or_default(),
            edge_properties: self.edge_properties.unwrap_or_default(),
        }
    }
}

#[cfg(feature = "search")]
impl<G: StaticGraphViewOps> SearchableGraphOps for G {
    fn get_index_spec(&self) -> Result<IndexSpec, GraphError> {
        self.get_storage()
            .map_or(Err(GraphError::IndexingNotSupported), |storage| {
                storage.get_index_spec()
            })
    }

    fn search_nodes<F: TryAsCompositeFilter>(
        &self,
        filter: F,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<'static, G>>, GraphError> {
        if let Some(storage) = self.get_storage() {
            let guard = storage.get_index().read_recursive();
            if let Some(searcher) = guard.searcher() {
                return searcher.search_nodes(self, filter, limit, offset);
            }
        }

        fallback_filter_nodes(self, &filter.try_as_composite_node_filter()?, limit, offset)
    }

    fn search_edges<F: TryAsCompositeFilter>(
        &self,
        filter: F,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<Self>>, GraphError> {
        if let Some(storage) = self.get_storage() {
            let guard = storage.get_index().read_recursive();
            if let Some(searcher) = guard.searcher() {
                return searcher.search_edges(self, filter, limit, offset);
            }
        }

        fallback_filter_edges(self, &filter.try_as_composite_edge_filter()?, limit, offset)
    }

    fn search_exploded_edges<F: TryAsCompositeFilter>(
        &self,
        filter: F,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<Self>>, GraphError> {
        if let Some(storage) = self.get_storage() {
            let guard = storage.get_index().read();
            if let Some(searcher) = guard.searcher() {
                return searcher.search_exploded_edges(self, filter, limit, offset);
            }
        }

        fallback_filter_exploded_edges(
            self,
            &filter.try_as_composite_exploded_edge_filter()?,
            limit,
            offset,
        )
    }

    fn is_indexed(&self) -> bool {
        self.get_storage().is_some_and(|s| s.is_indexed())
    }
}

pub trait StaticGraphViewOps: GraphView + 'static {}

impl<G: GraphView + 'static> StaticGraphViewOps for G {}

impl<'graph, G> InternalFilter<'graph> for G
where
    G: GraphView + 'graph,
{
    type Graph = G;
    type Filtered<Next: GraphViewOps<'graph> + 'graph> = Next;

    fn base_graph(&self) -> &Self::Graph {
        self
    }

    fn apply_filter<Next: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: Next,
    ) -> Self::Filtered<Next> {
        filtered_graph
    }
}
