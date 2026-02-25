#[cfg(feature = "io")]
use crate::serialise::GraphPaths;
use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, LayerIds, VID},
    db::{
        api::{
            properties::{internal::InternalMetadataOps, Metadata, Properties},
            state::{ops::filter::NodeTypeFilterOp, Index},
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
    prelude::*,
};
use ahash::HashSet;
use db4_graph::TemporalGraph;
use itertools::Itertools;
use raphtory_api::{
    atomic_extra::atomic_usize_from_mut_slice,
    core::{
        entities::{
            properties::meta::{Meta, PropMapper, STATIC_GRAPH_LAYER_ID},
            EID,
        },
        storage::{arc_str::ArcStr, timeindex::EventTime},
        Direction,
    },
};
use raphtory_core::utils::iter::GenLockedIter;
use raphtory_storage::{
    graph::{
        edges::edge_storage_ops::EdgeStorageOps, graph::GraphStorage,
        nodes::node_storage_ops::NodeStorageOps,
    },
    mutation::{
        addition_ops::{InternalAdditionOps, SessionAdditionOps},
        MutationError,
    },
};
use rayon::prelude::*;
use rustc_hash::FxHashSet;
use std::{
    path::Path,
    sync::{atomic::Ordering, Arc},
};
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

fn materialize_impl(
    graph: &impl GraphView,
    path: Option<&Path>,
    config: Config,
) -> Result<MaterializedGraph, GraphError> {
    let storage = graph.core_graph().lock();
    let mut node_meta = Meta::new_for_nodes();
    let mut edge_meta = Meta::new_for_edges();
    let mut graph_props_meta = Meta::new_for_graph_props();

    node_meta.set_metadata_mapper(graph.node_meta().metadata_mapper().deep_clone());
    node_meta.set_temporal_prop_mapper(graph.node_meta().temporal_prop_mapper().deep_clone());
    edge_meta.set_metadata_mapper(graph.edge_meta().metadata_mapper().deep_clone());
    edge_meta.set_temporal_prop_mapper(graph.edge_meta().temporal_prop_mapper().deep_clone());
    graph_props_meta.set_metadata_mapper(graph.graph_props_meta().metadata_mapper().deep_clone());
    graph_props_meta
        .set_temporal_prop_mapper(graph.graph_props_meta().temporal_prop_mapper().deep_clone());

    let layer_meta = edge_meta.layer_meta();

    // NOTE: layers must be set in layer_meta before the TemporalGraph is initialized to
    // make sure empty layers are created.
    let layer_map: Vec<_> = match graph.layer_ids() {
        LayerIds::None => {
            // no layers to map
            vec![]
        }
        LayerIds::All => {
            let layers = storage.edge_meta().layer_meta().keys();
            let mut layer_map = vec![0; storage.edge_meta().layer_meta().num_all_fields()];

            for (id, name) in storage.edge_meta().layer_meta().ids().zip(layers.iter()) {
                let new_id = layer_meta.get_or_create_id(name).inner();
                layer_map[id] = new_id;
            }

            layer_map
        }
        LayerIds::One(l_id) => {
            let mut layer_map = vec![0; storage.edge_meta().layer_meta().num_all_fields()];
            let layer_name = storage.edge_meta().get_layer_name_by_id(*l_id);
            let new_id = layer_meta.get_or_create_id(&layer_name).inner();

            layer_map[*l_id] = new_id;
            layer_map
        }
        LayerIds::Multiple(ids) => {
            let mut layer_map = vec![0; storage.edge_meta().layer_meta().num_all_fields()];
            let layers = storage.edge_meta().layer_meta().all_keys();

            for id in ids {
                let layer_name = &layers[id];
                let new_id = layer_meta.get_or_create_id(layer_name).inner();
                layer_map[id] = new_id;
            }

            layer_map
        }
    };

    node_meta.set_layer_mapper(layer_meta.clone());

    // Create new WAL file for the new materialized graph.
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
    };

    if let Some(latest) = graph.latest_time() {
        temporal_graph.update_time(latest);
    };

    // Set event counter to be the same as old graph to avoid any possibility for duplicate event ids
    temporal_graph
        .storage()
        .set_event_id(storage.read_event_id());

    let temporal_graph = Arc::new(temporal_graph);
    let graph_storage = GraphStorage::from(temporal_graph.clone());

    {
        // scope for the write lock
        let mut node_map = vec![VID::default(); storage.unfiltered_num_nodes()];
        let node_map_shared = atomic_usize_from_mut_slice(bytemuck::cast_slice_mut(&mut node_map));

        // reverse index pos -> new_vid
        let index = Index::for_graph(graph);
        graph.nodes().par_iter().for_each(|node| {
            let vid = node.node;
            if let Some(pos) = index.index(&vid) {
                let new_vid = temporal_graph.storage().nodes().reserve_vid(pos);
                node_map_shared[pos].store(new_vid.index(), Ordering::Relaxed);
            }
        });

        let get_new_vid = |old_vid: VID, index: &Index<VID>, node_map: &[VID]| -> VID {
            let pos = index
                .index(&old_vid)
                .expect("old_vid should exist in index");
            node_map[pos]
        };
        let mut new_storage = graph_storage.write_lock()?;

        for layer_id in &layer_map {
            new_storage.nodes.ensure_layer(*layer_id);
        }

        new_storage.nodes.par_iter_mut().try_for_each(|shard| {
            for node in graph.nodes().iter() {
                let new_id = get_new_vid(node.node, &index, &node_map);
                let gid = node.id();

                if let Some(node_pos) = shard.resolve_pos(new_id) {
                    let mut writer = shard.writer();

                    if let Some(node_type) = node.node_type() {
                        let new_type_id = graph_storage
                            .node_meta()
                            .node_type_meta()
                            .get_or_create_id(&node_type)
                            .inner();
                        writer.store_node_id_and_node_type(
                            node_pos,
                            STATIC_GRAPH_LAYER_ID,
                            gid.as_ref(),
                            new_type_id,
                        );
                    } else {
                        writer.store_node_id(node_pos, STATIC_GRAPH_LAYER_ID, gid.clone());
                    }

                    graph_storage
                        .write_session()?
                        .set_node(gid.as_ref(), new_id)?;

                    for (t, row) in node.rows() {
                        writer.add_props(t, node_pos, STATIC_GRAPH_LAYER_ID, row);
                    }

                    writer.update_c_props(
                        node_pos,
                        STATIC_GRAPH_LAYER_ID,
                        node.metadata_ids()
                            .filter_map(|id| node.get_metadata(id).map(|prop| (id, prop))),
                    );
                }
            }

            Ok::<(), MutationError>(())
        })?;

        let mut new_eids = vec![];
        let mut max_eid = 0usize;
        for (row, _) in graph.edges().iter().enumerate() {
            let new_eid = new_storage.graph().storage().edges().reserve_new_eid(row);
            new_eids.push(new_eid);
            max_eid = new_eid.0.max(max_eid);
        }
        new_storage.resize_segments_to_eid(EID(max_eid));

        for layer_id in &layer_map {
            new_storage.edges.ensure_layer(*layer_id);
        }

        new_storage.edges.par_iter_mut().try_for_each(|shard| {
            for (row, edge) in graph.edges().iter().enumerate() {
                let src = get_new_vid(edge.edge.src(), &index, &node_map);
                let dst = get_new_vid(edge.edge.dst(), &index, &node_map);
                let eid = new_eids[row];
                if let Some(edge_pos) = shard.resolve_pos(eid) {
                    let mut writer = shard.writer();
                    // make the edge for the first time
                    writer.add_static_edge(Some(edge_pos), src, dst, false);

                    for edge in edge.explode_layers() {
                        let layer = layer_map[edge.edge.layer().unwrap()];
                        for edge in edge.explode() {
                            let t = edge.edge.time().unwrap();
                            writer.add_edge(t, edge_pos, src, dst, [], layer);
                        }
                        //TODO: move this in edge.row()
                        for (t, t_props) in edge
                            .properties()
                            .temporal()
                            .values()
                            .map(|tp| {
                                let prop_id = tp.id();
                                tp.iter_indexed()
                                    .map(|(t, prop)| (t, prop_id, prop))
                                    .collect::<Vec<_>>()
                            })
                            .kmerge_by(|(t, _, _), (t2, _, _)| t <= t2)
                            .chunk_by(|(t, _, _)| *t)
                            .into_iter()
                        {
                            let props = t_props
                                .map(|(_, prop_id, prop)| (prop_id, prop))
                                .collect::<Vec<_>>();
                            writer.add_edge(t, edge_pos, src, dst, props, layer);
                        }
                        writer.update_c_props(
                            edge_pos,
                            src,
                            dst,
                            layer,
                            edge.metadata_ids().filter_map(move |prop_id| {
                                edge.get_metadata(prop_id).map(|prop| (prop_id, prop))
                            }),
                        );
                    }

                    let time_semantics = graph.edge_time_semantics();
                    let edge_entry = graph.core_edge(edge.edge.pid());
                    for (t, layer) in time_semantics.edge_deletion_history(
                        edge_entry.as_ref(),
                        graph,
                        graph.layer_ids(),
                    ) {
                        let layer = layer_map[layer];
                        writer.delete_edge(t, edge_pos, src, dst, layer);
                    }
                }
            }
            Ok::<(), MutationError>(())
        })?;

        new_storage.nodes.par_iter_mut().try_for_each(|shard| {
            for (row, edge) in graph.edges().iter().enumerate() {
                let eid = new_eids[row];
                let src_id = get_new_vid(edge.edge.src(), &index, &node_map);
                let dst_id = get_new_vid(edge.edge.dst(), &index, &node_map);
                let maybe_src_pos = shard.resolve_pos(src_id);
                let maybe_dst_pos = shard.resolve_pos(dst_id);

                if let Some(node_pos) = maybe_src_pos {
                    let mut writer = shard.writer();
                    writer.add_static_outbound_edge(node_pos, dst_id, eid);
                }

                if let Some(node_pos) = maybe_dst_pos {
                    let mut writer = shard.writer();
                    writer.add_static_inbound_edge(node_pos, src_id, eid);
                }

                for e in edge.explode_layers() {
                    let layer = layer_map[e.edge.layer().unwrap()];
                    if let Some(node_pos) = maybe_src_pos {
                        let mut writer = shard.writer();
                        writer.add_outbound_edge::<i64>(
                            None,
                            node_pos,
                            dst_id,
                            eid.with_layer(layer),
                        );
                    }
                    if let Some(node_pos) = maybe_dst_pos {
                        let mut writer = shard.writer();
                        writer.add_inbound_edge::<i64>(
                            None,
                            node_pos,
                            src_id,
                            eid.with_layer(layer),
                        );
                    }
                }

                for e in edge.explode() {
                    if let Some(src_pos) = maybe_src_pos {
                        let mut writer = shard.writer();

                        let t = e.time().expect("exploded edge should have time");
                        let l = layer_map[e.edge.layer().unwrap()];
                        writer.update_timestamp(t, src_pos, eid.with_layer(l));
                    }
                    if let Some(dst_pos) = maybe_dst_pos {
                        if maybe_src_pos.is_none_or(|src_pos| src_pos != dst_pos) {
                            let mut writer = shard.writer();

                            let t = e.time().expect("exploded edge should have time");
                            let l = layer_map[e.edge.layer().unwrap()];
                            writer.update_timestamp(t, dst_pos, eid.with_layer(l));
                        }
                    }
                }

                let edge_time_semantics = graph.edge_time_semantics();
                let edge_entry = graph.core_edge(edge.edge.pid());
                for (t, layer) in edge_time_semantics.edge_deletion_history(
                    edge_entry.as_ref(),
                    graph,
                    graph.layer_ids(),
                ) {
                    let layer = layer_map[layer];
                    if let Some(src_pos) = maybe_src_pos {
                        let mut writer = shard.writer();
                        writer.update_timestamp(t, src_pos, eid.with_layer_deletion(layer));
                    }
                    if let Some(dst_pos) = maybe_dst_pos {
                        if maybe_src_pos.is_none_or(|src_pos| src_pos != dst_pos) {
                            let mut writer = shard.writer();
                            writer.update_timestamp(t, dst_pos, eid.with_layer_deletion(layer));
                        }
                    }
                }
            }

            Ok::<(), MutationError>(())
        })?;

        // Copy over graph properties
        {
            let graph_writer = new_storage.graph_props.writer();
            // Copy temporal properties
            for (prop_name, temporal_prop) in graph.properties().temporal().iter() {
                let prop_id = graph_storage
                    .graph_props_meta()
                    .temporal_prop_mapper()
                    .get_or_create_id(&prop_name)
                    .inner();

                for (t, prop_value) in temporal_prop.iter_indexed() {
                    graph_writer.add_properties(t, [(prop_id, prop_value)]);
                }
            }

            // Copy metadata (constant properties)
            let metadata_props: Vec<_> = graph
                .metadata()
                .iter_filtered()
                .map(|(prop_name, prop_value)| {
                    let prop_id = graph_storage
                        .graph_props_meta()
                        .metadata_mapper()
                        .get_or_create_id(&prop_name)
                        .inner();
                    (prop_id, prop_value)
                })
                .collect();

            if !metadata_props.is_empty() {
                graph_writer.update_metadata(metadata_props);
            }
        }
    }

    Ok(graph.new_base_graph(graph_storage))
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
        match self.filter_state() {
            FilterState::Neither => self.earliest_time_global().map(EventTime::start), // TODO: change earliest_time_global() to return EventTime
            _ => self
                .properties()
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
                .min(),
        }
    }

    /// Get the `EventTime` of the latest activity in the graph.
    #[inline]
    fn latest_time(&self) -> Option<EventTime> {
        match self.filter_state() {
            FilterState::Neither => self.latest_time_global().map(EventTime::end), // TODO: change latest_time_global to return EventTime
            _ => self
                .properties()
                .temporal()
                .values()
                .flat_map(|prop| prop.history().latest_time())
                .max()
                .into_iter()
                .chain(self.nodes().latest_time().par_iter_values().flatten().max())
                .max(),
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
    pub(crate) fn diff(existing: &IndexSpec, requested: &IndexSpec) -> Option<IndexSpec> {
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

    pub(crate) fn union(existing: &IndexSpec, other: &IndexSpec) -> IndexSpec {
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
