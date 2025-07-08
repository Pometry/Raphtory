use crate::{
    core::{
        entities::{nodes::node_ref::AsNodeRef, LayerIds, VID},
        storage::timeindex::AsTime,
    },
    db::{
        api::{
            properties::{internal::ConstantPropertiesOps, Properties},
            view::{internal::*, *},
        },
        graph::{
            create_node_type_filter,
            edge::EdgeView,
            edges::Edges,
            node::NodeView,
            nodes::Nodes,
            views::{
                cached_view::CachedView,
                filter::{
                    model::{AsEdgeFilter, AsNodeFilter},
                    node_type_filtered_graph::NodeTypeFilteredGraph,
                },
                node_subgraph::NodeSubgraph,
                valid_graph::ValidGraph,
            },
        },
    },
    errors::GraphError,
    prelude::*,
};
use ahash::HashSet;
use chrono::{DateTime, Utc};
use db4_graph::TemporalGraph;
use raphtory_api::{
    atomic_extra::atomic_usize_from_mut_slice,
    core::{
        entities::{
            properties::meta::{Meta, PropMapper},
            EID,
        },
        storage::{arc_str::ArcStr, timeindex::TimeIndexEntry},
        Direction,
    },
    GraphType,
};
use raphtory_core::utils::iter::GenLockedIter;
use raphtory_storage::{
    graph::{
        edges::edge_storage_ops::EdgeStorageOps, graph::GraphStorage,
        nodes::node_storage_ops::NodeStorageOps,
    },
    mutation::{addition_ops::InternalAdditionOps, MutationError},
};
use rayon::prelude::*;
use rustc_hash::FxHashSet;
use std::sync::{atomic::Ordering, Arc};

/// This trait GraphViewOps defines operations for accessing
/// information about a graph. The trait has associated types
/// that are used to define the type of the nodes, edges
/// and the corresponding iterators.
///
pub trait GraphViewOps<'graph>: BoxableGraphView + Sized + Clone + 'graph {
    /// Return an iterator over all edges in the graph.
    fn edges(&self) -> Edges<'graph, Self, Self>;

    /// Return a View of the nodes in the Graph
    fn nodes(&self) -> Nodes<'graph, Self, Self>;

    /// Get a graph clone
    ///
    /// # Arguments
    ///
    /// Returns:
    /// Graph - Returns clone of the graph
    fn materialize(&self) -> Result<MaterializedGraph, GraphError>;

    fn subgraph<I: IntoIterator<Item = V>, V: AsNodeRef>(&self, nodes: I) -> NodeSubgraph<Self>;

    fn cache_view(&self) -> CachedView<Self>;

    fn valid(&self) -> Result<ValidGraph<Self>, GraphError>;

    fn subgraph_node_types<I: IntoIterator<Item = V>, V: AsRef<str>>(
        &self,
        nodes_types: I,
    ) -> NodeTypeFilteredGraph<Self>;

    fn exclude_nodes<I: IntoIterator<Item = V>, V: AsNodeRef>(
        &self,
        nodes: I,
    ) -> NodeSubgraph<Self>;

    /// Return all the layer ids in the graph
    fn unique_layers(&self) -> BoxedIter<ArcStr>;
    /// Timestamp of earliest activity in the graph
    fn earliest_time(&self) -> Option<i64>;

    /// UTC DateTime of earliest activity in the graph
    fn earliest_date_time(&self) -> Option<DateTime<Utc>> {
        self.earliest_time()?.dt()
    }
    /// Timestamp of latest activity in the graph
    fn latest_time(&self) -> Option<i64>;

    /// UTC DateTime of latest activity in the graph
    fn latest_date_time(&self) -> Option<DateTime<Utc>> {
        self.latest_time()?.dt()
    }
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
    fn node<T: AsNodeRef>(&self, v: T) -> Option<NodeView<'graph, Self, Self>>;

    /// Get an edge `(src, dst)`.
    fn edge<T: AsNodeRef>(&self, src: T, dst: T) -> Option<EdgeView<Self, Self>>;

    /// Get all property values of this graph.
    ///
    /// Returns:
    ///
    /// A view of the properties of the graph
    fn properties(&self) -> Properties<Self>;
}

#[cfg(feature = "search")]
pub trait SearchableGraphOps: Sized {
    fn get_index_spec(&self) -> Result<IndexSpec, GraphError>;

    fn search_nodes<F: AsNodeFilter>(
        &self,
        filter: F,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<'static, Self>>, GraphError>;

    fn search_edges<F: AsEdgeFilter>(
        &self,
        filter: F,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<Self>>, GraphError>;

    fn is_indexed(&self) -> bool;
}

impl<'graph, G: GraphView + 'graph> GraphViewOps<'graph> for G {
    fn edges(&self) -> Edges<'graph, Self, Self> {
        let graph = self.clone();
        let edges: Arc<dyn Fn() -> BoxedLIter<'graph, EdgeRef> + Send + Sync + 'graph> =
            match graph.node_list() {
                NodeList::All { .. } => Arc::new(move || {
                    let edges = graph.core_graph().owned_edges();
                    let layer_ids = graph.layer_ids().clone();
                    let graph = graph.clone();
                    GenLockedIter::from(
                        (edges, layer_ids, graph),
                        move |(edges, layer_ids, graph)| {
                            let iter = edges.iter(layer_ids);
                            match graph.filter_state() {
                                FilterState::Neither => iter.map(|e| e.out_ref()).into_dyn_boxed(),
                                FilterState::Both => {
                                    let nodes = graph.core_graph().core_nodes();
                                    iter.filter_map(move |e| {
                                        (graph.filter_edge(e, graph.layer_ids())
                                            && graph.filter_node(nodes.node_entry(e.src()))
                                            && graph.filter_node(nodes.node_entry(e.dst())))
                                        .then_some(e.out_ref())
                                    })
                                    .into_dyn_boxed()
                                }
                                FilterState::Nodes => {
                                    let nodes = graph.core_graph().core_nodes();
                                    iter.filter_map(move |e| {
                                        (graph.filter_node(nodes.node_entry(e.src()))
                                            && graph.filter_node(nodes.node_entry(e.dst())))
                                        .then_some(e.out_ref())
                                    })
                                    .into_dyn_boxed()
                                }
                                FilterState::Edges | FilterState::BothIndependent => iter
                                    .filter_map(move |e| {
                                        graph
                                            .filter_edge(e, graph.layer_ids())
                                            .then_some(e.out_ref())
                                    })
                                    .into_dyn_boxed(),
                            }
                        },
                    )
                    .into_dyn_boxed()
                }),
                NodeList::List { elems } => Arc::new(move || {
                    let cg = graph.core_graph().lock();
                    let graph = graph.clone();
                    elems
                        .clone()
                        .into_iter()
                        .flat_map(move |node| {
                            node_edges(cg.clone(), graph.clone(), node, Direction::OUT)
                        })
                        .into_dyn_boxed()
                }),
            };
        Edges {
            base_graph: self.clone(),
            graph: self.clone(),
            edges,
        }
    }

    fn nodes(&self) -> Nodes<'graph, Self, Self> {
        let graph = self.clone();
        Nodes::new(graph)
    }

    fn materialize(&self) -> Result<MaterializedGraph, GraphError> {
        let storage = self.core_graph().lock();

        // preserve all property mappings
        let mut node_meta = Meta::new();
        let mut edge_meta = Meta::new();

        node_meta.set_const_prop_meta(self.node_meta().const_prop_meta().deep_clone());
        node_meta.set_temporal_prop_meta(self.node_meta().temporal_prop_meta().deep_clone());
        edge_meta.set_const_prop_meta(self.edge_meta().const_prop_meta().deep_clone());
        edge_meta.set_temporal_prop_meta(self.edge_meta().temporal_prop_meta().deep_clone());

        let mut g = TemporalGraph::new_with_meta(None, node_meta, edge_meta);
        // Copy all graph properties
        g.graph_meta = self.graph_meta().deep_clone().into();

        let layer_map: Vec<_> = match self.layer_ids() {
            LayerIds::None => {
                // no layers to map
                vec![]
            }
            LayerIds::All => {
                let mut layer_map = vec![0; self.unfiltered_num_layers() + 1];
                let layers = storage.edge_meta().layer_meta().get_keys();
                for id in 0..layers.len() {
                    let new_id = g.resolve_layer(Some(&layers[id]))?.inner();
                    layer_map[id] = new_id;
                }
                layer_map
            }
            LayerIds::One(l_id) => {
                let mut layer_map = vec![0; self.unfiltered_num_layers() + 1];
                let new_id =
                    g.resolve_layer(Some(&storage.edge_meta().get_layer_name_by_id(*l_id)))?;
                layer_map[*l_id] = new_id.inner();
                layer_map
            }
            LayerIds::Multiple(ids) => {
                let mut layer_map = vec![0; self.unfiltered_num_layers() + 1];
                let layers = storage.edge_meta().layer_meta().get_keys();
                for id in ids {
                    let new_id = g.resolve_layer(Some(&layers[id]))?.inner();
                    layer_map[id] = new_id;
                }
                layer_map
            }
        };

        if let Some(earliest) = self.earliest_time() {
            g.update_time(TimeIndexEntry::start(earliest));
        } else {
            return Ok(self.new_base_graph(g.into()));
        };

        if let Some(latest) = self.latest_time() {
            g.update_time(TimeIndexEntry::end(latest));
        } else {
            return Ok(self.new_base_graph(g.into()));
        };

        // Set event counter to be the same as old graph to avoid any possibility for duplicate event ids
        g.storage().set_event_id(storage.read_event_id());

        let g = GraphStorage::from(g);

        {
            // scope for the write lock
            let mut new_storage = g.write_lock()?;

            println!("NODE COUNT {}", self.count_nodes());
            new_storage.resize_chunks_to_num_nodes(self.count_nodes());
            for layer_id in &layer_map {
                new_storage.nodes.ensure_layer(*layer_id);
            }

            let mut node_map = vec![VID::default(); storage.unfiltered_num_nodes()];
            let node_map_shared =
                atomic_usize_from_mut_slice(bytemuck::cast_slice_mut(&mut node_map));

            new_storage.nodes.par_iter_mut().try_for_each(|shard| {
                for (index, node) in self.nodes().iter().enumerate() {
                    let new_id = VID(index);
                    let gid = node.id();
                    node_map_shared[node.node.index()].store(new_id.index(), Ordering::Relaxed);
                    if let Some(node_pos) = shard.resolve_pos(new_id) {
                        let mut writer = shard.writer();
                        if let Some(node_type) = node.node_type() {
                            let new_type_id = g
                                .node_meta()
                                .node_type_meta()
                                .get_or_create_id(&node_type)
                                .inner();
                            writer.store_node_id_and_node_type(
                                node_pos,
                                0,
                                gid.as_ref(),
                                new_type_id,
                                0,
                            );
                        } else {
                            writer.store_node_id(node_pos, 0, gid.as_ref(), 0);
                        }

                        for (t, row) in node.rows() {
                            writer.add_props(t, node_pos, 0, row, 0);
                        }

                        writer.update_c_props(
                            node_pos,
                            0,
                            node.const_prop_ids()
                                .filter_map(|id| node.get_const_prop(id).map(|prop| (id, prop))),
                            0,
                        );
                    }
                }
                Ok::<(), MutationError>(())
            })?;

            new_storage.resize_chunks_to_num_edges(self.count_edges());

            for layer_id in &layer_map {
                new_storage.edges.ensure_layer(*layer_id);
            }

            new_storage.edges.par_iter_mut().try_for_each(|shard| {
                for (eid, edge) in self.edges().iter().enumerate() {
                    let src = node_map[edge.edge.src().index()];
                    let dst = node_map[edge.edge.dst().index()];
                    let eid = EID(eid);
                    if let Some(edge_pos) = shard.resolve_pos(eid) {
                        let mut writer = shard.writer();
                        // make the edge for the first time
                        writer.add_static_edge(Some(edge_pos), src, dst, 0, 0, Some(false));

                        for edge in edge.explode_layers() {
                            let layer = layer_map[edge.edge.layer().unwrap()];
                            for edge in edge.explode() {
                                let t = edge.edge.time().unwrap();
                                writer.add_edge(
                                    t,
                                    Some(edge_pos),
                                    src,
                                    dst,
                                    [],
                                    layer,
                                    0,
                                    Some(true),
                                );
                            }
                            for t_prop in edge.properties().temporal().values() {
                                let prop_id = t_prop.id();
                                for (t, prop_value) in t_prop.iter_indexed() {
                                    writer.add_edge(
                                        t,
                                        Some(edge_pos),
                                        src,
                                        dst,
                                        [(prop_id, prop_value)],
                                        layer,
                                        0,
                                        Some(true),
                                    );
                                }
                            }
                            writer.update_c_props(
                                edge_pos,
                                src,
                                dst,
                                layer,
                                edge.const_prop_ids().filter_map(move |prop_id| {
                                    edge.get_const_prop(prop_id).map(|prop| (prop_id, prop))
                                }),
                            );
                        }

                        let time_semantics = self.edge_time_semantics();
                        let edge_entry = self.core_edge(edge.edge.pid());
                        for (t, layer) in time_semantics.edge_deletion_history(
                            edge_entry.as_ref(),
                            self,
                            self.layer_ids(),
                        ) {
                            writer.delete_edge(t, Some(edge_pos), src, dst, layer, 0, Some(true));
                        }
                    }
                }
                Ok::<(), MutationError>(())
            })?;

            new_storage.nodes.par_iter_mut().try_for_each(|shard| {
                for (eid, edge) in self.edges().iter().enumerate() {
                    let eid = EID(eid);
                    let src_id = node_map[edge.edge.src().index()];
                    let dst_id = node_map[edge.edge.dst().index()];

                    if let Some(node_pos) = shard.resolve_pos(src_id) {
                        let mut writer = shard.writer();
                        writer.add_static_outbound_edge(node_pos, dst_id, eid, 0);
                    }

                    if let Some(node_pos) = shard.resolve_pos(dst_id) {
                        let mut writer = shard.writer();
                        writer.add_static_inbound_edge(node_pos, src_id, eid, 0);
                    }

                    for e in edge.explode_layers() {
                        let layer = layer_map[e.edge.layer().unwrap()];
                        if let Some(node_pos) = shard.resolve_pos(src_id) {
                            let mut writer = shard.writer();
                            writer.add_outbound_edge::<i64>(
                                None,
                                node_pos,
                                dst_id,
                                eid.with_layer(layer),
                                0,
                            );
                        }
                        if let Some(node_pos) = shard.resolve_pos(dst_id) {
                            let mut writer = shard.writer();
                            writer.add_inbound_edge::<i64>(
                                None,
                                node_pos,
                                src_id,
                                eid.with_layer(layer),
                                0,
                            );
                        }
                    }

                    for e in edge.explode() {
                        if let Some(node_pos) = shard.resolve_pos(src_id) {
                            let mut writer = shard.writer();

                            let t = e.time_and_index().expect("exploded edge should have time");
                            let l = layer_map[e.edge.layer().unwrap()];
                            writer.update_timestamp(t, node_pos, eid.with_layer(l), 0);
                        }
                        if let Some(node_pos) = shard.resolve_pos(dst_id) {
                            let mut writer = shard.writer();

                            let t = e.time_and_index().expect("exploded edge should have time");
                            let l = layer_map[e.edge.layer().unwrap()];
                            writer.update_timestamp(t, node_pos, eid.with_layer(l), 0);
                        }
                    }

                    let edge_time_semantics = self.edge_time_semantics();
                    let edge_entry = self.core_edge(edge.edge.pid());
                    for (t, layer) in edge_time_semantics.edge_deletion_history(
                        edge_entry.as_ref(),
                        self,
                        self.layer_ids(),
                    ) {
                        let src = node_map[edge.edge.src().index()];
                        let dst = node_map[edge.edge.dst().index()];

                        if let Some(node_pos) = shard.resolve_pos(src) {
                            let mut writer = shard.writer();
                            writer.update_deletion_time(
                                t,
                                node_pos,
                                eid.with_layer_deletion(layer),
                                0,
                            );
                        }
                        if let Some(node_pos) = shard.resolve_pos(dst) {
                            let mut writer = shard.writer();
                            writer.update_deletion_time(
                                t,
                                node_pos,
                                eid.with_layer_deletion(layer),
                                0,
                            );
                        }
                    }
                }

                Ok::<(), MutationError>(())
            })?;
        }

        Ok(self.new_base_graph(g))
    }

    fn subgraph<I: IntoIterator<Item = V>, V: AsNodeRef>(&self, nodes: I) -> NodeSubgraph<G> {
        NodeSubgraph::new(self.clone(), nodes)
    }

    fn cache_view(&self) -> CachedView<G> {
        CachedView::new(self.clone())
    }

    fn valid(&self) -> Result<ValidGraph<Self>, GraphError> {
        match self.graph_type() {
            GraphType::EventGraph => Err(GraphError::EventGraphNoValidView),
            GraphType::PersistentGraph => Ok(ValidGraph::new(self.clone())),
        }
    }

    fn subgraph_node_types<I: IntoIterator<Item = V>, V: AsRef<str>>(
        &self,
        node_types: I,
    ) -> NodeTypeFilteredGraph<Self> {
        let node_types_filter =
            create_node_type_filter(self.node_meta().node_type_meta(), node_types);
        NodeTypeFilteredGraph::new(self.clone(), node_types_filter)
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

    #[inline]
    fn earliest_time(&self) -> Option<i64> {
        match self.filter_state() {
            FilterState::Neither => self.earliest_time_global(),
            _ => self
                .properties()
                .temporal()
                .values()
                .flat_map(|prop| prop.history().next())
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

    #[inline]
    fn latest_time(&self) -> Option<i64> {
        match self.filter_state() {
            FilterState::Neither => self.latest_time_global(),
            _ => self
                .properties()
                .temporal()
                .values()
                .flat_map(|prop| prop.history_rev().next())
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
        match self.filter_state() {
            FilterState::Neither => {
                if matches!(self.layer_ids(), LayerIds::All) {
                    self.unfiltered_num_edges()
                } else {
                    self.core_edges().as_ref().count(self.layer_ids())
                }
            }
            FilterState::Both => {
                let edges = self.core_edges();
                let nodes = self.core_nodes();
                edges
                    .as_ref()
                    .par_iter(self.layer_ids())
                    .filter(|e| {
                        self.filter_edge(e.as_ref(), self.layer_ids())
                            && self.filter_node(nodes.node_entry(e.src()))
                            && self.filter_node(nodes.node_entry(e.dst()))
                    })
                    .count()
            }
            FilterState::Nodes => {
                let edges = self.core_edges();
                let nodes = self.core_nodes();
                edges
                    .as_ref()
                    .par_iter(self.layer_ids())
                    .filter(|e| {
                        self.filter_node(nodes.node_entry(e.src()))
                            && self.filter_node(nodes.node_entry(e.dst()))
                    })
                    .count()
            }
            FilterState::Edges | FilterState::BothIndependent => {
                let edges = self.core_edges();
                edges
                    .as_ref()
                    .par_iter(self.layer_ids())
                    .filter(|e| self.filter_edge(e.as_ref(), self.layer_ids()))
                    .count()
            }
        }
    }

    fn count_temporal_edges(&self) -> usize {
        let core_edges = self.core_edges();
        let layer_ids = self.layer_ids();
        let edge_time_semantics = self.edge_time_semantics();
        match self.filter_state() {
            FilterState::Neither => core_edges
                .as_ref()
                .par_iter(layer_ids)
                .map(move |edge| edge_time_semantics.edge_exploded_count(edge.as_ref(), self))
                .sum(),
            FilterState::Both => {
                let nodes = self.core_nodes();
                core_edges
                    .as_ref()
                    .par_iter(layer_ids)
                    .filter(|e| {
                        self.filter_edge(e.as_ref(), self.layer_ids())
                            && self.filter_node(nodes.node_entry(e.src()))
                            && self.filter_node(nodes.node_entry(e.dst()))
                    })
                    .map(move |e| edge_time_semantics.edge_exploded_count(e.as_ref(), self))
                    .sum()
            }
            FilterState::Nodes => {
                let nodes = self.core_nodes();
                core_edges
                    .as_ref()
                    .par_iter(layer_ids)
                    .filter(|e| {
                        self.filter_node(nodes.node_entry(e.src()))
                            && self.filter_node(nodes.node_entry(e.dst()))
                    })
                    .map(move |edge| edge_time_semantics.edge_exploded_count(edge.as_ref(), self))
                    .sum()
            }
            FilterState::Edges | FilterState::BothIndependent => core_edges
                .as_ref()
                .par_iter(layer_ids)
                .filter(|e| self.filter_edge(e.as_ref(), self.layer_ids()))
                .map(move |edge| edge_time_semantics.edge_exploded_count(edge.as_ref(), self))
                .sum(),
        }
    }

    #[inline]
    fn has_node<T: AsNodeRef>(&self, v: T) -> bool {
        if let Some(node_id) = self.internalise_node(v.as_node_ref()) {
            if self.nodes_filtered() {
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

    fn node<T: AsNodeRef>(&self, v: T) -> Option<NodeView<'graph, Self, Self>> {
        let v = v.as_node_ref();
        let vid = self.internalise_node(v)?;
        if self.nodes_filtered() {
            let core_node = self.core_node(vid);
            if !self.filter_node(core_node.as_ref()) {
                return None;
            }
        }
        Some(NodeView::new_internal(self.clone(), vid))
    }

    fn edge<T: AsNodeRef>(&self, src: T, dst: T) -> Option<EdgeView<Self, Self>> {
        let layer_ids = self.layer_ids();
        let src = self.internalise_node(src.as_node_ref())?;
        let dst = self.internalise_node(dst.as_node_ref())?;
        let src_node = self.core_node(src);
        match self.filter_state() {
            FilterState::Neither => {
                let edge_ref = src_node.find_edge(dst, layer_ids)?;
                Some(EdgeView::new(self.clone(), edge_ref))
            }
            FilterState::Both => {
                if !self.filter_node(src_node.as_ref()) {
                    return None;
                }
                let edge_ref = src_node.find_edge(dst, layer_ids)?;
                if !self.filter_edge(self.core_edge(edge_ref.pid()).as_ref(), layer_ids) {
                    return None;
                }
                if !self.filter_node(self.core_node(dst).as_ref()) {
                    return None;
                }
                Some(EdgeView::new(self.clone(), edge_ref))
            }
            FilterState::Nodes => {
                if !self.filter_node(src_node.as_ref()) {
                    return None;
                }
                let edge_ref = src_node.find_edge(dst, layer_ids)?;
                if !self.filter_node(self.core_node(dst).as_ref()) {
                    return None;
                }
                Some(EdgeView::new(self.clone(), edge_ref))
            }
            FilterState::Edges | FilterState::BothIndependent => {
                let edge_ref = src_node.find_edge(dst, layer_ids)?;
                if !self.filter_edge(self.core_edge(edge_ref.pid()).as_ref(), layer_ids) {
                    return None;
                }
                Some(EdgeView::new(self.clone(), edge_ref))
            }
        }
    }

    fn properties(&self) -> Properties<Self> {
        Properties::new(self.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct IndexSpec {
    pub(crate) node_const_props: HashSet<usize>,
    pub(crate) node_temp_props: HashSet<usize>,
    pub(crate) edge_const_props: HashSet<usize>,
    pub(crate) edge_temp_props: HashSet<usize>,
}

impl IndexSpec {
    pub(crate) fn diff(existing: &IndexSpec, requested: &IndexSpec) -> Option<IndexSpec> {
        fn diff_props(existing: &HashSet<usize>, requested: &HashSet<usize>) -> HashSet<usize> {
            requested.difference(existing).copied().collect()
        }

        let node_const_props = diff_props(&existing.node_const_props, &requested.node_const_props);
        let node_temp_props = diff_props(&existing.node_temp_props, &requested.node_temp_props);
        let edge_const_props = diff_props(&existing.edge_const_props, &requested.edge_const_props);
        let edge_temp_props = diff_props(&existing.edge_temp_props, &requested.edge_temp_props);

        if node_const_props.is_empty()
            && node_temp_props.is_empty()
            && edge_const_props.is_empty()
            && edge_temp_props.is_empty()
        {
            None
        } else {
            Some(IndexSpec {
                node_const_props,
                node_temp_props,
                edge_const_props,
                edge_temp_props,
            })
        }
    }

    pub(crate) fn union(existing: &IndexSpec, other: &IndexSpec) -> IndexSpec {
        fn union_props(a: &HashSet<usize>, b: &HashSet<usize>) -> HashSet<usize> {
            a.union(b).copied().collect()
        }

        IndexSpec {
            node_const_props: union_props(&existing.node_const_props, &other.node_const_props),
            node_temp_props: union_props(&existing.node_temp_props, &other.node_temp_props),
            edge_const_props: union_props(&existing.edge_const_props, &other.edge_const_props),
            edge_temp_props: union_props(&existing.edge_temp_props, &other.edge_temp_props),
        }
    }

    pub fn props(&self, graph: &Graph) -> Vec<Vec<String>> {
        let extract_names = |props: &HashSet<usize>, meta: &PropMapper| {
            let mut names: Vec<String> = props
                .iter()
                .map(|prop_id| meta.get_name(*prop_id).to_string())
                .collect();
            names.sort();
            names
        };

        vec![
            extract_names(&self.node_const_props, graph.node_meta().const_prop_meta()),
            extract_names(
                &self.node_temp_props,
                graph.node_meta().temporal_prop_meta(),
            ),
            extract_names(&self.edge_const_props, graph.edge_meta().const_prop_meta()),
            extract_names(
                &self.edge_temp_props,
                graph.edge_meta().temporal_prop_meta(),
            ),
        ]
    }
}

#[derive(Clone)]
pub struct IndexSpecBuilder<G: BoxableGraphView + Sized + Clone + 'static> {
    pub graph: G,
    node_const_props: Option<HashSet<usize>>,
    node_temp_props: Option<HashSet<usize>>,
    edge_const_props: Option<HashSet<usize>>,
    edge_temp_props: Option<HashSet<usize>>,
}

impl<G: BoxableGraphView + Sized + Clone + 'static> IndexSpecBuilder<G> {
    pub fn new(graph: G) -> Self {
        Self {
            graph,
            node_const_props: None,
            node_temp_props: None,
            edge_const_props: None,
            edge_temp_props: None,
        }
    }

    pub fn with_all_node_props(mut self) -> Self {
        self.node_const_props = Some(Self::extract_props(
            self.graph.node_meta().const_prop_meta(),
        ));
        self.node_temp_props = Some(Self::extract_props(
            self.graph.node_meta().temporal_prop_meta(),
        ));
        self
    }

    pub fn with_all_const_node_props(mut self) -> Self {
        self.node_const_props = Some(Self::extract_props(
            self.graph.node_meta().const_prop_meta(),
        ));
        self
    }

    pub fn with_all_temp_node_props(mut self) -> Self {
        self.node_temp_props = Some(Self::extract_props(
            self.graph.node_meta().temporal_prop_meta(),
        ));
        self
    }

    pub fn with_const_node_props<S: AsRef<str>>(
        mut self,
        props: impl IntoIterator<Item = S>,
    ) -> Result<Self, GraphError> {
        self.node_const_props = Some(Self::extract_named_props(
            self.graph.node_meta().const_prop_meta(),
            props,
        )?);
        Ok(self)
    }

    pub fn with_temp_node_props<S: AsRef<str>>(
        mut self,
        props: impl IntoIterator<Item = S>,
    ) -> Result<Self, GraphError> {
        self.node_temp_props = Some(Self::extract_named_props(
            self.graph.node_meta().temporal_prop_meta(),
            props,
        )?);
        Ok(self)
    }

    pub fn with_all_edge_props(mut self) -> Self {
        self.edge_const_props = Some(Self::extract_props(
            self.graph.edge_meta().const_prop_meta(),
        ));
        self.edge_temp_props = Some(Self::extract_props(
            self.graph.edge_meta().temporal_prop_meta(),
        ));
        self
    }

    pub fn with_all_edge_const_props(mut self) -> Self {
        self.edge_const_props = Some(Self::extract_props(
            self.graph.edge_meta().const_prop_meta(),
        ));
        self
    }

    pub fn with_all_temp_edge_props(mut self) -> Self {
        self.edge_temp_props = Some(Self::extract_props(
            self.graph.edge_meta().temporal_prop_meta(),
        ));
        self
    }

    pub fn with_const_edge_props<S: AsRef<str>>(
        mut self,
        props: impl IntoIterator<Item = S>,
    ) -> Result<Self, GraphError> {
        self.edge_const_props = Some(Self::extract_named_props(
            self.graph.edge_meta().const_prop_meta(),
            props,
        )?);
        Ok(self)
    }

    pub fn with_temp_edge_props<S: AsRef<str>>(
        mut self,
        props: impl IntoIterator<Item = S>,
    ) -> Result<Self, GraphError> {
        self.edge_temp_props = Some(Self::extract_named_props(
            self.graph.edge_meta().temporal_prop_meta(),
            props,
        )?);
        Ok(self)
    }

    fn extract_props(meta: &PropMapper) -> HashSet<usize> {
        (0..meta.len()).collect()
    }

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

    pub fn build(self) -> IndexSpec {
        IndexSpec {
            node_const_props: self.node_const_props.unwrap_or_default(),
            node_temp_props: self.node_temp_props.unwrap_or_default(),
            edge_const_props: self.edge_const_props.unwrap_or_default(),
            edge_temp_props: self.edge_temp_props.unwrap_or_default(),
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

    fn search_nodes<F: AsNodeFilter>(
        &self,
        filter: F,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<'static, G>>, GraphError> {
        let index = self
            .get_storage()
            .and_then(|s| s.get_index())
            .ok_or(GraphError::IndexNotCreated)?;
        index.searcher().search_nodes(self, filter, limit, offset)
    }

    fn search_edges<F: AsEdgeFilter>(
        &self,
        filter: F,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<Self>>, GraphError> {
        let index = self
            .get_storage()
            .and_then(|s| s.get_index())
            .ok_or(GraphError::IndexNotCreated)?;
        index.searcher().search_edges(self, filter, limit, offset)
    }

    fn is_indexed(&self) -> bool {
        self.get_storage()
            .map_or(false, |s| s.get_index().is_some())
    }
}

pub trait StaticGraphViewOps: GraphView + 'static {}

impl<G: GraphView + 'static> StaticGraphViewOps for G {}

impl<'graph, G: GraphViewOps<'graph> + 'graph> OneHopFilter<'graph> for G {
    type BaseGraph = G;
    type FilteredGraph = G;
    type Filtered<GH: GraphViewOps<'graph> + 'graph> = GH;

    fn current_filter(&self) -> &Self::FilteredGraph {
        self
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        self
    }

    fn one_hop_filtered<GH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GH,
    ) -> Self::Filtered<GH> {
        filtered_graph
    }
}

#[cfg(test)]
mod test_exploded_edges {
    use crate::{prelude::*, test_storage};
    use itertools::Itertools;

    #[test]
    fn test_add_node_properties_ordered_by_secondary_index() {
        let graph: Graph = Graph::new();
        graph.add_node((0, 3), 0, [("prop", "1")], None).unwrap();
        graph.add_node((0, 2), 0, [("prop", "2")], None).unwrap();
        graph.add_node((0, 1), 0, [("prop", "3")], None).unwrap();

        let props = graph
            .node("0")
            .map(|node| {
                node.properties()
                    .temporal()
                    .get("prop")
                    .unwrap()
                    .values()
                    .map(|x| x.to_string())
                    .collect_vec()
            })
            .unwrap();

        assert_eq!(
            props,
            vec!["3".to_string(), "2".to_string(), "1".to_string()]
        );
    }

    #[test]
    fn test_add_node_properties_overwritten_for_same_secondary_index() {
        let graph: Graph = Graph::new();
        graph.add_node((0, 1), 0, [("prop", "1")], None).unwrap();
        graph.add_node((0, 1), 0, [("prop", "2")], None).unwrap();
        graph.add_node((0, 1), 0, [("prop", "3")], None).unwrap();

        let props = graph
            .node(0)
            .map(|node| {
                node.properties()
                    .temporal()
                    .get("prop")
                    .unwrap()
                    .values()
                    .map(|x| x.to_string())
                    .collect_vec()
            })
            .unwrap();

        assert_eq!(props, vec!["3".to_string()]);

        let graph: Graph = Graph::new();
        graph.add_node((0, 1), 0, [("prop", "1")], None).unwrap();
        graph.add_node((0, 2), 0, [("prop", "2")], None).unwrap();
        graph.add_node((0, 2), 0, [("prop", "3")], None).unwrap();

        let props = graph
            .node(0)
            .map(|node| {
                node.properties()
                    .temporal()
                    .get("prop")
                    .unwrap()
                    .values()
                    .map(|x| x.to_string())
                    .collect_vec()
            })
            .unwrap();

        assert_eq!(props, vec!["1".to_string(), "3".to_string()]);
    }

    #[test]
    fn test_create_node_properties_ordered_by_secondary_index() {
        let graph: Graph = Graph::new();
        graph.create_node((0, 3), 0, [("prop", "1")], None).unwrap();
        graph.add_node((0, 2), 0, [("prop", "2")], None).unwrap();
        graph.add_node((0, 1), 0, [("prop", "3")], None).unwrap();

        let props = graph
            .node("0")
            .map(|node| {
                node.properties()
                    .temporal()
                    .get("prop")
                    .unwrap()
                    .values()
                    .map(|x| x.to_string())
                    .collect_vec()
            })
            .unwrap();

        assert_eq!(
            props,
            vec!["3".to_string(), "2".to_string(), "1".to_string()]
        );
    }

    #[test]
    fn test_create_node_properties_overwritten_for_same_secondary_index() {
        let graph: Graph = Graph::new();
        graph.create_node((0, 1), 0, [("prop", "1")], None).unwrap();
        graph.add_node((0, 1), 0, [("prop", "2")], None).unwrap();
        graph.add_node((0, 1), 0, [("prop", "3")], None).unwrap();

        let props = graph
            .node(0)
            .map(|node| {
                node.properties()
                    .temporal()
                    .get("prop")
                    .unwrap()
                    .values()
                    .map(|x| x.to_string())
                    .collect_vec()
            })
            .unwrap();

        assert_eq!(props, vec!["3".to_string()]);

        let graph: Graph = Graph::new();
        graph.create_node((0, 1), 0, [("prop", "1")], None).unwrap();
        graph.add_node((0, 2), 0, [("prop", "2")], None).unwrap();
        graph.add_node((0, 2), 0, [("prop", "3")], None).unwrap();

        let props = graph
            .node(0)
            .map(|node| {
                node.properties()
                    .temporal()
                    .get("prop")
                    .unwrap()
                    .values()
                    .map(|x| x.to_string())
                    .collect_vec()
            })
            .unwrap();

        assert_eq!(props, vec!["1".to_string(), "3".to_string()]);
    }

    #[test]
    fn test_add_edge_properties_ordered_by_secondary_index() {
        let graph: Graph = Graph::new();
        graph.add_edge((0, 3), 0, 1, [("prop", "1")], None).unwrap();
        graph.add_edge((0, 2), 0, 1, [("prop", "2")], None).unwrap();
        graph.add_edge((0, 1), 0, 1, [("prop", "3")], None).unwrap();

        let props = graph
            .edge(0, 1)
            .map(|edge| {
                edge.properties()
                    .temporal()
                    .get("prop")
                    .unwrap()
                    .values()
                    .map(|x| x.to_string())
                    .collect_vec()
            })
            .unwrap();

        assert_eq!(
            props,
            vec!["3".to_string(), "2".to_string(), "1".to_string()]
        );
    }

    #[test]
    fn test_add_edge_properties_overwritten_for_same_secondary_index() {
        let graph: Graph = Graph::new();
        graph.add_edge((0, 1), 0, 1, [("prop", "1")], None).unwrap();
        graph.add_edge((0, 1), 0, 1, [("prop", "2")], None).unwrap();
        graph.add_edge((0, 1), 0, 1, [("prop", "3")], None).unwrap();

        let props = graph
            .edge(0, 1)
            .map(|edge| {
                edge.properties()
                    .temporal()
                    .get("prop")
                    .unwrap()
                    .values()
                    .map(|x| x.to_string())
                    .collect_vec()
            })
            .unwrap();

        assert_eq!(props, vec!["3".to_string()]);

        let graph: Graph = Graph::new();
        graph.add_edge((0, 1), 0, 1, [("prop", "1")], None).unwrap();
        graph.add_edge((0, 2), 0, 1, [("prop", "2")], None).unwrap();
        graph.add_edge((0, 2), 0, 1, [("prop", "3")], None).unwrap();

        let props = graph
            .edge(0, 1)
            .map(|edge| {
                edge.properties()
                    .temporal()
                    .get("prop")
                    .unwrap()
                    .values()
                    .map(|x| x.to_string())
                    .collect_vec()
            })
            .unwrap();

        assert_eq!(props, vec!["1".to_string(), "3".to_string()]);
    }

    #[test]
    fn test_add_properties_properties_ordered_by_secondary_index() {
        let graph: Graph = Graph::new();
        graph.add_properties((0, 3), [("prop", "1")]).unwrap();
        graph.add_properties((0, 2), [("prop", "2")]).unwrap();
        graph.add_properties((0, 1), [("prop", "3")]).unwrap();

        let props = graph
            .properties()
            .temporal()
            .get("prop")
            .unwrap()
            .values()
            .map(|x| x.to_string())
            .collect_vec();

        assert_eq!(
            props,
            vec!["3".to_string(), "2".to_string(), "1".to_string()]
        );
    }

    #[test]
    fn test_add_properties_properties_overwritten_for_same_secondary_index() {
        let graph: Graph = Graph::new();
        graph.add_properties((0, 1), [("prop", "1")]).unwrap();
        graph.add_properties((0, 1), [("prop", "2")]).unwrap();
        graph.add_properties((0, 1), [("prop", "3")]).unwrap();

        let props = graph
            .properties()
            .temporal()
            .get("prop")
            .unwrap()
            .values()
            .map(|x| x.to_string())
            .collect_vec();

        assert_eq!(props, vec!["3".to_string()]);

        let graph: Graph = Graph::new();
        graph.add_edge((0, 1), 0, 1, NO_PROPS, None).unwrap();
        graph.add_edge((0, 2), 0, 1, NO_PROPS, None).unwrap();
        graph.add_edge((0, 2), 0, 1, NO_PROPS, None).unwrap();

        graph.add_properties((0, 1), [("prop", "1")]).unwrap();
        graph.add_properties((0, 2), [("prop", "2")]).unwrap();
        graph.add_properties((0, 2), [("prop", "3")]).unwrap();

        let props = graph
            .properties()
            .temporal()
            .get("prop")
            .unwrap()
            .values()
            .map(|x| x.to_string())
            .collect_vec();

        assert_eq!(props, vec!["1".to_string(), "3".to_string()]);
    }

    #[test]
    fn test_node_add_updates_properties_ordered_by_secondary_index() {
        let graph: Graph = Graph::new();
        graph.add_node(0, 0, NO_PROPS, None).unwrap();

        graph
            .node(0)
            .unwrap()
            .add_updates((0, 3), [("prop", "1")])
            .unwrap();
        graph
            .node(0)
            .unwrap()
            .add_updates((0, 2), [("prop", "2")])
            .unwrap();
        graph
            .node(0)
            .unwrap()
            .add_updates((0, 1), [("prop", "3")])
            .unwrap();

        let props = graph
            .node("0")
            .map(|node| {
                node.properties()
                    .temporal()
                    .get("prop")
                    .unwrap()
                    .values()
                    .map(|x| x.to_string())
                    .collect_vec()
            })
            .unwrap();

        assert_eq!(
            props,
            vec!["3".to_string(), "2".to_string(), "1".to_string()]
        );
    }

    #[test]
    fn test_node_add_updates_properties_overwritten_for_same_secondary_index() {
        let graph: Graph = Graph::new();
        graph.add_node(0, 0, NO_PROPS, None).unwrap();
        graph.add_node(0, 0, NO_PROPS, None).unwrap();
        graph.add_node(0, 0, NO_PROPS, None).unwrap();

        graph
            .node(0)
            .unwrap()
            .add_updates((0, 1), [("prop", "1")])
            .unwrap();
        graph
            .node(0)
            .unwrap()
            .add_updates((0, 1), [("prop", "2")])
            .unwrap();
        graph
            .node(0)
            .unwrap()
            .add_updates((0, 1), [("prop", "3")])
            .unwrap();

        let props = graph
            .node("0")
            .map(|node| {
                node.properties()
                    .temporal()
                    .get("prop")
                    .unwrap()
                    .values()
                    .map(|x| x.to_string())
                    .collect_vec()
            })
            .unwrap();

        assert_eq!(props, vec!["3".to_string()]);

        let graph: Graph = Graph::new();
        graph.add_node(0, 0, NO_PROPS, None).unwrap();
        graph.add_node(0, 0, NO_PROPS, None).unwrap();
        graph.add_node(0, 0, NO_PROPS, None).unwrap();

        graph.add_node(0, 0, NO_PROPS, None).unwrap();
        graph.add_node(0, 0, NO_PROPS, None).unwrap();
        graph.add_node(0, 0, NO_PROPS, None).unwrap();

        graph
            .node(0)
            .unwrap()
            .add_updates((0, 1), [("prop", "1")])
            .unwrap();
        graph
            .node(0)
            .unwrap()
            .add_updates((0, 2), [("prop", "2")])
            .unwrap();
        graph
            .node(0)
            .unwrap()
            .add_updates((0, 2), [("prop", "3")])
            .unwrap();

        let props = graph
            .node("0")
            .map(|node| {
                node.properties()
                    .temporal()
                    .get("prop")
                    .unwrap()
                    .values()
                    .map(|x| x.to_string())
                    .collect_vec()
            })
            .unwrap();

        assert_eq!(props, vec!["1".to_string(), "3".to_string()]);
    }

    #[test]
    fn test_edge_add_updates_properties_ordered_by_secondary_index() {
        let graph: Graph = Graph::new();
        graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();

        graph
            .edge(0, 1)
            .unwrap()
            .add_updates((0, 3), [("prop", "1")], None)
            .unwrap();
        graph
            .edge(0, 1)
            .unwrap()
            .add_updates((0, 2), [("prop", "2")], None)
            .unwrap();
        graph
            .edge(0, 1)
            .unwrap()
            .add_updates((0, 1), [("prop", "3")], None)
            .unwrap();

        let props = graph
            .edge(0, 1)
            .map(|edge| {
                edge.properties()
                    .temporal()
                    .get("prop")
                    .unwrap()
                    .values()
                    .map(|x| x.to_string())
                    .collect_vec()
            })
            .unwrap();

        assert_eq!(
            props,
            vec!["3".to_string(), "2".to_string(), "1".to_string()]
        );
    }

    #[test]
    fn test_edge_add_updates_properties_overwritten_for_same_secondary_index() {
        let graph: Graph = Graph::new();
        graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();

        graph
            .edge(0, 1)
            .unwrap()
            .add_updates((0, 1), [("prop", "1")], None)
            .unwrap();
        graph
            .edge(0, 1)
            .unwrap()
            .add_updates((0, 1), [("prop", "2")], None)
            .unwrap();
        graph
            .edge(0, 1)
            .unwrap()
            .add_updates((0, 1), [("prop", "3")], None)
            .unwrap();

        let props = graph
            .edge(0, 1)
            .map(|edge| {
                edge.properties()
                    .temporal()
                    .get("prop")
                    .unwrap()
                    .values()
                    .map(|x| x.to_string())
                    .collect_vec()
            })
            .unwrap();

        assert_eq!(props, vec!["3".to_string()]);

        let graph: Graph = Graph::new();
        graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();

        graph
            .edge(0, 1)
            .unwrap()
            .add_updates((0, 1), [("prop", "1")], None)
            .unwrap();
        graph
            .edge(0, 1)
            .unwrap()
            .add_updates((0, 2), [("prop", "2")], None)
            .unwrap();
        graph
            .edge(0, 1)
            .unwrap()
            .add_updates((0, 2), [("prop", "3")], None)
            .unwrap();

        let props = graph
            .edge(0, 1)
            .map(|edge| {
                edge.properties()
                    .temporal()
                    .get("prop")
                    .unwrap()
                    .values()
                    .map(|x| x.to_string())
                    .collect_vec()
            })
            .unwrap();

        assert_eq!(props, vec!["1".to_string(), "3".to_string()]);
    }

    #[test]
    fn test_exploded_edges() {
        let graph: Graph = Graph::new();
        graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        graph.add_edge(1, 0, 1, NO_PROPS, None).unwrap();
        graph.add_edge(2, 0, 1, NO_PROPS, None).unwrap();
        graph.add_edge(3, 0, 1, NO_PROPS, None).unwrap();
        test_storage!(&graph, |graph| {
            assert_eq!(graph.count_temporal_edges(), 4)
        });
    }
}

#[cfg(test)]
mod test_materialize {
    use crate::{
        db::graph::graph::assert_graph_equal,
        prelude::*,
        test_storage,
        test_utils::{build_edge_list, build_graph_from_edge_list},
    };
    use proptest::{arbitrary::any, proptest};
    use raphtory_api::core::storage::arc_str::OptionAsStr;
    use raphtory_storage::core_ops::CoreGraphOps;
    use std::ops::Range;

    #[test]
    fn test_materialize() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, [("layer1", "1")], Some("1")).unwrap();
        g.add_edge(0, 1, 2, [("layer2", "2")], Some("2")).unwrap();

        let gm = g.materialize().unwrap();

        assert_graph_equal(&g, &gm);
        assert_eq!(
            gm.nodes().name().iter_values().collect::<Vec<String>>(),
            vec!["1", "2"]
        );

        assert!(g
            .layers("2")
            .unwrap()
            .edge(1, 2)
            .unwrap()
            .properties()
            .temporal()
            .get("layer1")
            .and_then(|prop| prop.latest())
            .is_none());
        assert!(gm
            .into_events()
            .unwrap()
            .layers("2")
            .unwrap()
            .edge(1, 2)
            .unwrap()
            .properties()
            .temporal()
            .get("layer1")
            .and_then(|prop| prop.latest())
            .is_none());
    }

    #[test]
    fn test_graph_properties() {
        let g = Graph::new();
        g.add_properties(1, [("test", "test")]).unwrap();
        g.add_constant_properties([("test_constant", "test2")])
            .unwrap();

        test_storage!(&g, |g| {
            let gm = g.materialize().unwrap();
            assert_graph_equal(&g, &gm);
        });
    }

    #[test]
    fn materialize_prop_test() {
        proptest!(|(edges in build_edge_list(100, 100), w in any::<Range<i64>>())| {
            let g = build_graph_from_edge_list(&edges);
            test_storage!(&g, |g| {
                let gm = g.materialize().unwrap();
                assert_graph_equal(&g, &gm);
                let gw = g.window(w.start, w.end);
                let gmw = gw.materialize().unwrap();
                assert_graph_equal(&gw, &gmw);
            });
        })
    }

    #[test]
    fn test_subgraph() {
        let g = Graph::new();
        g.add_node(0, 1, NO_PROPS, None).unwrap();
        g.add_node(0, 2, NO_PROPS, None).unwrap();
        g.add_node(0, 3, NO_PROPS, None).unwrap();
        g.add_node(0, 4, NO_PROPS, None).unwrap();
        g.add_node(0, 5, NO_PROPS, None).unwrap();

        let nodes_subgraph = g.subgraph(vec![4, 5]);
        assert_eq!(
            nodes_subgraph
                .nodes()
                .name()
                .iter_values()
                .collect::<Vec<String>>(),
            vec!["4", "5"]
        );
        let gm = nodes_subgraph.materialize().unwrap();
        assert_graph_equal(&nodes_subgraph, &gm);
    }

    #[test]
    fn test_exclude_nodes() {
        let g = Graph::new();
        g.add_node(0, 1, NO_PROPS, None).unwrap();
        g.add_node(0, 2, NO_PROPS, None).unwrap();
        g.add_node(0, 3, NO_PROPS, None).unwrap();
        g.add_node(0, 4, NO_PROPS, None).unwrap();
        g.add_node(0, 5, NO_PROPS, None).unwrap();

        let exclude_nodes_subgraph = g.exclude_nodes(vec![4, 5]);
        assert_eq!(
            exclude_nodes_subgraph
                .nodes()
                .name()
                .iter_values()
                .collect::<Vec<String>>(),
            vec!["1", "2", "3"]
        );
        let gm = exclude_nodes_subgraph.materialize().unwrap();
        assert_graph_equal(&exclude_nodes_subgraph, &gm);
    }

    #[test]
    fn testing_node_types() {
        let graph = Graph::new();
        graph.add_node(0, "A", NO_PROPS, None).unwrap();
        graph.add_node(1, "B", NO_PROPS, Some("H")).unwrap();

        test_storage!(&graph, |graph| {
            let node_a = graph.node("A").unwrap();
            let node_b = graph.node("B").unwrap();
            let node_a_type = node_a.node_type();
            let node_a_type_str = node_a_type.as_str();

            assert_eq!(node_a_type_str, None);
            assert_eq!(node_b.node_type().as_str(), Some("H"));
        });

        // Nodes with No type can be overwritten
        let node_a = graph.add_node(1, "A", NO_PROPS, Some("TYPEA")).unwrap();
        assert_eq!(node_a.node_type().as_str(), Some("TYPEA"));

        // Check that overwriting a node type returns an error
        assert!(graph.add_node(2, "A", NO_PROPS, Some("TYPEB")).is_err());
        // Double check that the type did not actually change
        assert_eq!(graph.node("A").unwrap().node_type().as_str(), Some("TYPEA"));
        // Check that the update is not added to the graph
        let all_node_types = graph.get_all_node_types();
        assert_eq!(all_node_types.len(), 2);
    }

    #[test]
    fn changing_property_type_errors() {
        let g = Graph::new();
        let props_0 = [("test", Prop::U64(1))];
        let props_1 = [("test", Prop::F64(0.1))];
        g.add_properties(0, props_0.clone()).unwrap();
        assert!(g.add_properties(1, props_1.clone()).is_err());

        g.add_node(0, 1, props_0.clone(), None).unwrap();
        assert!(g.add_node(1, 1, props_1.clone(), None).is_err());

        g.add_edge(0, 1, 2, props_0.clone(), None).unwrap();
        assert!(g.add_edge(1, 1, 2, props_1.clone(), None).is_err());
    }

    #[test]
    fn test_edge_layer_properties() {
        let g = Graph::new();
        g.add_edge(1, "A", "B", [("greeting", "howdy")], Some("layer 1"))
            .unwrap();
        g.add_edge(2, "A", "B", [("greeting", "ola")], Some("layer 2"))
            .unwrap();
        g.add_edge(2, "A", "B", [("greeting", "hello")], Some("layer 2"))
            .unwrap();
        g.add_edge(3, "A", "B", [("greeting", "namaste")], Some("layer 3"))
            .unwrap();

        let edge_ab = g.edge("A", "B").unwrap();
        let props = edge_ab
            .properties()
            .iter()
            .filter_map(|(k, v)| v.map(move |v| (k.to_string(), v.to_string())))
            .collect::<Vec<_>>();
        assert_eq!(props, vec![("greeting".to_string(), "namaste".to_string())]);
    }
}
