use crate::{
    core::{
        entities::{graph::tgraph::TemporalGraph, nodes::node_ref::AsNodeRef, LayerIds, VID},
        storage::timeindex::AsTime,
        utils::errors::GraphError,
    },
    db::{
        api::{
            mutation::internal::InternalAdditionOps,
            properties::{
                internal::{ConstPropertiesOps, TemporalPropertiesOps, TemporalPropertiesRowView},
                Properties,
            },
            storage::graph::{
                edges::edge_storage_ops::EdgeStorageOps, nodes::node_storage_ops::NodeStorageOps,
            },
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
            },
        },
    },
};
use chrono::{DateTime, Utc};
use raphtory_api::{
    atomic_extra::atomic_usize_from_mut_slice,
    core::{
        entities::EID,
        storage::{arc_str::ArcStr, timeindex::TimeIndexEntry},
        Direction,
    },
};
use rayon::prelude::*;
use rustc_hash::FxHashSet;
use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::{atomic::Ordering, Arc},
};
#[cfg(feature = "search")]
use zip::ZipArchive;

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
    fn node<T: AsNodeRef>(&self, v: T) -> Option<NodeView<Self, Self>>;

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
    fn create_index(&self) -> Result<(), GraphError>;

    fn create_index_in_ram(&self) -> Result<(), GraphError>;

    fn load_index(&self, path: &PathBuf) -> Result<(), GraphError>;

    fn persist_index_to_disk(&self, path: &PathBuf) -> Result<(), GraphError>;

    fn persist_index_to_disk_zip(&self, path: &PathBuf) -> Result<(), GraphError>;

    fn search_nodes<F: AsNodeFilter>(
        &self,
        filter: F,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<Self>>, GraphError>;

    fn search_edges<F: AsEdgeFilter>(
        &self,
        filter: F,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<Self>>, GraphError>;

    fn is_indexed(&self) -> bool;
}

impl<'graph, G: BoxableGraphView + Sized + Clone + 'graph> GraphViewOps<'graph> for G {
    fn edges(&self) -> Edges<'graph, Self, Self> {
        let graph = self.clone();
        let edges = Arc::new(move || {
            let core_graph = graph.core_graph().lock();
            core_graph.into_edges_iter(graph.clone()).into_dyn_boxed()
        });
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
        let mut g = TemporalGraph::default();

        // Copy all graph properties
        g.graph_meta = self.graph_meta().deep_clone();

        // preserve all property mappings
        g.node_meta
            .set_const_prop_meta(self.node_meta().const_prop_meta().deep_clone());
        g.node_meta
            .set_temporal_prop_meta(self.node_meta().temporal_prop_meta().deep_clone());
        g.edge_meta
            .set_const_prop_meta(self.edge_meta().const_prop_meta().deep_clone());
        g.edge_meta
            .set_temporal_prop_meta(self.edge_meta().temporal_prop_meta().deep_clone());

        let layer_map: Vec<_> = match self.layer_ids() {
            LayerIds::None => {
                return Ok(self.new_base_graph(g.into()));
            }
            LayerIds::All => {
                let mut layer_map = vec![0; self.unfiltered_num_layers()];
                let layers = storage.edge_meta().layer_meta().get_keys();
                for id in 1..layers.len() {
                    let new_id = g.resolve_layer(Some(&layers[id]))?.inner();
                    layer_map[id] = new_id;
                }
                layer_map
            }
            LayerIds::One(l_id) => {
                let mut layer_map = vec![0; self.unfiltered_num_layers()];
                if *l_id > 0 {
                    let new_id =
                        g.resolve_layer(Some(&storage.edge_meta().get_layer_name_by_id(*l_id)))?;
                    layer_map[*l_id] = new_id.inner();
                }
                layer_map
            }
            LayerIds::Multiple(ids) => {
                let mut layer_map = vec![0; self.unfiltered_num_layers()];
                let ids = if ids.0[0] == 0 { &ids.0[1..] } else { &ids.0 };
                let layers = storage.edge_meta().layer_meta().get_keys();
                for id in ids {
                    let new_id = g.resolve_layer(Some(&layers[*id]))?.inner();
                    layer_map[*id] = new_id;
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
        g.event_counter
            .fetch_max(storage.read_event_id(), Ordering::Relaxed);

        {
            // scope for the write lock
            let mut new_storage = g.write_lock()?;
            new_storage.nodes.resize(self.count_nodes());

            let mut node_map = vec![VID::default(); storage.unfiltered_num_nodes()];
            let node_map_shared =
                atomic_usize_from_mut_slice(bytemuck::cast_slice_mut(&mut node_map));

            new_storage.nodes.par_iter_mut().try_for_each(|mut shard| {
                for (index, node) in self.nodes().iter().enumerate() {
                    let new_id = VID(index);
                    let gid = node.id();
                    if let Some(mut new_node) = shard.set(new_id, gid.as_ref()) {
                        node_map_shared[node.node.index()].store(index, Ordering::Relaxed);
                        if let Some(node_type) = node.node_type() {
                            let new_type_id = g
                                .node_meta
                                .node_type_meta()
                                .get_or_create_id(&node_type)
                                .inner();
                            new_node.node_store_mut().node_type = new_type_id;
                        }
                        g.logical_to_physical.set(gid.as_ref(), new_id)?;

                        for (t, rows) in node.rows() {
                            let prop_offset = new_node.t_props_log_mut().push(rows)?;
                            new_node.node_store_mut().update_t_prop_time(t, prop_offset);
                        }

                        for c_prop_id in node.const_prop_ids() {
                            if let Some(prop_value) = node.get_const_prop(c_prop_id) {
                                new_node
                                    .node_store_mut()
                                    .add_constant_prop(c_prop_id, prop_value)?;
                            }
                        }
                    }
                }
                Ok::<(), GraphError>(())
            })?;

            new_storage.edges.par_iter_mut().try_for_each(|mut shard| {
                for (eid, edge) in self.edges().iter().enumerate() {
                    if let Some(mut new_edge) = shard.get_mut(EID(eid)) {
                        let edge_store = new_edge.edge_store_mut();
                        edge_store.src = node_map[edge.edge.src().index()];
                        edge_store.dst = node_map[edge.edge.dst().index()];
                        edge_store.eid = EID(eid);
                        for edge in edge.explode_layers() {
                            let old_layer = LayerIds::All.constrain_from_edge(edge.edge);
                            let layer = layer_map[edge.edge.layer().unwrap()];
                            let additions = new_edge.additions_mut(layer);
                            for edge in edge.explode() {
                                let t = edge.edge.time().unwrap();
                                additions.insert(t);
                            }
                            for t_prop in edge.temporal_prop_ids() {
                                for (t, prop_value) in
                                    self.temporal_edge_prop_hist(edge.edge, t_prop, &old_layer)
                                {
                                    new_edge.layer_mut(layer).add_prop(t, t_prop, prop_value)?;
                                }
                            }
                            for c_prop in edge.const_prop_ids() {
                                if let Some(prop_value) = edge.get_const_prop(c_prop) {
                                    new_edge
                                        .layer_mut(layer)
                                        .add_constant_prop(c_prop, prop_value)?;
                                }
                            }
                            if self.include_deletions() {
                                let mut deletion_history =
                                    self.edge_deletion_history(edge.edge, &old_layer).peekable();
                                if deletion_history.peek().is_some() {
                                    let edge_deletions = new_edge.deletions_mut(layer_map[layer]);
                                    for t in deletion_history {
                                        edge_deletions.insert(t);
                                    }
                                }
                            }
                        }
                    }
                }
                Ok::<(), GraphError>(())
            })?;

            new_storage.nodes.par_iter_mut().try_for_each(|mut shard| {
                for (eid, edge) in self.edges().iter().enumerate() {
                    if let Some(src_node) = shard.get_mut(node_map[edge.edge.src().index()]) {
                        for t in self.edge_history(edge.edge, self.layer_ids()) {
                            src_node.update_time(t, EID(eid));
                        }
                        for ee in edge.explode_layers() {
                            src_node.add_edge(
                                node_map[edge.edge.dst().index()],
                                Direction::OUT,
                                ee.edge.layer().unwrap(),
                                EID(eid),
                            );
                        }
                    }
                    if let Some(dst_node) = shard.get_mut(node_map[edge.edge.dst().index()]) {
                        for t in self.edge_history(edge.edge, self.layer_ids()) {
                            dst_node.update_time(t, EID(eid));
                        }
                        for ee in edge.explode_layers() {
                            dst_node.add_edge(
                                node_map[edge.edge.src().index()],
                                Direction::IN,
                                ee.edge.layer().unwrap(),
                                EID(eid),
                            );
                        }
                    }

                    if self.include_deletions() {
                        for t in self.edge_deletion_history(edge.edge, self.layer_ids()) {
                            if let Some(src_node) = shard.get_mut(node_map[edge.edge.src().index()])
                            {
                                src_node.update_time(t, edge.edge.pid());
                            }
                            if let Some(dst_node) = shard.get_mut(node_map[edge.edge.dst().index()])
                            {
                                dst_node.update_time(t, edge.edge.pid());
                            }
                        }
                    }
                }

                Ok::<(), GraphError>(())
            })?;
        }

        Ok(self.new_base_graph(g.into()))
    }

    fn subgraph<I: IntoIterator<Item = V>, V: AsNodeRef>(&self, nodes: I) -> NodeSubgraph<G> {
        NodeSubgraph::new(self.clone(), nodes)
    }

    fn cache_view(&self) -> CachedView<G> {
        CachedView::new(self.clone())
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
        self.earliest_time_global()
    }

    #[inline]
    fn latest_time(&self) -> Option<i64> {
        self.latest_time_global()
    }

    #[inline]
    fn count_nodes(&self) -> usize {
        if !self.node_list_trusted() {
            let node_list = self.node_list();
            let core_nodes = self.core_nodes();
            let layer_ids = self.layer_ids();
            match node_list {
                NodeList::All { .. } => core_nodes
                    .as_ref()
                    .par_iter()
                    .filter(move |v| self.filter_node(*v, layer_ids))
                    .count(),
                NodeList::List { elems } => elems
                    .par_iter()
                    .filter(move |&id| self.filter_node(core_nodes.node_entry(id), layer_ids))
                    .count(),
            }
        } else {
            self.node_list().len()
        }
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
                            && self.filter_node(nodes.node_entry(e.src()), self.layer_ids())
                            && self.filter_node(nodes.node_entry(e.dst()), self.layer_ids())
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
                        self.filter_node(nodes.node_entry(e.src()), self.layer_ids())
                            && self.filter_node(nodes.node_entry(e.dst()), self.layer_ids())
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
        match self.filter_state() {
            FilterState::Neither => core_edges
                .as_ref()
                .par_iter(layer_ids)
                .map(move |edge| self.edge_exploded_count(edge.as_ref(), layer_ids))
                .sum(),
            FilterState::Both => {
                let nodes = self.core_nodes();
                core_edges
                    .as_ref()
                    .par_iter(layer_ids)
                    .filter(|e| {
                        self.filter_edge(e.as_ref(), self.layer_ids())
                            && self.filter_node(nodes.node_entry(e.src()), self.layer_ids())
                            && self.filter_node(nodes.node_entry(e.dst()), self.layer_ids())
                    })
                    .map(move |e| self.edge_exploded_count(e.as_ref(), layer_ids))
                    .sum()
            }
            FilterState::Nodes => {
                let nodes = self.core_nodes();
                core_edges
                    .as_ref()
                    .par_iter(layer_ids)
                    .filter(|e| {
                        self.filter_node(nodes.node_entry(e.src()), self.layer_ids())
                            && self.filter_node(nodes.node_entry(e.dst()), self.layer_ids())
                    })
                    .map(move |e| self.edge_exploded_count(e.as_ref(), layer_ids))
                    .sum()
            }
            FilterState::Edges | FilterState::BothIndependent => core_edges
                .as_ref()
                .par_iter(layer_ids)
                .filter(|e| self.filter_edge(e.as_ref(), self.layer_ids()))
                .map(move |e| self.edge_exploded_count(e.as_ref(), layer_ids))
                .sum(),
        }
    }

    #[inline]
    fn has_node<T: AsNodeRef>(&self, v: T) -> bool {
        if let Some(node_id) = self.internalise_node(v.as_node_ref()) {
            if self.nodes_filtered() {
                let node = self.core_node_entry(node_id);
                self.filter_node(node.as_ref(), self.layer_ids())
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

    fn node<T: AsNodeRef>(&self, v: T) -> Option<NodeView<Self, Self>> {
        let v = v.as_node_ref();
        let vid = self.internalise_node(v)?;
        if self.nodes_filtered() {
            let core_node = self.core_node_entry(vid);
            if !self.filter_node(core_node.as_ref(), self.layer_ids()) {
                return None;
            }
        }
        Some(NodeView::new_internal(self.clone(), vid))
    }

    fn edge<T: AsNodeRef>(&self, src: T, dst: T) -> Option<EdgeView<Self, Self>> {
        let layer_ids = self.layer_ids();
        let src = self.internalise_node(src.as_node_ref())?;
        let dst = self.internalise_node(dst.as_node_ref())?;
        let src_node = self.core_node_entry(src);
        match self.filter_state() {
            FilterState::Neither => {
                let edge_ref = src_node.find_edge(dst, layer_ids)?;
                Some(EdgeView::new(self.clone(), edge_ref))
            }
            FilterState::Both => {
                if !self.filter_node(src_node.as_ref(), self.layer_ids()) {
                    return None;
                }
                let edge_ref = src_node.find_edge(dst, layer_ids)?;
                if !self.filter_edge(self.core_edge(edge_ref.pid()).as_ref(), layer_ids) {
                    return None;
                }
                if !self.filter_node(self.core_node_entry(dst).as_ref(), layer_ids) {
                    return None;
                }
                Some(EdgeView::new(self.clone(), edge_ref))
            }
            FilterState::Nodes => {
                if !self.filter_node(src_node.as_ref(), self.layer_ids()) {
                    return None;
                }
                let edge_ref = src_node.find_edge(dst, layer_ids)?;
                if !self.filter_node(self.core_node_entry(dst).as_ref(), layer_ids) {
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

#[cfg(feature = "search")]
impl<G: BoxableGraphView + Sized + Clone + 'static> SearchableGraphOps for G {
    fn create_index(&self) -> Result<(), GraphError> {
        self.get_storage()
            .map_or(Err(GraphError::IndexingNotSupported), |storage| {
                storage.get_or_create_index(None)?;
                Ok(())
            })
    }

    fn create_index_in_ram(&self) -> Result<(), GraphError> {
        self.get_storage()
            .map_or(Err(GraphError::IndexingNotSupported), |storage| {
                storage.get_or_create_index_in_ram()?;
                Ok(())
            })
    }

    fn load_index(&self, path: &PathBuf) -> Result<(), GraphError> {
        fn has_index<P: AsRef<Path>>(zip_path: P) -> Result<bool, GraphError> {
            let file = File::open(&zip_path)?;
            let mut archive = ZipArchive::new(file)?;

            for i in 0..archive.len() {
                let entry = archive.by_index(i)?;
                let entry_path = Path::new(entry.name());

                if let Some(first_component) = entry_path.components().next() {
                    if first_component.as_os_str() == "index" {
                        return Ok(true);
                    }
                }
            }

            Ok(false)
        }

        self.get_storage()
            .map_or(Err(GraphError::IndexingNotSupported), |storage| {
                if path.is_file() {
                    if has_index(path)? {
                        storage.get_or_create_index(Some(path.clone()))?;
                    } else {
                        return Ok(()); // Skip if no index in zip
                    }
                } else {
                    let index_path = path.join("index");
                    if index_path.exists() && index_path.read_dir()?.next().is_some() {
                        storage.get_or_create_index(Some(index_path.clone()))?;
                    }
                }

                Ok(())
            })
    }

    fn persist_index_to_disk(&self, path: &PathBuf) -> Result<(), GraphError> {
        let path = path.join("index");
        self.get_storage()
            .map_or(Err(GraphError::IndexingNotSupported), |storage| {
                storage.persist_index_to_disk(&path)?;
                Ok(())
            })
    }

    fn persist_index_to_disk_zip(&self, path: &PathBuf) -> Result<(), GraphError> {
        self.get_storage()
            .map_or(Err(GraphError::IndexingNotSupported), |storage| {
                storage.persist_index_to_disk_zip(&path)?;
                Ok(())
            })
    }

    fn search_nodes<F: AsNodeFilter>(
        &self,
        filter: F,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<Self>>, GraphError> {
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

pub trait StaticGraphViewOps: for<'graph> GraphViewOps<'graph> + 'static {}

impl<G: for<'graph> GraphViewOps<'graph> + 'static> StaticGraphViewOps for G {}

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
        db::{api::view::internal::CoreGraphOps, graph::graph::assert_graph_equal},
        prelude::*,
        test_storage,
        test_utils::{build_edge_list, build_graph_from_edge_list},
    };
    use proptest::{arbitrary::any, proptest};
    use raphtory_api::core::storage::arc_str::OptionAsStr;
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
