use crate::{
    core::{
        entities::{graph::tgraph::TemporalGraph, nodes::node_ref::AsNodeRef, LayerIds, VID},
        storage::timeindex::AsTime,
        utils::errors::GraphError,
    },
    db::{
        api::{
            mutation::{internal::InternalAdditionOps, AdditionOps, PropertyAdditionOps},
            properties::{internal::TemporalPropertiesOps, Properties},
            storage::graph::{
                edges::edge_storage_ops::EdgeStorageOps, nodes::node_storage_ops::NodeStorageOps,
            },
            view::{internal::*, *},
        },
        graph::{
            edge::EdgeView,
            edges::Edges,
            node::NodeView,
            nodes::Nodes,
            views::{
                node_subgraph::NodeSubgraph, node_type_filtered_subgraph::TypeFilteredSubgraph,
            },
        },
    },
    prelude::DeletionOps,
};
use chrono::{DateTime, Utc};
use itertools::Itertools;
use raphtory_api::core::{
    entities::EID,
    storage::{
        arc_str::{ArcStr, OptionAsStr},
        timeindex::TimeIndexEntry,
    },
    Direction,
};
use rayon::prelude::*;
use rustc_hash::FxHashSet;
use std::{borrow::Borrow, sync::Arc};

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

    fn subgraph_node_types<I: IntoIterator<Item = V>, V: Borrow<str>>(
        &self,
        nodes_types: I,
    ) -> TypeFilteredSubgraph<Self>;

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
        let g = TemporalGraph::default();
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
                let ids = if ids[0] == 0 { &ids[1..] } else { ids };
                let layers = storage.edge_meta().layer_meta().get_keys();
                for id in ids {
                    let new_id = g.resolve_layer(Some(&layers[*id]))?.inner();
                    layer_map[*id] = new_id;
                }
                layer_map
            }
        };

        let mut node_map = vec![VID::default(); storage.unfiltered_num_nodes()];
        storage
            .nodes_par_opt(self, None)
            .zip(node_map.par_iter_mut())
            .try_for_each(|(node, entry)| {
                if let Some(node) = node {
                    let node_type_id = node.node_type_id();
                    let new_id = if node_type_id != 0 {
                        g.resolve_node_and_type(
                            node.id(),
                            self.node_meta()
                                .get_node_type_name_by_id(node_type_id)
                                .as_str()
                                .unwrap(),
                        )?
                        .inner()
                        .0
                        .inner()
                    } else {
                        g.resolve_node(node.id())?.inner()
                    };
                    *entry = new_id;
                }
                Ok::<(), GraphError>(())
            })?;

        {
            // scope for the write lock
            let mut new_storage = g.write_lock()?;
            new_storage.edges.par_iter_mut().try_for_each(|mut shard| {
                for (eid, edge) in self.edges().iter().enumerate() {
                    if let Some(mut new_edge) = shard.get_mut(EID(eid)) {
                        let edge_store = new_edge.edge_store_mut();
                        edge_store.src = node_map[edge.edge.src().index()];
                        edge_store.dst = node_map[edge.edge.dst().index()];
                        edge_store.eid = EID(eid);
                        for e in edge.explode() {
                            let t = e.edge.time().unwrap();
                            let layer = layer_map[*e.edge.layer().unwrap()];
                            let edge_additions = new_edge.additions_mut(layer);
                            edge_additions.insert(e.edge.time().unwrap());
                            let t_props = e.properties().temporal();
                            let mut props_iter = t_props.iter_latest().peekable();
                            if props_iter.peek().is_some() {
                                let edge_layer = new_edge.layer_mut(layer);
                                for (prop_name, prop_value) in props_iter {
                                    let prop_id = g
                                        .resolve_edge_property(
                                            &prop_name,
                                            prop_value.dtype(),
                                            false,
                                        )?
                                        .inner();
                                    edge_layer.add_prop(t, prop_id, prop_value)?;
                                }
                            }
                        }
                        for e in edge.explode_layers() {
                            let layer = layer_map[*e.edge.layer().unwrap()];
                            let c_props = e.properties().constant();
                            let mut props_iter = c_props.iter().peekable();
                            if props_iter.peek().is_some() {
                                let edge_layer = new_edge.layer_mut(layer);
                                for (prop_name, prop_value) in props_iter {
                                    let prop_id = g
                                        .resolve_edge_property(
                                            &prop_name,
                                            prop_value.dtype(),
                                            true,
                                        )?
                                        .inner();
                                    edge_layer.add_constant_prop(prop_id, prop_value)?;
                                }
                            }
                        }
                        if self.include_deletions() {
                            for e in edge.explode_layers() {
                                let layer = *e.edge.layer().unwrap();
                                let layer_ids = LayerIds::One(layer);
                                let mut deletion_history =
                                    self.edge_deletion_history(edge.edge, &layer_ids).peekable();
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
                        for ee in edge.explode_layers() {
                            src_node.add_edge(
                                node_map[edge.edge.dst().index()],
                                Direction::OUT,
                                *ee.edge.layer().unwrap(),
                                EID(eid),
                            );
                        }
                    }
                    if let Some(dst_node) = shard.get_mut(node_map[edge.edge.dst().index()]) {
                        for ee in edge.explode_layers() {
                            dst_node.add_edge(
                                node_map[edge.edge.src().index()],
                                Direction::IN,
                                *ee.edge.layer().unwrap(),
                                EID(eid),
                            );
                        }
                    }
                }
                let nodes = self.nodes();
                for node in nodes.iter() {
                    if let Some(new_node) = shard.get_mut(node_map[node.node.index()]) {
                        if let Some(earliest) = node.earliest_time() {
                            // explicitly add node earliest_time to handle PersistentGraph
                            new_node.update_time(TimeIndexEntry::start(earliest))
                        }
                        for t in node.history() {
                            new_node.update_time(TimeIndexEntry::start(t));
                        }
                        for prop_id in node.temporal_prop_ids() {
                            let prop_name = self.node_meta().temporal_prop_meta().get_name(prop_id);
                            let prop_type = self
                                .node_meta()
                                .temporal_prop_meta()
                                .get_dtype(prop_id)
                                .unwrap();
                            let new_prop_id = g
                                .resolve_node_property(&prop_name, prop_type, false)?
                                .inner();
                            for (t, prop_value) in self.temporal_node_prop_hist(node.node, prop_id)
                            {
                                new_node.add_prop(t, new_prop_id, prop_value)?;
                            }
                        }
                        for (c_prop_name, prop_value) in node.properties().constant().iter() {
                            let prop_id = g
                                .resolve_node_property(&c_prop_name, prop_value.dtype(), true)?
                                .inner();
                            new_node.add_constant_prop(prop_id, prop_value)?;
                        }
                    }
                }

                Ok::<(), GraphError>(())
            })?;

            g.add_constant_properties(self.properties().constant())?;
        }

        Ok(self.new_base_graph(g.into()))
    }

    fn subgraph<I: IntoIterator<Item = V>, V: AsNodeRef>(&self, nodes: I) -> NodeSubgraph<G> {
        let _layer_ids = self.layer_ids();
        let nodes: FxHashSet<VID> = nodes
            .into_iter()
            .flat_map(|v| (&self).node(v).map(|v| v.node))
            .collect();
        NodeSubgraph::new(self.clone(), nodes)
    }

    fn subgraph_node_types<I: IntoIterator<Item = V>, V: Borrow<str>>(
        &self,
        nodes_types: I,
    ) -> TypeFilteredSubgraph<Self> {
        let meta = self.node_meta().node_type_meta();
        let r = nodes_types
            .into_iter()
            .flat_map(|nt| meta.get_id(nt.borrow()))
            .collect_vec();
        TypeFilteredSubgraph::new(self.clone(), r)
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
            .map(|node| node.node)
            .collect();

        NodeSubgraph::new(self.clone(), nodes_to_include)
    }

    /// Return all the layer ids in the graph
    fn unique_layers(&self) -> BoxedIter<ArcStr> {
        self.get_layer_names_from_ids(self.layer_ids())
    }

    fn earliest_time(&self) -> Option<i64> {
        self.earliest_time_global()
    }

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
                    .filter(|v| self.filter_node(*v, layer_ids))
                    .count(),
                NodeList::List { nodes } => nodes
                    .par_iter()
                    .filter(|&&id| self.filter_node(core_nodes.node_entry(id), layer_ids))
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
                    .par_iter(self.layer_ids().clone())
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
                    .par_iter(self.layer_ids().clone())
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
                    .par_iter(self.layer_ids().clone())
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
                .par_iter(layer_ids.clone())
                .map(|edge| self.edge_exploded_count(edge.as_ref(), layer_ids))
                .sum(),
            FilterState::Both => {
                let nodes = self.core_nodes();
                core_edges
                    .as_ref()
                    .par_iter(layer_ids.clone())
                    .filter(|e| {
                        self.filter_edge(e.as_ref(), self.layer_ids())
                            && self.filter_node(nodes.node_entry(e.src()), self.layer_ids())
                            && self.filter_node(nodes.node_entry(e.dst()), self.layer_ids())
                    })
                    .map(|e| self.edge_exploded_count(e.as_ref(), layer_ids))
                    .sum()
            }
            FilterState::Nodes => {
                let nodes = self.core_nodes();
                core_edges
                    .as_ref()
                    .par_iter(layer_ids.clone())
                    .filter(|e| {
                        self.filter_node(nodes.node_entry(e.src()), self.layer_ids())
                            && self.filter_node(nodes.node_entry(e.dst()), self.layer_ids())
                    })
                    .map(|e| self.edge_exploded_count(e.as_ref(), layer_ids))
                    .sum()
            }
            FilterState::Edges | FilterState::BothIndependent => core_edges
                .as_ref()
                .par_iter(layer_ids.clone())
                .filter(|e| self.filter_edge(e.as_ref(), self.layer_ids()))
                .map(|e| self.edge_exploded_count(e.as_ref(), layer_ids))
                .sum(),
        }
    }

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
                if !self.filter_edge(self.core_edge(edge_ref.into()).as_ref(), layer_ids) {
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
                if !self.filter_edge(self.core_edge(edge_ref.into()).as_ref(), layer_ids) {
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
    use crate::{db::api::view::internal::CoreGraphOps, prelude::*, test_storage};
    use raphtory_api::core::storage::arc_str::OptionAsStr;

    #[test]
    fn test_materialize() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, [("layer1", "1")], Some("1")).unwrap();
        g.add_edge(0, 1, 2, [("layer2", "2")], Some("2")).unwrap();

        let gm = g.materialize().unwrap();
        assert_eq!(
            gm.nodes().name().values().collect::<Vec<String>>(),
            vec!["1", "2"]
        );

        assert!(!g
            .layers("2")
            .unwrap()
            .edge(1, 2)
            .unwrap()
            .properties()
            .temporal()
            .contains("layer1"));
        assert!(!gm
            .into_events()
            .unwrap()
            .layers("2")
            .unwrap()
            .edge(1, 2)
            .unwrap()
            .properties()
            .temporal()
            .contains("layer1"));
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
                .values()
                .collect::<Vec<String>>(),
            vec!["4", "5"]
        );
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
                .values()
                .collect::<Vec<String>>(),
            vec!["1", "2", "3"]
        );
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
}
