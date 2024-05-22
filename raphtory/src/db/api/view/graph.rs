use crate::{
    core::{
        entities::{graph::tgraph::InternalGraph, nodes::node_ref::AsNodeRef, LayerIds, VID},
        storage::timeindex::AsTime,
        utils::errors::GraphError,
        ArcStr, OptionAsStr,
    },
    db::{
        api::{
            mutation::{internal::InternalAdditionOps, AdditionOps, PropertyAdditionOps},
            properties::Properties,
            storage::{
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
    prelude::{DeletionOps, NO_PROPS},
};
use chrono::{DateTime, Utc};
use itertools::Itertools;
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
            let core_graph = graph.core_graph();
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
        let g = InternalGraph::default();

        // make sure we preserve all layers even if they are empty
        // skip default layer
        for layer in self.unique_layers().skip(1) {
            g.resolve_layer(Some(&layer));
        }
        // Add edges first so we definitely have all associated nodes (important in case of persistent edges)
        for e in self.edges() {
            // FIXME: this needs to be verified
            for ee in e.explode_layers() {
                let layer_id = *ee.edge.layer().expect("exploded layers");
                let layer_ids = LayerIds::One(layer_id);
                let layer_name = self.get_layer_name(layer_id);
                let layer_name: Option<&str> = if layer_id == 0 {
                    None
                } else {
                    Some(&layer_name)
                };

                for ee in ee.explode() {
                    g.add_edge(
                        ee.time().expect("exploded edge"),
                        ee.src().name(),
                        ee.dst().name(),
                        ee.properties().temporal().collect_properties(),
                        layer_name,
                    )?;
                }

                if self.include_deletions() {
                    for t in self.edge_deletion_history(e.edge, &layer_ids) {
                        g.delete_edge(t, e.src().id(), e.dst().id(), layer_name)?;
                    }
                }

                g.edge(ee.src().id(), ee.dst().id())
                    .expect("edge added")
                    .add_constant_properties(ee.properties().constant(), layer_name)?;
            }
        }

        for v in self.nodes().iter() {
            let v_type_string = v.node_type(); //stop it being dropped
            let v_type_str = v_type_string.as_str();
            for h in v.history() {
                g.add_node(h, v.name(), NO_PROPS, v_type_str)?;
            }
            for (name, prop_view) in v.properties().temporal().iter() {
                for (t, prop) in prop_view.iter() {
                    g.add_node(t, v.name(), [(name.clone(), prop)], v_type_str)?;
                }
            }
            g.node(v.id())
                .expect("node added")
                .add_constant_properties(v.properties().constant())?;
        }

        g.add_constant_properties(self.properties().constant())?;

        Ok(self.new_base_graph(g))
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
                    .filter(|&v| self.filter_node(v, layer_ids))
                    .count(),
                NodeList::List { nodes } => nodes
                    .par_iter()
                    .filter(|&&id| self.filter_node(core_nodes.node_ref(id), layer_ids))
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
                    .filter(|&e| {
                        self.filter_edge(e, self.layer_ids())
                            && self.filter_node(nodes.node_ref(e.src()), self.layer_ids())
                            && self.filter_node(nodes.node_ref(e.dst()), self.layer_ids())
                    })
                    .count()
            }
            FilterState::Nodes => {
                let edges = self.core_edges();
                let nodes = self.core_nodes();
                edges
                    .as_ref()
                    .par_iter(self.layer_ids().clone())
                    .filter(|&e| {
                        self.filter_node(nodes.node_ref(e.src()), self.layer_ids())
                            && self.filter_node(nodes.node_ref(e.dst()), self.layer_ids())
                    })
                    .count()
            }
            FilterState::Edges | FilterState::BothIndependent => {
                let edges = self.core_edges();
                edges
                    .as_ref()
                    .par_iter(self.layer_ids().clone())
                    .filter(|&e| self.filter_edge(e, self.layer_ids()))
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
                .map(|edge| self.edge_exploded_count(edge, layer_ids))
                .sum(),
            FilterState::Both => {
                let nodes = self.core_nodes();
                core_edges
                    .as_ref()
                    .par_iter(layer_ids.clone())
                    .filter(|&e| {
                        self.filter_edge(e, self.layer_ids())
                            && self.filter_node(nodes.node_ref(e.src()), self.layer_ids())
                            && self.filter_node(nodes.node_ref(e.dst()), self.layer_ids())
                    })
                    .map(|e| self.edge_exploded_count(e, layer_ids))
                    .sum()
            }
            FilterState::Nodes => {
                let nodes = self.core_nodes();
                core_edges
                    .as_ref()
                    .par_iter(layer_ids.clone())
                    .filter(|&e| {
                        self.filter_node(nodes.node_ref(e.src()), self.layer_ids())
                            && self.filter_node(nodes.node_ref(e.dst()), self.layer_ids())
                    })
                    .map(|e| self.edge_exploded_count(e, layer_ids))
                    .sum()
            }
            FilterState::Edges | FilterState::BothIndependent => core_edges
                .as_ref()
                .par_iter(layer_ids.clone())
                .filter(|&e| self.filter_edge(e, self.layer_ids()))
                .map(|e| self.edge_exploded_count(e, layer_ids))
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
    use crate::{db::api::view::StaticGraphViewOps, prelude::*};
    use tempfile::TempDir;

    #[test]
    fn test_exploded_edges() {
        let graph: Graph = Graph::new();
        graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        graph.add_edge(1, 0, 1, NO_PROPS, None).unwrap();
        graph.add_edge(2, 0, 1, NO_PROPS, None).unwrap();
        graph.add_edge(3, 0, 1, NO_PROPS, None).unwrap();

        let test_dir = TempDir::new().unwrap();
        #[cfg(feature = "arrow")]
        let arrow_graph = graph.persist_as_arrow(test_dir.path()).unwrap();

        fn test<G: StaticGraphViewOps>(graph: &G) {
            assert_eq!(graph.count_temporal_edges(), 4)
        }
        test(&graph);
        #[cfg(feature = "arrow")]
        test(&arrow_graph);
    }
}

#[cfg(test)]
mod test_materialize {
    use crate::{
        core::OptionAsStr,
        db::api::view::{internal::CoreGraphOps, StaticGraphViewOps},
        prelude::*,
    };
    use tempfile::TempDir;

    #[test]
    fn test_materialize() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, [("layer1", "1")], Some("1")).unwrap();
        g.add_edge(0, 1, 2, [("layer2", "2")], Some("2")).unwrap();

        let gm = g.materialize().unwrap();
        assert!(gm
            .nodes()
            .name()
            .collect::<Vec<String>>()
            .eq(&vec!["1", "2"]));

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
            nodes_subgraph.nodes().name().collect::<Vec<String>>(),
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
                .collect::<Vec<String>>(),
            vec!["1", "2", "3"]
        );
    }

    #[test]
    fn testing_node_types() {
        let graph = Graph::new();
        graph.add_node(0, "A", NO_PROPS, None).unwrap();
        graph.add_node(1, "B", NO_PROPS, Some(&"H")).unwrap();
        let test_dir = TempDir::new().unwrap();
        #[cfg(feature = "arrow")]
        let arrow_graph = graph.persist_as_arrow(test_dir.path()).unwrap();
        fn test<G: StaticGraphViewOps>(graph: &G) {
            let node_a = graph.node("A").unwrap();
            let node_b = graph.node("B").unwrap();
            let node_a_type = node_a.node_type();
            let node_a_type_str = node_a_type.as_str();

            assert_eq!(node_a_type_str, None);
            assert_eq!(node_b.node_type().as_str(), Some("H"));
        }
        test(&graph);
        // FIXME: Node types not yet supported (Issue #51)
        // test(&arrow_graph);

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
