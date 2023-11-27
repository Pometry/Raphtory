use crate::{
    core::{
        entities::{
            graph::tgraph::InnerTemporalGraph, vertices::vertex_ref::VertexRef, LayerIds, VID,
        },
        utils::errors::GraphError,
        ArcStr,
    },
    db::{
        api::{
            mutation::{AdditionOps, PropertyAdditionOps},
            properties::Properties,
            view::{internal::*, *},
        },
        graph::{
            edge::EdgeView, vertex::VertexView, vertices::Vertices,
            views::vertex_subgraph::VertexSubgraph,
        },
    },
    prelude::{DeletionOps, NO_PROPS},
};
use rustc_hash::FxHashSet;

/// This trait GraphViewOps defines operations for accessing
/// information about a graph. The trait has associated types
/// that are used to define the type of the vertices, edges
/// and the corresponding iterators.
///
pub trait GraphViewOps<'graph>: BoxableGraphView<'graph> + Sized + Clone + 'graph {
    /// Return an iterator over all edges in the graph.
    fn edges(&self) -> Box<dyn Iterator<Item = EdgeView<Self, Self>> + Send + 'graph>;

    /// Return a View of the vertices in the Graph
    fn vertices(&self) -> Vertices<'graph, Self, Self>;

    /// Get a graph clone
    ///
    /// # Arguments
    ///
    /// Returns:
    /// Graph - Returns clone of the graph
    fn materialize(&self) -> Result<MaterializedGraph, GraphError>;
    fn subgraph<I: IntoIterator<Item = V>, V: Into<VertexRef>>(
        &self,
        vertices: I,
    ) -> VertexSubgraph<Self>;
    /// Return all the layer ids in the graph
    fn unique_layers(&self) -> BoxedIter<ArcStr>;
    /// Timestamp of earliest activity in the graph
    fn earliest_time(&self) -> Option<i64>;
    /// Timestamp of latest activity in the graph
    fn latest_time(&self) -> Option<i64>;
    /// Return the number of vertices in the graph.
    fn count_vertices(&self) -> usize;

    /// Check if the graph is empty.
    fn is_empty(&self) -> bool {
        self.count_vertices() == 0
    }

    /// Return the number of edges in the graph.
    fn count_edges(&self) -> usize;

    // Return the number of temporal edges in the graph.
    fn count_temporal_edges(&self) -> usize;

    /// Check if the graph contains a vertex `v`.
    fn has_vertex<T: Into<VertexRef>>(&self, v: T) -> bool;

    /// Check if the graph contains an edge given a pair of vertices `(src, dst)`.
    fn has_edge<T: Into<VertexRef>, L: Into<Layer>>(&self, src: T, dst: T, layer: L) -> bool;

    /// Get a vertex `v`.
    fn vertex<T: Into<VertexRef>>(&self, v: T) -> Option<VertexView<Self, Self>>;

    /// Get an edge `(src, dst)`.
    fn edge<T: Into<VertexRef>>(&self, src: T, dst: T) -> Option<EdgeView<Self, Self>>;

    /// Get all property values of this graph.
    ///
    /// Returns:
    ///
    /// A view of the properties of the graph
    fn properties(&self) -> Properties<Self>;
}

impl<'graph, G: BoxableGraphView<'graph> + Sized + Clone + 'graph> GraphViewOps<'graph> for G {
    fn edges(&self) -> Box<dyn Iterator<Item = EdgeView<Self, Self>> + Send + 'graph> {
        let layer_ids = self.layer_ids();
        let edge_filter = self.edge_filter();
        let g = self.clone();
        self.edge_refs(layer_ids, edge_filter)
            .map(move |eref| EdgeView::new(g.clone(), eref))
            .into_dyn_boxed()
    }

    fn vertices(&self) -> Vertices<'graph, Self, Self> {
        let graph = self.clone();
        Vertices::new(graph)
    }
    fn materialize(&self) -> Result<MaterializedGraph, GraphError> {
        let g = InnerTemporalGraph::default();
        // Add edges first so we definitely have all associated vertices (important in case of persistent edges)
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
                    for t in self.edge_deletion_history(e.edge, layer_ids) {
                        g.delete_edge(t, e.src().id(), e.dst().id(), layer_name)?;
                    }
                }

                g.edge(ee.src().id(), ee.dst().id())
                    .expect("edge added")
                    .add_constant_properties(ee.properties().constant(), layer_name)?;
            }
        }

        for v in self.vertices().iter() {
            for h in v.history() {
                g.add_vertex(h, v.name(), NO_PROPS)?;
            }
            for (name, prop_view) in v.properties().temporal().iter() {
                for (t, prop) in prop_view.iter() {
                    g.add_vertex(t, v.name(), [(name.clone(), prop)])?;
                }
            }
            g.vertex(v.id())
                .expect("vertex added")
                .add_constant_properties(v.properties().constant())?;
        }

        g.add_constant_properties(self.properties().constant())?;

        Ok(self.new_base_graph(g))
    }
    fn subgraph<I: IntoIterator<Item = V>, V: Into<VertexRef>>(
        &self,
        vertices: I,
    ) -> VertexSubgraph<G> {
        let filter = self.edge_filter();
        let layer_ids = self.layer_ids();
        let vertices: FxHashSet<VID> = vertices
            .into_iter()
            .flat_map(|v| self.internal_vertex_ref(v.into(), &layer_ids, filter))
            .collect();
        VertexSubgraph::new(self.clone(), vertices)
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

    fn count_vertices(&self) -> usize {
        self.vertices_len(self.layer_ids(), self.edge_filter())
    }

    #[inline]
    fn count_edges(&self) -> usize {
        self.edges_len(self.layer_ids(), self.edge_filter())
    }

    fn count_temporal_edges(&self) -> usize {
        self.temporal_edges_len(self.layer_ids(), self.edge_filter())
    }

    fn has_vertex<T: Into<VertexRef>>(&self, v: T) -> bool {
        self.has_vertex_ref(v.into(), &self.layer_ids(), self.edge_filter())
    }

    fn has_edge<T: Into<VertexRef>, L: Into<Layer>>(&self, src: T, dst: T, layer: L) -> bool {
        let src_ref = src.into();
        let dst_ref = dst.into();
        let layers = self.layer_ids_from_names(layer.into());
        if let Some(src) = self.internalise_vertex(src_ref) {
            if let Some(dst) = self.internalise_vertex(dst_ref) {
                return self.has_edge_ref(src, dst, &layers, self.edge_filter());
            }
        }
        false
    }

    fn vertex<T: Into<VertexRef>>(&self, v: T) -> Option<VertexView<Self, Self>> {
        let v = v.into();
        self.internal_vertex_ref(v, &self.layer_ids(), self.edge_filter())
            .map(|v| VertexView::new_internal(self.clone(), v))
    }

    fn edge<T: Into<VertexRef>>(&self, src: T, dst: T) -> Option<EdgeView<Self, Self>> {
        let layer_ids = self.layer_ids();
        let edge_filter = self.edge_filter();
        if let Some(src) = self.internal_vertex_ref(src.into(), &layer_ids, edge_filter) {
            if let Some(dst) = self.internal_vertex_ref(dst.into(), &layer_ids, edge_filter) {
                return self
                    .edge_ref(src, dst, &layer_ids, edge_filter)
                    .map(|e| EdgeView::new(self.clone(), e));
            }
        }
        None
    }

    fn properties(&self) -> Properties<Self> {
        Properties::new(self.clone())
    }
}

pub trait StaticGraphViewOps: for<'graph> GraphViewOps<'graph> + 'static {}

impl<G: for<'graph> GraphViewOps<'graph> + 'static> StaticGraphViewOps for G {}

impl<'graph, G: GraphViewOps<'graph> + 'graph> OneHopFilter<'graph> for G {
    type Graph = G;
    type Filtered<GH: GraphViewOps<'graph> + 'graph> = GH;

    fn current_filter(&self) -> &Self::Graph {
        &self
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
    use crate::prelude::*;

    #[test]
    fn test_exploded_edges() {
        let g: Graph = Graph::new();
        g.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        g.add_edge(1, 0, 1, NO_PROPS, None).unwrap();
        g.add_edge(2, 0, 1, NO_PROPS, None).unwrap();
        g.add_edge(3, 0, 1, NO_PROPS, None).unwrap();

        assert_eq!(g.count_temporal_edges(), 4)
    }
}

#[cfg(test)]
mod test_materialize {
    use crate::prelude::*;

    #[test]
    fn test_materialize() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, [("layer1", "1")], Some("1")).unwrap();
        g.add_edge(0, 1, 2, [("layer2", "2")], Some("2")).unwrap();

        let gm = g.materialize().unwrap();
        assert!(gm
            .vertices()
            .name()
            .collect::<Vec<String>>()
            .eq(&vec!["1", "2"]));

        assert!(!g
            .layer("2")
            .unwrap()
            .edge(1, 2)
            .unwrap()
            .properties()
            .temporal()
            .contains("layer1"));
        assert!(!gm
            .into_events()
            .unwrap()
            .layer("2")
            .unwrap()
            .edge(1, 2)
            .unwrap()
            .properties()
            .temporal()
            .contains("layer1"));
    }

    #[test]
    fn changing_property_type_errors() {
        let g = Graph::new();
        let props_0 = [("test", Prop::U64(1))];
        let props_1 = [("test", Prop::F64(0.1))];
        g.add_properties(0, props_0.clone()).unwrap();
        assert!(g.add_properties(1, props_1.clone()).is_err());

        g.add_vertex(0, 1, props_0.clone()).unwrap();
        assert!(g.add_vertex(1, 1, props_1.clone()).is_err());

        g.add_edge(0, 1, 2, props_0.clone(), None).unwrap();
        assert!(g.add_edge(1, 1, 2, props_1.clone(), None).is_err());
    }
}
