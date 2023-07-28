use crate::{
    core::{
        entities::{
            graph::tgraph::InnerTemporalGraph, vertices::vertex_ref::VertexRef, LayerIds, VID,
        },
        utils::{errors::GraphError, time::IntoTime},
    },
    db::{
        api::{
            mutation::{AdditionOps, PropertyAdditionOps},
            properties::Properties,
            view::{internal::*, layer::LayerOps, *},
        },
        graph::{
            edge::EdgeView,
            vertex::VertexView,
            vertices::Vertices,
            views::{
                layer_graph::LayeredGraph, vertex_subgraph::VertexSubgraph,
                window_graph::WindowedGraph,
            },
        },
    },
    prelude::NO_PROPS,
};
use itertools::Itertools;
use rustc_hash::FxHashSet;

/// This trait GraphViewOps defines operations for accessing
/// information about a graph. The trait has associated types
/// that are used to define the type of the vertices, edges
/// and the corresponding iterators.
pub trait GraphViewOps: BoxableGraphView + Clone + Sized {
    fn subgraph<I: IntoIterator<Item = V>, V: Into<VertexRef>>(
        &self,
        vertices: I,
    ) -> VertexSubgraph<Self>;
    /// Return all the layer ids in the graph
    fn get_unique_layers(&self) -> Vec<String>;
    /// Timestamp of earliest activity in the graph
    fn earliest_time(&self) -> Option<i64>;
    /// Timestamp of latest activity in the graph
    fn latest_time(&self) -> Option<i64>;
    /// Return the number of vertices in the graph.
    fn num_vertices(&self) -> usize;

    /// Check if the graph is empty.
    fn is_empty(&self) -> bool {
        self.num_vertices() == 0
    }

    /// Return the number of edges in the graph.
    fn num_edges(&self) -> usize;

    /// Check if the graph contains a vertex `v`.
    fn has_vertex<T: Into<VertexRef>>(&self, v: T) -> bool;

    /// Check if the graph contains an edge given a pair of vertices `(src, dst)`.
    fn has_edge<T: Into<VertexRef>, L: Into<Layer>>(&self, src: T, dst: T, layer: L) -> bool;

    /// Get a vertex `v`.
    fn vertex<T: Into<VertexRef>>(&self, v: T) -> Option<VertexView<Self>>;

    /// Return a View of the vertices in the Graph
    fn vertices(&self) -> Vertices<Self>;

    /// Get an edge `(src, dst)`.
    fn edge<T: Into<VertexRef>, L: Into<Layer>>(
        &self,
        src: T,
        dst: T,
        layer: L,
    ) -> Option<EdgeView<Self>>;

    /// Return an iterator over all edges in the graph.
    fn edges(&self) -> Box<dyn Iterator<Item = EdgeView<Self>> + Send>;

    /// Get all property values of this graph.
    ///
    /// # Returns
    ///
    /// A view of the properties of the graph
    fn properties(&self) -> Properties<Self>;

    /// Get a graph clone
    ///
    /// # Arguments
    ///
    /// # Returns
    /// Graph - Returns clone of the graph
    fn materialize(&self) -> Result<MaterializedGraph, GraphError>;
}

impl<G: BoxableGraphView + Sized + Clone> GraphViewOps for G {
    fn subgraph<I: IntoIterator<Item = V>, V: Into<VertexRef>>(
        &self,
        vertices: I,
    ) -> VertexSubgraph<G> {
        let vertices: FxHashSet<VID> = vertices
            .into_iter()
            .flat_map(|v| self.local_vertex_ref(v.into()))
            .collect();
        VertexSubgraph::new(self.clone(), vertices)
    }

    /// Return all the layer ids in the graph
    fn get_unique_layers(&self) -> Vec<String> {
        self.get_unique_layers_internal()
            .into_iter()
            .map(|id| self.get_layer_name_by_id(id))
            .collect_vec()
    }

    fn earliest_time(&self) -> Option<i64> {
        self.earliest_time_global()
    }

    fn latest_time(&self) -> Option<i64> {
        self.latest_time_global()
    }

    fn num_vertices(&self) -> usize {
        self.vertices_len()
    }

    fn num_edges(&self) -> usize {
        self.edges_len(LayerIds::All)
    }

    fn has_vertex<T: Into<VertexRef>>(&self, v: T) -> bool {
        self.has_vertex_ref(v.into())
    }

    fn has_edge<T: Into<VertexRef>, L: Into<Layer>>(&self, src: T, dst: T, layer: L) -> bool {
        match self.get_layer_id(layer.into()) {
            Some(layer_id) => self.has_edge_ref(src.into(), dst.into(), layer_id),
            None => false,
        }
    }

    fn vertex<T: Into<VertexRef>>(&self, v: T) -> Option<VertexView<Self>> {
        let v = v.into();
        self.local_vertex_ref(v)
            .map(|v| VertexView::new_local(self.clone(), v))
    }

    fn vertices(&self) -> Vertices<Self> {
        let graph = self.clone();
        Vertices::new(graph)
    }

    fn edge<T: Into<VertexRef>, L: Into<Layer>>(
        &self,
        src: T,
        dst: T,
        layer: L,
    ) -> Option<EdgeView<Self>> {
        let layer_id = self.get_layer_id(layer.into())?;

        self.edge_ref(src.into(), dst.into(), layer_id)
            .map(|e| EdgeView::new(self.clone(), e))
    }

    fn edges(&self) -> Box<dyn Iterator<Item = EdgeView<Self>> + Send> {
        Box::new(self.vertices().iter().flat_map(|v| v.out_edges()))
    }

    fn properties(&self) -> Properties<Self> {
        Properties::new(self.clone())
    }

    fn materialize(&self) -> Result<MaterializedGraph, GraphError> {
        todo!()
        // let g = InnerTemporalGraph::default();
        // // Add edges first so we definitely have all associated vertices (important in case of persistent edges)
        // for e in self.edges() {
        //     // FIXME: this needs to be verified
        //     for ee in e.explode_layers().explode() {
        //         g.add_edge(
        //             ee.time().unwrap(),
        //             ee.src().id(),
        //             ee.dst().id(),
        //             ee.properties().temporal().collect_properties(),
        //             layer,
        //         )?;
        //     }
        //     if self.include_deletions() {
        //         for t in self.edge_deletion_history(e.edge, LayerIds::All) {
        //             for layer in layer_names.iter() {
        //                 g.delete_edge(t, e.src().id(), e.dst().id(), Some(layer))?;
        //             }
        //         }
        //     }
        //
        //     g.add_edge_properties(e.src().id(), e.dst().id(), e.properties().constant(), layer)?;
        // }
        //
        // for v in self.vertices().iter() {
        //     for h in v.history() {
        //         g.add_vertex(h, v.id(), NO_PROPS)?;
        //     }
        //     for (name, prop_view) in v.properties().temporal().iter() {
        //         for (t, prop) in prop_view.iter() {
        //             g.add_vertex(t, v.id(), [(name.clone(), prop)])?;
        //         }
        //     }
        //     g.add_vertex_properties(v.id(), v.properties().constant())?;
        // }
        //
        // g.add_static_properties(self.properties().constant())?;
        //
        // Ok(self.new_base_graph(g))
    }
}

impl<G: GraphViewOps> TimeOps for G {
    type WindowedViewType = WindowedGraph<Self>;

    fn start(&self) -> Option<i64> {
        self.view_start()
    }

    fn end(&self) -> Option<i64> {
        self.view_end()
    }

    fn window<T: IntoTime>(&self, t_start: T, t_end: T) -> WindowedGraph<Self> {
        WindowedGraph::new(self.clone(), t_start, t_end)
    }
}

impl<G: GraphViewOps> LayerOps for G {
    type LayeredViewType = LayeredGraph<G>;

    fn default_layer(&self) -> Self::LayeredViewType {
        LayeredGraph::new(self.clone(), 0.into())
    }

    fn layer<L: Into<Layer>>(&self, layers: L) -> Option<Self::LayeredViewType> {
        let layers = layers.into();
        let ids = self.get_layer_id(layers)?;
        Some(LayeredGraph::new(self.clone(), ids))
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
        assert!(!g
            .layer("2")
            .unwrap()
            .edge(1, 2, Layer::All)
            .unwrap()
            .has_property("layer1", false));
        assert!(!gm
            .into_events()
            .unwrap()
            .layer("2")
            .unwrap()
            .edge(1, 2, Layer::All)
            .unwrap()
            .has_property("layer1", false));
    }
}
