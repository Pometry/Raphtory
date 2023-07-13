use crate::db::api::properties::TemporalProperties;
use crate::db::api::properties::{Properties, StaticProperties};
use crate::{
    core::{
        entities::{graph::tgraph::InnerTemporalGraph, vertices::vertex_ref::VertexRef, VID},
        utils::{errors::GraphError, time::IntoTime},
    },
    db::{
        api::{
            mutation::{AdditionOps, PropertyAdditionOps},
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
    fn has_edge<T: Into<VertexRef>>(&self, src: T, dst: T, layer: Option<&str>) -> bool;

    /// Get a vertex `v`.
    fn vertex<T: Into<VertexRef>>(&self, v: T) -> Option<VertexView<Self>>;

    /// Return a View of the vertices in the Graph
    fn vertices(&self) -> Vertices<Self>;

    /// Get an edge `(src, dst)`.
    fn edge<T: Into<VertexRef>>(
        &self,
        src: T,
        dst: T,
        layer: Option<&str>,
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
            .filter(|id| *id != 0) // the default layer has no name
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
        self.edges_len(None)
    }

    fn has_vertex<T: Into<VertexRef>>(&self, v: T) -> bool {
        self.has_vertex_ref(v.into())
    }

    fn has_edge<T: Into<VertexRef>>(&self, src: T, dst: T, layer: Option<&str>) -> bool {
        match self.get_layer_id(layer) {
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

    fn edge<T: Into<VertexRef>>(
        &self,
        src: T,
        dst: T,
        layer: Option<&str>,
    ) -> Option<EdgeView<Self>> {
        let layer_id = match layer {
            Some(_) => self.get_layer_id(layer)?,
            None => {
                let layers = self.get_unique_layers_internal();
                match layers[..] {
                    [layer_id] => layer_id, // if only one layer we search the edge there
                    _ => 0,                 // if more than one, we point to the default one
                }
            }
        };
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
        let g = InnerTemporalGraph::default();
        // Add edges first so we definitely have all associated vertices (important in case of persistent edges)
        for e in self.edges() {
            let layer_name = &e.layer_name().to_string();
            let mut layer: Option<&str> = None;
            if layer_name != "default layer" {
                layer = Some(layer_name)
            }
            for ee in e.explode() {
                g.add_edge(
                    ee.time().unwrap(),
                    ee.src().id(),
                    ee.dst().id(),
                    ee.properties().temporal().collect_properties(),
                    layer,
                )?;
            }
            if self.include_deletions() {
                for t in self.edge_deletion_history(e.edge) {
                    g.delete_edge(t, e.src().id(), e.dst().id(), layer)?;
                }
            }

            g.add_edge_properties(e.src().id(), e.dst().id(), e.properties().meta(), layer)?;
        }

        for v in self.vertices().iter() {
            for h in v.history() {
                g.add_vertex(h, v.id(), NO_PROPS)?;
            }
            for (name, prop_view) in v.properties().temporal().iter() {
                for (t, prop) in prop_view.iter() {
                    g.add_vertex(t, v.id(), [(name.clone(), prop)])?;
                }
            }
            g.add_vertex_properties(v.id(), v.properties().meta())?;
        }

        g.add_static_properties(self.properties().meta())?;

        Ok(self.new_base_graph(g))
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
        LayeredGraph::new(self.clone(), 0)
    }

    fn layer(&self, name: &str) -> Option<Self::LayeredViewType> {
        let id = self.get_layer_id(Some(name))?;
        Some(LayeredGraph::new(self.clone(), id))
    }
}
