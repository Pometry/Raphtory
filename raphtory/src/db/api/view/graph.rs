use crate::{
    core::{
        entities::{graph::tgraph::InnerTemporalGraph, vertices::vertex_ref::VertexRef, VID},
        utils::{errors::GraphError, time::IntoTime},
        Prop,
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
};
use itertools::Itertools;
use rustc_hash::FxHashSet;
use std::collections::HashMap;

/// This trait GraphViewOps defines operations for accessing
/// information about a graph. The trait has associated types
/// that are used to define the type of the vertices, edges
/// and the corresponding iterators.
pub trait GraphViewOps: BoxableGraphView + Clone + Sized {
    fn subgraph<I: IntoIterator<Item = V>, V: Into<VertexRef>>(
        &self,
        vertices: I,
    ) -> VertexSubgraph<Self>;
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

    /// Gets the property value of this graph given the name of the property.
    fn property(&self, name: &str, include_static: bool) -> Option<Prop>;

    /// Get the temporal property value of this graph.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to retrieve.
    ///
    /// # Returns
    ///
    /// A vector of `(i64, Prop)` tuples where the `i64` value is the timestamp of the
    /// property value and `Prop` is the value itself.
    fn property_history(&self, name: &str) -> Vec<(i64, Prop)>;

    /// Get all property values of this graph.
    ///
    /// # Arguments
    ///
    /// * `include_static` - If `true` then static properties are included in the result.
    ///
    /// # Returns
    ///
    /// A HashMap with the names of the properties as keys and the property values as values.
    fn properties(&self, include_static: bool) -> HashMap<String, Prop>;

    /// Get all temporal property values of this graph.
    ///
    /// # Returns
    ///
    /// A HashMap with the names of the properties as keys and a vector of `(i64, Prop)` tuples
    /// as values. The `i64` value is the timestamp of the property value and `Prop`
    /// is the value itself.
    fn property_histories(&self) -> HashMap<String, Vec<(i64, Prop)>>;

    /// Get the names of all properties of this graph.
    ///
    /// # Arguments
    ///
    /// * `include_static` - If `true` then static properties are included in the result.
    ///
    /// # Returns
    ///
    /// A vector of the names of the properties of this vertex.
    fn property_names(&self, include_static: bool) -> Vec<String>;

    /// Checks if a property exists on this graph.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to check for.
    /// * `include_static` - If `true` then static properties are included in the result.
    ///
    /// # Returns
    ///
    /// `true` if the property exists, otherwise `false`.
    fn has_property(&self, name: &str, include_static: bool) -> bool;

    /// Checks if a static property exists on this graph.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to check for.
    ///
    /// # Returns
    ///
    /// `true` if the property exists, otherwise `false`.
    fn has_static_property(&self, name: &str) -> bool;

    /// Get the static property value of this graph.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to retrieve.
    ///
    /// # Returns
    ///
    /// The value of the property if it exists, otherwise `None`.
    fn static_property(&self, name: &str) -> Option<Prop>;

    /// Get the static properties value of this graph.
    ///
    /// # Arguments
    ///
    /// # Returns
    ///
    /// HashMap<String, Prop> - Return all static properties identified by their names
    fn static_properties(&self) -> HashMap<String, Prop>;

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

    fn property(&self, name: &str, include_static: bool) -> Option<Prop> {
        let props = self.property_history(name);
        match props.last() {
            None => {
                if include_static {
                    self.static_prop(name)
                } else {
                    None
                }
            }
            Some((_, prop)) => Some(prop.clone()),
        }
    }

    fn property_history(&self, name: &str) -> Vec<(i64, Prop)> {
        self.temporal_prop_vec(name)
    }

    fn properties(&self, include_static: bool) -> HashMap<String, Prop> {
        let mut props: HashMap<String, Prop> = self
            .property_histories()
            .iter()
            .map(|(key, values)| (key.clone(), values.last().unwrap().1.clone()))
            .collect();

        if include_static {
            for prop_name in self.static_prop_names() {
                if let Some(prop) = self.static_prop(&prop_name) {
                    props.insert(prop_name, prop);
                }
            }
        }
        props
    }

    fn property_histories(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        self.temporal_props()
    }

    fn property_names(&self, include_static: bool) -> Vec<String> {
        let mut names: Vec<String> = self.temporal_prop_names();
        if include_static {
            names.extend(self.static_prop_names())
        }
        names
    }

    fn has_property(&self, name: &str, include_static: bool) -> bool {
        (!self.property_history(name).is_empty())
            || (include_static && self.static_prop_names().contains(&name.to_owned()))
    }

    fn has_static_property(&self, name: &str) -> bool {
        self.static_prop_names().contains(&name.to_owned())
    }

    fn static_property(&self, name: &str) -> Option<Prop> {
        self.static_prop(name)
    }

    fn static_properties(&self) -> HashMap<String, Prop> {
        self.static_props()
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
                    ee.properties(false),
                    layer,
                )?;
            }
            if self.include_deletions() {
                for t in self.edge_deletion_history(e.edge) {
                    g.delete_edge(t, e.src().id(), e.dst().id(), layer)?;
                }
            }

            g.add_edge_properties(
                e.src().id(),
                e.dst().id(),
                e.static_properties().pairs(),
                layer,
            )?;
        }

        for v in self.vertices().iter() {
            for h in v.history() {
                g.add_vertex(h, v.id(), [])?;
            }
            for (name, prop_view) in v.properties().pairs() {
                for (t, prop) in prop_view.pairs() {
                    g.add_vertex(t, v.id(), [(name.clone(), prop)])?;
                }
            }
            g.add_vertex_properties(v.id(), v.static_properties().pairs())?;
        }

        g.add_static_properties(self.static_properties())?;

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
