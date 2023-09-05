//! Defines the `Edge` struct, which represents an edge in the graph.
//!
//! Edges are used to define directed connections between verticies in the graph.
//! Edges are identified by a unique ID, can have a direction (Ingoing, Outgoing, or Both)
//! and can have properties associated with them.
//!

use super::views::layer_graph::LayeredGraph;
use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, VID},
        storage::{locked_view::LockedView, timeindex::TimeIndexEntry},
        utils::{
            errors::{GraphError, MutateGraphError},
            time::IntoTime,
        },
    },
    db::{
        api::{
            mutation::{
                internal::{InternalAdditionOps, InternalPropertyAdditionOps},
                CollectProperties, TryIntoInputTime,
            },
            properties::{
                internal::{
                    ConstPropertiesOps, Key, TemporalPropertiesOps, TemporalPropertyViewOps,
                },
                Properties,
            },
            view::{internal::Static, BoxedIter, EdgeViewInternalOps, LayerOps},
        },
        graph::{vertex::VertexView, views::window_graph::WindowedGraph},
    },
    prelude::*,
};
use std::{
    fmt::{Debug, Formatter},
    iter,
};

/// A view of an edge in the graph.
#[derive(Clone)]
pub struct EdgeView<G: GraphViewOps> {
    /// A view of an edge in the graph.
    pub graph: G,
    /// A reference to the edge.
    pub edge: EdgeRef,
}

impl<G: GraphViewOps> Static for EdgeView<G> {}

impl<G: GraphViewOps> EdgeView<G> {
    pub fn new(graph: G, edge: EdgeRef) -> Self {
        Self { graph, edge }
    }
}

impl<G: GraphViewOps + InternalAdditionOps> EdgeView<G> {
    fn resolve_layer(&self, layer: Option<&str>) -> Result<usize, GraphError> {
        match layer {
            Some(name) => match self.edge.layer() {
                Some(l_id) => self
                    .graph
                    .get_layer_id(name)
                    .filter(|id| id == l_id)
                    .ok_or_else(|| GraphError::InvalidLayer(name.to_owned())),
                None => Ok(self.graph.resolve_layer(layer)),
            },
            None => Ok(self.edge.layer().copied().unwrap_or(0)),
        }
    }
}

impl<G: GraphViewOps> PartialEq for EdgeView<G> {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

impl<G: GraphViewOps> EdgeViewInternalOps<G, VertexView<G>> for EdgeView<G> {
    fn graph(&self) -> G {
        self.graph.clone()
    }

    fn eref(&self) -> EdgeRef {
        self.edge
    }

    fn new_vertex(&self, v: VID) -> VertexView<G> {
        VertexView::new_internal(self.graph(), v)
    }

    fn new_edge(&self, e: EdgeRef) -> Self {
        Self {
            graph: self.graph(),
            edge: e,
        }
    }
}

impl<G: GraphViewOps + InternalPropertyAdditionOps + InternalAdditionOps> EdgeView<G> {
    /// Add constant properties for the edge
    ///
    /// # Arguments
    ///
    ///     props: Property key-value pairs to add
    ///     layer: The layer to which properties should be added. If the edge view is restricted to a
    ///            single layer, `None` will add the properties to that layer and `Some("name")`
    ///            fails unless the layer matches the edge view. If the edge view is not restricted
    ///            to a single layer, `None` sets the properties on the default layer and `Some("name")`
    ///            sets the properties on layer `"name"` and fails if that layer doesn't exist.
    pub fn add_constant_properties<C: CollectProperties>(
        &self,
        props: C,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let props = props.collect_properties();
        let input_layer_id = self.resolve_layer(layer)?;

        self.graph
            .internal_add_edge_properties(self.edge.pid(), props, input_layer_id)
            .map_err(|err| {
                MutateGraphError::IllegalEdgePropertyChange {
                    src_id: self.src().id(),
                    dst_id: self.dst().id(),
                    source: err,
                }
                .into()
            })
    }

    pub fn add_updates<C: CollectProperties, T: TryIntoInputTime>(
        &self,
        time: T,
        props: C,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let t = TimeIndexEntry::from_input(&self.graph, time)?;
        let layer_id = self.resolve_layer(layer)?;

        self.graph.internal_add_edge(
            t,
            self.src().id(),
            self.dst().id(),
            props.collect_properties(),
            layer_id,
        )?;
        Ok(())
    }
}

impl<G: GraphViewOps> ConstPropertiesOps for EdgeView<G> {
    fn const_property_keys<'a>(&'a self) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a> {
        let layer_ids = self.graph.layer_ids().constrain_from_edge(self.edge);
        self.graph.static_edge_prop_names(self.edge, layer_ids)
    }

    fn get_const_property(&self, key: &str) -> Option<Prop> {
        let layer_ids = self.graph.layer_ids().constrain_from_edge(self.edge);
        self.graph.static_edge_prop(self.edge, key, layer_ids)
    }
}

impl<G: GraphViewOps> TemporalPropertyViewOps for EdgeView<G> {
    fn temporal_history(&self, id: &Key) -> Vec<i64> {
        let layer_ids = self.graph.layer_ids().constrain_from_edge(self.edge);
        self.graph
            .temporal_edge_prop_vec(self.edge, id, layer_ids)
            .into_iter()
            .map(|(t, _)| t)
            .collect()
    }

    fn temporal_values(&self, id: &String) -> Vec<Prop> {
        let layer_ids = self.graph.layer_ids().constrain_from_edge(self.edge);
        self.graph
            .temporal_edge_prop_vec(self.edge, id, layer_ids)
            .into_iter()
            .map(|(_, v)| v)
            .collect()
    }
}

impl<G: GraphViewOps> TemporalPropertiesOps for EdgeView<G> {
    fn temporal_property_keys<'a>(
        &'a self,
    ) -> Box<(dyn Iterator<Item = LockedView<'a, String>> + 'a)> {
        Box::new(
            self.graph
                .temporal_edge_prop_names(self.edge, self.graph.layer_ids())
                .filter(|k| self.get_temporal_property(k).is_some()),
        )
    }

    fn get_temporal_property(&self, key: &str) -> Option<String> {
        let layer_ids = self.graph.layer_ids();
        (!self
            .graph
            .temporal_edge_prop_vec(self.edge, key, layer_ids)
            .is_empty())
        .then_some(key.to_owned())
    }
}

impl<G: GraphViewOps> EdgeViewOps for EdgeView<G> {
    type Graph = G;
    type Vertex = VertexView<G>;
    type EList = BoxedIter<Self>;

    fn explode(&self) -> Self::EList {
        let ev = self.clone();
        match self.edge.time() {
            Some(_) => Box::new(iter::once(ev)),
            None => {
                let layer_ids = self.graph.layer_ids().constrain_from_edge(self.edge);
                let e = self.edge;
                let ex_iter = self.graph.edge_exploded(e, layer_ids);
                // FIXME: use duration
                Box::new(ex_iter.map(move |ex| ev.new_edge(ex)))
            }
        }
    }

    fn explode_layers(&self) -> Self::EList {
        let ev = self.clone();
        match self.edge.layer() {
            Some(_) => Box::new(iter::once(ev)),
            None => {
                let e = self.edge;
                let ex_iter = self.graph.edge_layers(e, self.graph.layer_ids());
                Box::new(ex_iter.map(move |ex| ev.new_edge(ex)))
            }
        }
    }
}

impl<G: GraphViewOps> Debug for EdgeView<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EdgeView({}, {})",
            self.graph.vertex(self.edge.src()).unwrap().id(),
            self.graph.vertex(self.edge.dst()).unwrap().id()
        )
    }
}

impl<G: GraphViewOps> From<EdgeView<G>> for EdgeRef {
    fn from(value: EdgeView<G>) -> Self {
        value.edge
    }
}

impl<G: GraphViewOps> TimeOps for EdgeView<G> {
    type WindowedViewType = EdgeView<WindowedGraph<G>>;

    fn start(&self) -> Option<i64> {
        self.graph.start()
    }

    fn end(&self) -> Option<i64> {
        self.graph.end()
    }

    fn window<T: IntoTime>(&self, t_start: T, t_end: T) -> Self::WindowedViewType {
        EdgeView {
            graph: self.graph.window(t_start, t_end),
            edge: self.edge,
        }
    }
}

impl<G: GraphViewOps> LayerOps for EdgeView<G> {
    type LayeredViewType = EdgeView<LayeredGraph<G>>;

    fn default_layer(&self) -> Self::LayeredViewType {
        EdgeView {
            graph: self.graph.default_layer(),
            edge: self.edge,
        }
    }

    fn layer<L: Into<Layer>>(&self, name: L) -> Option<Self::LayeredViewType> {
        let layer_ids = self
            .graph
            .layer_ids_from_names(name.into())
            .constrain_from_edge(self.edge);
        self.graph
            .has_edge_ref(
                self.edge.src(),
                self.edge.dst(),
                &layer_ids,
                self.graph.edge_filter(),
            )
            .then(|| EdgeView {
                graph: LayeredGraph::new(self.graph.clone(), layer_ids),
                edge: self.edge,
            })
    }
}

/// Implement `EdgeListOps` trait for an iterator of `EdgeView` objects.
///
/// This implementation enables the use of the `src` and `dst` methods to retrieve the vertices
/// connected to the edges inside the iterator.
impl<G: GraphViewOps> EdgeListOps for BoxedIter<EdgeView<G>> {
    type Graph = G;
    type Vertex = VertexView<G>;
    type Edge = EdgeView<G>;
    type ValueType<T> = T;

    /// Specifies the associated type for an iterator over vertices.
    type VList = Box<dyn Iterator<Item = VertexView<G>> + Send>;

    /// Specifies the associated type for the iterator over edges.
    type IterType<T> = Box<dyn Iterator<Item = T> + Send>;

    fn properties(self) -> Self::IterType<Properties<Self::Edge>> {
        Box::new(self.map(move |e| e.properties()))
    }

    /// Returns an iterator over the source vertices of the edges in the iterator.
    fn src(self) -> Self::VList {
        Box::new(self.map(|e| e.src()))
    }

    /// Returns an iterator over the destination vertices of the edges in the iterator.
    fn dst(self) -> Self::VList {
        Box::new(self.map(|e| e.dst()))
    }

    fn id(self) -> Self::IterType<(u64, u64)> {
        Box::new(self.map(|e| e.id()))
    }

    /// returns an iterator of exploded edges that include an edge at each point in time
    fn explode(self) -> Self {
        Box::new(self.flat_map(move |e| e.explode()))
    }

    /// Gets the earliest times of a list of edges
    fn earliest_time(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|e| e.earliest_time()))
    }

    /// Gets the latest times of a list of edges
    fn latest_time(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|e| e.latest_time()))
    }
}

impl<G: GraphViewOps> EdgeListOps for BoxedIter<BoxedIter<EdgeView<G>>> {
    type Graph = G;
    type Vertex = VertexView<G>;
    type Edge = EdgeView<G>;
    type ValueType<T> = Box<dyn Iterator<Item = T> + Send>;
    type VList = Box<dyn Iterator<Item = Box<dyn Iterator<Item = VertexView<G>> + Send>> + Send>;
    type IterType<T> = Box<dyn Iterator<Item = Box<dyn Iterator<Item = T> + Send>> + Send>;

    fn properties(self) -> Self::IterType<Properties<Self::Edge>> {
        Box::new(self.map(move |it| it.properties()))
    }

    fn src(self) -> Self::VList {
        Box::new(self.map(|it| it.src()))
    }

    fn dst(self) -> Self::VList {
        Box::new(self.map(|it| it.dst()))
    }

    fn id(self) -> Self::IterType<(u64, u64)> {
        Box::new(self.map(|it| it.id()))
    }

    fn explode(self) -> Self {
        Box::new(self.map(move |it| it.explode()))
    }

    /// Gets the earliest times of a list of edges
    fn earliest_time(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|e| e.earliest_time()))
    }

    /// Gets the latest times of a list of edges
    fn latest_time(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|e| e.latest_time()))
    }
}

pub type EdgeList<G> = Box<dyn Iterator<Item = EdgeView<G>> + Send>;

#[cfg(test)]
mod test_edge {
    use crate::{core::IntoPropMap, prelude::*};
    use itertools::Itertools;
    use std::collections::HashMap;

    #[test]
    fn test_properties() {
        let g = Graph::new();
        let props = [("test".to_string(), "test".into_prop())];
        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.add_edge(2, 1, 2, props.clone(), None).unwrap();

        let e1 = g.edge(1, 2).unwrap();
        let e1_w = g.window(0, 1).edge(1, 2).unwrap();
        assert_eq!(HashMap::from_iter(e1.properties().as_vec()), props.into());
        assert!(e1_w.properties().as_vec().is_empty())
    }

    #[test]
    fn test_constant_properties() {
        let g = Graph::new();
        g.add_edge(1, 1, 2, NO_PROPS, Some("layer 1"))
            .unwrap()
            .add_constant_properties([("test_prop", "test_val")], Some("layer 1"))
            .unwrap();
        g.add_edge(1, 2, 3, NO_PROPS, Some("layer 2"))
            .unwrap()
            .add_constant_properties([("test_prop", "test_val")], Some("layer 2"))
            .unwrap();

        assert_eq!(
            g.edge(1, 2)
                .unwrap()
                .properties()
                .constant()
                .get("test_prop"),
            Some([("layer 1", "test_val")].into_prop_map())
        );
        assert_eq!(
            g.edge(2, 3)
                .unwrap()
                .properties()
                .constant()
                .get("test_prop"),
            Some([("layer 2", "test_val")].into_prop_map())
        );
        for e in g.edges() {
            for ee in e.explode() {
                assert_eq!(
                    ee.properties().constant().get("test_prop"),
                    Some("test_val".into())
                )
            }
        }
    }

    #[test]
    fn test_property_additions() {
        let g = Graph::new();
        let props = [("test", "test")];
        let e1 = g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        e1.add_updates(2, props, None).unwrap(); // same layer works
        assert!(e1.add_updates(2, props, Some("test2")).is_err()); // different layer is error
        let e = g.edge(1, 2).unwrap();
        e.add_updates(2, props, Some("test2")).unwrap(); // non-restricted edge view can create new layers
        let layered_views = e.explode_layers().collect_vec();
        for ev in layered_views {
            let layer_names = ev.layer_names();
            let layer = layer_names[0].as_str();
            assert!(ev.add_updates(1, props, Some("test")).is_err()); // restricted edge view cannot create updates in different layer
            ev.add_updates(1, [("test2", layer)], None).unwrap() // this will add an update to the same layer as the view (not the default layer)
        }
        let e1_w = e1.window(0, 1);
        assert_eq!(
            e1.properties().as_map(),
            props
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.into_prop()))
                .chain([("test2".to_string(), "_default".into_prop())])
                .collect()
        );
        assert_eq!(e1_w.properties().as_map(), HashMap::default())
    }

    #[test]
    fn test_constant_property_additions() {
        let g = Graph::new();
        let e = g.add_edge(0, 1, 2, NO_PROPS, Some("test")).unwrap();
        assert!(e.add_constant_properties([("test", "test")], None).is_err());
        assert!(e
            .add_constant_properties([("test", "test")], Some("test2"))
            .is_err());
        e.add_constant_properties([("test", "test")], Some("test"))
            .unwrap();
        assert_eq!(e.properties().get("test"), Some("test".into()))
    }
}
