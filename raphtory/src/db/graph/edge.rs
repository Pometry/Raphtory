//! Defines the `Edge` struct, which represents an edge in the graph.
//!
//! Edges are used to define directed connections between verticies in the graph.
//! Edges are identified by a unique ID, can have a direction (Ingoing, Outgoing, or Both)
//! and can have properties associated with them.
//!

use crate::db::api::properties::internal::{
    StaticProperties, StaticPropertiesOps, TemporalProperties, TemporalPropertiesOps,
    TemporalPropertyViewOps,
};
use crate::db::api::view::internal::{GraphPropertiesOps, Static};
use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, vertices::vertex_ref::VertexRef},
        utils::time::IntoTime,
    },
    db::{
        api::view::{BoxedIter, EdgeViewInternalOps},
        graph::{vertex::VertexView, views::window_graph::WindowedGraph},
    },
    prelude::*,
};
use std::{
    collections::HashMap,
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

impl<G: GraphViewOps> EdgeViewInternalOps<G, VertexView<G>> for EdgeView<G> {
    fn graph(&self) -> G {
        self.graph.clone()
    }

    fn eref(&self) -> EdgeRef {
        self.edge
    }

    fn new_vertex(&self, v: VertexRef) -> VertexView<G> {
        VertexView::new(self.graph(), v)
    }

    fn new_edge(&self, e: EdgeRef) -> Self {
        Self {
            graph: self.graph(),
            edge: e,
        }
    }
}

impl<G: GraphViewOps> StaticPropertiesOps for EdgeView<G> {
    fn static_property_keys(&self) -> Vec<String> {
        self.graph.static_edge_prop_names(self.edge)
    }

    fn get_static_property(&self, key: &str) -> Option<Prop> {
        self.graph.static_edge_prop(self.edge, key)
    }
}

impl<G: GraphViewOps> TemporalPropertyViewOps for EdgeView<G> {
    fn temporal_history(&self, id: &String) -> Vec<i64> {
        self.graph
            .temporal_edge_prop_vec(self.edge, id)
            .into_iter()
            .map(|(t, _)| t)
            .collect()
    }

    fn temporal_values(&self, id: &String) -> Vec<Prop> {
        self.graph
            .temporal_edge_prop_vec(self.edge, id)
            .into_iter()
            .map(|(_, v)| v)
            .collect()
    }
}

impl<G: GraphViewOps> TemporalPropertiesOps for EdgeView<G> {
    fn temporal_property_keys(&self) -> Vec<String> {
        self.graph.temporal_edge_prop_names(self.edge)
    }

    fn get_temporal_property(&self, key: &str) -> Option<String> {
        (!self.graph.temporal_edge_prop_vec(self.edge, key).is_empty()).then_some(key.to_owned())
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
                let e = self.edge;
                let ex_iter = self.graph.edge_t(e);
                // FIXME: use duration
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

    fn properties(self) -> Self::IterType<TemporalProperties<Self::Edge>> {
        Box::new(self.map(move |e| e.properties()))
    }

    fn static_properties(self) -> Self::IterType<StaticProperties<EdgeView<G>>> {
        Box::new(self.map(move |e| e.static_properties()))
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

    fn properties(self) -> Self::IterType<TemporalProperties<Self::Edge>> {
        Box::new(self.map(move |it| it.properties()))
    }

    fn static_properties(self) -> Self::IterType<StaticProperties<EdgeView<G>>> {
        Box::new(self.map(move |it| it.static_properties()))
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
    use crate::db::api::mutation::Properties;
    use crate::prelude::*;
    use std::collections::HashMap;

    #[test]
    fn test_properties() {
        let g = Graph::new();
        let props = [("test".to_string(), Prop::Str("test".to_string()))];
        g.add_edge(0, 1, 2, [], None).unwrap();
        g.add_edge(2, 1, 2, props.clone(), None).unwrap();

        let e1 = g.edge(1, 2, None).unwrap();
        let e1_w = g.window(0, 1).edge(1, 2, None).unwrap();
        assert_eq!(
            HashMap::from_iter(e1.properties().collect_properties()),
            props.into()
        );
        assert!(e1_w.properties().collect_properties().is_empty())
    }
}
