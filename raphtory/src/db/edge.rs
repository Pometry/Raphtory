//! Defines the `Edge` struct, which represents an edge in the graph.
//!
//! Edges are used to define directed connections between verticies in the graph.
//! Edges are identified by a unique ID, can have a direction (Ingoing, Outgoing, or Both)
//! and can have properties associated with them.
//!

use crate::core::edge_ref::EdgeRef;
use crate::core::time::IntoTime;
use crate::core::vertex_ref::VertexRef;
use crate::core::Prop;
use crate::db::graph_window::WindowedGraph;
use crate::db::vertex::VertexView;
use crate::db::view_api::edge::{EdgeViewInternalOps, EdgeViewOps};
use crate::db::view_api::*;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::iter;
use std::sync::Arc;

/// A view of an edge in the graph.
#[derive(Clone)]
pub struct EdgeView<G: GraphViewOps> {
    /// A view of an edge in the graph.
    pub graph: Arc<G>,
    /// A reference to the edge.
    pub edge: EdgeRef,
}

impl<G: GraphViewOps> EdgeView<G> {
    pub fn new(graph: Arc<G>, edge: EdgeRef) -> Self {
        Self { graph, edge }
    }
}

impl<G: GraphViewOps> EdgeViewInternalOps<G, VertexView<G>> for EdgeView<G> {
    fn graph(&self) -> Arc<G> {
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

impl<G: GraphViewOps> EdgeViewOps for EdgeView<G> {
    type Graph = G;
    type Vertex = VertexView<G>;
    type EList = BoxedIter<Self>;

    fn explode(&self) -> Self::EList {
        let ev = self.clone();
        match self.edge.time() {
            Some(t) => Box::new(iter::once(ev)),
            None => {
                let e = self.edge;
                let ts = self.graph.edge_timestamps(self.edge, None);
                Box::new(ts.into_iter().map(move |t| ev.new_edge(e.at(t))))
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
            graph: Arc::new(self.graph.window(t_start, t_end)),
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
    type IterType = Box<dyn Iterator<Item = EdgeView<G>> + Send>;

    fn has_property(self, name: String, include_static: bool) -> BoxedIter<bool> {
        let r: Vec<_> = self
            .map(|e| e.has_property(name.clone(), include_static))
            .collect();
        Box::new(r.into_iter())
    }

    fn property(self, name: String, include_static: bool) -> BoxedIter<Option<Prop>> {
        let r: Vec<_> = self
            .map(|e| e.property(name.clone(), include_static))
            .collect();
        Box::new(r.into_iter())
    }

    fn properties(self, include_static: bool) -> BoxedIter<HashMap<String, Prop>> {
        let r: Vec<_> = self.map(|e| e.properties(include_static)).collect();
        Box::new(r.into_iter())
    }

    fn property_names(self, include_static: bool) -> BoxedIter<Vec<String>> {
        let r: Vec<_> = self.map(|e| e.property_names(include_static)).collect();
        Box::new(r.into_iter())
    }

    fn has_static_property(self, name: String) -> BoxedIter<bool> {
        let r: Vec<_> = self.map(|e| e.has_static_property(name.clone())).collect();
        Box::new(r.into_iter())
    }

    fn static_property(self, name: String) -> BoxedIter<Option<Prop>> {
        let r: Vec<_> = self.map(|e| e.static_property(name.clone())).collect();
        Box::new(r.into_iter())
    }

    fn property_history(self, name: String) -> BoxedIter<Vec<(i64, Prop)>> {
        let r: Vec<_> = self.map(|e| e.property_history(name.clone())).collect();
        Box::new(r.into_iter())
    }

    fn property_histories(self) -> BoxedIter<HashMap<String, Vec<(i64, Prop)>>> {
        let r: Vec<_> = self.map(|e| e.property_histories()).collect();
        Box::new(r.into_iter())
    }

    /// Returns an iterator over the source vertices of the edges in the iterator.
    fn src(self) -> Self::VList {
        Box::new(self.map(|e| e.src()))
    }

    /// Returns an iterator over the destination vertices of the edges in the iterator.
    fn dst(self) -> Self::VList {
        Box::new(self.into_iter().map(|e| e.dst()))
    }

    /// returns an iterator of exploded edges that include an edge at each point in time
    fn explode(self) -> Self::IterType {
        Box::new(self.flat_map(move |e| e.explode()))
    }

    /// Gets the earliest times of a list of edges
    fn earliest_time(self) -> BoxedIter<i64> {
        let r: Vec<i64> = self.flat_map(move |e| e.earliest_time()).collect();
        Box::new(r.into_iter())
    }

    /// Gets the latest times of a list of edges
    fn latest_time(self) -> BoxedIter<i64> {
        let r: Vec<i64> = self.flat_map(move |e| e.latest_time()).collect();
        Box::new(r.into_iter())
    }
}

impl<G: GraphViewOps> EdgeListOps for BoxedIter<BoxedIter<EdgeView<G>>> {
    type Graph = G;
    type Vertex = VertexView<G>;
    type Edge = EdgeView<G>;
    type ValueType<T> = Box<dyn Iterator<Item = T> + Send>;
    type VList = Box<dyn Iterator<Item = Box<dyn Iterator<Item = VertexView<G>> + Send>> + Send>;
    type IterType = Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>> + Send>;

    fn has_property(self, name: String, include_static: bool) -> BoxedIter<Self::ValueType<bool>> {
        Box::new(self.map(move |it| {
            let name = name.clone();
            let iter: Self::ValueType<bool> =
                Box::new(it.map(move |e| e.has_property(name.clone(), include_static)));
            iter
        }))
    }

    fn property(
        self,
        name: String,
        include_static: bool,
    ) -> BoxedIter<Self::ValueType<Option<Prop>>> {
        Box::new(self.map(move |it| it.property(name.clone(), include_static)))
    }

    fn properties(self, include_static: bool) -> BoxedIter<Self::ValueType<HashMap<String, Prop>>> {
        Box::new(self.map(move |it| it.properties(include_static)))
    }

    fn property_names(self, include_static: bool) -> BoxedIter<Self::ValueType<Vec<String>>> {
        Box::new(self.map(move |it| it.property_names(include_static)))
    }

    fn has_static_property(self, name: String) -> BoxedIter<Self::ValueType<bool>> {
        Box::new(self.map(move |it| it.has_static_property(name.clone())))
    }

    fn static_property(self, name: String) -> BoxedIter<Self::ValueType<Option<Prop>>> {
        Box::new(self.map(move |it| it.static_property(name.clone())))
    }

    fn property_history(self, name: String) -> BoxedIter<Self::ValueType<Vec<(i64, Prop)>>> {
        Box::new(self.map(move |it| it.property_history(name.clone())))
    }

    fn property_histories(self) -> BoxedIter<Self::ValueType<HashMap<String, Vec<(i64, Prop)>>>> {
        Box::new(self.map(|it| it.property_histories()))
    }

    fn src(self) -> Self::VList {
        Box::new(self.map(|it| it.src()))
    }

    fn dst(self) -> Self::VList {
        Box::new(self.map(|it| it.dst()))
    }

    fn explode(self) -> Self::IterType {
        Box::new(self.map(move |it| it.explode()))
    }

    /// Gets the earliest times of a list of edges
    fn earliest_time(self) -> BoxedIter<i64> {
        let r: Vec<i64> = self.flat_map(move |e| e.earliest_time()).collect();
        Box::new(r.into_iter())
    }

    /// Gets the latest times of a list of edges
    fn latest_time(self) -> BoxedIter<i64> {
        let r: Vec<i64> = self.flat_map(move |e| e.latest_time()).collect();
        Box::new(r.into_iter())
    }
}

pub type EdgeList<G> = Box<dyn Iterator<Item = EdgeView<G>> + Send>;
