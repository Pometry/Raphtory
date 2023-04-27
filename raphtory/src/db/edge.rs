//! Defines the `Edge` struct, which represents an edge in the graph.
//!
//! Edges are used to define directed connections between verticies in the graph.
//! Edges are identified by a unique ID, can have a direction (Ingoing, Outgoing, or Both)
//! and can have properties associated with them.
//!

use crate::core::tgraph::{EdgeRef, VertexRef};
use crate::core::Direction;
use crate::core::Prop;
use crate::db::graph_window::WindowedGraph;
use crate::db::vertex::VertexView;
use crate::db::view_api::{BoxedIter, EdgeListOps, GraphViewOps, TimeOps};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::iter;
use std::ops::Range;

/// A view of an edge in the graph.
#[derive(Clone)]
pub struct EdgeView<G: GraphViewOps> {
    /// A view of an edge in the graph.
    pub graph: G,
    /// A reference to the edge.
    pub edge: EdgeRef,
}

impl<G: GraphViewOps> Debug for EdgeView<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EdgeView({}, {})",
            self.edge.src_g_id, self.edge.dst_g_id
        )
    }
}

impl<G: GraphViewOps> EdgeView<G> {
    /// Creates a new `EdgeView`.
    ///
    /// # Arguments
    ///
    /// * `graph` - A reference to the graph.
    /// * `edge` - A reference to the edge.
    ///
    /// # Returns
    ///
    /// A new `EdgeView`.
    pub(crate) fn new(graph: G, edge: EdgeRef) -> Self {
        EdgeView { graph, edge }
    }

    /// Returns a reference to the underlying edge reference.
    pub fn as_ref(&self) -> EdgeRef {
        self.edge
    }
}

impl<G: GraphViewOps> From<EdgeView<G>> for EdgeRef {
    fn from(value: EdgeView<G>) -> Self {
        value.edge
    }
}

impl<G: GraphViewOps> EdgeView<G> {
    pub fn property(&self, name: String, include_static: bool) -> Option<Prop> {
        let props = self.property_history(name.clone());
        match props.last() {
            None => {
                if include_static {
                    self.graph.static_edge_prop(self.edge, name)
                } else {
                    None
                }
            }
            Some((_, prop)) => Some(prop.clone()),
        }
    }

    pub fn property_history(&self, name: String) -> Vec<(i64, Prop)> {
        match self.edge.time {
            None => self.graph.temporal_edge_props_vec(self.edge, name),
            Some(_) => self.graph.temporal_edge_props_vec_window(
                self.edge,
                name,
                self.edge.time.unwrap(),
                self.edge.time.unwrap() + 1,
            ),
        }
    }

    pub fn history(&self) -> Vec<i64> {
        self.graph.edge_timestamps(self.edge, None)
    }

    pub fn properties(&self, include_static: bool) -> HashMap<String, Prop> {
        let mut props: HashMap<String, Prop> = self
            .property_histories()
            .iter()
            .map(|(key, values)| (key.clone(), values.last().unwrap().1.clone()))
            .collect();

        if include_static {
            for prop_name in self.graph.static_edge_prop_names(self.edge) {
                if let Some(prop) = self.graph.static_edge_prop(self.edge, prop_name.clone()) {
                    props.insert(prop_name, prop);
                }
            }
        }
        props
    }

    pub fn property_histories(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        // match on the self.edge.time option property and run two function s
        // one for static and one for temporal
        match self.edge.time {
            None => self.graph.temporal_edge_props(self.edge),
            Some(_) => self.graph.temporal_edge_props_window(
                self.edge,
                self.edge.time.unwrap(),
                self.edge.time.unwrap() + 1,
            ),
        }
    }

    pub fn property_names(&self, include_static: bool) -> Vec<String> {
        let mut names: Vec<String> = self.graph.temporal_edge_prop_names(self.edge);
        if include_static {
            names.extend(self.graph.static_edge_prop_names(self.edge))
        }
        names
    }
    pub fn has_property(&self, name: String, include_static: bool) -> bool {
        (!self.property_history(name.clone()).is_empty())
            || (include_static && self.graph.static_edge_prop_names(self.edge).contains(&name))
    }

    pub fn has_static_property(&self, name: String) -> bool {
        self.graph.static_edge_prop_names(self.edge).contains(&name)
    }

    pub fn static_property(&self, name: String) -> Option<Prop> {
        self.graph.static_edge_prop(self.edge, name)
    }

    /// Returns the source vertex of the edge.
    pub fn src(&self) -> VertexView<G> {
        //FIXME: Make local ids on EdgeReference optional
        let vertex = VertexRef {
            g_id: self.edge.src_g_id,
            pid: None,
        };
        VertexView::new(self.graph.clone(), vertex)
    }

    pub fn dst(&self) -> VertexView<G> {
        //FIXME: Make local ids on EdgeReference optional
        let vertex = VertexRef {
            g_id: self.edge.dst_g_id,
            pid: None,
        };
        VertexView::new(self.graph.clone(), vertex)
    }

    /// Gets the id of the edge
    pub fn id(&self) -> usize {
        self.edge.edge_id
    }

    /// Explodes an edge and returns all instances it had been updated as seperate edges
    pub fn explode(&self) -> BoxedIter<EdgeView<G>> {
        let vertex = VertexRef {
            g_id: self.edge.src_g_id,
            pid: None,
        };

        if self.edge.time.is_some() {
            Box::new(iter::once(self.clone()))
        } else {
            let r: Vec<EdgeView<G>> = self
                .graph
                .vertex_edges_t(vertex, Direction::OUT, None)
                .filter(|e| e.edge_id == self.edge.edge_id)
                .map(|e| EdgeView::new(self.graph.clone(), e))
                .collect();
            Box::new(r.into_iter())
        }
    }

    /// Gets the edge object from the vertex
    fn get_edges(&self) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        let vertex = VertexRef {
            g_id: self.edge.src_g_id,
            pid: None,
        };
        self.graph.vertex_edges_t(vertex, Direction::OUT, None)
    }

    /// Gets the first time an edge was seen
    pub fn earliest_time(&self) -> Option<i64> {
        self.get_edges()
            .filter(|e| e.edge_id == self.edge.edge_id)
            .map(|e| e.time.unwrap())
            .min()
    }

    /// Gets the latest time an edge was updated
    pub fn latest_time(&self) -> Option<i64> {
        self.get_edges()
            .filter(|e| e.edge_id == self.edge.edge_id)
            .map(|e| e.time.unwrap())
            .max()
    }

    pub fn time(&self) -> Option<i64> {
        self.edge.time
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

    fn window(&self, t_start: i64, t_end: i64) -> Self::WindowedViewType {
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
    type ValueType<T: Send + Sync> = T;

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
    type ValueType<T: Send + Sync> = Box<dyn Iterator<Item = T> + Send>;
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
