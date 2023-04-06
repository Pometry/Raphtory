//! Defines the `Edge` struct, which represents an edge in the graph.
//!
//! Edges are used to define directed connections between verticies in the graph.
//! Edges are identified by a unique ID, can have a direction (Ingoing, Outgoing, or Both)
//! and can have properties associated with them.
//!

use crate::vertex::VertexView;
use crate::view_api::vertex::BoxedIter;
use crate::view_api::{EdgeListOps, GraphViewOps};
use docbrown_core::tgraph::{EdgeRef, VertexRef};
use docbrown_core::Prop;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

/// A view of an edge in the graph.
pub struct EdgeView<G: GraphViewOps> {
    /// A view of an edge in the graph.
    graph: G,
    /// A reference to the edge.
    edge: EdgeRef,
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
        self.graph.temporal_edge_props_vec(self.edge, name)
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
        self.graph.temporal_edge_props(self.edge)
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

    pub fn id(&self) -> usize {
        self.edge.edge_id
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
}

pub type EdgeList<G> = Box<dyn Iterator<Item = EdgeView<G>> + Send>;
