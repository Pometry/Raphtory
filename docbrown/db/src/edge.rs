//! Defines the `Edge` struct, which represents an edge in the graph.
//!
//! Edges are used to define directed connections between verticies in the graph.
//! Edges are identified by a unique ID, can have a direction (Ingoing, Outgoing, or Both)
//! and can have properties associated with them.
//!

use crate::vertex::VertexView;
use crate::view_api::{EdgeListOps, GraphViewOps};
use docbrown_core::tgraph::{EdgeRef, VertexRef};
use docbrown_core::tgraph_shard::errors::GraphError;
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
                    match self.graph.static_edge_prop(self.edge, name) {
                        None => None,
                        Some(prop) => Some(prop),
                    }
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
                match self.graph.static_edge_prop(self.edge, prop_name.clone()) {
                    Some(prop) => {
                        props.insert(prop_name, prop);
                    }
                    None => {}
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
        (! self.property_history(name.clone()).is_empty())
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
impl<G: GraphViewOps> EdgeListOps for Box<dyn Iterator<Item = EdgeView<G>> + Send> {
    type Graph = G;

    /// Specifies the associated type for an iterator over vertices.
    type VList = Box<dyn Iterator<Item = VertexView<G>> + Send>;

    /// Specifies the associated type for the iterator over edges.
    type IterType = Box<dyn Iterator<Item = EdgeView<G>> + Send>;

    fn has_property(
        self,
        name: String,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = bool> + Send> {
        let r: Vec<_> = self
            .map(|e| e.has_property(name.clone(), include_static.clone()))
            .collect();
        Box::new(r.into_iter())
    }

    fn property(
        self,
        name: String,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = Option<Prop>> + Send> {
        let r: Vec<_> = self
            .map(|e| e.property(name.clone(), include_static.clone()))
            .collect();
        Box::new(r.into_iter())
    }

    fn properties(
        self,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = HashMap<String, Prop>> + Send> {
        let r: Vec<_> = self.map(|e| e.properties(include_static.clone())).collect();
        Box::new(r.into_iter())
    }

    fn property_names(self, include_static: bool) -> Box<dyn Iterator<Item = Vec<String>> + Send> {
        let r: Vec<_> = self
            .map(|e| e.property_names(include_static.clone()))
            .collect();
        Box::new(r.into_iter())
    }

    fn has_static_property(self, name: String) -> Box<dyn Iterator<Item = bool> + Send> {
        let r: Vec<_> = self.map(|e| e.has_static_property(name.clone())).collect();
        Box::new(r.into_iter())
    }

    fn static_property(self, name: String) -> Box<dyn Iterator<Item = Option<Prop>> + Send> {
        let r: Vec<_> = self.map(|e| e.static_property(name.clone())).collect();
        Box::new(r.into_iter())
    }

    fn property_history(self, name: String) -> Box<dyn Iterator<Item = Vec<(i64, Prop)>> + Send> {
        let r: Vec<_> = self.map(|e| e.property_history(name.clone())).collect();
        Box::new(r.into_iter())
    }

    fn property_histories(
        self,
    ) -> Box<dyn Iterator<Item = HashMap<String, Vec<(i64, Prop)>>> + Send> {
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

pub type EdgeList<G> = Box<dyn Iterator<Item = EdgeView<G>> + Send>;
