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
    /// Returns the properties associated with the given property name for the edge.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property.
    ///
    /// # Returns
    ///
    /// A vector of tuples containing the timestamp and the property value.
    pub fn prop(&self, name: String) -> Vec<(i64, Prop)> {
        self.graph.temporal_edge_props_vec(self.edge, name)
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
