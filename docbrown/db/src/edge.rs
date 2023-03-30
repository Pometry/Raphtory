//! Defines the `Edge` struct, which represents an edge in the graph.
//!
//! Edges are used to define directed connections between verticies in the graph.
//! Edges are identified by a unique ID, can have a direction (Ingoing, Outgoing, or Both)
//! and can have properties associated with them.
//!

use crate::vertex::VertexView;
use crate::view_api::internal::GraphViewInternalOps;
use crate::view_api::{EdgeListOps, EdgeViewOps};
use docbrown_core::tgraph::{EdgeRef, VertexRef};
use docbrown_core::tgraph_shard::errors::GraphError;
use docbrown_core::Prop;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// A view of an edge in the graph.
pub struct EdgeView<G: GraphViewInternalOps> {
    /// A reference to the graph.
    graph: Arc<G>,
    /// A reference to the edge.
    edge: EdgeRef,
}

impl<G: GraphViewInternalOps> Debug for EdgeView<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EdgeView({}, {})",
            self.edge.src_g_id, self.edge.dst_g_id
        )
    }
}

impl<G: GraphViewInternalOps> EdgeView<G> {
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
    pub(crate) fn new(graph: Arc<G>, edge: EdgeRef) -> Self {
        EdgeView { graph, edge }
    }

    /// Returns a reference to the underlying edge reference.
    pub fn as_ref(&self) -> EdgeRef {
        self.edge
    }
}

impl<G: GraphViewInternalOps> Into<EdgeRef> for EdgeView<G> {
    fn into(self) -> EdgeRef {
        self.edge
    }
}

impl<G: GraphViewInternalOps + 'static + Send + Sync> EdgeViewOps for EdgeView<G> {
    type Vertex = VertexView<G>;

    /// Returns the properties associated with the given property name for the edge.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property.
    ///
    /// # Returns
    ///
    /// A vector of tuples containing the timestamp and the property value.
    fn prop(&self, name: String) -> Result<Vec<(i64, Prop)>, GraphError> {
        self.graph.temporal_edge_props_vec(self.edge, name)
    }

    /// Returns the source vertex of the edge.
    fn src(&self) -> Self::Vertex {
        //FIXME: Make local ids on EdgeReference optional
        let vertex = VertexRef {
            g_id: self.edge.src_g_id,
            pid: None,
        };
        Self::Vertex::new(self.graph.clone(), vertex)
    }

    /// Returns the destination vertex of the edge.
    fn dst(&self) -> Self::Vertex {
        //FIXME: Make local ids on EdgeReference optional
        let vertex = VertexRef {
            g_id: self.edge.dst_g_id,
            pid: None,
        };
        Self::Vertex::new(self.graph.clone(), vertex)
    }

    /// Returns the ID of the edge.
    fn id(&self) -> usize {
        self.edge.edge_id
    }
}

/// Implement `EdgeListOps` trait for an iterator of `EdgeView` objects.
///
/// This implementation enables the use of the `src` and `dst` methods to retrieve the vertices
/// connected to the edges inside the iterator.
impl<G: GraphViewInternalOps + 'static + Send + Sync> EdgeListOps
    for Box<dyn Iterator<Item = EdgeView<G>> + Send>
{
    /// Specifies the associated type for vertex.
    type Vertex = VertexView<G>;

    /// Specifies the associated type for an iterator over vertices.
    type VList = Box<dyn Iterator<Item = Self::Vertex> + Send>;

    /// Specifies the associated type for an edge.
    type Edge = EdgeView<G>;

    /// Specifies the associated type for the iterator over edges.
    type IterType = Box<dyn Iterator<Item = Self::Edge> + Send>;

    /// Returns an iterator over the source vertices of the edges in the iterator.
    fn src(self) -> Self::VList {
        Box::new(self.into_iter().map(|e| e.src()))
    }

    /// Returns an iterator over the destination vertices of the edges in the iterator.
    fn dst(self) -> Self::VList {
        Box::new(self.into_iter().map(|e| e.dst()))
    }
}
