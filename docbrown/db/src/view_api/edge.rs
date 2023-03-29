use crate::view_api::vertex::VertexViewOps;
use crate::view_api::VertexListOps;
use docbrown_core::tgraph_shard::errors::GraphError;
use docbrown_core::Prop;

/// This trait defines the operations that can be
/// performed on an edge in a temporal graph view.
pub trait EdgeViewOps: Sized + Send + Sync {
    type Vertex: VertexViewOps<Edge = Self>;

    /// gets a property of an edge with the given name
    /// includes the timestamp of the property
    fn prop(&self, name: String) -> Result<Vec<(i64, Prop)>, GraphError>;

    /// gets the source vertex of an edge
    fn src(&self) -> Self::Vertex;

    /// gets the destination vertex of an edge
    fn dst(&self) -> Self::Vertex;

    /// gets the id of an edge
    fn id(&self) -> usize;
}

/// This trait defines the operations that can be
/// performed on a list of edges in a temporal graph view.
pub trait EdgeListOps:
    IntoIterator<Item = Self::Edge, IntoIter = Self::IterType> + Sized + Send
{
    /// The type of vertex on the edge list
    type Vertex: VertexViewOps;

    /// the type of list of vertices
    type VList: VertexListOps;

    /// the type of edge
    type Edge: EdgeViewOps<Vertex = Self::Vertex>;

    /// the type of iterator
    type IterType: Iterator<Item = Self::Edge> + Send;

    /// gets the source vertices of the edges in the list
    fn src(self) -> Self::VList;

    /// gets the destination vertices of the edges in the list
    fn dst(self) -> Self::VList;
}
