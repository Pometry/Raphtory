use crate::view_api::edge::EdgeViewOps;
use crate::view_api::vertex::VertexViewOps;
use docbrown_core::tgraph_shard::errors::GraphError;
use docbrown_core::vertex::InputVertex;

pub trait GraphViewOps: Send + Sync {
    type Vertex: VertexViewOps<Edge = Self::Edge>;
    type VertexIter: Iterator<Item = Self::Vertex> + Send;
    type Vertices: IntoIterator<Item = Self::Vertex, IntoIter = Self::VertexIter> + Send;
    type Edge: EdgeViewOps<Vertex = Self::Vertex>;
    type Edges: IntoIterator<Item = Self::Edge>;

    fn num_vertices(&self) -> Result<usize, GraphError>;
    fn earliest_time(&self) -> Result<Option<i64>, GraphError>;
    fn latest_time(&self) -> Result<Option<i64>, GraphError>;
    fn is_empty(&self) -> Result<bool, GraphError> {
        Ok(self.num_vertices()? == 0)
    }
    fn num_edges(&self) -> Result<usize, GraphError>;
    fn has_vertex<T: InputVertex>(&self, v: T) -> Result<bool, GraphError>;
    fn has_edge<T: InputVertex>(&self, src: T, dst: T) -> Result<bool, GraphError>;
    fn vertex<T: InputVertex>(&self, v: T) -> Result<Option<Self::Vertex>, GraphError>;
    fn vertices(&self) -> Self::Vertices;
    fn edge<T: InputVertex>(&self, src: T, dst: T) -> Result<Option<Self::Edge>, GraphError>;
    fn edges(&self) -> Self::Edges;
    fn vertices_shard(&self, shard: usize) -> Self::Vertices;
}
