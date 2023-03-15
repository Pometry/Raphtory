use crate::view_api::edge::EdgeViewOps;
use crate::view_api::vertex::VertexViewOps;

pub trait GraphViewOps {
    type Vertex: VertexViewOps<Edge = Self::Edge>;
    type Vertices: IntoIterator<Item = Self::Vertex>;
    type Edge: EdgeViewOps<Vertex = Self::Vertex>;
    type Edges: IntoIterator<Item = Self::Edge>;

    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn edges_len(&self) -> usize;
    fn has_vertex(&self, v: u64) -> bool;
    fn has_edge(&self, src: u64, dst: u64) -> bool;
    fn vertex(&self, v: u64) -> Option<Self::Vertex>;
    fn vertices(&self) -> Self::Vertices;
    fn edge(&self, src: u64, dst: u64) -> Option<Self::Edge>;
    fn edges(&self) -> Self::Edges;
}
