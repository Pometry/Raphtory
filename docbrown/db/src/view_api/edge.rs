use crate::view_api::vertex::VertexViewOps;
use docbrown_core::Prop;

pub trait EdgeViewOps: Sized {
    type Vertex: VertexViewOps<Edge = Self>;

    fn prop(&self, name: String) -> Vec<(i64, Prop)>;
    fn src(&self) -> Self::Vertex;
    fn dst(&self) -> Self::Vertex;
}

pub trait EdgeListOps: IntoIterator<Item = Self::Edge, IntoIter = Self::IterType> + Sized {
    type Vertex: VertexViewOps;
    type Edge: EdgeViewOps<Vertex = Self::Vertex>;
    type IterType: Iterator<Item = Self::Edge>;
}
