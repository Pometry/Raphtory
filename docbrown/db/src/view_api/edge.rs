use crate::view_api::vertex::VertexViewOps;
use crate::view_api::VertexListOps;
use docbrown_core::Prop;

pub trait EdgeViewOps: Sized + Send + Sync {
    type Vertex: VertexViewOps<Edge = Self>;

    fn prop(&self, name: String) -> Vec<(i64, Prop)>;
    fn src(&self) -> Self::Vertex;
    fn dst(&self) -> Self::Vertex;
}

pub trait EdgeListOps:
    IntoIterator<Item = Self::Edge, IntoIter = Self::IterType> + Sized + Send
{
    type Vertex: VertexViewOps;
    type VList: VertexListOps;
    type Edge: EdgeViewOps<Vertex = Self::Vertex>;
    type IterType: Iterator<Item = Self::Edge> + Send;

    fn src(self) -> Self::VList;
    fn dst(self) -> Self::VList;
}
