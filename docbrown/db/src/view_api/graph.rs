use crate::view_api::edge::EdgeViewOps;
use crate::view_api::vertex::VertexViewOps;
use docbrown_core::eval::LocalVRef;
use docbrown_core::tgraph::{EdgeRef, VertexRef};
use docbrown_core::{Direction, Prop};
use std::collections::HashMap;
use docbrown_core::vertex::InputVertex;

pub trait GraphViewOps: Send + Sync {
    type Vertex: VertexViewOps<Edge = Self::Edge>;
    type VertexIter: Iterator<Item = Self::Vertex> + Send;
    type Vertices: IntoIterator<Item = Self::Vertex, IntoIter = Self::VertexIter> + Send;
    type Edge: EdgeViewOps<Vertex = Self::Vertex>;
    type Edges: IntoIterator<Item = Self::Edge>;

    fn num_vertices(&self) -> usize;
    fn earliest_time(&self) -> Option<i64>;
    fn latest_time(&self) -> Option<i64>;
    fn is_empty(&self) -> bool {
        self.num_vertices() == 0
    }
    fn num_edges(&self) -> usize;
    fn has_vertex<T: InputVertex>(&self, v: T) -> bool;
    fn has_edge<T: InputVertex>(&self, src: T, dst: T) -> bool;
    fn vertex<T: InputVertex>(&self, v: T) -> Option<Self::Vertex>;
    fn vertices(&self) -> Self::Vertices;
    fn edge<T: InputVertex>(&self, src: T, dst: T) -> Option<Self::Edge>;
    fn edges(&self) -> Self::Edges;
}
