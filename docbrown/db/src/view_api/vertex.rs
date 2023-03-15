use crate::view_api::edge::{EdgeListOps, EdgeViewOps};
use docbrown_core::Prop;
use std::collections::HashMap;

pub trait VertexViewOps: Sized {
    type Edge: EdgeViewOps<Vertex = Self>;
    type VList: VertexListOps<Vertex = Self, Edge = Self::Edge, EList = Self::EList>;
    type EList: EdgeListOps<Vertex = Self, Edge = Self::Edge>;

    fn id(&self) -> u64;

    fn prop(&self, name: String) -> Vec<(i64, Prop)>;

    fn props(&self) -> HashMap<String, Vec<(i64, Prop)>>;

    fn degree(&self) -> usize;

    fn in_degree(&self) -> usize;

    fn out_degree(&self) -> usize;

    fn edges(&self) -> Self::EList;

    fn in_edges(&self) -> Self::EList;

    fn out_edges(&self) -> Self::EList;

    fn neighbours(&self) -> Self::VList;

    fn in_neighbours(&self) -> Self::VList;

    fn out_neighbours(&self) -> Self::VList;
}

pub trait VertexListOps:
    IntoIterator<Item = Self::Vertex, IntoIter = Self::IterType> + Sized
{
    type Vertex: VertexViewOps<Edge = Self::Edge>;
    type Edge: EdgeViewOps<Vertex = Self::Vertex>;
    type EList: EdgeListOps<Vertex = Self::Vertex, Edge = Self::Edge>;
    type IterType: Iterator<Item = Self::Vertex>;
    type ValueIterType<U>: IntoIterator<Item = U>;

    fn id(self) -> Self::ValueIterType<u64>;

    fn prop(self, name: String) -> Self::ValueIterType<Vec<(i64, Prop)>>;

    fn props(self) -> Self::ValueIterType<HashMap<String, Vec<(i64, Prop)>>>;

    fn degree(self) -> Self::ValueIterType<usize>;

    fn in_degree(self) -> Self::ValueIterType<usize>;

    fn out_degree(self) -> Self::ValueIterType<usize>;

    fn edges(self) -> Self::EList;

    fn in_edges(self) -> Self::EList;

    fn out_edges(self) -> Self::EList;

    fn neighbours(self) -> Self;

    fn in_neighbours(self) -> Self;

    fn out_neighbours(self) -> Self;
}
