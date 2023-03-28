use crate::view_api::edge::{EdgeListOps, EdgeViewOps};
use docbrown_core::{tgraph_shard::errors::GraphError, Direction, Prop};
use std::collections::HashMap;

pub trait VertexViewOps: Sized + Send + Sync {
    type Edge: EdgeViewOps<Vertex = Self>;
    type VList: VertexListOps<Vertex = Self, Edge = Self::Edge, EList = Self::EList>;
    type EList: EdgeListOps<Vertex = Self, Edge = Self::Edge>;

    fn id(&self) -> u64;

    fn prop(&self, name: String) -> Result<Vec<(i64, Prop)>, GraphError>;

    fn props(&self) -> Result<HashMap<String, Vec<(i64, Prop)>>, GraphError>;

    fn degree(&self) -> Result<usize, GraphError>;

    fn degree_window(&self, t_start: i64, t_end: i64) -> Result<usize, GraphError>;

    fn in_degree(&self) -> Result<usize, GraphError>;

    fn in_degree_window(&self, t_start: i64, t_end: i64) -> Result<usize, GraphError>;

    fn out_degree(&self) -> Result<usize, GraphError>;

    fn out_degree_window(&self, t_start: i64, t_end: i64) -> Result<usize, GraphError>;

    fn edges(&self) -> Self::EList;

    fn edges_window(&self, t_start: i64, t_end: i64) -> Self::EList;

    fn in_edges(&self) -> Self::EList;

    fn in_edges_window(&self, t_start: i64, t_end: i64) -> Self::EList;

    fn out_edges(&self) -> Self::EList;

    fn out_edges_window(&self, t_start: i64, t_end: i64) -> Self::EList;

    fn neighbours(&self) -> Self::VList;

    fn neighbours_window(&self, t_start: i64, t_end: i64) -> Self::VList;

    fn in_neighbours(&self) -> Self::VList;

    fn in_neighbours_window(&self, t_start: i64, t_end: i64) -> Self::VList;

    fn out_neighbours(&self) -> Self::VList;

    fn out_neighbours_window(&self, t_start: i64, t_end: i64) -> Self::VList;
}

pub trait VertexListOps:
    IntoIterator<Item = Self::Vertex, IntoIter = Self::IterType> + Sized + Send
{
    type Vertex: VertexViewOps<Edge = Self::Edge>;
    type Edge: EdgeViewOps<Vertex = Self::Vertex>;
    type EList: EdgeListOps<Vertex = Self::Vertex, Edge = Self::Edge>;
    type IterType: Iterator<Item = Self::Vertex> + Send;
    type ValueIterType<U>: Iterator<Item = U> + Send;

    fn id(self) -> Self::ValueIterType<u64>;

    fn prop(self, name: String) -> Result<Self::ValueIterType<Vec<(i64, Prop)>>, GraphError>;

    fn props(self) -> Result<Self::ValueIterType<HashMap<String, Vec<(i64, Prop)>>>, GraphError>;

    fn degree(self) -> Result<Self::ValueIterType<usize>, GraphError>;

    fn degree_window(
        self,
        t_start: i64,
        t_end: i64,
    ) -> Result<Self::ValueIterType<usize>, GraphError>;

    fn in_degree(self) -> Result<Self::ValueIterType<usize>, GraphError>;

    fn in_degree_window(
        self,
        t_start: i64,
        t_end: i64,
    ) -> Result<Self::ValueIterType<usize>, GraphError>;

    fn out_degree(self) -> Result<Self::ValueIterType<usize>, GraphError>;

    fn out_degree_window(
        self,
        t_start: i64,
        t_end: i64,
    ) -> Result<Self::ValueIterType<usize>, GraphError>;

    fn edges(self) -> Self::EList;

    fn edges_window(self, t_start: i64, t_end: i64) -> Self::EList;

    fn in_edges(self) -> Self::EList;

    fn in_edges_window(self, t_start: i64, t_end: i64) -> Self::EList;

    fn out_edges(self) -> Self::EList;

    fn out_edges_window(self, t_start: i64, t_end: i64) -> Self::EList;

    fn neighbours(self) -> Self;

    fn neighbours_window(self, t_start: i64, t_end: i64) -> Self;

    fn in_neighbours(self) -> Self;

    fn in_neighbours_window(self, t_start: i64, t_end: i64) -> Self;

    fn out_neighbours(self) -> Self;

    fn out_neighbours_window(self, t_start: i64, t_end: i64) -> Self;
}
