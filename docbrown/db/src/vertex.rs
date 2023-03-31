//! Defines the `Vertex` struct, which represents a vertex in the graph.

use crate::edge::EdgeView;
use crate::graph::Graph;
use crate::view_api::internal::GraphViewInternalOps;
use crate::view_api::{VertexListOps, VertexViewOps};
use docbrown_core::tgraph::VertexRef;
use docbrown_core::tgraph_shard::errors::GraphError;
use docbrown_core::{Direction, Prop};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct VertexView<G: GraphViewInternalOps> {
    //FIXME: Not sure Arc is good here, maybe this should just own a graph and rely on cheap clone...
    graph: Arc<G>,
    vertex: VertexRef,
}

impl<G: GraphViewInternalOps> Into<VertexRef> for VertexView<G> {
    fn into(self) -> VertexRef {
        self.vertex
    }
}

impl<G: GraphViewInternalOps> VertexView<G> {
    /// Creates a new `VertexView` wrapping a vertex reference and a graph.
    pub(crate) fn new(graph: Arc<G>, vertex: VertexRef) -> VertexView<G> {
        VertexView { graph, vertex }
    }

    /// Returns a reference to the wrapped vertex reference.
    pub(crate) fn as_ref(&self) -> VertexRef {
        self.vertex
    }
}

/// The `VertexViewOps` trait provides operations that can be performed on a vertex in a graph.
/// It is implemented for the `VertexView` struct.
impl<G: GraphViewInternalOps + 'static + Send + Sync> VertexViewOps for VertexView<G> {
    /// The type of edge that this vertex can have.
    type Edge = EdgeView<G>;
    /// The type of the vertex list iterator.
    type VList = Box<dyn Iterator<Item = Self> + Send>;
    /// The type of the edge list iterator.
    type EList = Box<dyn Iterator<Item = Self::Edge> + Send>;

    /// Get the ID of this vertex.
    ///
    /// # Returns
    ///
    /// The ID of this vertex.
    fn id(&self) -> u64 {
        self.vertex.g_id
    }

    /// Get the temporal property value of this vertex.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to retrieve.
    ///
    /// # Returns
    ///
    /// A vector of `(i64, Prop)` tuples where the `i64` value is the timestamp of the
    /// property value and `Prop` is the value itself.
    fn prop(&self, name: String) -> Result<Vec<(i64, Prop)>, GraphError> {
        self.graph.temporal_vertex_prop_vec(self.vertex, name)
    }

    /// Get all temporal property values of this vertex.
    ///
    /// # Returns
    ///
    /// A HashMap with the names of the properties as keys and a vector of `(i64, Prop)` tuples
    /// as values. The `i64` value is the timestamp of the property value and `Prop`
    /// is the value itself.
    fn props(&self) -> Result<HashMap<String, Vec<(i64, Prop)>>, GraphError> {
        self.graph.temporal_vertex_props(self.vertex)
    }

    /// Get the degree of this vertex (i.e., the number of edges that are incident to it).
    ///
    /// # Returns
    ///
    /// The degree of this vertex.
    fn degree(&self) -> Result<usize, GraphError> {
        self.graph.degree(self.vertex, Direction::BOTH)
    }

    /// Get the degree of this vertex in the given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start of the time window (inclusive).
    /// * `t_end` - The end of the time window (exclusive).
    ///
    /// # Returns
    ///
    /// The degree of this vertex in the given time window.
    fn degree_window(&self, t_start: i64, t_end: i64) -> Result<usize, GraphError> {
        self.graph
            .degree_window(self.vertex, t_start, t_end, Direction::BOTH)
    }

    /// Get the in-degree of this vertex (i.e., the number of edges that point into it).
    ///
    /// # Returns
    ///
    /// The in-degree of this vertex.
    fn in_degree(&self) -> Result<usize, GraphError> {
        self.graph.degree(self.vertex, Direction::IN)
    }

    /// Get the in-degree of this vertex in the given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start of the time window (inclusive).
    /// * `t_end` - The end of the time window (exclusive).
    ///
    /// # Returns
    ///
    /// The in-degree of this vertex in the given time window.
    fn in_degree_window(&self, t_start: i64, t_end: i64) -> Result<usize, GraphError> {
        self.graph
            .degree_window(self.vertex, t_start, t_end, Direction::IN)
    }

    /// Get the out-degree of this vertex (i.e., the number of edges that point out of it).
    ///
    /// # Returns
    ///
    /// The out-degree of this vertex.
    fn out_degree(&self) -> Result<usize, GraphError> {
        self.graph.degree(self.vertex, Direction::OUT)
    }

    /// Get the out-degree of this vertex in the given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start of the time window (inclusive
    /// * `t_end` - The end of the time window (exclusive).
    ///
    /// # Returns
    ///
    /// The out-degree of this vertex in the given time window.
    fn out_degree_window(&self, t_start: i64, t_end: i64) -> Result<usize, GraphError> {
        self.graph
            .degree_window(self.vertex, t_start, t_end, Direction::OUT)
    }

    /// Get the edges that are incident to this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the edges that are incident to this vertex.
    fn edges(&self) -> Self::EList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges(self.vertex, Direction::BOTH)
                .map(move |e| Self::Edge::new(g.clone(), e)),
        )
    }

    /// Get the edges that are incident to this vertex in the given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start of the time window (inclusive).
    /// * `t_end` - The end of the time window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the edges that are incident to this vertex in the given time window.
    fn edges_window(&self, t_start: i64, t_end: i64) -> Self::EList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges_window(self.vertex, t_start, t_end, Direction::BOTH)
                .map(move |e| Self::Edge::new(g.clone(), e)),
        )
    }

    /// Get the edges that point into this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the edges that point into this vertex.
    fn in_edges(&self) -> Self::EList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges(self.vertex, Direction::IN)
                .map(move |e| Self::Edge::new(g.clone(), e)),
        )
    }

    /// Get the edges that point into this vertex in the given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start of the time window (inclusive).
    /// * `t_end` - The end of the time window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the edges that point into this vertex in the given time window.
    fn in_edges_window(&self, t_start: i64, t_end: i64) -> Self::EList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges_window(self.vertex, t_start, t_end, Direction::IN)
                .map(move |e| Self::Edge::new(g.clone(), e)),
        )
    }

    /// Get the edges that point out of this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the edges that point out of this vertex.
    fn out_edges(&self) -> Self::EList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges(self.vertex, Direction::OUT)
                .map(move |e| Self::Edge::new(g.clone(), e)),
        )
    }

    /// Get the edges that point out of this vertex in the given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start of the time window (inclusive).
    /// * `t_end` - The end of the time window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the edges that point out of this vertex in the given time window.
    fn out_edges_window(&self, t_start: i64, t_end: i64) -> Self::EList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges_window(self.vertex, t_start, t_end, Direction::OUT)
                .map(move |e| Self::Edge::new(g.clone(), e)),
        )
    }

    /// Get the neighbours of this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the neighbours of this vertex.
    fn neighbours(&self) -> Self::VList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .neighbours(self.vertex, Direction::BOTH)
                .map(move |v| Self::new(g.clone(), v)),
        )
    }

    /// Get the neighbours of this vertex in the given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start of the time window (inclusive).
    /// * `t_end` - The end of the time window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the neighbours of this vertex in the given time window.
    fn neighbours_window(&self, t_start: i64, t_end: i64) -> Self::VList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .neighbours_window(self.vertex, t_start, t_end, Direction::BOTH)
                .map(move |v| Self::new(g.clone(), v)),
        )
    }

    /// Get the neighbours of this vertex that point into this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the neighbours of this vertex that point into this vertex.
    fn in_neighbours(&self) -> Self::VList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .neighbours(self.vertex, Direction::IN)
                .map(move |v| Self::new(g.clone(), v)),
        )
    }

    /// Get the neighbours of this vertex that point into this vertex in the given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start of the time window (inclusive).
    /// * `t_end` - The end of the time window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the neighbours of this vertex that point into this vertex in the given time window.
    fn in_neighbours_window(&self, t_start: i64, t_end: i64) -> Self::VList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .neighbours_window(self.vertex, t_start, t_end, Direction::IN)
                .map(move |v| Self::new(g.clone(), v)),
        )
    }

    /// Get the neighbours of this vertex that point out of this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the neighbours of this vertex that point out of this vertex.
    fn out_neighbours(&self) -> Self::VList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .neighbours(self.vertex, Direction::OUT)
                .map(move |v| Self::new(g.clone(), v)),
        )
    }

    /// Get the neighbours of this vertex that point out of this vertex in the given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start of the time window (inclusive).
    /// * `t_end` - The end of the time window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the neighbours of this vertex that point out of this vertex in the given time window.
    fn out_neighbours_window(&self, t_start: i64, t_end: i64) -> Self::VList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .neighbours_window(self.vertex, t_start, t_end, Direction::OUT)
                .map(move |v| Self::new(g.clone(), v)),
        )
    }
}

/// Implementation of the VertexListOps trait for an iterator of VertexView objects.
///
impl<G: GraphViewInternalOps + 'static + Send + Sync> VertexListOps
    for Box<dyn Iterator<Item = VertexView<G>> + Send>
{
    type Vertex = VertexView<G>;
    type Edge = EdgeView<G>;
    type EList = Box<dyn Iterator<Item = Self::Edge> + Send>;
    type IterType = Box<dyn Iterator<Item = Self::Vertex> + Send>;
    type ValueIterType<U> = Box<dyn Iterator<Item = U> + Send>;

    /// Get the vertex ids in this list.
    ///
    /// # Returns
    ///
    /// An iterator over the vertex ids in this list.
    fn id(self) -> Self::ValueIterType<u64> {
        Box::new(self.map(|v| v.id()))
    }

    /// Get the vertex properties in this list.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to get.
    ///
    /// # Returns
    ///
    /// An iterator over the vertex properties in this list.
    fn prop(self, name: String) -> Result<Self::ValueIterType<Vec<(i64, Prop)>>, GraphError> {
        let r: Result<Vec<_>, _> = self.map(move |v| v.prop(name.clone())).collect();
        Ok(Box::new(r?.into_iter()))
    }

    /// Get all vertex properties in this list.
    ///
    /// # Returns
    ///
    /// An iterator over all vertex properties in this list.
    fn props(self) -> Result<Self::ValueIterType<HashMap<String, Vec<(i64, Prop)>>>, GraphError> {
        let r: Result<Vec<_>, _> = self.map(|v| v.props()).collect();
        Ok(Box::new(r?.into_iter()))
    }

    /// Get the degree of this vertices
    ///
    /// # Returns
    ///
    /// An iterator over the degree of this vertices
    fn degree(self) -> Result<Self::ValueIterType<usize>, GraphError> {
        let r: Result<Vec<_>, _> = self.map(|v| v.degree()).collect();
        Ok(Box::new(r?.into_iter()))
    }

    /// Get the degree of this vertices in the given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start of the time window (inclusive).
    /// * `t_end` - The end of the time window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the degree of this vertices in the given time window.
    fn degree_window(
        self,
        t_start: i64,
        t_end: i64,
    ) -> Result<Self::ValueIterType<usize>, GraphError> {
        let r: Result<Vec<_>, _> = self.map(move |v| v.degree_window(t_start, t_end)).collect();
        Ok(Box::new(r?.into_iter()))
    }

    /// Get the in degree of these vertices
    ///
    /// # Returns
    ///
    /// An iterator over the in degree of these vertices
    fn in_degree(self) -> Result<Self::ValueIterType<usize>, GraphError> {
        let r: Result<Vec<_>, _> = self.map(|v| v.in_degree()).collect();
        Ok(Box::new(r?.into_iter()))
    }

    /// Get the in degree of these vertices in the given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start of the time window (inclusive).
    /// * `t_end` - The end of the time window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the in degree of these vertices in the given time window.
    fn in_degree_window(
        self,
        t_start: i64,
        t_end: i64,
    ) -> Result<Self::ValueIterType<usize>, GraphError> {
        let r: Result<Vec<_>, _> = self
            .map(move |v| v.in_degree_window(t_start, t_end))
            .collect();
        Ok(Box::new(r?.into_iter()))
    }

    /// Get the out degree of these vertices
    ///
    /// # Returns
    ///
    /// An iterator over the out degree of these vertices
    fn out_degree(self) -> Result<Self::ValueIterType<usize>, GraphError> {
        let r: Result<Vec<_>, _> = self.map(|v| v.out_degree()).collect();
        Ok(Box::new(r?.into_iter()))
    }

    /// Get the out degree of these vertices in the given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start of the time window (inclusive).
    /// * `t_end` - The end of the time window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the out degree of these vertices in the given time window.
    fn out_degree_window(
        self,
        t_start: i64,
        t_end: i64,
    ) -> Result<Self::ValueIterType<usize>, GraphError> {
        let r: Result<Vec<_>, _> = self
            .map(move |v| v.out_degree_window(t_start, t_end))
            .collect();
        Ok(Box::new(r?.into_iter()))
    }

    /// Get the edges of these vertices.
    ///
    /// # Returns
    ///
    /// An iterator over the edges of these vertices.
    fn edges(self) -> Self::EList {
        Box::new(self.flat_map(|v| v.edges()))
    }

    /// Get the edges of these vertices in the given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start of the time window (inclusive).
    /// * `t_end` - The end of the time window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the edges of these vertices in the given time window.
    fn edges_window(self, t_start: i64, t_end: i64) -> Self::EList {
        Box::new(self.flat_map(move |v| v.edges_window(t_start, t_end)))
    }

    /// Get the in edges of these vertices.
    ///
    /// # Returns
    ///
    /// An iterator over the in edges of these vertices.
    fn in_edges(self) -> Self::EList {
        Box::new(self.flat_map(|v| v.in_edges()))
    }

    /// Get the in edges of these vertices in the given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start of the time window (inclusive).
    /// * `t_end` - The end of the time window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the in edges of these vertices in the given time window.
    fn in_edges_window(self, t_start: i64, t_end: i64) -> Self::EList {
        Box::new(self.flat_map(move |v| v.in_edges_window(t_start, t_end)))
    }

    /// Get the out edges of these vertices.
    ///
    /// # Returns
    ///
    /// An iterator over the out edges of these vertices.
    fn out_edges(self) -> Self::EList {
        Box::new(self.flat_map(|v| v.out_edges()))
    }

    /// Get the out edges of these vertices in the given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start of the time window (inclusive).
    /// * `t_end` - The end of the time window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the out edges of these vertices in the given time window.
    fn out_edges_window(self, t_start: i64, t_end: i64) -> Self::EList {
        Box::new(self.flat_map(move |v| v.out_edges_window(t_start, t_end)))
    }

    /// Get the neighbours of these vertices.
    ///
    /// # Returns
    ///
    /// An iterator over the neighbours of these vertices.
    fn neighbours(self) -> Self {
        Box::new(self.flat_map(|v| v.neighbours()))
    }

    /// Get the neighbours of these vertices in the given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start of the time window (inclusive).
    /// * `t_end` - The end of the time window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the neighbours of these vertices in the given time window.
    fn neighbours_window(self, t_start: i64, t_end: i64) -> Self {
        Box::new(self.flat_map(move |v| v.neighbours_window(t_start, t_end)))
    }

    /// Get the in neighbours of these vertices.
    ///
    /// # Returns
    ///
    /// An iterator over the in neighbours of these vertices.
    fn in_neighbours(self) -> Self {
        Box::new(self.flat_map(|v| v.in_neighbours()))
    }

    /// Get the in neighbours of these vertices in the given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start of the time window (inclusive).
    /// * `t_end` - The end of the time window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the in neighbours of these vertices in the given time window.
    fn in_neighbours_window(self, t_start: i64, t_end: i64) -> Self {
        Box::new(self.flat_map(move |v| v.in_neighbours_window(t_start, t_end)))
    }

    /// Get the out neighbours of these vertices.
    ///
    /// # Returns
    ///
    /// An iterator over the out neighbours of these vertices.
    fn out_neighbours(self) -> Self {
        Box::new(self.flat_map(|v| v.out_neighbours()))
    }

    /// Get the out neighbours of these vertices in the given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start of the time window (inclusive).
    /// * `t_end` - The end of the time window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the out neighbours of these vertices in the given time window.
    fn out_neighbours_window(self, t_start: i64, t_end: i64) -> Self {
        Box::new(self.flat_map(move |v| v.out_neighbours_window(t_start, t_end)))
    }
}

#[cfg(test)]
mod vertex_test {
    use crate::view_api::*;

    #[test]
    fn test_all_degrees_window() {
        let g = crate::graph_loader::lotr_graph::lotr_graph(4);

        assert_eq!(g.num_edges().unwrap(), 701);
        assert_eq!(g.vertex("Gandalf").unwrap().unwrap().degree().unwrap(), 49);
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .unwrap()
                .degree_window(1356, 24792)
                .unwrap(),
            34
        );
        assert_eq!(
            g.vertex("Gandalf").unwrap().unwrap().in_degree().unwrap(),
            24
        );
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .unwrap()
                .in_degree_window(1356, 24792)
                .unwrap(),
            16
        );
        assert_eq!(
            g.vertex("Gandalf").unwrap().unwrap().out_degree().unwrap(),
            35
        );
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .unwrap()
                .out_degree_window(1356, 24792)
                .unwrap(),
            20
        );
    }

    #[test]
    fn test_all_neighbours_window() {
        let g = crate::graph_loader::lotr_graph::lotr_graph(4);

        assert_eq!(g.num_edges().unwrap(), 701);
        assert_eq!(
            g.vertex("Gandalf").unwrap().unwrap().neighbours().count(),
            49
        );
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .unwrap()
                .neighbours_window(1356, 24792)
                .count(),
            34
        );
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .unwrap()
                .in_neighbours()
                .count(),
            24
        );
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .unwrap()
                .in_neighbours_window(1356, 24792)
                .count(),
            16
        );
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .unwrap()
                .out_neighbours()
                .count(),
            35
        );
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .unwrap()
                .out_neighbours_window(1356, 24792)
                .count(),
            20
        );
    }

    #[test]
    fn test_all_edges_window() {
        let g = crate::graph_loader::lotr_graph::lotr_graph(4);

        assert_eq!(g.num_edges().unwrap(), 701);
        assert_eq!(g.vertex("Gandalf").unwrap().unwrap().edges().count(), 59);
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .unwrap()
                .edges_window(1356, 24792)
                .count(),
            36
        );
        assert_eq!(g.vertex("Gandalf").unwrap().unwrap().in_edges().count(), 24);
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .unwrap()
                .in_edges_window(1356, 24792)
                .count(),
            16
        );
        assert_eq!(
            g.vertex("Gandalf").unwrap().unwrap().out_edges().count(),
            35
        );
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .unwrap()
                .out_edges_window(1356, 24792)
                .count(),
            20
        );
    }
}
