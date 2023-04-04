//! Defines the `Vertex` struct, which represents a vertex in the graph.

use crate::edge::{EdgeList, EdgeView};
use crate::path::{Operations, PathFromVertex};
use crate::view_api::{GraphViewOps, VertexListOps};
use docbrown_core::tgraph::VertexRef;
use docbrown_core::tgraph_shard::errors::GraphError;
use docbrown_core::{Direction, Prop};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct VertexView<G: GraphViewOps> {
    pub graph: G,
    vertex: VertexRef,
}

impl<G: GraphViewOps> From<VertexView<G>> for VertexRef {
    fn from(value: VertexView<G>) -> Self {
        value.vertex
    }
}

impl<G: GraphViewOps> From<&VertexView<G>> for VertexRef {
    fn from(value: &VertexView<G>) -> Self {
        value.vertex
    }
}

impl<G: GraphViewOps> VertexView<G> {
    /// Creates a new `VertexView` wrapping a vertex reference and a graph.
    pub(crate) fn new(graph: G, vertex: VertexRef) -> VertexView<G> {
        VertexView { graph, vertex }
    }
}

/// View of a Vertex in a Graph
impl<G: GraphViewOps> VertexView<G> {
    /// Get the ID of this vertex.
    ///
    /// # Returns
    ///
    /// The ID of this vertex.
    pub fn id(&self) -> u64 {
        self.vertex.g_id
    }

    /// Get the name of this vertex if a user has set one otherwise it returns the ID.
    ///
    /// # Returns
    ///
    /// The name of the vertex if one exists, otherwise the ID as a string.
    pub fn name(&self) -> String {
        match self.static_property("_id".to_string()) {
            None => self.id().to_string(),
            Some(prop) => prop.to_string(),
        }
    }

    pub fn property(&self, name: String, include_static: bool) -> Option<Prop> {
        let props = self.property_history(name.clone());
        match props.last() {
            None => {
                if include_static {
                    match self.graph.static_vertex_prop(self.vertex, name) {
                        None => None,
                        Some(prop) => Some(prop),
                    }
                } else {
                    None
                }
            }
            Some((_, prop)) => Some(prop.clone()),
        }
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
    pub fn property_history(&self, name: String) -> Vec<(i64, Prop)> {
        self.graph.temporal_vertex_prop_vec(self.vertex, name)
    }

    pub fn properties(&self, include_static: bool) -> HashMap<String, Prop> {
        let mut props: HashMap<String, Prop> = self
            .property_histories()
            .iter()
            .map(|(key, values)| (key.clone(), values.last().unwrap().1.clone()))
            .collect();

        if include_static {
            for prop_name in self.graph.static_vertex_prop_names(self.vertex) {
                match self
                    .graph
                    .static_vertex_prop(self.vertex, prop_name.clone())
                {
                    Some(prop) => {
                        props.insert(prop_name, prop);
                    }
                    None => {}
                }
            }
        }
        props
    }

    /// Get all temporal property values of this vertex.
    ///
    /// # Returns
    ///
    /// A HashMap with the names of the properties as keys and a vector of `(i64, Prop)` tuples
    /// as values. The `i64` value is the timestamp of the property value and `Prop`
    /// is the value itself.
    pub fn property_histories(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph.temporal_vertex_props(self.vertex)
    }

    pub fn property_names(&self, include_static: bool) -> Vec<String> {
        let mut names: Vec<String> = self.graph.temporal_vertex_prop_names(self.vertex);
        if include_static {
            names.extend(self.graph.static_vertex_prop_names(self.vertex))
        }
        names
    }

    pub fn has_property(&self, name: String, include_static: bool) -> bool {
        (! self.property_history(name.clone()).is_empty())
            || (include_static && self.graph.static_vertex_prop_names(self.vertex).contains(&name))
    }

    pub fn has_static_property(&self, name: String) -> bool {
        self.graph
            .static_vertex_prop_names(self.vertex)
            .contains(&name)
    }

    pub fn static_property(&self, name: String) -> Option<Prop> {
        self.graph.static_vertex_prop(self.vertex, name)
    }

    /// Get the degree of this vertex (i.e., the number of edges that are incident to it).
    ///
    /// # Returns
    ///
    /// The degree of this vertex.
    pub fn degree(&self) -> usize {
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
    pub fn degree_window(&self, t_start: i64, t_end: i64) -> usize {
        self.graph
            .degree_window(self.vertex, t_start, t_end, Direction::BOTH)
    }

    /// Get the in-degree of this vertex (i.e., the number of edges that point into it).
    ///
    /// # Returns
    ///
    /// The in-degree of this vertex.
    pub fn in_degree(&self) -> usize {
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
    pub fn in_degree_window(&self, t_start: i64, t_end: i64) -> usize {
        self.graph
            .degree_window(self.vertex, t_start, t_end, Direction::IN)
    }

    /// Get the out-degree of this vertex (i.e., the number of edges that point out of it).
    ///
    /// # Returns
    ///
    /// The out-degree of this vertex.
    pub fn out_degree(&self) -> usize {
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
    pub fn out_degree_window(&self, t_start: i64, t_end: i64) -> usize {
        self.graph
            .degree_window(self.vertex, t_start, t_end, Direction::OUT)
    }

    /// Get the edges that are incident to this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the edges that are incident to this vertex.
    pub fn edges(&self) -> Box<dyn Iterator<Item = EdgeView<G>> + Send> {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges(self.vertex, Direction::BOTH)
                .map(move |e| EdgeView::new(g.clone(), e)),
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
    pub fn edges_window(&self, t_start: i64, t_end: i64) -> EdgeList<G> {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges_window(self.vertex, t_start, t_end, Direction::BOTH)
                .map(move |e| EdgeView::new(g.clone(), e)),
        )
    }

    /// Get the edges that point into this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the edges that point into this vertex.
    pub fn in_edges(&self) -> EdgeList<G> {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges(self.vertex, Direction::IN)
                .map(move |e| EdgeView::new(g.clone(), e)),
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
    pub fn in_edges_window(&self, t_start: i64, t_end: i64) -> EdgeList<G> {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges_window(self.vertex, t_start, t_end, Direction::IN)
                .map(move |e| EdgeView::new(g.clone(), e)),
        )
    }

    /// Get the edges that point out of this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the edges that point out of this vertex.
    pub fn out_edges(&self) -> EdgeList<G> {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges(self.vertex, Direction::OUT)
                .map(move |e| EdgeView::new(g.clone(), e)),
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
    pub fn out_edges_window(&self, t_start: i64, t_end: i64) -> EdgeList<G> {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges_window(self.vertex, t_start, t_end, Direction::OUT)
                .map(move |e| EdgeView::new(g.clone(), e)),
        )
    }

    /// Get the neighbours of this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the neighbours of this vertex.
    pub fn neighbours(&self) -> PathFromVertex<G> {
        let g = self.graph.clone();
        PathFromVertex::new(
            g,
            self,
            Operations::Neighbours {
                dir: Direction::BOTH,
            },
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
    pub fn neighbours_window(&self, t_start: i64, t_end: i64) -> PathFromVertex<G> {
        let g = self.graph.clone();
        PathFromVertex::new(
            g,
            self,
            Operations::NeighboursWindow {
                dir: Direction::BOTH,
                t_start,
                t_end,
            },
        )
    }

    /// Get the neighbours of this vertex that point into this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the neighbours of this vertex that point into this vertex.
    pub fn in_neighbours(&self) -> PathFromVertex<G> {
        let g = self.graph.clone();
        PathFromVertex::new(g, self, Operations::Neighbours { dir: Direction::IN })
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
    pub fn in_neighbours_window(&self, t_start: i64, t_end: i64) -> PathFromVertex<G> {
        let g = self.graph.clone();
        PathFromVertex::new(
            g,
            self,
            Operations::NeighboursWindow {
                dir: Direction::IN,
                t_start,
                t_end,
            },
        )
    }

    /// Get the neighbours of this vertex that point out of this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the neighbours of this vertex that point out of this vertex.
    pub fn out_neighbours(&self) -> PathFromVertex<G> {
        let g = self.graph.clone();
        PathFromVertex::new(
            g,
            self,
            Operations::Neighbours {
                dir: Direction::OUT,
            },
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
    pub fn out_neighbours_window(&self, t_start: i64, t_end: i64) -> PathFromVertex<G> {
        let g = self.graph.clone();
        PathFromVertex::new(
            g,
            self,
            Operations::NeighboursWindow {
                dir: Direction::OUT,
                t_start,
                t_end,
            },
        )
    }
}

/// Implementation of the VertexListOps trait for an iterator of VertexView objects.
///
impl<G: GraphViewOps> VertexListOps for Box<dyn Iterator<Item = VertexView<G>> + Send> {
    type Graph = G;
    type IterType = Box<dyn Iterator<Item = VertexView<Self::Graph>> + Send>;
    type EList = Box<dyn Iterator<Item = EdgeView<Self::Graph>> + Send>;
    type ValueIterType<U> = Box<dyn Iterator<Item = U> + Send>;

    /// Get the vertex ids in this list.
    ///
    /// # Returns
    ///
    /// An iterator over the vertex ids in this list.
    fn id(self) -> Self::ValueIterType<u64> {
        Box::new(self.map(|v| v.id()))
    }

    fn name(self) -> Self::ValueIterType<String> {
        Box::new(self.map(|v| v.name()))
    }

    fn property(self, name: String, include_static: bool) -> Self::ValueIterType<Option<Prop>> {
        let r: Vec<_> = self
            .map(|v| v.property(name.clone(), include_static.clone()))
            .collect();
        Box::new(r.into_iter())
    }

    fn property_history(self, name: String) -> Self::ValueIterType<Vec<(i64, Prop)>> {
        let r: Vec<_> = self.map(|v| v.property_history(name.clone())).collect();
        Box::new(r.into_iter())
    }

    fn properties(self, include_static: bool) -> Self::ValueIterType<HashMap<String, Prop>> {
        let r: Vec<_> = self.map(|v| v.properties(include_static.clone())).collect();
        Box::new(r.into_iter())
    }

    fn property_histories(self) -> Self::ValueIterType<HashMap<String, Vec<(i64, Prop)>>> {
        let r: Vec<_> = self.map(|v| v.property_histories()).collect();
        Box::new(r.into_iter())
    }

    fn property_names(self, include_static: bool) -> Self::ValueIterType<Vec<String>> {
        let r: Vec<_> = self
            .map(|v| v.property_names(include_static.clone()))
            .collect();
        Box::new(r.into_iter())
    }

    fn has_property(self, name: String, include_static: bool) -> Self::ValueIterType<bool> {
        let r: Vec<_> = self
            .map(|v| v.has_property(name.clone(), include_static.clone()))
            .collect();
        Box::new(r.into_iter())
    }

    fn has_static_property(self, name: String) -> Self::ValueIterType<bool> {
        let r: Vec<_> = self.map(|v| v.has_static_property(name.clone())).collect();
        Box::new(r.into_iter())
    }

    fn static_property(self, name: String) -> Self::ValueIterType<Option<Prop>> {
        let r: Vec<_> = self.map(|v| v.static_property(name.clone())).collect();
        Box::new(r.into_iter())
    }

    /// Get the degree of this vertices
    ///
    /// # Returns
    ///
    /// An iterator over the degree of this vertices
    fn degree(self) -> Self::ValueIterType<usize> {
        let r: Vec<_> = self.map(|v| v.degree()).collect();
        Box::new(r.into_iter())
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
    fn degree_window(self, t_start: i64, t_end: i64) -> Self::ValueIterType<usize> {
        let r: Vec<_> = self.map(move |v| v.degree_window(t_start, t_end)).collect();
        Box::new(r.into_iter())
    }

    /// Get the in degree of these vertices
    ///
    /// # Returns
    ///
    /// An iterator over the in degree of these vertices
    fn in_degree(self) -> Self::ValueIterType<usize> {
        let r: Vec<_> = self.map(|v| v.in_degree()).collect();
        Box::new(r.into_iter())
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
    fn in_degree_window(self, t_start: i64, t_end: i64) -> Self::ValueIterType<usize> {
        let r: Vec<_> = self
            .map(move |v| v.in_degree_window(t_start, t_end))
            .collect();
        Box::new(r.into_iter())
    }

    /// Get the out degree of these vertices
    ///
    /// # Returns
    ///
    /// An iterator over the out degree of these vertices
    fn out_degree(self) -> Self::ValueIterType<usize> {
        let r: Vec<_> = self.map(|v| v.out_degree()).collect();
        Box::new(r.into_iter())
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
    fn out_degree_window(self, t_start: i64, t_end: i64) -> Self::ValueIterType<usize> {
        let r: Vec<_> = self
            .map(move |v| v.out_degree_window(t_start, t_end))
            .collect();
        Box::new(r.into_iter())
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

        assert_eq!(g.num_edges(), 701);
        assert_eq!(g.vertex("Gandalf").unwrap().degree(), 49);
        assert_eq!(g.vertex("Gandalf").unwrap().degree_window(1356, 24792), 34);
        assert_eq!(g.vertex("Gandalf").unwrap().in_degree(), 24);
        assert_eq!(
            g.vertex("Gandalf").unwrap().in_degree_window(1356, 24792),
            16
        );
        assert_eq!(g.vertex("Gandalf").unwrap().out_degree(), 35);
        assert_eq!(
            g.vertex("Gandalf").unwrap().out_degree_window(1356, 24792),
            20
        );
    }

    #[test]
    fn test_all_neighbours_window() {
        let g = crate::graph_loader::lotr_graph::lotr_graph(4);

        assert_eq!(g.num_edges(), 701);
        assert_eq!(g.vertex("Gandalf").unwrap().neighbours().iter().count(), 49);
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .neighbours_window(1356, 24792)
                .iter()
                .count(),
            34
        );
        assert_eq!(
            g.vertex("Gandalf").unwrap().in_neighbours().iter().count(),
            24
        );
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .in_neighbours_window(1356, 24792)
                .iter()
                .count(),
            16
        );
        assert_eq!(
            g.vertex("Gandalf").unwrap().out_neighbours().iter().count(),
            35
        );
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .out_neighbours_window(1356, 24792)
                .iter()
                .count(),
            20
        );
    }

    #[test]
    fn test_all_edges_window() {
        let g = crate::graph_loader::lotr_graph::lotr_graph(4);

        assert_eq!(g.num_edges(), 701);
        assert_eq!(g.vertex("Gandalf").unwrap().edges().count(), 59);
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .edges_window(1356, 24792)
                .count(),
            36
        );
        assert_eq!(g.vertex("Gandalf").unwrap().in_edges().count(), 24);
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .in_edges_window(1356, 24792)
                .count(),
            16
        );
        assert_eq!(g.vertex("Gandalf").unwrap().out_edges().count(), 35);
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .out_edges_window(1356, 24792)
                .count(),
            20
        );
    }
}
