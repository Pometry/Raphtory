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
    pub(crate) fn new(graph: Arc<G>, vertex: VertexRef) -> VertexView<G> {
        VertexView { graph, vertex }
    }

    pub(crate) fn as_ref(&self) -> VertexRef {
        self.vertex
    }
}

impl<G: GraphViewInternalOps + 'static + Send + Sync> VertexViewOps for VertexView<G> {
    type Edge = EdgeView<G>;
    type VList = Box<dyn Iterator<Item = Self> + Send>;
    type EList = Box<dyn Iterator<Item = Self::Edge> + Send>;

    fn id(&self) -> u64 {
        self.vertex.g_id
    }

    fn prop(&self, name: String) -> Result<Vec<(i64, Prop)>, GraphError> {
        self.graph.temporal_vertex_prop_vec(self.vertex, name)
    }

    fn props(&self) -> Result<HashMap<String, Vec<(i64, Prop)>>, GraphError> {
        self.graph.temporal_vertex_props(self.vertex)
    }

    fn degree(&self) -> Result<usize, GraphError> {
        self.graph.degree(self.vertex, Direction::BOTH)
    }

    fn degree_window(&self, t_start: i64, t_end: i64) -> Result<usize, GraphError> {
        self.graph
            .degree_window(self.vertex, t_start, t_end, Direction::BOTH)
    }

    fn in_degree(&self) -> Result<usize, GraphError> {
        self.graph.degree(self.vertex, Direction::IN)
    }

    fn in_degree_window(&self, t_start: i64, t_end: i64) -> Result<usize, GraphError> {
        self.graph
            .degree_window(self.vertex, t_start, t_end, Direction::IN)
    }

    fn out_degree(&self) -> Result<usize, GraphError> {
        self.graph.degree(self.vertex, Direction::OUT)
    }

    fn out_degree_window(&self, t_start: i64, t_end: i64) -> Result<usize, GraphError> {
        self.graph
            .degree_window(self.vertex, t_start, t_end, Direction::OUT)
    }

    fn edges(&self) -> Self::EList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges(self.vertex, Direction::BOTH)
                .map(move |e| Self::Edge::new(g.clone(), e)),
        )
    }

    fn edges_window(&self, t_start: i64, t_end: i64) -> Self::EList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges_window(self.vertex, t_start, t_end, Direction::BOTH)
                .map(move |e| Self::Edge::new(g.clone(), e)),
        )
    }

    fn in_edges(&self) -> Self::EList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges(self.vertex, Direction::IN)
                .map(move |e| Self::Edge::new(g.clone(), e)),
        )
    }

    fn in_edges_window(&self, t_start: i64, t_end: i64) -> Self::EList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges_window(self.vertex, t_start, t_end, Direction::IN)
                .map(move |e| Self::Edge::new(g.clone(), e)),
        )
    }

    fn out_edges(&self) -> Self::EList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges(self.vertex, Direction::OUT)
                .map(move |e| Self::Edge::new(g.clone(), e)),
        )
    }

    fn out_edges_window(&self, t_start: i64, t_end: i64) -> Self::EList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges_window(self.vertex, t_start, t_end, Direction::OUT)
                .map(move |e| Self::Edge::new(g.clone(), e)),
        )
    }

    fn neighbours(&self) -> Self::VList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .neighbours(self.vertex, Direction::BOTH)
                .map(move |v| Self::new(g.clone(), v)),
        )
    }

    fn neighbours_window(&self, t_start: i64, t_end: i64) -> Self::VList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .neighbours_window(self.vertex, t_start, t_end, Direction::BOTH)
                .map(move |v| Self::new(g.clone(), v)),
        )
    }

    fn in_neighbours(&self) -> Self::VList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .neighbours(self.vertex, Direction::IN)
                .map(move |v| Self::new(g.clone(), v)),
        )
    }

    fn in_neighbours_window(&self, t_start: i64, t_end: i64) -> Self::VList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .neighbours_window(self.vertex, t_start, t_end, Direction::IN)
                .map(move |v| Self::new(g.clone(), v)),
        )
    }

    fn out_neighbours(&self) -> Self::VList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .neighbours(self.vertex, Direction::OUT)
                .map(move |v| Self::new(g.clone(), v)),
        )
    }

    fn out_neighbours_window(&self, t_start: i64, t_end: i64) -> Self::VList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .neighbours_window(self.vertex, t_start, t_end, Direction::OUT)
                .map(move |v| Self::new(g.clone(), v)),
        )
    }
}

impl<G: GraphViewInternalOps + 'static + Send + Sync> VertexListOps
    for Box<dyn Iterator<Item = VertexView<G>> + Send>
{
    type Vertex = VertexView<G>;
    type Edge = EdgeView<G>;
    type EList = Box<dyn Iterator<Item = Self::Edge> + Send>;
    type IterType = Box<dyn Iterator<Item = Self::Vertex> + Send>;
    type ValueIterType<U> = Box<dyn Iterator<Item = U> + Send>;

    fn id(self) -> Self::ValueIterType<u64> {
        Box::new(self.map(|v| v.id()))
    }

    fn prop(self, name: String) -> Result<Self::ValueIterType<Vec<(i64, Prop)>>, GraphError> {
        let r: Result<Vec<_>, _> = self.map(move |v| v.prop(name.clone())).collect();
        Ok(Box::new(r?.into_iter()))
    }

    fn props(self) -> Result<Self::ValueIterType<HashMap<String, Vec<(i64, Prop)>>>, GraphError> {
        let r: Result<Vec<_>, _> = self.map(|v| v.props()).collect();
        Ok(Box::new(r?.into_iter()))
    }

    fn degree(self) -> Result<Self::ValueIterType<usize>, GraphError> {
        let r: Result<Vec<_>, _> = self.map(|v| v.degree()).collect();
        Ok(Box::new(r?.into_iter()))
    }

    fn degree_window(
        self,
        t_start: i64,
        t_end: i64,
    ) -> Result<Self::ValueIterType<usize>, GraphError> {
        let r: Result<Vec<_>, _> = self.map(move |v| v.degree_window(t_start, t_end)).collect();
        Ok(Box::new(r?.into_iter()))
    }

    fn in_degree(self) -> Result<Self::ValueIterType<usize>, GraphError> {
        let r: Result<Vec<_>, _> = self.map(|v| v.in_degree()).collect();
        Ok(Box::new(r?.into_iter()))
    }

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

    fn out_degree(self) -> Result<Self::ValueIterType<usize>, GraphError> {
        let r: Result<Vec<_>, _> = self.map(|v| v.out_degree()).collect();
        Ok(Box::new(r?.into_iter()))
    }

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

    fn edges(self) -> Self::EList {
        Box::new(self.flat_map(|v| v.edges()))
    }

    fn edges_window(self, t_start: i64, t_end: i64) -> Self::EList {
        Box::new(self.flat_map(move |v| v.edges_window(t_start, t_end)))
    }

    fn in_edges(self) -> Self::EList {
        Box::new(self.flat_map(|v| v.in_edges()))
    }

    fn in_edges_window(self, t_start: i64, t_end: i64) -> Self::EList {
        Box::new(self.flat_map(move |v| v.in_edges_window(t_start, t_end)))
    }

    fn out_edges(self) -> Self::EList {
        Box::new(self.flat_map(|v| v.out_edges()))
    }

    fn out_edges_window(self, t_start: i64, t_end: i64) -> Self::EList {
        Box::new(self.flat_map(move |v| v.out_edges_window(t_start, t_end)))
    }

    fn neighbours(self) -> Self {
        Box::new(self.flat_map(|v| v.neighbours()))
    }

    fn neighbours_window(self, t_start: i64, t_end: i64) -> Self {
        Box::new(self.flat_map(move |v| v.neighbours_window(t_start, t_end)))
    }

    fn in_neighbours(self) -> Self {
        Box::new(self.flat_map(|v| v.in_neighbours()))
    }

    fn in_neighbours_window(self, t_start: i64, t_end: i64) -> Self {
        Box::new(self.flat_map(move |v| v.in_neighbours_window(t_start, t_end)))
    }

    fn out_neighbours(self) -> Self {
        Box::new(self.flat_map(|v| v.out_neighbours()))
    }

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
