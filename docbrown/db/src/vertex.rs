use crate::edge::EdgeView;
use crate::view_api::internal::GraphViewInternalOps;
use crate::view_api::{VertexListOps, VertexViewOps};
use docbrown_core::tgraph::VertexRef;
use docbrown_core::{Direction, Prop};
use std::collections::HashMap;
use std::sync::Arc;

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

    fn prop(&self, name: String) -> Vec<(i64, Prop)> {
        self.graph.vertex_prop_vec(self.vertex, name)
    }

    fn props(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph.vertex_props(self.vertex)
    }

    fn degree(&self) -> usize {
        self.graph.degree(self.vertex, Direction::BOTH)
    }

    fn in_degree(&self) -> usize {
        self.graph.degree(self.vertex, Direction::IN)
    }

    fn out_degree(&self) -> usize {
        self.graph.degree(self.vertex, Direction::OUT)
    }

    fn edges(&self) -> Self::EList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges(self.vertex, Direction::BOTH)
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

    fn out_edges(&self) -> Self::EList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges(self.vertex, Direction::OUT)
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

    fn in_neighbours(&self) -> Self::VList {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .neighbours(self.vertex, Direction::IN)
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

    fn prop(self, name: String) -> Self::ValueIterType<Vec<(i64, Prop)>> {
        Box::new(self.map(move |v| v.prop(name.clone())))
    }

    fn props(self) -> Self::ValueIterType<HashMap<String, Vec<(i64, Prop)>>> {
        Box::new(self.map(|v| v.props()))
    }

    fn degree(self) -> Self::ValueIterType<usize> {
        Box::new(self.map(|v| v.degree()))
    }

    fn in_degree(self) -> Self::ValueIterType<usize> {
        Box::new(self.map(|v| v.in_degree()))
    }

    fn out_degree(self) -> Self::ValueIterType<usize> {
        Box::new(self.map(|v| v.out_degree()))
    }

    fn edges(self) -> Self::EList {
        Box::new(self.flat_map(|v| v.edges()))
    }

    fn in_edges(self) -> Self::EList {
        Box::new(self.flat_map(|v| v.in_edges()))
    }

    fn out_edges(self) -> Self::EList {
        Box::new(self.flat_map(|v| v.out_edges()))
    }

    fn neighbours(self) -> Self {
        Box::new(self.flat_map(|v| v.neighbours()))
    }

    fn in_neighbours(self) -> Self {
        Box::new(self.flat_map(|v| v.in_neighbours()))
    }

    fn out_neighbours(self) -> Self {
        Box::new(self.flat_map(|v| v.out_neighbours()))
    }
}
