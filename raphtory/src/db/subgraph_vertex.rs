use crate::core::edge_ref::EdgeRef;
use crate::core::tgraph2::VID;
use crate::core::vertex_ref::VertexRef;
use crate::core::{Direction, Prop};
use crate::db::view_api::internal::{
    Base, GraphOps, InheritCoreOps, InheritMaterialize, InheritTimeSemantics,
};
use crate::db::view_api::GraphViewOps;
use rustc_hash::FxHashSet;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct VertexSubgraph<G: GraphViewOps> {
    graph: G,
    vertices: Arc<FxHashSet<VID>>,
}

impl<G: GraphViewOps> Base for VertexSubgraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G: GraphViewOps> InheritCoreOps for VertexSubgraph<G> {}

impl<G: GraphViewOps> InheritTimeSemantics for VertexSubgraph<G> {}

impl<G: GraphViewOps> InheritMaterialize for VertexSubgraph<G> {}

impl<G: GraphViewOps> VertexSubgraph<G> {
    pub(crate) fn new(graph: G, vertices: FxHashSet<VID>) -> Self {
        Self {
            graph,
            vertices: Arc::new(vertices),
        }
    }
}

impl<G: GraphViewOps> GraphOps for VertexSubgraph<G> {
    fn local_vertex_ref(&self, v: VertexRef) -> Option<VID> {
        self.graph
            .local_vertex_ref(v)
            .filter(|v| self.vertices.contains(v))
    }

    fn get_unique_layers_internal(&self) -> Vec<usize> {
        self.graph.get_unique_layers_internal()
    }

    fn get_layer_id(&self, key: Option<&str>) -> Option<usize> {
        self.graph.get_layer_id(key)
    }

    fn vertices_len(&self) -> usize {
        self.vertices.len()
    }

    fn edges_len(&self, layer: Option<usize>) -> usize {
        self.vertices
            .iter()
            .map(|v| self.degree(*v, Direction::OUT, layer))
            .sum()
    }

    fn has_edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> bool {
        self.has_vertex_ref(src)
            && self.has_vertex_ref(dst)
            && self.graph.has_edge_ref(src, dst, layer)
    }

    fn has_vertex_ref(&self, v: VertexRef) -> bool {
        self.local_vertex_ref(v).is_some()
    }

    fn degree(&self, v: LocalVertexRef, d: Direction, layer: Option<usize>) -> usize {
        self.vertex_edges(v, d, layer).count()
    }

    fn vertex_ref(&self, v: u64) -> Option<VID> {
        self.local_vertex_ref(v.into())
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VID> + Send> {
        // this sucks but seems to be the only way currently (see also http://smallcultfollowing.com/babysteps/blog/2018/09/02/rust-pattern-iterating-an-over-a-rc-vec-t/)
        let verts = Vec::from_iter(self.vertices.iter().copied());
        Box::new(verts.into_iter())
    }

    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> Option<EdgeRef> {
        if self.has_vertex_ref(src) && self.has_vertex_ref(dst) {
            self.graph.edge_ref(src, dst, layer)
        } else {
            None
        }
    }

    fn edge_refs(&self, layer: Option<usize>) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        let g1 = self.clone();
        Box::new(
            self.vertex_refs()
                .flat_map(move |v| g1.vertex_edges(v, Direction::OUT, layer)),
        )
    }

    fn vertex_edges(
        &self,
        v: VID,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        let g = self.clone();
        Box::new(
            self.graph
                .vertex_edges(v, d, layer)
                .filter(move |&e| g.has_vertex_ref(e.remote())),
        )
    }

    fn neighbours(
        &self,
        v: VID,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        Box::new(self.vertex_edges(v, d, layer).map(|e| e.remote()))
    }
}

#[cfg(test)]
mod subgraph_tests {
    use crate::db::graph::Graph;
    use crate::db::mutation_api::AdditionOps;
    use crate::db::view_api::*;

    #[test]
    fn test_materialize_no_edges() {
        let g = Graph::new(1);

        g.add_vertex(1, 1, []).unwrap();
        g.add_vertex(2, 2, []).unwrap();
        let sg = g.subgraph([1, 2]);
        assert_eq!(sg.materialize().unwrap().into_events().unwrap(), sg);
    }
}
