use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, vertices::vertex_ref::VertexRef, LayerIds, EID, VID},
        Direction,
    },
    db::api::{
        properties::internal::InheritPropertiesOps,
        view::{
            internal::{Base, GraphOps, InheritCoreOps, InheritMaterialize, InheritTimeSemantics},
            Layer,
        },
    },
    prelude::GraphViewOps,
};
use itertools::Itertools;
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
impl<G: GraphViewOps> InheritPropertiesOps for VertexSubgraph<G> {}

impl<G: GraphViewOps> InheritMaterialize for VertexSubgraph<G> {}

impl<G: GraphViewOps> VertexSubgraph<G> {
    pub fn new(graph: G, vertices: FxHashSet<VID>) -> Self {
        Self {
            graph,
            vertices: Arc::new(vertices),
        }
    }
}

impl<G: GraphViewOps> GraphOps for VertexSubgraph<G> {
    fn layer_ids(&self) -> LayerIds {
        self.graph.layer_ids()
    }

    fn local_vertex_ref(&self, v: VertexRef) -> Option<VID> {
        self.graph
            .local_vertex_ref(v)
            .filter(|v| self.vertices.contains(v))
    }

    fn find_edge_id(&self, e_id: EID) -> Option<EdgeRef> {
        let edge_ref = self.graph.find_edge_id(e_id)?;
        let vid_src = self.local_vertex_ref(edge_ref.src())?;
        let vid_dst = self.local_vertex_ref(edge_ref.dst())?;

        if self.vertices.contains(&vid_src) && self.vertices.contains(&vid_dst) {
            Some(edge_ref)
        } else {
            None
        }
    }

    fn layer_ids_from_names(&self, key: Layer) -> LayerIds {
        self.graph.layer_ids_from_names(key)
    }

    fn edge_layer_ids(&self, e_id: EID) -> LayerIds {
        self.graph.edge_layer_ids(e_id)
    }

    fn vertices_len(&self) -> usize {
        self.vertices.len()
    }

    fn edges_len(&self, layer: LayerIds) -> usize {
        self.vertices
            .iter()
            .map(|v| self.degree(*v, Direction::OUT, layer.clone()))
            .sum()
    }

    fn has_edge_ref(&self, src: VertexRef, dst: VertexRef, layer: LayerIds) -> bool {
        self.has_vertex_ref(src)
            && self.has_vertex_ref(dst)
            && self.graph.has_edge_ref(src, dst, layer)
    }

    fn has_vertex_ref(&self, v: VertexRef) -> bool {
        self.local_vertex_ref(v).is_some()
    }

    fn degree(&self, v: VID, d: Direction, layer: LayerIds) -> usize {
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

    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: LayerIds) -> Option<EdgeRef> {
        if self.has_vertex_ref(src) && self.has_vertex_ref(dst) {
            self.graph.edge_ref(src, dst, layer)
        } else {
            None
        }
    }

    fn edge_refs(&self, layer: LayerIds) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        let g1 = self.clone();
        Box::new(
            self.vertex_refs()
                .flat_map(move |v| g1.vertex_edges(v, Direction::OUT, layer.clone())),
        )
    }

    fn vertex_edges(
        &self,
        v: VID,
        d: Direction,
        layer: LayerIds,
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
        layers: LayerIds,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        match d {
            Direction::BOTH => Box::new(
                self.neighbours(v, Direction::IN, layers.clone())
                    .merge(self.neighbours(v, Direction::OUT, layers))
                    .dedup(),
            ),
            _ => Box::new(self.vertex_edges(v, d, layers).map(|e| e.remote())),
        }
    }
}

#[cfg(test)]
mod subgraph_tests {
    use crate::{algorithms::triangle_count::triangle_count, prelude::*};

    #[test]
    fn test_materialize_no_edges() {
        let g = Graph::new();

        g.add_vertex(1, 1, NO_PROPS).unwrap();
        g.add_vertex(2, 2, NO_PROPS).unwrap();
        let sg = g.subgraph([1, 2]);

        let actual = sg.materialize().unwrap().into_events().unwrap();
        assert_eq!(actual, sg);
    }

    #[test]
    fn test_remove_degree1_triangle_count() {
        let graph = Graph::new();
        let edges = vec![
            (1, 2, 1),
            (1, 3, 2),
            (1, 4, 3),
            (3, 1, 4),
            (3, 4, 5),
            (3, 5, 6),
            (4, 5, 7),
            (5, 6, 8),
            (5, 8, 9),
            (7, 5, 10),
            (8, 5, 11),
            (1, 9, 12),
            (9, 1, 13),
            (6, 3, 14),
            (4, 8, 15),
            (8, 3, 16),
            (5, 10, 17),
            (10, 5, 18),
            (10, 8, 19),
            (1, 11, 20),
            (11, 1, 21),
            (9, 11, 22),
            (11, 9, 23),
        ];
        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }
        let subgraph = graph.subgraph(graph.vertices().into_iter().filter(|v| v.degree() > 1));
        let ts = triangle_count(&subgraph, None);
        let tg = triangle_count(&graph, None);
        assert_eq!(ts, tg)
    }
}
