use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, vertices::vertex_ref::VertexRef, LayerIds, EID, VID},
        Direction,
    },
    db::api::{
        properties::internal::InheritPropertiesOps,
        view::internal::{
            Base, EdgeFilter, EdgeFilterOps, GraphOps, Immutable, InheritCoreOps, InheritLayerOps,
            InheritMaterialize, InheritTimeSemantics, Static,
        },
    },
    prelude::GraphViewOps,
};
use itertools::Itertools;
use rayon::prelude::*;
use rustc_hash::FxHashSet;
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

#[derive(Clone)]
pub struct VertexSubgraph<G: GraphViewOps> {
    graph: G,
    vertices: Arc<FxHashSet<VID>>,
    edge_filter: EdgeFilter,
}

impl<G: GraphViewOps> Static for VertexSubgraph<G> {}

impl<G: GraphViewOps + Debug> Debug for VertexSubgraph<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VertexSubgraph")
            .field("graph", &self.graph)
            .field("vertices", &self.vertices)
            .finish()
    }
}

impl<G: GraphViewOps> Base for VertexSubgraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G: GraphViewOps> Immutable for VertexSubgraph<G> {}

impl<G: GraphViewOps> InheritCoreOps for VertexSubgraph<G> {}
impl<G: GraphViewOps> InheritTimeSemantics for VertexSubgraph<G> {}
impl<G: GraphViewOps> InheritPropertiesOps for VertexSubgraph<G> {}
impl<G: GraphViewOps> InheritMaterialize for VertexSubgraph<G> {}
impl<G: GraphViewOps> InheritLayerOps for VertexSubgraph<G> {}

impl<G: GraphViewOps> VertexSubgraph<G> {
    pub fn new(graph: G, vertices: FxHashSet<VID>) -> Self {
        let vertices = Arc::new(vertices);
        let vertices_cloned = vertices.clone();
        let edge_filter: EdgeFilter = match graph.edge_filter().cloned() {
            Some(f) => Arc::new(move |e, l| {
                vertices_cloned.contains(&e.src()) && vertices_cloned.contains(&e.dst()) && f(e, l)
            }),
            None => Arc::new(move |e, _l| {
                vertices_cloned.contains(&e.src()) && vertices_cloned.contains(&e.dst())
            }),
        };
        Self {
            graph,
            vertices,
            edge_filter,
        }
    }
}

impl<G: GraphViewOps> EdgeFilterOps for VertexSubgraph<G> {
    #[inline]
    fn edge_filter(&self) -> Option<&EdgeFilter> {
        Some(&self.edge_filter)
    }
}

impl<G: GraphViewOps> GraphOps for VertexSubgraph<G> {
    fn internal_vertex_ref(
        &self,
        v: VertexRef,
        layer_ids: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Option<VID> {
        self.graph
            .internal_vertex_ref(v, layer_ids, filter)
            .filter(|v| self.vertices.contains(v))
    }

    fn find_edge_id(
        &self,
        e_id: EID,
        layer_ids: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Option<EdgeRef> {
        self.graph
            .find_edge_id(e_id, layer_ids, filter)
            .filter(|e| self.vertices.contains(&e.src()) && self.vertices.contains(&e.dst()))
    }

    fn vertices_len(&self, _layer_ids: LayerIds, _filter: Option<&EdgeFilter>) -> usize {
        self.vertices.len()
    }

    fn edges_len(&self, layer: LayerIds, filter: Option<&EdgeFilter>) -> usize {
        self.vertices
            .par_iter()
            .map(|v| self.degree(*v, Direction::OUT, &layer, filter))
            .sum()
    }

    fn has_edge_ref(
        &self,
        src: VID,
        dst: VID,
        layer: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> bool {
        self.graph.has_edge_ref(src, dst, layer, filter)
    }

    fn has_vertex_ref(
        &self,
        v: VertexRef,
        layer_ids: &LayerIds,
        edge_filter: Option<&EdgeFilter>,
    ) -> bool {
        self.internal_vertex_ref(v, layer_ids, edge_filter)
            .is_some()
    }

    fn degree(&self, v: VID, d: Direction, layer: &LayerIds, filter: Option<&EdgeFilter>) -> usize {
        self.graph.degree(v, d, layer, filter)
    }

    fn vertex_ref(&self, v: u64, layers: &LayerIds, filter: Option<&EdgeFilter>) -> Option<VID> {
        self.internal_vertex_ref(v.into(), layers, filter)
    }

    fn vertex_refs(
        &self,
        _layers: LayerIds,
        _filter: Option<&EdgeFilter>,
    ) -> Box<dyn Iterator<Item = VID> + Send> {
        // this sucks but seems to be the only way currently (see also http://smallcultfollowing.com/babysteps/blog/2018/09/02/rust-pattern-iterating-an-over-a-rc-vec-t/)
        let verts = Vec::from_iter(self.vertices.iter().copied());
        Box::new(verts.into_iter())
    }

    fn edge_ref(
        &self,
        src: VID,
        dst: VID,
        layer: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Option<EdgeRef> {
        self.graph.edge_ref(src, dst, layer, filter)
    }

    fn edge_refs(
        &self,
        layer: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        let g1 = self.clone();
        let vertices = self.vertices.clone().iter().copied().collect_vec();
        let filter = filter.cloned();
        Box::new(
            vertices.into_iter().flat_map(move |v| {
                g1.vertex_edges(v, Direction::OUT, layer.clone(), filter.as_ref())
            }),
        )
    }

    fn vertex_edges(
        &self,
        v: VID,
        d: Direction,
        layer: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.graph.vertex_edges(v, d, layer, filter)
    }

    fn neighbours(
        &self,
        v: VID,
        d: Direction,
        layers: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Box<dyn Iterator<Item = VID> + Send> {
        self.graph.neighbours(v, d, layers, filter)
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
