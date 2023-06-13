use crate::core::edge_ref::EdgeRef;
use crate::core::timeindex::TimeIndex;
use crate::core::tprop::TProp;
use crate::core::vertex_ref::{LocalVertexRef, VertexRef};
use crate::core::{Direction, Prop};
use crate::db::view_api::internal::time_semantics::InheritTimeSemantics;
use crate::db::view_api::internal::GraphViewInternalOps;
use crate::db::view_api::GraphViewOps;
use rustc_hash::FxHashSet;
use std::collections::HashMap;
use std::iter;
use std::ops::Range;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct VertexSubgraph<G: GraphViewOps> {
    graph: G,
    vertices: Arc<FxHashSet<LocalVertexRef>>,
}

impl<G: GraphViewOps> VertexSubgraph<G> {
    pub(crate) fn new(graph: G, vertices: FxHashSet<LocalVertexRef>) -> Self {
        Self {
            graph,
            vertices: Arc::new(vertices),
        }
    }
}

impl<G: GraphViewOps> InheritTimeSemantics for VertexSubgraph<G> {
    type Internal = G;

    fn graph(&self) -> &Self::Internal {
        &self.graph
    }
}

impl<G: GraphViewOps> GraphViewInternalOps for VertexSubgraph<G> {
    fn local_vertex_ref(&self, v: VertexRef) -> Option<LocalVertexRef> {
        self.graph
            .local_vertex_ref(v)
            .filter(|v| self.vertices.contains(v))
    }

    fn get_unique_layers_internal(&self) -> Vec<usize> {
        self.graph.get_unique_layers_internal()
    }

    fn get_layer_name_by_id(&self, layer_id: usize) -> String {
        self.graph.get_layer_name_by_id(layer_id)
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

    fn vertex_ref(&self, v: u64) -> Option<LocalVertexRef> {
        self.local_vertex_ref(v.into())
    }

    fn vertex_id(&self, v: LocalVertexRef) -> u64 {
        self.graph.vertex_id(v)
    }

    fn vertex_earliest_time(&self, v: LocalVertexRef) -> Option<i64> {
        self.vertex_edges(v, Direction::BOTH, None)
            .flat_map(|e| self.graph.edge_timestamps(e, None).first().copied())
            .min()
    }

    fn vertex_latest_time(&self, v: LocalVertexRef) -> Option<i64> {
        self.vertex_edges(v, Direction::BOTH, None)
            .flat_map(|e| self.graph.edge_timestamps(e, None).last().copied())
            .max()
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        // this sucks but seems to be the only way currently (see also http://smallcultfollowing.com/babysteps/blog/2018/09/02/rust-pattern-iterating-an-over-a-rc-vec-t/)
        let verts = Vec::from_iter(self.vertices.iter().copied());
        Box::new(verts.into_iter())
    }

    fn vertex_refs_shard(&self, shard: usize) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        // FIXME: if keep shards, they need to support views (i.e., implement GraphViewInternalOps, this is terrible!)
        Box::new(self.vertex_refs().filter(move |&v| v.shard_id == shard))
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
        v: LocalVertexRef,
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
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        Box::new(self.vertex_edges(v, d, layer).map(|e| e.remote()))
    }

    fn static_vertex_prop(&self, v: LocalVertexRef, name: String) -> Option<Prop> {
        self.graph.static_vertex_prop(v, name)
    }

    fn static_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        self.graph.static_vertex_prop_names(v)
    }

    fn temporal_vertex_prop(&self, v: LocalVertexRef, name: String) -> Option<&TProp> {
        todo!()
    }

    fn temporal_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        self.graph.temporal_vertex_prop_names(v)
    }

    fn static_edge_prop(&self, e: EdgeRef, name: String) -> Option<Prop> {
        self.graph.static_edge_prop(e, name)
    }

    fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.graph.static_edge_prop_names(e)
    }

    fn temporal_edge_prop(&self, e: EdgeRef, name: String) -> Option<&TProp> {
        todo!()
    }

    fn temporal_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.graph.temporal_edge_prop_names(e)
    }

    fn num_shards(&self) -> usize {
        self.graph.num_shards()
    }
}
