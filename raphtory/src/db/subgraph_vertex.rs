use crate::core::edge_ref::EdgeRef;
use crate::core::vertex_ref::{LocalVertexRef, VertexRef};
use crate::core::{Direction, Prop};
use crate::db::view_api::internal::{GraphOps, InheritCoreOps, TimeSemantics};
use crate::db::view_api::{BoxedIter, GraphViewOps};
use itertools::Itertools;
use rayon::prelude::*;
use rustc_hash::FxHashSet;
use std::ops::Range;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct VertexSubgraph<G: GraphViewOps> {
    graph: G,
    vertices: Arc<FxHashSet<LocalVertexRef>>,
}

impl<G: GraphViewOps> InheritCoreOps for VertexSubgraph<G> {
    type Internal = G;

    fn graph(&self) -> &Self::Internal {
        &self.graph
    }
}

impl<G: GraphViewOps> VertexSubgraph<G> {
    pub(crate) fn new(graph: G, vertices: FxHashSet<LocalVertexRef>) -> Self {
        Self {
            graph,
            vertices: Arc::new(vertices),
        }
    }
}

impl<G: GraphViewOps> TimeSemantics for VertexSubgraph<G> {
    fn vertex_earliest_time(&self, v: LocalVertexRef) -> Option<i64> {
        self.vertex_edges(v, Direction::BOTH, None)
            .flat_map(|e| self.edge_earliest_time(e))
            .min()
    }

    fn vertex_latest_time(&self, v: LocalVertexRef) -> Option<i64> {
        self.vertex_edges(v, Direction::BOTH, None)
            .flat_map(|e| self.edge_latest_time(e))
            .max()
    }

    fn view_start(&self) -> Option<i64> {
        self.graph.view_start()
    }

    fn view_end(&self) -> Option<i64> {
        self.graph.view_end()
    }

    fn earliest_time_global(&self) -> Option<i64> {
        self.vertices
            .par_iter()
            .flat_map(|&v| self.vertex_earliest_time(v))
            .min()
    }

    fn latest_time_global(&self) -> Option<i64> {
        self.vertices
            .par_iter()
            .flat_map(|&v| self.vertex_latest_time(v))
            .max()
    }

    fn earliest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.vertices
            .par_iter()
            .flat_map(|&v| self.vertex_earliest_time_window(v, t_start, t_end))
            .min()
    }

    fn latest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.vertices
            .par_iter()
            .flat_map(|&v| self.vertex_latest_time_window(v, t_start, t_end))
            .max()
    }

    fn vertex_earliest_time_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<i64> {
        self.vertex_edges(v, Direction::BOTH, None)
            .flat_map(|e| self.edge_earliest_time_window(e, t_start..t_end))
            .min()
    }

    fn vertex_latest_time_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<i64> {
        self.vertex_edges(v, Direction::BOTH, None)
            .flat_map(|e| self.edge_latest_time_window(e, t_start..t_end))
            .max()
    }

    fn include_vertex_window(&self, v: LocalVertexRef, w: Range<i64>) -> bool {
        self.graph.include_vertex_window(v, w)
    }

    fn include_edge_window(&self, e: EdgeRef, w: Range<i64>) -> bool {
        self.graph.include_edge_window(e, w)
    }

    fn vertex_history(&self, v: LocalVertexRef) -> Vec<i64> {
        self.vertex_edges(v, Direction::BOTH, None)
            .map(|e| {
                self.graph
                    .edge_t(e)
                    .map(|e| e.time().expect("just exploded"))
            })
            .kmerge()
            .dedup()
            .collect()
    }

    fn vertex_history_window(&self, v: LocalVertexRef, w: Range<i64>) -> Vec<i64> {
        self.vertex_edges(v, Direction::BOTH, None)
            .map(move |e| {
                self.graph
                    .edge_window_t(e, w.clone())
                    .map(|e| e.time().expect("just exploded"))
            })
            .kmerge()
            .dedup()
            .collect()
    }

    fn edge_t(&self, e: EdgeRef) -> BoxedIter<EdgeRef> {
        self.graph.edge_t(e)
    }

    fn edge_window_t(&self, e: EdgeRef, w: Range<i64>) -> BoxedIter<EdgeRef> {
        self.graph.edge_window_t(e, w)
    }

    fn edge_earliest_time(&self, e: EdgeRef) -> Option<i64> {
        self.graph.edge_earliest_time(e)
    }

    fn edge_earliest_time_window(&self, e: EdgeRef, w: Range<i64>) -> Option<i64> {
        self.graph.edge_earliest_time_window(e, w)
    }

    fn edge_latest_time(&self, e: EdgeRef) -> Option<i64> {
        self.graph.edge_earliest_time(e)
    }

    fn edge_latest_time_window(&self, e: EdgeRef, w: Range<i64>) -> Option<i64> {
        self.graph.edge_latest_time_window(e, w)
    }

    fn temporal_prop_vec(&self, name: &str) -> Vec<(i64, Prop)> {
        self.graph.temporal_prop_vec(name)
    }

    fn temporal_prop_vec_window(&self, name: &str, t_start: i64, t_end: i64) -> Vec<(i64, Prop)> {
        self.graph.temporal_prop_vec_window(name, t_start, t_end)
    }

    fn temporal_vertex_prop_vec(&self, v: LocalVertexRef, name: &str) -> Vec<(i64, Prop)> {
        self.graph.temporal_vertex_prop_vec(v, name)
    }

    fn temporal_vertex_prop_vec_window(
        &self,
        v: LocalVertexRef,
        name: &str,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph
            .temporal_vertex_prop_vec_window(v, name, t_start, t_end)
    }

    fn temporal_edge_prop_vec_window(
        &self,
        e: EdgeRef,
        name: &str,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph
            .temporal_edge_prop_vec_window(e, name, t_start, t_end)
    }

    fn temporal_edge_prop_vec(&self, e: EdgeRef, name: &str) -> Vec<(i64, Prop)> {
        self.graph.temporal_edge_prop_vec(e, name)
    }
}

impl<G: GraphViewOps> GraphOps for VertexSubgraph<G> {
    fn local_vertex_ref(&self, v: VertexRef) -> Option<LocalVertexRef> {
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

    fn vertex_ref(&self, v: u64) -> Option<LocalVertexRef> {
        self.local_vertex_ref(v.into())
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
}
