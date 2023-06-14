use crate::core::edge_ref::EdgeRef;
use crate::core::vertex_ref::{LocalVertexRef, VertexRef};
use crate::core::{Direction, Prop};
use crate::db::view_api::internal::GraphViewInternalOps;
use crate::db::view_api::GraphViewOps;
use rustc_hash::FxHashSet;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct VertexSubgraph<G: GraphViewOps> {
    graph: G,
    vertices: Arc<FxHashSet<LocalVertexRef>>,
}

impl<G: GraphViewOps> VertexSubgraph<G> {
    pub(crate) fn new(graph: G, vertices: FxHashSet<LocalVertexRef>) -> Self {
        Self {graph, vertices: Arc::new(vertices)}
    }
}


impl<G: GraphViewOps> GraphViewInternalOps for VertexSubgraph<G> {
    fn local_vertex(&self, v: VertexRef) -> Option<LocalVertexRef> {
        self.graph
            .local_vertex(v)
            .filter(|v| self.vertices.contains(v))
    }

    fn local_vertex_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<LocalVertexRef> {
        self.graph
            .local_vertex_window(v, t_start, t_end)
            .filter(|v| self.vertices.contains(v))
    }

    fn get_unique_layers_internal(&self) -> Vec<usize> {
        self.graph.get_unique_layers_internal()
    }

    fn get_layer_name_by_id(&self, layer_id: usize) -> String {
        self.graph.get_layer_name_by_id(layer_id)
    }

    fn get_layer(&self, key: Option<&str>) -> Option<usize> {
        self.graph.get_layer(key)
    }

    fn view_start(&self) -> Option<i64> {
        self.graph.view_start()
    }

    fn view_end(&self) -> Option<i64> {
        self.graph.view_end()
    }

    fn earliest_time_global(&self) -> Option<i64> {
        self.vertices
            .iter()
            .flat_map(|v| self.graph.vertex_earliest_time(*v))
            .min()
    }

    fn earliest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.vertices
            .iter()
            .flat_map(|v| self.graph.vertex_earliest_time_window(*v, t_start, t_end))
            .min()
    }

    fn latest_time_global(&self) -> Option<i64> {
        self.vertices
            .iter()
            .flat_map(|v| self.graph.vertex_latest_time(*v))
            .max()
    }

    fn latest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.vertices
            .iter()
            .flat_map(|v| self.graph.vertex_latest_time_window(*v, t_start, t_end))
            .max()
    }

    fn vertices_len(&self) -> usize {
        self.vertices.len()
    }

    fn vertices_len_window(&self, t_start: i64, t_end: i64) -> usize {
        self.vertices
            .iter()
            .filter(|&&v| {
                self.graph
                    .has_vertex_ref_window(VertexRef::Local(v), t_start, t_end)
            })
            .count()
    }

    fn edges_len(&self, layer: Option<usize>) -> usize {
        self.vertices
            .iter()
            .map(|v| self.degree(*v, Direction::OUT, layer))
            .sum()
    }

    fn edges_len_window(&self, t_start: i64, t_end: i64, layer: Option<usize>) -> usize {
        self.vertices
            .iter()
            .map(|v| self.degree_window(*v, t_start, t_end, Direction::OUT, layer))
            .sum()
    }

    fn has_edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> bool {
        self.has_vertex_ref(src)
            && self.has_vertex_ref(dst)
            && self.graph.has_edge_ref(src, dst, layer)
    }

    fn has_edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
        layer: usize,
    ) -> bool {
        self.has_vertex_ref(src)
            && self.has_vertex_ref(dst)
            && self
                .graph
                .has_edge_ref_window(src, dst, t_start, t_end, layer)
    }

    fn has_vertex_ref(&self, v: VertexRef) -> bool {
        self.local_vertex(v).is_some()
    }

    fn has_vertex_ref_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> bool {
        self.local_vertex_window(v, t_start, t_end).is_some()
    }

    fn degree(&self, v: LocalVertexRef, d: Direction, layer: Option<usize>) -> usize {
        self.vertex_edges(v, d, layer).count()
    }

    fn degree_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> usize {
        self.vertex_edges_window(v, t_start, t_end, d, layer)
            .count()
    }

    fn vertex_ref(&self, v: u64) -> Option<LocalVertexRef> {
        self.local_vertex(v.into())
    }

    fn vertex_id(&self, v: LocalVertexRef) -> u64 {
        self.graph.vertex_id(v)
    }

    fn vertex_ref_window(&self, v: u64, t_start: i64, t_end: i64) -> Option<LocalVertexRef> {
        self.local_vertex_window(v.into(), t_start, t_end)
    }

    fn vertex_earliest_time(&self, v: LocalVertexRef) -> Option<i64> {
        self.vertex_edges(v, Direction::BOTH, None)
            .flat_map(|e| self.graph.edge_timestamps(e, None).first().copied())
            .min()
    }

    fn vertex_earliest_time_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<i64> {
        self.vertex_edges(v, Direction::BOTH, None)
            .flat_map(|e| {
                self.graph
                    .edge_timestamps(e, Some(t_start..t_end))
                    .first()
                    .copied()
            })
            .min()
    }

    fn vertex_latest_time(&self, v: LocalVertexRef) -> Option<i64> {
        self.vertex_edges(v, Direction::BOTH, None)
            .flat_map(|e| self.graph.edge_timestamps(e, None).last().copied())
            .max()
    }

    fn vertex_latest_time_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<i64> {
        self.vertex_edges(v, Direction::BOTH, None)
            .flat_map(|e| {
                self.graph
                    .edge_timestamps(e, Some(t_start..t_end))
                    .last()
                    .copied()
            })
            .max()
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        // this sucks but seems to be the only way currently (see also http://smallcultfollowing.com/babysteps/blog/2018/09/02/rust-pattern-iterating-an-over-a-rc-vec-t/)
        let verts = Vec::from_iter(self.vertices.iter().copied());
        Box::new(verts.into_iter())
    }

    fn vertex_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        let g = self.clone();
        Box::new(
            self.vertex_refs()
                .filter(move |&v| g.has_vertex_ref_window(VertexRef::Local(v), t_start, t_end)),
        )
    }

    fn vertex_refs_shard(&self, shard: usize) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        // FIXME: if keep shards, they need to support views (i.e., implement GraphViewInternalOps, this is terrible!)
        Box::new(self.vertex_refs().filter(move |&v| v.shard_id == shard))
    }

    fn vertex_refs_window_shard(
        &self,
        shard: usize,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        // FIXME: if keep shards, they need to support views (i.e., implement GraphViewInternalOps, this is terrible!)
        Box::new(
            self.vertex_refs_window(t_start, t_end)
                .filter(move |&v| v.shard_id == shard),
        )
    }

    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> Option<EdgeRef> {
        if self.has_vertex_ref(src) && self.has_vertex_ref(dst) {
            self.graph.edge_ref(src, dst, layer)
        } else {
            None
        }
    }

    fn edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
        layer: usize,
    ) -> Option<EdgeRef> {
        if self.has_vertex_ref(src) && self.has_vertex_ref(dst) {
            self.graph.edge_ref_window(src, dst, t_start, t_end, layer)
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

    fn edge_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        let g1 = self.clone();
        Box::new(
            self.vertex_refs().flat_map(move |v| {
                g1.vertex_edges_window(v, t_start, t_end, Direction::OUT, layer)
            }),
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

    fn vertex_edges_t(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        // FIXME: Could be improved if we had an edge_t function as it calls filter too many times
        let g = self.clone();
        Box::new(
            self.graph
                .vertex_edges_t(v, d, layer)
                .filter(move |&e| g.has_vertex_ref(e.remote())),
        )
    }

    fn vertex_edges_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        let g = self.clone();
        Box::new(
            self.graph
                .vertex_edges_window(v, t_start, t_end, d, layer)
                .filter(move |&e| g.has_vertex_ref(e.remote())),
        )
    }

    fn vertex_edges_window_t(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        // FIXME: Could be improved if we had an edge_t function as it calls filter too many times
        let g = self.clone();
        Box::new(
            self.graph
                .vertex_edges_window_t(v, t_start, t_end, d, layer)
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

    fn neighbours_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        Box::new(
            self.vertex_edges_window(v, t_start, t_end, d, layer)
                .map(|e| e.remote()),
        )
    }

    fn static_vertex_prop(&self, v: LocalVertexRef, name: String) -> Option<Prop> {
        self.graph.static_vertex_prop(v, name)
    }

    fn static_vertex_props(&self, v: LocalVertexRef) -> HashMap<String, Prop> {
        self.graph.static_vertex_props(v)
    }

    fn static_prop(&self, name: String) -> Option<Prop> {
        self.graph.static_prop(name)
    }

    fn static_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        self.graph.static_vertex_prop_names(v)
    }

    fn static_prop_names(&self) -> Vec<String> {
        self.graph.static_prop_names()
    }

    fn temporal_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        self.graph.temporal_vertex_prop_names(v)
    }

    fn temporal_prop_names(&self) -> Vec<String> {
        self.graph.temporal_prop_names()
    }

    fn temporal_vertex_prop_vec(&self, v: LocalVertexRef, name: String) -> Vec<(i64, Prop)> {
        self.graph.temporal_vertex_prop_vec(v, name)
    }

    fn temporal_prop_vec(&self, name: String) -> Vec<(i64, Prop)> {
        self.graph.temporal_prop_vec(name)
    }

    fn vertex_timestamps(&self, v: LocalVertexRef) -> Vec<i64> {
        self.graph.vertex_timestamps(v)
    }

    fn vertex_timestamps_window(&self, v: LocalVertexRef, t_start: i64, t_end: i64) -> Vec<i64> {
        self.graph.vertex_timestamps_window(v, t_start, t_end)
    }

    fn temporal_vertex_prop_vec_window(
        &self,
        v: LocalVertexRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph
            .temporal_vertex_prop_vec_window(v, name, t_start, t_end)
    }

    fn temporal_prop_vec_window(&self, name: String, t_start: i64, t_end: i64) -> Vec<(i64, Prop)> {
        self.graph.temporal_prop_vec_window(name, t_start, t_end)
    }

    fn temporal_vertex_props(&self, v: LocalVertexRef) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph.temporal_vertex_props(v)
    }

    fn temporal_props(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph.temporal_props()
    }

    fn temporal_vertex_props_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph.temporal_vertex_props_window(v, t_start, t_end)
    }

    fn temporal_props_window(&self, t_start: i64, t_end: i64) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph.temporal_props_window(t_start, t_end)
    }

    fn static_edge_prop(&self, e: EdgeRef, name: String) -> Option<Prop> {
        self.graph.static_edge_prop(e, name)
    }

    fn static_edge_props(&self, e: EdgeRef) -> HashMap<String, Prop> {
        self.graph.static_edge_props(e)
    }

    fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.graph.static_edge_prop_names(e)
    }

    fn temporal_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.graph.temporal_edge_prop_names(e)
    }

    fn temporal_edge_props_vec(&self, e: EdgeRef, name: String) -> Vec<(i64, Prop)> {
        self.graph.temporal_edge_props_vec(e, name)
    }

    fn temporal_edge_props_vec_window(
        &self,
        e: EdgeRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph
            .temporal_edge_props_vec_window(e, name, t_start, t_end)
    }

    fn edge_timestamps(&self, e: EdgeRef, window: Option<Range<i64>>) -> Vec<i64> {
        self.graph.edge_timestamps(e, window)
    }

    fn temporal_edge_props(&self, e: EdgeRef) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph.temporal_edge_props(e)
    }

    fn temporal_edge_props_window(
        &self,
        e: EdgeRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph.temporal_edge_props_window(e, t_start, t_end)
    }

    fn num_shards(&self) -> usize {
        self.graph.num_shards()
    }
}
