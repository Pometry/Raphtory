use docbrown_core::tgraph::{EdgeRef, VertexRef};
use docbrown_core::{Direction, Prop};
use docbrown_db::graph::Graph;
use docbrown_db::view_api::internal::GraphViewInternalOps;
use docbrown_db::view_api::GraphViewOps;
use std::collections::HashMap;
use std::sync::Arc;

trait DynamicGraphView: GraphViewInternalOps + Send + Sync + 'static {}

impl<G: GraphViewInternalOps + Send + Sync + 'static> DynamicGraphView for G {}

#[derive(Clone)]
pub struct DynamicGraph(Arc<dyn DynamicGraphView>);

impl DynamicGraph {
    pub fn new<G: GraphViewOps>(graph: G) -> DynamicGraph {
        Self(Arc::new(graph))
    }
}

impl From<Graph> for DynamicGraph {
    fn from(value: Graph) -> Self {
        Self(Arc::new(value))
    }
}

impl GraphViewInternalOps for DynamicGraph {
    fn view_start(&self) -> Option<i64> {
        self.0.view_start()
    }

    fn view_end(&self) -> Option<i64> {
        self.0.view_end()
    }

    fn earliest_time_global(&self) -> Option<i64> {
        self.0.earliest_time_global()
    }

    fn earliest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.0.earliest_time_window(t_start, t_end)
    }

    fn latest_time_global(&self) -> Option<i64> {
        self.0.latest_time_global()
    }

    fn latest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.0.latest_time_window(t_start, t_end)
    }

    fn vertices_len(&self) -> usize {
        self.0.vertices_len()
    }

    fn vertices_len_window(&self, t_start: i64, t_end: i64) -> usize {
        self.0.vertices_len_window(t_start, t_end)
    }

    fn edges_len(&self) -> usize {
        self.0.edges_len()
    }

    fn edges_len_window(&self, t_start: i64, t_end: i64) -> usize {
        self.0.edges_len_window(t_start, t_end)
    }

    fn has_edge_ref(&self, src: VertexRef, dst: VertexRef) -> bool {
        self.0.has_edge_ref(src, dst)
    }

    fn has_edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
    ) -> bool {
        self.0.has_edge_ref_window(src, dst, t_start, t_end)
    }

    fn has_vertex_ref(&self, v: VertexRef) -> bool {
        self.0.has_vertex_ref(v)
    }

    fn has_vertex_ref_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> bool {
        self.0.has_vertex_ref_window(v, t_start, t_end)
    }

    fn degree(&self, v: VertexRef, d: Direction) -> usize {
        self.0.degree(v, d)
    }

    fn degree_window(&self, v: VertexRef, t_start: i64, t_end: i64, d: Direction) -> usize {
        self.0.degree_window(v, t_start, t_end, d)
    }

    fn vertex_ref(&self, v: u64) -> Option<VertexRef> {
        self.0.vertex_ref(v)
    }

    fn vertex_ref_window(&self, v: u64, t_start: i64, t_end: i64) -> Option<VertexRef> {
        self.0.vertex_ref_window(v, t_start, t_end)
    }

    fn vertex_earliest_time(&self, v: VertexRef) -> Option<i64> {
        self.0.vertex_earliest_time(v)
    }

    fn vertex_earliest_time_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> Option<i64> {
        self.0.vertex_latest_time_window(v, t_start, t_end)
    }

    fn vertex_latest_time(&self, v: VertexRef) -> Option<i64> {
        self.0.vertex_latest_time(v)
    }

    fn vertex_latest_time_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> Option<i64> {
        self.0.vertex_latest_time_window(v, t_start, t_end)
    }

    fn vertex_ids(&self) -> Box<dyn Iterator<Item = u64> + Send> {
        self.0.vertex_ids()
    }

    fn vertex_ids_window(&self, t_start: i64, t_end: i64) -> Box<dyn Iterator<Item = u64> + Send> {
        self.0.vertex_ids_window(t_start, t_end)
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.0.vertex_refs()
    }

    fn vertex_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.0.vertex_refs_window(t_start, t_end)
    }

    fn vertex_refs_shard(&self, shard: usize) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.0.vertex_refs_shard(shard)
    }

    fn vertex_refs_window_shard(
        &self,
        shard: usize,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.0.vertex_refs_window_shard(shard, t_start, t_end)
    }

    fn edge_ref(&self, src: VertexRef, dst: VertexRef) -> Option<EdgeRef> {
        self.0.edge_ref(src, dst)
    }

    fn edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<EdgeRef> {
        self.0.edge_ref_window(src, dst, t_start, t_end)
    }

    fn edge_refs(&self) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.0.edge_refs()
    }

    fn edge_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.0.edge_refs_window(t_start, t_end)
    }

    fn vertex_edges(&self, v: VertexRef, d: Direction) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.0.vertex_edges(v, d)
    }

    fn vertex_edges_t(
        &self,
        v: VertexRef,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.0.vertex_edges_t(v, d)
    }

    fn vertex_edges_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.0.vertex_edges_window(v, t_start, t_end, d)
    }

    fn vertex_edges_window_t(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.0.vertex_edges_window_t(v, t_start, t_end, d)
    }

    fn neighbours(&self, v: VertexRef, d: Direction) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.0.neighbours(v, d)
    }

    fn neighbours_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.0.neighbours_window(v, t_start, t_end, d)
    }

    fn neighbours_ids(&self, v: VertexRef, d: Direction) -> Box<dyn Iterator<Item = u64> + Send> {
        self.0.neighbours_ids(v, d)
    }

    fn neighbours_ids_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send> {
        self.0.neighbours_ids_window(v, t_start, t_end, d)
    }

    fn static_vertex_prop(&self, v: VertexRef, name: String) -> Option<Prop> {
        self.0.static_vertex_prop(v, name)
    }

    fn static_vertex_prop_names(&self, v: VertexRef) -> Vec<String> {
        self.0.static_vertex_prop_names(v)
    }

    fn temporal_vertex_prop_names(&self, v: VertexRef) -> Vec<String> {
        self.0.temporal_vertex_prop_names(v)
    }

    fn temporal_vertex_prop_vec(&self, v: VertexRef, name: String) -> Vec<(i64, Prop)> {
        self.0.temporal_vertex_prop_vec(v, name)
    }

    fn temporal_vertex_prop_vec_window(
        &self,
        v: VertexRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.0
            .temporal_vertex_prop_vec_window(v, name, t_start, t_end)
    }

    fn temporal_vertex_props(&self, v: VertexRef) -> HashMap<String, Vec<(i64, Prop)>> {
        self.0.temporal_vertex_props(v)
    }

    fn temporal_vertex_props_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        self.0.temporal_vertex_props_window(v, t_start, t_end)
    }

    fn static_edge_prop(&self, e: EdgeRef, name: String) -> Option<Prop> {
        self.0.static_edge_prop(e, name)
    }

    fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.0.static_edge_prop_names(e)
    }

    fn temporal_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.0.temporal_edge_prop_names(e)
    }

    fn temporal_edge_props_vec(&self, e: EdgeRef, name: String) -> Vec<(i64, Prop)> {
        self.0.temporal_edge_props_vec(e, name)
    }

    fn temporal_edge_props_vec_window(
        &self,
        e: EdgeRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.0
            .temporal_edge_props_vec_window(e, name, t_start, t_end)
    }

    fn temporal_edge_props(&self, e: EdgeRef) -> HashMap<String, Vec<(i64, Prop)>> {
        self.0.temporal_edge_props(e)
    }

    fn temporal_edge_props_window(
        &self,
        e: EdgeRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        self.0.temporal_edge_props_window(e, t_start, t_end)
    }

    fn num_shards(&self) -> usize {
        self.0.num_shards()
    }

    fn vertices_shard(&self, shard_id: usize) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.0.vertices_shard(shard_id)
    }

    fn vertices_shard_window(
        &self,
        shard_id: usize,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.0.vertices_shard_window(shard_id, t_start, t_end)
    }
}
