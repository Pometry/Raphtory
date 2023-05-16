use std::{collections::HashMap, ops::Range};

use raphtory::{
    core::{
        edge_ref::EdgeRef,
        vertex_ref::{LocalVertexRef, VertexRef},
        Direction, Prop,
    },
    db::view_api::{internal::GraphViewInternalOps, GraphViewOps},
};

use super::Graph;

impl GraphViewInternalOps for Graph {
    fn get_unique_layers_internal(&self) -> Vec<usize> {
        self.0.graph().get_unique_layers_internal()
    }

    fn get_layer(&self, key: Option<&str>) -> Option<usize> {
        self.0.graph().get_layer(key)
    }

    fn get_layer_name_by_id(&self, layer_id: usize) -> String {
        self.0.graph().get_layer_name_by_id(layer_id)
    }

    fn view_start(&self) -> Option<i64> {
        self.0.graph().view_start()
    }

    fn view_end(&self) -> Option<i64> {
        self.0.graph().view_end()
    }

    fn earliest_time_global(&self) -> Option<i64> {
        self.0.graph().earliest_time_global()
    }

    fn earliest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.0.graph().earliest_time_window(t_start, t_end)
    }

    fn latest_time_global(&self) -> Option<i64> {
        self.0.graph().latest_time_global()
    }

    fn latest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.0.graph().latest_time_window(t_start, t_end)
    }

    fn vertices_len(&self) -> usize {
        self.0.graph().vertices_len()
    }

    fn vertices_len_window(&self, t_start: i64, t_end: i64) -> usize {
        self.0.graph().vertices_len_window(t_start, t_end)
    }

    fn edges_len(&self, layer: Option<usize>) -> usize {
        self.0.graph().edges_len(layer)
    }

    fn edges_len_window(&self, t_start: i64, t_end: i64, layer: Option<usize>) -> usize {
        self.0.graph().edges_len_window(t_start, t_end, layer)
    }

    fn has_edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> bool {
        self.0.graph().has_edge_ref(src, dst, layer)
    }

    fn has_edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
        layer: usize,
    ) -> bool {
        self.0
            .graph()
            .has_edge_ref_window(src, dst, t_start, t_end, layer)
    }

    fn has_vertex_ref(&self, v: VertexRef) -> bool {
        self.0.graph().has_vertex_ref(v)
    }

    fn has_vertex_ref_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> bool {
        self.0.graph().has_vertex_ref_window(v, t_start, t_end)
    }

    fn degree(&self, v: LocalVertexRef, d: Direction, layer: Option<usize>) -> usize {
        self.0.graph().degree(v, d, layer)
    }

    fn vertex_timestamps(&self, v: LocalVertexRef) -> Vec<i64> {
        self.0.graph().vertex_timestamps(v)
    }

    fn vertex_timestamps_window(&self, v: LocalVertexRef, t_start: i64, t_end: i64) -> Vec<i64> {
        self.0.graph().vertex_timestamps_window(v, t_start, t_end)
    }

    fn edge_timestamps(&self, e: EdgeRef, window: Option<Range<i64>>) -> Vec<i64> {
        self.0.graph().edge_timestamps(e, window)
    }

    fn degree_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> usize {
        self.0.graph().degree_window(v, t_start, t_end, d, layer)
    }

    fn vertex_ref(&self, v: u64) -> Option<LocalVertexRef> {
        self.0.graph().vertex_ref(v)
    }

    fn vertex_ref_window(&self, v: u64, t_start: i64, t_end: i64) -> Option<LocalVertexRef> {
        self.0.graph().vertex_ref_window(v, t_start, t_end)
    }

    fn vertex_earliest_time(&self, v: LocalVertexRef) -> Option<i64> {
        self.0.graph().vertex_earliest_time(v)
    }

    fn vertex_earliest_time_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<i64> {
        self.0
            .graph()
            .vertex_earliest_time_window(v, t_start, t_end)
    }

    fn vertex_latest_time(&self, v: LocalVertexRef) -> Option<i64> {
        self.0.graph().vertex_latest_time(v)
    }

    fn vertex_latest_time_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<i64> {
        self.0.graph().vertex_latest_time_window(v, t_start, t_end)
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        self.0.graph().vertex_refs()
    }

    fn vertex_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        self.0.graph().vertex_refs_window(t_start, t_end)
    }

    fn vertex_refs_shard(&self, shard: usize) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        self.0.graph().vertex_refs_shard(shard)
    }

    fn vertex_refs_window_shard(
        &self,
        shard: usize,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        self.0
            .graph()
            .vertex_refs_window_shard(shard, t_start, t_end)
    }

    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> Option<EdgeRef> {
        self.0.graph().edge_ref(src, dst, layer)
    }

    fn edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
        layer: usize,
    ) -> Option<EdgeRef> {
        self.0
            .graph()
            .edge_ref_window(src, dst, t_start, t_end, layer)
    }

    fn edge_refs(&self, layer: Option<usize>) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.0.graph().edge_refs(layer)
    }

    fn edge_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.0.graph().edge_refs_window(t_start, t_end, layer)
    }

    fn vertex_edges_t(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.0.graph().vertex_edges_t(v, d, layer)
    }

    fn vertex_edges_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.0
            .graph()
            .vertex_edges_window(v, t_start, t_end, d, layer)
    }

    fn vertex_edges_window_t(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.0
            .graph()
            .vertex_edges_window_t(v, t_start, t_end, d, layer)
    }

    fn neighbours(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.0.graph().neighbours(v, d, layer)
    }

    fn neighbours_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.0
            .graph()
            .neighbours_window(v, t_start, t_end, d, layer)
    }

    fn static_vertex_prop(&self, v: LocalVertexRef, name: String) -> Option<Prop> {
        self.0.graph().static_vertex_prop(v, name)
    }

    fn static_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        self.0.graph().static_vertex_prop_names(v)
    }

    fn temporal_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        self.0.graph().temporal_vertex_prop_names(v)
    }

    fn temporal_vertex_prop_vec(&self, v: LocalVertexRef, name: String) -> Vec<(i64, Prop)> {
        self.0.graph().temporal_vertex_prop_vec(v, name)
    }

    fn temporal_vertex_prop_vec_window(
        &self,
        v: LocalVertexRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.0
            .graph()
            .temporal_vertex_prop_vec_window(v, name, t_start, t_end)
    }

    fn temporal_vertex_props(&self, v: LocalVertexRef) -> HashMap<String, Vec<(i64, Prop)>> {
        self.0.graph().temporal_vertex_props(v)
    }

    fn temporal_vertex_props_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        self.0
            .graph()
            .temporal_vertex_props_window(v, t_start, t_end)
    }

    fn static_edge_prop(&self, e: EdgeRef, name: String) -> Option<Prop> {
        self.0.graph().static_edge_prop(e, name)
    }

    fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.0.graph().static_edge_prop_names(e)
    }

    fn temporal_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.0.graph().temporal_edge_prop_names(e)
    }

    fn temporal_edge_props_vec(&self, e: EdgeRef, name: String) -> Vec<(i64, Prop)> {
        self.0.graph().temporal_edge_props_vec(e, name)
    }

    fn temporal_edge_props_vec_window(
        &self,
        e: EdgeRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.0
            .graph()
            .temporal_edge_props_vec_window(e, name, t_start, t_end)
    }

    fn temporal_edge_props(&self, e: EdgeRef) -> HashMap<String, Vec<(i64, Prop)>> {
        self.0.graph().temporal_edge_props(e)
    }

    fn temporal_edge_props_window(
        &self,
        e: EdgeRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        self.0.graph().temporal_edge_props_window(e, t_start, t_end)
    }

    fn num_shards(&self) -> usize {
        self.0.graph().num_shards()
    }

    fn vertices_shard(&self, shard_id: usize) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        self.0.graph().vertices_shard(shard_id)
    }

    fn vertices_shard_window(
        &self,
        shard_id: usize,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        self.0
            .graph()
            .vertices_shard_window(shard_id, t_start, t_end)
    }

    fn vertex_edges(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.0.graph().vertex_edges(v, d, layer)
    }

    fn local_vertex(&self, v: VertexRef) -> Option<LocalVertexRef> {
        self.0.graph().local_vertex(v)
    }

    fn local_vertex_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<LocalVertexRef> {
        self.0.graph().local_vertex_window(v, t_start, t_end)
    }

    fn vertex_id(&self, v: LocalVertexRef) -> u64 {
        self.0.graph().vertex_id(v)
    }
}
