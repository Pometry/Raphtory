use docbrown_core::tgraph::{EdgeRef, VertexRef};
use docbrown_core::{Direction, Prop};
use std::collections::HashMap;

pub trait GraphViewInternalOps {
    fn vertices_len(&self) -> usize;

    fn vertices_len_window(&self, t_start: i64, t_end: i64) -> usize;

    fn edges_len(&self) -> usize;

    fn edges_len_window(&self, t_start: i64, t_end: i64) -> usize;

    fn has_edge_ref<V1: Into<VertexRef>, V2: Into<VertexRef>>(&self, src: V1, dst: V2) -> bool;

    fn has_edge_ref_window<V1: Into<VertexRef>, V2: Into<VertexRef>>(
        &self,
        src: V1,
        dst: V2,
        t_start: i64,
        t_end: i64,
    ) -> bool;

    fn has_vertex_ref<V: Into<VertexRef>>(&self, v: V) -> bool;

    fn has_vertex_ref_window<V: Into<VertexRef>>(&self, v: V, t_start: i64, t_end: i64) -> bool;

    fn degree(&self, v: VertexRef, d: Direction) -> usize;

    fn degree_window(&self, v: VertexRef, t_start: i64, t_end: i64, d: Direction) -> usize;

    fn vertex_ref(&self, v: u64) -> Option<VertexRef>;

    fn vertex_ref_window(&self, v: u64, t_start: i64, t_end: i64) -> Option<VertexRef>;

    fn vertex_ids(&self) -> Box<dyn Iterator<Item = u64> + Send>;

    fn vertex_ids_window(&self, t_start: i64, t_end: i64) -> Box<dyn Iterator<Item = u64> + Send>;

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VertexRef> + Send>;

    fn vertex_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send>;

    fn vertex_refs_window_shard(
        &self,
        shard: usize,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send>;

    fn vertices_par<O, F>(&self, f: F) -> Box<dyn Iterator<Item = O>>
    where
        O: Send + 'static,
        F: Fn(VertexRef) -> O + Send + Sync + Copy;

    fn fold_par<S, F, F2>(&self, f: F, agg: F2) -> Option<S>
    where
        S: Send + 'static,
        F: Fn(VertexRef) -> S + Send + Sync + Copy,
        F2: Fn(S, S) -> S + Sync + Send + Copy;

    fn vertices_window_par<O, F>(
        &self,
        t_start: i64,
        t_end: i64,
        f: F,
    ) -> Box<dyn Iterator<Item = O>>
    where
        O: Send + 'static,
        F: Fn(VertexRef) -> O + Send + Sync + Copy;

    fn fold_window_par<S, F, F2>(&self, t_start: i64, t_end: i64, f: F, agg: F2) -> Option<S>
    where
        S: Send + 'static,
        F: Fn(VertexRef) -> S + Send + Sync + Copy,
        F2: Fn(S, S) -> S + Sync + Send + Copy;

    fn edge_ref<V1: Into<VertexRef>, V2: Into<VertexRef>>(
        &self,
        src: V1,
        dst: V2,
    ) -> Option<EdgeRef>;

    fn edge_ref_window<V1: Into<VertexRef>, V2: Into<VertexRef>>(
        &self,
        src: V1,
        dst: V2,
        t_start: i64,
        t_end: i64,
    ) -> Option<EdgeRef>;

    fn edge_refs(&self) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

    fn edge_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

    fn vertex_edges(&self, v: VertexRef, d: Direction) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

    fn vertex_edges_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

    fn vertex_edges_window_t(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

    fn neighbours(&self, v: VertexRef, d: Direction) -> Box<dyn Iterator<Item = VertexRef> + Send>;

    fn neighbours_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send>;

    fn neighbours_ids(&self, v: VertexRef, d: Direction) -> Box<dyn Iterator<Item = u64> + Send>;

    fn neighbours_ids_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send>;

    fn static_vertex_prop(&self, v: VertexRef, name: String) -> Option<Prop>;

    fn static_vertex_prop_keys(&self, v: VertexRef) -> Vec<String>;

    fn temporal_vertex_prop_vec(&self, v: VertexRef, name: String) -> Vec<(i64, Prop)>;

    fn temporal_vertex_prop_vec_window(
        &self,
        v: VertexRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)>;

    fn temporal_vertex_props(&self, v: VertexRef) -> HashMap<String, Vec<(i64, Prop)>>;

    fn temporal_vertex_props_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>>;

    fn static_edge_prop(&self, e: EdgeRef, name: String) -> Option<Prop>;

    fn static_edge_prop_keys(&self, e: EdgeRef) -> Vec<String>;

    fn temporal_edge_props_vec(&self, e: EdgeRef, name: String) -> Vec<(i64, Prop)>;

    fn temporal_edge_props_vec_window(
        &self,
        e: EdgeRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)>;

    fn temporal_edge_props(&self, e: EdgeRef) -> HashMap<String, Vec<(i64, Prop)>>;

    fn temporal_edge_props_window(
        &self,
        e: EdgeRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>>;
}
