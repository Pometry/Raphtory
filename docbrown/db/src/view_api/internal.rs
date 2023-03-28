use docbrown_core::tgraph::{EdgeRef, VertexRef};
use docbrown_core::tgraph_shard::errors::GraphError;
use docbrown_core::{Direction, Prop};
use std::collections::HashMap;

pub trait GraphViewInternalOps {
    fn vertices_len(&self) -> Result<usize, GraphError>;

    fn vertices_len_window(&self, t_start: i64, t_end: i64) -> usize;

    fn edges_len(&self) -> Result<usize, GraphError>;

    fn edges_len_window(&self, t_start: i64, t_end: i64) -> usize;

    fn has_edge_ref<V1: Into<VertexRef>, V2: Into<VertexRef>>(
        &self,
        src: V1,
        dst: V2,
    ) -> Result<bool, GraphError>;

    fn has_edge_ref_window<V1: Into<VertexRef>, V2: Into<VertexRef>>(
        &self,
        src: V1,
        dst: V2,
        t_start: i64,
        t_end: i64,
    ) -> Result<bool, GraphError>;

    fn has_vertex_ref<V: Into<VertexRef>>(&self, v: V) -> Result<bool, GraphError>;

    fn has_vertex_ref_window<V: Into<VertexRef>>(
        &self,
        v: V,
        t_start: i64,
        t_end: i64,
    ) -> Result<bool, GraphError>;

    fn degree(&self, v: VertexRef, d: Direction) -> Result<usize, GraphError>;

    fn degree_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Result<usize, GraphError>;

    fn vertex_ref(&self, v: u64) -> Result<Option<VertexRef>, GraphError>;

    fn vertex_ref_window(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
    ) -> Result<Option<VertexRef>, GraphError>;

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

    fn edge_ref<V1: Into<VertexRef>, V2: Into<VertexRef>>(
        &self,
        src: V1,
        dst: V2,
    ) -> Result<Option<EdgeRef>, GraphError>;

    fn edge_ref_window<V1: Into<VertexRef>, V2: Into<VertexRef>>(
        &self,
        src: V1,
        dst: V2,
        t_start: i64,
        t_end: i64,
    ) -> Result<Option<EdgeRef>, GraphError>;

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

    fn static_vertex_prop(&self, v: VertexRef, name: String) -> Result<Option<Prop>, GraphError>;

    fn static_vertex_prop_keys(&self, v: VertexRef) -> Result<Vec<String>, GraphError>;

    fn temporal_vertex_prop_vec(
        &self,
        v: VertexRef,
        name: String,
    ) -> Result<Vec<(i64, Prop)>, GraphError>;

    fn temporal_vertex_prop_vec_window(
        &self,
        v: VertexRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Result<Vec<(i64, Prop)>, GraphError>;

    fn temporal_vertex_props(
        &self,
        v: VertexRef,
    ) -> Result<HashMap<String, Vec<(i64, Prop)>>, GraphError>;

    fn temporal_vertex_props_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Result<HashMap<String, Vec<(i64, Prop)>>, GraphError>;

    fn static_edge_prop(&self, e: EdgeRef, name: String) -> Result<Option<Prop>, GraphError>;

    fn static_edge_prop_keys(&self, e: EdgeRef) -> Result<Vec<String>, GraphError>;

    fn temporal_edge_props_vec(
        &self,
        e: EdgeRef,
        name: String,
    ) -> Result<Vec<(i64, Prop)>, GraphError>;

    fn temporal_edge_props_vec_window(
        &self,
        e: EdgeRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Result<Vec<(i64, Prop)>, GraphError>;

    fn temporal_edge_props(&self, e: EdgeRef) -> HashMap<String, Vec<(i64, Prop)>>;

    fn temporal_edge_props_window(
        &self,
        e: EdgeRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>>;
}
