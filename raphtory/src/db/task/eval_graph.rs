// use std::{borrow::Cow, cell::RefCell, sync::Arc};

// use crate::{
//     core::state::{compute_state::ComputeState, shuffle_state::ShuffleComputeState},
//     db::view_api::{internal::GraphViewInternalOps, GraphViewOps},
// };

// #[derive(Clone, Debug)]
// pub struct EvalGraph<'a, G: GraphViewOps, CS: ComputeState> {
//     g: Arc<G>,
//     shard_state: Arc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
//     global_state: Arc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
//     local_state: Arc<RefCell<ShuffleComputeState<CS>>>,
// }

// impl<'a, G: GraphViewOps, CS: ComputeState> EvalGraph<'a, G, CS> {
//     pub fn new(
//         g: Arc<G>,
//         shard_state: Arc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
//         global_state: Arc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
//         local_state: Arc<RefCell<ShuffleComputeState<CS>>>,
//     ) -> Self {
//         Self {
//             g,
//             shard_state,
//             global_state,
//             local_state,
//         }
//     }
// }

// impl<'a, G: GraphViewOps, CS: ComputeState> GraphViewInternalOps for EvalGraph<'a, G, CS> {
//     fn get_layer(&self, key: Option<&str>) -> Option<usize> {
//         self.g.get_layer(key)
//     }

//     fn view_start(&self) -> Option<i64> {
//         self.g.view_start()
//     }

//     fn view_end(&self) -> Option<i64> {
//         self.g.view_end()
//     }

//     fn earliest_time_global(&self) -> Option<i64> {
//         self.g.earliest_time_global()
//     }

//     fn earliest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
//         self.g.earliest_time_window(t_start, t_end)
//     }

//     fn latest_time_global(&self) -> Option<i64> {
//         self.g.latest_time_global()
//     }

//     fn latest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
//         self.g.latest_time_window(t_start, t_end)
//     }

//     fn vertices_len(&self) -> usize {
//         self.g.vertices_len()
//     }

//     fn vertices_len_window(&self, t_start: i64, t_end: i64) -> usize {
//         self.g.vertices_len_window(t_start, t_end)
//     }

//     fn edges_len(&self, layer: Option<usize>) -> usize {
//         self.g.edges_len(layer)
//     }

//     fn edges_len_window(&self, t_start: i64, t_end: i64, layer: Option<usize>) -> usize {
//         self.g.edges_len_window(t_start, t_end, layer)
//     }

//     fn has_edge_ref(
//         &self,
//         src: crate::core::tgraph::VertexRef,
//         dst: crate::core::tgraph::VertexRef,
//         layer: usize,
//     ) -> bool {
//         self.g.has_edge_ref(src, dst, layer)
//     }

//     fn has_edge_ref_window(
//         &self,
//         src: crate::core::tgraph::VertexRef,
//         dst: crate::core::tgraph::VertexRef,
//         t_start: i64,
//         t_end: i64,
//         layer: usize,
//     ) -> bool {
//         self.g.has_edge_ref_window(src, dst, t_start, t_end, layer)
//     }

//     fn has_vertex_ref(&self, v: crate::core::tgraph::VertexRef) -> bool {
//         self.g.has_vertex_ref(v)
//     }

//     fn has_vertex_ref_window(
//         &self,
//         v: crate::core::tgraph::VertexRef,
//         t_start: i64,
//         t_end: i64,
//     ) -> bool {
//         self.g.has_vertex_ref_window(v, t_start, t_end)
//     }

//     fn degree(
//         &self,
//         v: crate::core::tgraph::VertexRef,
//         d: crate::core::Direction,
//         layer: Option<usize>,
//     ) -> usize {
//         self.g.degree(v, d, layer)
//     }

//     fn degree_window(
//         &self,
//         v: crate::core::tgraph::VertexRef,
//         t_start: i64,
//         t_end: i64,
//         d: crate::core::Direction,
//         layer: Option<usize>,
//     ) -> usize {
//         self.g.degree_window(v, t_start, t_end, d, layer)
//     }

//     fn vertex_ref(&self, v: u64) -> Option<crate::core::tgraph::VertexRef> {
//         self.g.vertex_ref(v)
//     }

//     fn lookup_by_pid_and_shard(
//         &self,
//         pid: usize,
//         shard: usize,
//     ) -> Option<crate::core::tgraph::VertexRef> {
//         self.g.lookup_by_pid_and_shard(pid, shard)
//     }

//     fn vertex_ref_window(
//         &self,
//         v: u64,
//         t_start: i64,
//         t_end: i64,
//     ) -> Option<crate::core::tgraph::VertexRef> {
//         self.g.vertex_ref_window(v, t_start, t_end)
//     }

//     fn vertex_earliest_time(&self, v: crate::core::tgraph::VertexRef) -> Option<i64> {
//         self.g.vertex_earliest_time(v)
//     }

//     fn vertex_earliest_time_window(
//         &self,
//         v: crate::core::tgraph::VertexRef,
//         t_start: i64,
//         t_end: i64,
//     ) -> Option<i64> {
//         self.g.vertex_earliest_time_window(v, t_start, t_end)
//     }

//     fn vertex_latest_time(&self, v: crate::core::tgraph::VertexRef) -> Option<i64> {
//         self.g.vertex_latest_time(v)
//     }

//     fn vertex_latest_time_window(
//         &self,
//         v: crate::core::tgraph::VertexRef,
//         t_start: i64,
//         t_end: i64,
//     ) -> Option<i64> {
//         self.g.vertex_latest_time_window(v, t_start, t_end)
//     }

//     fn vertex_ids(&self) -> Box<dyn Iterator<Item = u64> + Send> {
//         self.g.vertex_ids()
//     }

//     fn vertex_ids_window(&self, t_start: i64, t_end: i64) -> Box<dyn Iterator<Item = u64> + Send> {
//         self.g.vertex_ids_window(t_start, t_end)
//     }

//     fn vertex_refs(&self) -> Box<dyn Iterator<Item = crate::core::tgraph::VertexRef> + Send> {
//         self.g.vertex_refs()
//     }

//     fn vertex_refs_window(
//         &self,
//         t_start: i64,
//         t_end: i64,
//     ) -> Box<dyn Iterator<Item = crate::core::tgraph::VertexRef> + Send> {
//         self.g.vertex_refs_window(t_start, t_end)
//     }

//     fn vertex_refs_shard(
//         &self,
//         shard: usize,
//     ) -> Box<dyn Iterator<Item = crate::core::tgraph::VertexRef> + Send> {
//         self.g.vertex_refs_shard(shard)
//     }

//     fn vertex_refs_window_shard(
//         &self,
//         shard: usize,
//         t_start: i64,
//         t_end: i64,
//     ) -> Box<dyn Iterator<Item = crate::core::tgraph::VertexRef> + Send> {
//         self.g.vertex_refs_window_shard(shard, t_start, t_end)
//     }

//     fn edge_ref(
//         &self,
//         src: crate::core::tgraph::VertexRef,
//         dst: crate::core::tgraph::VertexRef,
//         layer: usize,
//     ) -> Option<crate::core::tgraph::EdgeRef> {
//         self.g.edge_ref(src, dst, layer)
//     }

//     fn edge_ref_window(
//         &self,
//         src: crate::core::tgraph::VertexRef,
//         dst: crate::core::tgraph::VertexRef,
//         t_start: i64,
//         t_end: i64,
//         layer: usize,
//     ) -> Option<crate::core::tgraph::EdgeRef> {
//         self.g.edge_ref_window(src, dst, t_start, t_end, layer)
//     }

//     fn vertex_edges_all_layers(
//         &self,
//         v: crate::core::tgraph::VertexRef,
//         d: crate::core::Direction,
//     ) -> Box<dyn Iterator<Item = crate::core::tgraph::EdgeRef> + Send> {
//         self.g.vertex_edges_all_layers(v, d)
//     }

//     fn vertex_edges_single_layer(
//         &self,
//         v: crate::core::tgraph::VertexRef,
//         d: crate::core::Direction,
//         layer: usize,
//     ) -> Box<dyn Iterator<Item = crate::core::tgraph::EdgeRef> + Send> {
//         todo!()
//     }

//     fn vertex_edges_t(
//         &self,
//         v: crate::core::tgraph::VertexRef,
//         d: crate::core::Direction,
//         layer: Option<usize>,
//     ) -> Box<dyn Iterator<Item = crate::core::tgraph::EdgeRef> + Send> {
//         todo!()
//     }

//     fn vertex_edges_window(
//         &self,
//         v: crate::core::tgraph::VertexRef,
//         t_start: i64,
//         t_end: i64,
//         d: crate::core::Direction,
//         layer: Option<usize>,
//     ) -> Box<dyn Iterator<Item = crate::core::tgraph::EdgeRef> + Send> {
//         todo!()
//     }

//     fn vertex_edges_window_t(
//         &self,
//         v: crate::core::tgraph::VertexRef,
//         t_start: i64,
//         t_end: i64,
//         d: crate::core::Direction,
//         layer: Option<usize>,
//     ) -> Box<dyn Iterator<Item = crate::core::tgraph::EdgeRef> + Send> {
//         todo!()
//     }

//     fn neighbours(
//         &self,
//         v: crate::core::tgraph::VertexRef,
//         d: crate::core::Direction,
//         layer: Option<usize>,
//     ) -> Box<dyn Iterator<Item = crate::core::tgraph::VertexRef> + Send> {
//         todo!()
//     }

//     fn neighbours_window(
//         &self,
//         v: crate::core::tgraph::VertexRef,
//         t_start: i64,
//         t_end: i64,
//         d: crate::core::Direction,
//         layer: Option<usize>,
//     ) -> Box<dyn Iterator<Item = crate::core::tgraph::VertexRef> + Send> {
//         todo!()
//     }

//     fn static_vertex_prop(
//         &self,
//         v: crate::core::tgraph::VertexRef,
//         name: String,
//     ) -> Option<crate::core::Prop> {
//         todo!()
//     }

//     fn static_vertex_prop_names(&self, v: crate::core::tgraph::VertexRef) -> Vec<String> {
//         todo!()
//     }

//     fn temporal_vertex_prop_names(&self, v: crate::core::tgraph::VertexRef) -> Vec<String> {
//         todo!()
//     }

//     fn temporal_vertex_prop_vec(
//         &self,
//         v: crate::core::tgraph::VertexRef,
//         name: String,
//     ) -> Vec<(i64, crate::core::Prop)> {
//         todo!()
//     }

//     fn vertex_timestamps(&self, v: crate::core::tgraph::VertexRef) -> Vec<i64> {
//         todo!()
//     }

//     fn vertex_timestamps_window(
//         &self,
//         v: crate::core::tgraph::VertexRef,
//         t_start: i64,
//         t_end: i64,
//     ) -> Vec<i64> {
//         todo!()
//     }

//     fn temporal_vertex_prop_vec_window(
//         &self,
//         v: crate::core::tgraph::VertexRef,
//         name: String,
//         t_start: i64,
//         t_end: i64,
//     ) -> Vec<(i64, crate::core::Prop)> {
//         todo!()
//     }

//     fn temporal_vertex_props(
//         &self,
//         v: crate::core::tgraph::VertexRef,
//     ) -> std::collections::HashMap<String, Vec<(i64, crate::core::Prop)>> {
//         todo!()
//     }

//     fn temporal_vertex_props_window(
//         &self,
//         v: crate::core::tgraph::VertexRef,
//         t_start: i64,
//         t_end: i64,
//     ) -> std::collections::HashMap<String, Vec<(i64, crate::core::Prop)>> {
//         todo!()
//     }

//     fn static_edge_prop(
//         &self,
//         e: crate::core::tgraph::EdgeRef,
//         name: String,
//     ) -> Option<crate::core::Prop> {
//         todo!()
//     }

//     fn static_edge_prop_names(&self, e: crate::core::tgraph::EdgeRef) -> Vec<String> {
//         todo!()
//     }

//     fn temporal_edge_prop_names(&self, e: crate::core::tgraph::EdgeRef) -> Vec<String> {
//         todo!()
//     }

//     fn temporal_edge_props_vec(
//         &self,
//         e: crate::core::tgraph::EdgeRef,
//         name: String,
//     ) -> Vec<(i64, crate::core::Prop)> {
//         todo!()
//     }

//     fn temporal_edge_props_vec_window(
//         &self,
//         e: crate::core::tgraph::EdgeRef,
//         name: String,
//         t_start: i64,
//         t_end: i64,
//     ) -> Vec<(i64, crate::core::Prop)> {
//         todo!()
//     }

//     fn edge_timestamps(
//         &self,
//         e: crate::core::tgraph::EdgeRef,
//         window: Option<std::ops::Range<i64>>,
//     ) -> Vec<i64> {
//         todo!()
//     }

//     fn temporal_edge_props(
//         &self,
//         e: crate::core::tgraph::EdgeRef,
//     ) -> std::collections::HashMap<String, Vec<(i64, crate::core::Prop)>> {
//         todo!()
//     }

//     fn temporal_edge_props_window(
//         &self,
//         e: crate::core::tgraph::EdgeRef,
//         t_start: i64,
//         t_end: i64,
//     ) -> std::collections::HashMap<String, Vec<(i64, crate::core::Prop)>> {
//         todo!()
//     }

//     fn num_shards(&self) -> usize {
//         todo!()
//     }

//     fn vertices_shard(
//         &self,
//         shard_id: usize,
//     ) -> Box<dyn Iterator<Item = crate::core::tgraph::VertexRef> + Send> {
//         todo!()
//     }

//     fn vertices_shard_window(
//         &self,
//         shard_id: usize,
//         t_start: i64,
//         t_end: i64,
//     ) -> Box<dyn Iterator<Item = crate::core::tgraph::VertexRef> + Send> {
//         todo!()
//     }
// }
