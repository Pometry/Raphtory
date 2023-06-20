use std::ops::Range;

use crate::core::{
    edge_ref::EdgeRef,
    tgraph2::{tgraph::InnerTemporalGraph, VID, timer::TimeCounterTrait},
    tgraph_shard::LockedView,
    tprop::TProp,
    vertex_ref::{LocalVertexRef, VertexRef},
    Direction, Prop,
};



use genawaiter::sync::GenBoxed;

use super::view_api::internal::{CoreGraphOps, GraphOps, TimeSemantics};

impl<const N: usize> CoreGraphOps for InnerTemporalGraph<N> {
    fn get_layer_name_by_id(&self, layer_id: usize) -> String {
        self.props_meta
            .get_layer_name_by_id(layer_id)
            .unwrap_or_else(|| panic!("layer id '{layer_id}' doesn't exist"))
            .to_string()
    }

    fn vertex_id(&self, v: LocalVertexRef) -> u64 {
        self.global_vertex_id(v.into())
            .unwrap_or_else(|| panic!("vertex id '{v:?}' doesn't exist"))
    }

    fn vertex_name(&self, v: LocalVertexRef) -> String {
        self.vertex_name(v.into())
    }

    fn edge_additions(&self, eref: EdgeRef) -> LockedView<crate::core::timeindex::TimeIndex> {
        todo!()
    }

    fn edge_deletions(&self, eref: EdgeRef) -> LockedView<crate::core::timeindex::TimeIndex> {
        todo!()
    }

    fn vertex_additions(&self, v: LocalVertexRef) -> LockedView<crate::core::timeindex::TimeIndex> {
        todo!()
    }

    fn localise_vertex_unchecked(&self, v: VertexRef) -> LocalVertexRef {
        match v {
            VertexRef::Local(l) => l,
            VertexRef::Remote(_) => {
                let vid = self.resolve_vertex_ref(&v).unwrap();
                let local = vid.as_local::<N>();
                LocalVertexRef {
                    shard_id: local.bucket,
                    pid: local.offset,
                }
            }
        }
    }

    fn static_prop_names(&self) -> Vec<String> {
        todo!()
    }

    fn static_prop(&self, name: &str) -> Option<Prop> {
        todo!()
    }

    fn temporal_prop_names(&self) -> Vec<String> {
        todo!()
    }

    fn temporal_prop(&self, name: &str) -> Option<LockedView<TProp>> {
        todo!()
    }

    fn static_vertex_prop(&self, v: LocalVertexRef, name: &str) -> Option<Prop> {
        todo!()
    }

    fn static_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        todo!()
    }

    fn temporal_vertex_prop(&self, v: LocalVertexRef, name: &str) -> Option<LockedView<TProp>> {
        todo!()
    }

    fn temporal_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        todo!()
    }

    fn static_edge_prop(&self, e: EdgeRef, name: &str) -> Option<Prop> {
        todo!()
    }

    fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        todo!()
    }

    fn temporal_edge_prop(&self, e: EdgeRef, name: &str) -> Option<LockedView<TProp>> {
        todo!()
    }

    fn temporal_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        todo!()
    }

    fn num_shards_internal(&self) -> usize {
        todo!()
    }
}

impl<const N: usize> GraphOps for InnerTemporalGraph<N> {
    fn local_vertex_ref(&self, v: VertexRef) -> Option<LocalVertexRef> {
        match v {
            VertexRef::Local(l) => Some(l),
            VertexRef::Remote(_) => {
                let vid = self.resolve_vertex_ref(&v)?;
                let local = vid.as_local::<N>();
                Some(LocalVertexRef {
                    shard_id: local.bucket,
                    pid: local.offset,
                })
            }
        }
    }

    fn get_unique_layers_internal(&self) -> Vec<usize> {
        self.props_meta.get_all_layers()
    }

    fn get_layer_id(&self, key: Option<&str>) -> Option<usize> {
        match key {
            Some(key) => self.props_meta.get_layer_id(key),
            None => Some(0),
        }
    }

    fn vertices_len(&self) -> usize {
        self.num_vertices()
    }

    fn edges_len(&self, layer: Option<usize>) -> usize {
        self.num_edges(layer)
    }

    fn degree(&self, v: LocalVertexRef, d: Direction, layer: Option<usize>) -> usize {
        todo!()
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        let iter = self.vertex_ids().map(|v_id| {
            let local_vid = v_id.as_local::<N>();
            LocalVertexRef {
                shard_id: local_vid.bucket,
                pid: local_vid.offset,
            }
        });
        Box::new(iter)
    }

    fn vertex_refs_shard(&self, shard: usize) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        todo!()
    }

    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> Option<EdgeRef> {
        let src = self.resolve_vertex_ref(&src)?;
        let dst = self.resolve_vertex_ref(&dst)?;

        self.find_edge(src, dst, layer)
            .map(|e_id| { 
                EdgeRef::LocalOut {
                e_pid: e_id.into(),
                shard_id: 0,
                layer_id: layer,
                src_pid: src.into(),
                dst_pid: dst.into(),
                time: None,
            } })
    }

    fn edge_refs(&self, layer: Option<usize>) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        todo!()
    }

    fn vertex_edges(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        let vid = self.resolve_vertex_ref(&VertexRef::Local(v)).unwrap();
        let v = self.vertex_arc(vid);

        let iter: GenBoxed<EdgeRef> = GenBoxed::new_boxed(|co| async move {
            for (other_v, eid) in v.edge_tuples(layer, d) {
                co.yield_(EdgeRef::LocalOut {
                    e_pid: eid.into(),
                    shard_id: 0,
                    layer_id: 0,
                    src_pid: vid.into(),
                    dst_pid: other_v.into(),
                    time: None,
                })
                .await;
            }
        });

        Box::new(iter.into_iter())
    }

    fn neighbours(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        todo!()
    }
}

impl<const N: usize> TimeSemantics for InnerTemporalGraph<N> {

    fn latest_time_global(&self) -> Option<i64> {
        Some(self.latest_time.get()).filter(|t| *t != i64::MIN)
    }

    fn earliest_time_global(&self) -> Option<i64> {
        Some(self.earliest_time.get()).filter(|t| *t != i64::MAX)
    }

    fn include_vertex_window(&self, v: LocalVertexRef, w: Range<i64>) -> bool {
        todo!()
    }

    fn include_edge_window(&self, e: EdgeRef, w: Range<i64>) -> bool {
        let shard = e.shard();
        let pid = e.pid();

        self.edge((shard * N + pid).into()).active(w)
    }

    fn edge_t(&self, e: EdgeRef) -> super::view_api::BoxedIter<EdgeRef> {
        todo!()
    }

    fn edge_window_t(&self, e: EdgeRef, w: Range<i64>) -> super::view_api::BoxedIter<EdgeRef> {
        todo!()
    }

    fn edge_earliest_time(&self, e: EdgeRef) -> Option<i64> {
        todo!()
    }

    fn edge_earliest_time_window(&self, e: EdgeRef, w: Range<i64>) -> Option<i64> {
        todo!()
    }

    fn edge_latest_time(&self, e: EdgeRef) -> Option<i64> {
        todo!()
    }

    fn edge_latest_time_window(&self, e: EdgeRef, w: Range<i64>) -> Option<i64> {
        todo!()
    }

    fn temporal_prop_vec(&self, name: &str) -> Vec<(i64, Prop)> {
        todo!()
    }

    fn temporal_prop_vec_window(&self, name: &str, t_start: i64, t_end: i64) -> Vec<(i64, Prop)> {
        todo!()
    }

    fn temporal_vertex_prop_vec(&self, v: LocalVertexRef, name: &str) -> Vec<(i64, Prop)> {
        todo!()
    }

    fn temporal_vertex_prop_vec_window(
        &self,
        v: LocalVertexRef,
        name: &str,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        todo!()
    }

    fn temporal_edge_prop_vec_window(
        &self,
        e: EdgeRef,
        name: &str,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        todo!()
    }

    fn temporal_edge_prop_vec(&self, e: EdgeRef, name: &str) -> Vec<(i64, Prop)> {
        todo!()
    }
}
