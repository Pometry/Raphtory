use std::ops::Range;

use crate::core::{
    edge_ref::EdgeRef,
    tgraph2::{tgraph::InnerTemporalGraph, timer::TimeCounterTrait, VID},
    tgraph_shard::LockedView,
    tprop::TProp,
    vertex_ref::VertexRef,
    Direction, Prop,
};

use genawaiter::sync::GenBoxed;

use super::view_api::internal::{CoreGraphOps, GraphOps, TimeSemantics};

impl<const N: usize> CoreGraphOps for InnerTemporalGraph<N> {
    fn get_layer_name_by_id(&self, layer_id: usize) -> String {
        self.vertex_props_meta
            .get_layer_name_by_id(layer_id)
            .unwrap_or_else(|| panic!("layer id '{layer_id}' doesn't exist"))
            .to_string()
    }

    fn vertex_id(&self, v: VID) -> u64 {
        self.global_vertex_id(v.into())
            .unwrap_or_else(|| panic!("vertex id '{v:?}' doesn't exist"))
    }

    fn vertex_name(&self, v: VID) -> String {
        self.vertex_name(v.into())
    }

    fn edge_additions(&self, eref: EdgeRef) -> LockedView<crate::core::timeindex::TimeIndex> {
        todo!()
    }

    fn edge_deletions(&self, eref: EdgeRef) -> LockedView<crate::core::timeindex::TimeIndex> {
        todo!()
    }

    fn vertex_additions(&self, v: VID) -> LockedView<crate::core::timeindex::TimeIndex> {
        todo!()
    }

    fn localise_vertex_unchecked(&self, v: VertexRef) -> VID {
        match v {
            VertexRef::Local(l) => l,
            VertexRef::Remote(_) => self.resolve_vertex_ref(&v).unwrap(),
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

    fn static_vertex_prop(&self, v: VID, name: &str) -> Option<Prop> {
        let entry = self.node_entry(v);
        let node = entry.value()?;
        let prop_id = self.vertex_find_prop(name, true)?;
        node.static_property(prop_id).map(|p|p.clone())
    }

    fn static_vertex_prop_names(&self, v: VID) -> Vec<String> {
        if let Some(node) = self.node_entry(v).value() {
            return node
                .static_prop_ids()
                .into_iter()
                .flat_map(|prop_id| self.reverse_prop_id(prop_id, true))
                .collect();
        }
        vec![]
    }

    fn temporal_vertex_prop(&self, v: VID, name: &str) -> Option<LockedView<TProp>> {
        todo!()
    }

    fn temporal_vertex_prop_names(&self, v: VID) -> Vec<String> {
        todo!()
    }

    fn static_edge_prop(&self, e: EdgeRef, name: &str) -> Option<Prop> {
        let entry = self.edge_entry(e.pid());
        let edge = entry.value()?;
        let prop_id = self.edge_find_prop(name, true)?;
        edge.static_property(prop_id, e.layer()).map(|p|p.clone())

    }

    fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        if let Some(edge) = self.edge_entry(e.pid()).value() {
            return edge
                .static_prop_ids(e.layer())
                .into_iter()
                .flat_map(|prop_id| self.edge_reverse_prop_id(prop_id, true))
                .collect();
        }
        vec![]
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
    fn local_vertex_ref(&self, v: VertexRef) -> Option<VID> {
        match v {
            VertexRef::Local(l) => Some(l),
            VertexRef::Remote(_) => {
                let vid = self.resolve_vertex_ref(&v)?;
                Some(vid)
            }
        }
    }

    fn get_unique_layers_internal(&self) -> Vec<usize> {
        self.vertex_props_meta.get_all_layers()
    }

    fn get_layer_id(&self, key: Option<&str>) -> Option<usize> {
        match key {
            Some(key) => self.edge_props_meta.get_layer_id(key),
            None => Some(0),
        }
    }

    fn vertices_len(&self) -> usize {
        self.num_vertices()
    }

    fn edges_len(&self, layer: Option<usize>) -> usize {
        self.num_edges(layer)
    }

    fn degree(&self, v: VID, d: Direction, layer: Option<usize>) -> usize {
        println!("degree: {:?} {:?} {:?}", v, d, layer);
        self.degree(v, d, layer)
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VID> + Send> {
        Box::new(self.vertex_ids())
    }

    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> Option<EdgeRef> {
        let src = self.resolve_vertex_ref(&src)?;
        let dst = self.resolve_vertex_ref(&dst)?;

        self.find_edge(src, dst, layer)
            .map(|e_id| EdgeRef::LocalOut {
                e_pid: e_id,
                layer_id: layer,
                src_pid: src,
                dst_pid: dst,
                time: None,
            })
    }

    fn edge_refs(&self, layer: Option<usize>) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        todo!()
    }

    fn vertex_edges(
        &self,
        v: VID,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        let vid = self.resolve_vertex_ref(&VertexRef::Local(v)).unwrap();
        let v = self.vertex_arc(vid);

        let iter: GenBoxed<EdgeRef> = GenBoxed::new_boxed(|co| async move {
            for (other_v, eid) in v.edge_tuples(layer, d) {
                co.yield_(EdgeRef::LocalOut {
                    e_pid: eid,
                    layer_id: 0,
                    src_pid: vid,
                    dst_pid: other_v,
                    time: None,
                })
                .await;
            }
        });

        Box::new(iter.into_iter())
    }

    fn neighbours(
        &self,
        v: VID,
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

    fn include_vertex_window(&self, v: VID, w: Range<i64>) -> bool {
        todo!()
    }

    fn include_edge_window(&self, e: EdgeRef, w: Range<i64>) -> bool {
        self.edge(e.pid()).active(w)
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

    fn temporal_vertex_prop_vec(&self, v: VID, name: &str) -> Vec<(i64, Prop)> {
        todo!()
    }

    fn temporal_vertex_prop_vec_window(
        &self,
        v: VID,
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
