use std::ops::{Deref, Range};

use crate::core::{
    edge_ref::EdgeRef,
    tgraph2::{tgraph::InnerTemporalGraph, timer::TimeCounterTrait, VID},
    tgraph_shard::LockedView,
    timeindex::{TimeIndexOps, TimeIndex},
    tprop::TProp,
    vertex_ref::VertexRef,
    Direction, Prop,
};

use genawaiter::sync::GenBoxed;

use super::view_api::{
    internal::{CoreGraphOps, GraphOps, TimeSemantics},
    BoxedIter,
};

impl<const N: usize> CoreGraphOps for InnerTemporalGraph<N> {
    fn get_layer_name_by_id(&self, layer_id: usize) -> String {
        self.edge_props_meta
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

    fn edge_additions(&self, eref: EdgeRef) -> LockedView<TimeIndex> {
        let edge = self.edge(eref.pid());
        edge.edge_additions(eref.layer()).unwrap()
    }

    fn edge_deletions(&self, eref: EdgeRef) -> LockedView<TimeIndex> {
        let edge = self.edge(eref.pid());
        edge.edge_deletions(eref.layer()).unwrap()
    }

    fn vertex_additions(&self, v: VID) -> LockedView<TimeIndex> {
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
        node.static_property(prop_id).map(|p| p.clone())
    }

    fn static_vertex_prop_names(&self, v: VID) -> Vec<String> {
        if let Some(node) = self.node_entry(v).value() {
            return node
                .static_prop_ids()
                .into_iter()
                .flat_map(|prop_id| self.vertex_reverse_prop_id(prop_id, true))
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
        let x = edge
            .unsafe_layer(e.layer())
            .static_property(prop_id)
            .map(|p| p.clone());
        x
    }

    fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        if let Some(edge) = self.edge_entry(e.pid()).value() {
            return edge
                .unsafe_layer(e.layer())
                .static_prop_ids()
                .into_iter()
                .flat_map(|prop_id| self.edge_reverse_prop_id(prop_id, true))
                .collect();
        }
        vec![]
    }

    fn temporal_edge_prop(&self, e: EdgeRef, name: &str) -> Option<LockedView<TProp>> {
        let edge = self.edge(e.pid());
        let prop_id = self.edge_find_prop(name, false)?;

        edge.temporal_property(e.layer(), prop_id)
    }

    fn temporal_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.temp_prop_ids(e.pid())
            .into_iter()
            .flat_map(|id| self.edge_reverse_prop_id(id, false))
            .collect()
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
        self.edge_props_meta.get_all_layers()
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
        self.degree(v, d, layer)
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VID> + Send> {
        Box::new(self.vertex_ids())
    }

    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> Option<EdgeRef> {
        let src = self.resolve_vertex_ref(&src)?;
        let dst = self.resolve_vertex_ref(&dst)?;

        self.find_edge(src, dst, Some(layer))
            .map(|e_id| EdgeRef::LocalOut {
                e_pid: e_id,
                layer_id: layer,
                src_pid: src,
                dst_pid: dst,
                time: None,
            })
    }

    fn edge_refs(&self, layer: Option<usize>) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        if let Some(layer_id) = layer {
            let iter = self
                .locked_edges()
                .filter_map(move |edge| {
                    if edge.has_layer(layer_id) {
                        Some((layer_id, edge))
                    } else {
                        None
                    }
                })
                .map(|(layer_id, edge)| {
                    let e_ref: EdgeRef = edge.deref().into();
                    e_ref.at_layer(layer_id)
                });
            Box::new(iter)
        } else {
            let iter = self.locked_edges().map(|edge| {
                let e_ref: EdgeRef = edge.deref().into();
                e_ref.at_layer(0)
            });
            Box::new(iter)
        }
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
            for e_ref in v.edge_tuples(layer, d) {
                co.yield_(e_ref).await;
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
        let vid = self.resolve_vertex_ref(&VertexRef::Local(v)).unwrap();
        let v = self.vertex_arc(vid);

        let iter: GenBoxed<VertexRef> = GenBoxed::new_boxed(|co| async move {
            for v_id in v.neighbours(layer, d) {
                co.yield_(VertexRef::Local(v_id)).await;
            }
        });

        Box::new(iter.into_iter())
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
        self.node_entry(v).timestamps().active(w)
    }

    fn include_edge_window(&self, e: EdgeRef, w: Range<i64>) -> bool {
        self.edge(e.pid()).active(e.layer(), w)
    }

    fn edge_t(&self, e: EdgeRef) -> BoxedIter<EdgeRef> {
        let arc = self.edge_arc(e.pid());
        let iter: GenBoxed<EdgeRef> = GenBoxed::new_boxed(|co| async move {
            for t in arc.timestamps(e.layer()) {
                co.yield_(e.at(*t)).await;
            }
        });
        Box::new(iter.into_iter())
    }

    fn edge_window_t(&self, e: EdgeRef, w: Range<i64>) -> BoxedIter<EdgeRef> {
        let arc = self.edge_arc(e.pid());
        let iter: GenBoxed<EdgeRef> = GenBoxed::new_boxed(|co| async move {
            for t in arc.timestamps_window(e.layer(), w) {
                co.yield_(e.at(*t)).await;
            }
        });
        Box::new(iter.into_iter())
    }

    fn edge_earliest_time(&self, e: EdgeRef) -> Option<i64> {
        self.edge_entry(e.pid())
            .value()
            .and_then(|edge| edge.unsafe_layer(e.layer()).timestamps().first())
    }

    fn edge_earliest_time_window(&self, e: EdgeRef, w: Range<i64>) -> Option<i64> {
        self.edge_entry(e.pid())
            .value()
            .and_then(|edge| edge.unsafe_layer(e.layer()).timestamps().range(w).first())
    }

    fn edge_latest_time(&self, e: EdgeRef) -> Option<i64> {
        self.edge_entry(e.pid())
            .value()
            .and_then(|edge| edge.unsafe_layer(e.layer()).timestamps().last())
    }

    fn edge_latest_time_window(&self, e: EdgeRef, w: Range<i64>) -> Option<i64> {
        self.edge_entry(e.pid())
            .value()
            .and_then(|edge| edge.unsafe_layer(e.layer()).timestamps().range(w).last())
    }

    fn vertex_earliest_time(&self, v: VID) -> Option<i64> {
        self.node_entry(v)
            .value()
            .and_then(|node| node.timestamps().first())
    }

    fn vertex_latest_time(&self, v: VID) -> Option<i64> {
        self.node_entry(v)
            .value()
            .and_then(|node| node.timestamps().last())
    }

    fn vertex_earliest_time_window(&self, v: VID, t_start: i64, t_end: i64) -> Option<i64> {
        self.node_entry(v)
            .value()
            .and_then(|node| node.timestamps().range(t_start..t_end).first())
    }

    fn vertex_latest_time_window(&self, v: VID, t_start: i64, t_end: i64) -> Option<i64> {
        self.node_entry(v)
            .value()
            .and_then(|node| node.timestamps().range(t_start..t_end).last())
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
        self.prop_vec_window(e.pid(), name, t_start, t_end, e.layer())
    }

    fn temporal_edge_prop_vec(&self, e: EdgeRef, name: &str) -> Vec<(i64, Prop)> {
        todo!()
    }

    fn vertex_history(&self, v: VID) -> Vec<i64> {
        self.node_entry(v).timestamps().iter().copied().collect()
    }

    fn vertex_history_window(&self, v: VID, w: Range<i64>) -> Vec<i64> {
        self.node_entry(v)
            .timestamps()
            .range(w)
            .iter()
            .copied()
            .collect()
    }
}
