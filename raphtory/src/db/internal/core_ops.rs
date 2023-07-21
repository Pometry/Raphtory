use crate::{
    core::{
        entities::{
            edges::edge_ref::EdgeRef, graph::tgraph::InnerTemporalGraph, properties::tprop::TProp,
            vertices::vertex_ref::VertexRef, LayerIds, VID,
        },
        storage::{
            locked_view::LockedView,
            timeindex::{LockedLayeredIndex, TimeIndex},
        },
    },
    db::api::view::internal::CoreGraphOps,
    prelude::Prop,
};

impl<const N: usize> CoreGraphOps for InnerTemporalGraph<N> {
    fn get_layer_name_by_id(&self, layer_id: usize) -> String {
        self.get_layer_name(layer_id)
    }

    fn vertex_id(&self, v: VID) -> u64 {
        self.global_vertex_id(v.into())
            .unwrap_or_else(|| panic!("vertex id '{v:?}' doesn't exist"))
    }

    fn vertex_name(&self, v: VID) -> String {
        self.vertex_name(v.into())
    }

    fn edge_additions(&self, eref: EdgeRef, layer_ids: LayerIds) -> LockedLayeredIndex<'_> {
        let edge = self.edge(eref.pid());
        edge.additions(layer_ids).unwrap()
    }

    fn vertex_additions(&self, v: VID) -> LockedView<TimeIndex> {
        let vertex = self.vertex(v);
        vertex.additions().unwrap()
    }

    fn localise_vertex_unchecked(&self, v: VertexRef) -> VID {
        match v {
            VertexRef::Local(l) => l,
            VertexRef::Remote(_) => self.resolve_vertex_ref(&v).unwrap(),
        }
    }

    fn static_prop_names(&self) -> Vec<String> {
        self.static_property_names()
    }

    fn static_prop(&self, name: &str) -> Option<Prop> {
        self.get_static_prop(name).map(|p| p.clone())
    }

    fn temporal_prop_names(&self) -> Vec<String> {
        self.temporal_property_names()
    }

    fn temporal_prop(&self, name: &str) -> Option<LockedView<TProp>> {
        self.get_temporal_prop(name)
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
        let vertex = self.vertex(v);
        let prop_id = self.vertex_find_prop(name, false)?;

        vertex.temporal_property(prop_id)
    }

    fn temporal_vertex_prop_names(&self, v: VID) -> Vec<String> {
        self.vertex_temp_prop_ids(v)
            .into_iter()
            .flat_map(|id| self.vertex_reverse_prop_id(id, false))
            .collect()
    }

    fn all_vertex_prop_names(&self, is_static: bool) -> Vec<String> {
        self.get_all_vertex_property_names(is_static)
    }

    fn all_edge_prop_names(&self, is_static: bool) -> Vec<String> {
        self.get_all_edge_property_names(is_static)
    }

    fn static_edge_prop(&self, e: EdgeRef, name: &str) -> Option<Prop> {
        let entry = self.edge_entry(e.pid());
        let prop_id = self.edge_find_prop(name, true)?;
        let layer = entry.unsafe_layer(0); // FIXME: this should take an array of layer ids
        let prop = layer.static_property(prop_id).map(|p| p.clone());
        prop
    }

    fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        if let Some(edge) = self.edge_entry(e.pid()).value() {
            return edge
                .unsafe_layer(0) // FIXME: very broken
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

        edge.temporal_property(0, prop_id) // FIXME: very broken
    }

    fn temporal_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.edge_temp_prop_ids(e.pid())
            .into_iter()
            .flat_map(|id| self.edge_reverse_prop_id(id, false))
            .collect()
    }

    fn unfiltered_num_vertices(&self) -> usize {
        self.internal_num_vertices()
    }
}
