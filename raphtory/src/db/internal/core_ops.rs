use crate::{
    core::{
        entities::{
            edges::edge_ref::EdgeRef,
            graph::tgraph::InnerTemporalGraph,
            properties::tprop::{LockedLayeredTProp, TProp},
            vertices::vertex_ref::VertexRef,
            LayerIds, VID,
        },
        storage::{
            locked_view::LockedView,
            timeindex::{LockedLayeredIndex, TimeIndex},
        },
    },
    db::api::view::internal::CoreGraphOps,
    prelude::Prop,
};
use itertools::Itertools;
use std::{collections::HashMap, iter};

impl<const N: usize> CoreGraphOps for InnerTemporalGraph<N> {
    fn unfiltered_num_vertices(&self) -> usize {
        self.inner().internal_num_vertices()
    }

    fn get_layer_names_from_ids(&self, layer_ids: LayerIds) -> Vec<String> {
        self.inner().layer_names(layer_ids)
    }

    fn vertex_id(&self, v: VID) -> u64 {
        self.inner()
            .global_vertex_id(v)
            .unwrap_or_else(|| panic!("vertex id '{v:?}' doesn't exist"))
    }

    fn vertex_name(&self, v: VID) -> String {
        self.inner().vertex_name(v)
    }

    fn edge_additions(&self, eref: EdgeRef, layer_ids: LayerIds) -> LockedLayeredIndex<'_> {
        let layer_ids = layer_ids.constrain_from_edge(eref);
        let edge = self.inner().edge(eref.pid());
        edge.additions(layer_ids).unwrap()
    }

    fn vertex_additions(&self, v: VID) -> LockedView<TimeIndex> {
        let vertex = self.inner().vertex(v);
        vertex.additions().unwrap()
    }

    fn localise_vertex_unchecked(&self, v: VertexRef) -> VID {
        match v {
            VertexRef::Local(l) => l,
            VertexRef::Remote(_) => self.inner().resolve_vertex_ref(&v).unwrap(),
        }
    }

    fn static_prop_names(&self) -> Vec<String> {
        self.inner()
            .static_property_names()
            .iter()
            .cloned()
            .collect()
    }

    fn static_prop(&self, name: &str) -> Option<Prop> {
        self.inner().get_static_prop(name)
    }

    fn temporal_prop_names(&self) -> Vec<String> {
        self.inner()
            .temporal_property_names()
            .iter()
            .cloned()
            .collect()
    }

    fn temporal_prop(&self, name: &str) -> Option<LockedView<TProp>> {
        self.inner().get_temporal_prop(name)
    }

    fn static_vertex_prop(&self, v: VID, name: &str) -> Option<Prop> {
        let entry = self.inner().node_entry(v);
        let node = entry.value()?;
        let prop_id = self.inner().vertex_find_prop(name, true)?;
        node.static_property(prop_id).cloned()
    }

    fn static_vertex_prop_names<'a>(
        &'a self,
        v: VID,
    ) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a> {
        let ids = self.inner().node_entry(v).static_prop_ids();
        Box::new(
            ids.into_iter()
                .flat_map(|prop_id| self.inner().vertex_reverse_prop_id(prop_id, true)),
        )
    }

    fn temporal_vertex_prop(&self, v: VID, name: &str) -> Option<LockedView<TProp>> {
        let vertex = self.inner().vertex(v);
        let prop_id = self.inner().vertex_find_prop(name, false)?;

        vertex.temporal_property(prop_id)
    }

    fn temporal_vertex_prop_names<'a>(
        &'a self,
        v: VID,
    ) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a> {
        Box::new(
            self.inner()
                .vertex_temp_prop_ids(v)
                .into_iter()
                .flat_map(|id| self.inner().vertex_reverse_prop_id(id, false)),
        )
    }

    fn all_vertex_prop_names(&self, is_static: bool) -> Vec<String> {
        self.inner().get_all_vertex_property_names(is_static)
    }

    fn all_edge_prop_names(&self, is_static: bool) -> Vec<String> {
        self.inner().get_all_edge_property_names(is_static)
    }

    fn static_edge_prop(&self, e: EdgeRef, name: &str, layer_ids: LayerIds) -> Option<Prop> {
        let layer_ids = layer_ids.constrain_from_edge(e);
        let entry = self.inner().edge_entry(e.pid());
        let prop_id = self.inner().edge_find_prop(name, true)?;
        match layer_ids {
            LayerIds::None => None,
            LayerIds::All => {
                let layers_ids = self.inner().get_all_layers();
                let num_layers = layers_ids.len();
                if num_layers == 1 {
                    let layer_id = layers_ids[0];
                    let layer = entry.unsafe_layer(layer_id);
                    layer.static_property(prop_id).cloned()
                } else {
                    let prop_map: HashMap<_, _> = self
                        .inner()
                        .get_all_layers()
                        .into_iter()
                        .flat_map(|id| {
                            let layer = entry.unsafe_layer(id);
                            layer
                                .static_property(prop_id)
                                .map(|p| (self.inner().get_layer_name(id), p.clone()))
                        })
                        .collect();
                    if prop_map.is_empty() {
                        None
                    } else {
                        Some(prop_map.into())
                    }
                }
            }
            LayerIds::One(id) => {
                let layer = entry.unsafe_layer(id);
                layer.static_property(prop_id).cloned()
            }
            LayerIds::Multiple(ids) => {
                let prop_map: HashMap<_, _> = ids
                    .iter()
                    .flat_map(|&id| {
                        let layer = entry.unsafe_layer(id);
                        layer
                            .static_property(prop_id)
                            .map(|p| (self.inner().get_layer_name(id), p.clone()))
                    })
                    .collect();
                if prop_map.is_empty() {
                    None
                } else {
                    Some(prop_map.into())
                }
            }
        }
    }

    fn static_edge_prop_names<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: LayerIds,
    ) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a> {
        let layer_ids = layer_ids.constrain_from_edge(e);
        let entry = self.inner().edge_entry(e.pid());
        match layer_ids {
            LayerIds::None => Box::new(iter::empty()),
            LayerIds::All => Box::new(
                entry
                    .layer_ids_iter()
                    .map(|id| entry.unsafe_layer(id).static_prop_ids())
                    .kmerge()
                    .dedup()
                    .flat_map(|id| self.inner().edge_reverse_prop_id(id, true)),
            ),
            LayerIds::One(id) => Box::new(
                entry
                    .unsafe_layer(id)
                    .static_prop_ids()
                    .into_iter()
                    .flat_map(|id| self.inner().edge_reverse_prop_id(id, true)),
            ),
            LayerIds::Multiple(ids) => Box::new(
                ids.iter()
                    .map(|id| entry.unsafe_layer(*id).static_prop_ids())
                    .kmerge()
                    .dedup()
                    .flat_map(|id| self.inner().edge_reverse_prop_id(id, true)),
            ),
        }
    }

    fn temporal_edge_prop(
        &self,
        e: EdgeRef,
        name: &str,
        layer_ids: LayerIds,
    ) -> Option<LockedLayeredTProp> {
        let layer_ids = layer_ids.constrain_from_edge(e);
        let edge = self.inner().edge(e.pid());
        let prop_id = self.inner().edge_find_prop(name, false)?;
        edge.temporal_property(layer_ids, prop_id)
    }

    fn temporal_edge_prop_names<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: LayerIds,
    ) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a> {
        let layer_ids = layer_ids.constrain_from_edge(e);
        let entry = self.inner().edge_entry(e.pid());
        match layer_ids {
            LayerIds::None => Box::new(iter::empty()),
            LayerIds::All => Box::new(
                entry
                    .temp_prop_ids(None)
                    .into_iter()
                    .flat_map(|id| self.inner().edge_reverse_prop_id(id, false)),
            ),
            LayerIds::One(id) => Box::new(
                entry
                    .temp_prop_ids(Some(id))
                    .into_iter()
                    .flat_map(|id| self.inner().edge_reverse_prop_id(id, false)),
            ),
            LayerIds::Multiple(ids) => Box::new(
                ids.iter()
                    .map(|id| entry.temp_prop_ids(Some(*id)))
                    .kmerge()
                    .dedup()
                    .flat_map(|id| self.inner().edge_reverse_prop_id(id, false)),
            ),
        }
    }
}

#[cfg(test)]
mod test_edges {
    use crate::prelude::*;
    #[test]
    fn test_static_edge_property_for_layers() {
        let g = Graph::new();

        g.add_edge(0, 1, 2, NO_PROPS, Some("layer1")).unwrap();
        g.add_edge_properties(1, 2, [("layer1", "1")], Some("layer1"))
            .unwrap();

        let e = g.edge(1, 2, "layer1").unwrap();
        assert!(e.properties().constant().contains("layer1"));
    }
}
