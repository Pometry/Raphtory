use crate::{
    core::{
        entities::{
            edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
            graph::tgraph::InnerTemporalGraph,
            properties::{
                props::{ArcReadLockedVec, Meta},
                tprop::{LockedLayeredTProp, TProp},
            },
            vertices::{vertex_ref::VertexRef, vertex_store::VertexStore},
            LayerIds, EID, VID,
        },
        storage::{
            locked_view::LockedView,
            timeindex::{LockedLayeredIndex, TimeIndex, TimeIndexEntry},
            ArcEntry,
        },
        ArcStr,
    },
    db::api::view::{internal::CoreGraphOps, BoxedIter},
    prelude::Prop,
};
use itertools::Itertools;
use std::{collections::HashMap, iter};

impl<const N: usize> CoreGraphOps for InnerTemporalGraph<N> {
    #[inline]
    fn unfiltered_num_vertices(&self) -> usize {
        self.inner().internal_num_vertices()
    }

    #[inline]
    fn vertex_meta(&self) -> &Meta {
        &self.inner().vertex_meta
    }

    #[inline]
    fn edge_meta(&self) -> &Meta {
        &self.inner().edge_meta
    }

    #[inline]
    fn get_layer_name(&self, layer_id: usize) -> Option<ArcStr> {
        self.inner().edge_meta.layer_meta().get_name(layer_id)
    }

    #[inline]
    fn get_layer_id(&self, name: &str) -> Option<usize> {
        self.inner().edge_meta.get_layer_id(name)
    }

    #[inline]
    fn get_layer_names_from_ids(&self, layer_ids: LayerIds) -> BoxedIter<ArcStr> {
        self.inner().layer_names(layer_ids)
    }

    #[inline]
    fn vertex_id(&self, v: VID) -> u64 {
        self.inner().global_vertex_id(v)
    }

    #[inline]
    fn vertex_name(&self, v: VID) -> String {
        self.inner().vertex_name(v)
    }

    #[inline]
    fn edge_additions(
        &self,
        eref: EdgeRef,
        layer_ids: LayerIds,
    ) -> LockedLayeredIndex<'_, TimeIndexEntry> {
        let layer_ids = layer_ids.constrain_from_edge(eref);
        let edge = self.inner().edge(eref.pid());
        edge.additions(layer_ids).unwrap()
    }

    #[inline]
    fn vertex_additions(&self, v: VID) -> LockedView<TimeIndex<i64>> {
        let vertex = self.inner().vertex(v);
        vertex.additions().unwrap()
    }

    #[inline]
    fn internalise_vertex(&self, v: VertexRef) -> Option<VID> {
        self.inner().resolve_vertex_ref(v)
    }

    #[inline]
    fn internalise_vertex_unchecked(&self, v: VertexRef) -> VID {
        match v {
            VertexRef::Internal(l) => l,
            VertexRef::External(_) => self.inner().resolve_vertex_ref(v).unwrap(),
        }
    }

    #[inline]
    fn constant_prop_names(&self) -> ArcReadLockedVec<ArcStr> {
        self.inner().constant_property_names()
    }

    #[inline]
    fn constant_prop(&self, name: &str) -> Option<Prop> {
        self.inner().get_constant_prop(name)
    }

    #[inline]
    fn temporal_prop_names(&self) -> ArcReadLockedVec<ArcStr> {
        self.inner().temporal_property_names()
    }

    #[inline]
    fn temporal_prop(&self, name: &str) -> Option<LockedView<TProp>> {
        self.inner().get_temporal_prop(name)
    }

    #[inline]
    fn constant_vertex_prop(&self, v: VID, name: &str) -> Option<Prop> {
        let entry = self.inner().node_entry(v);
        let node = entry.value();
        let prop_id = self.inner().vertex_find_prop(name, true)?;
        node.static_property(prop_id).cloned()
    }

    #[inline]
    fn constant_vertex_prop_names(&self, v: VID) -> BoxedIter<ArcStr> {
        let keys = self.inner().vertex_meta.constant_prop_meta().get_keys();
        let ids = self.inner().node_entry(v).constant_prop_ids();
        Box::new(ids.into_iter().map(move |prop_id| keys[prop_id].clone()))
    }

    #[inline]
    fn temporal_vertex_prop(&self, v: VID, name: &str) -> Option<LockedView<TProp>> {
        let vertex = self.inner().vertex(v);
        let prop_id = self.inner().vertex_find_prop(name, false)?;

        vertex.temporal_property(prop_id)
    }

    #[inline]
    fn temporal_vertex_prop_names(&self, v: VID) -> BoxedIter<ArcStr> {
        let keys = self.inner().vertex_meta.temporal_prop_meta().get_keys();
        Box::new(
            self.inner()
                .vertex_temp_prop_ids(v)
                .into_iter()
                .map(move |id| keys[id].clone()),
        )
    }

    #[inline]
    fn all_vertex_prop_names(&self, is_static: bool) -> ArcReadLockedVec<ArcStr> {
        self.inner().get_all_vertex_property_names(is_static)
    }

    #[inline]
    fn all_edge_prop_names(&self, is_static: bool) -> ArcReadLockedVec<ArcStr> {
        self.inner().get_all_edge_property_names(is_static)
    }

    fn constant_edge_prop(&self, e: EdgeRef, name: &str, layer_ids: LayerIds) -> Option<Prop> {
        let layer_ids = layer_ids.constrain_from_edge(e);
        let entry = self.inner().edge_entry(e.pid());
        let prop_id = self.inner().edge_find_prop(name, true)?;
        match layer_ids {
            LayerIds::None => None,
            LayerIds::All => {
                if self.inner().num_layers() == 1 {
                    // iterator has at most 1 element
                    entry
                        .layer_iter()
                        .next()
                        .and_then(|layer| layer.static_property(prop_id).cloned())
                } else {
                    let prop_map: HashMap<_, _> = entry
                        .layer_iter()
                        .enumerate()
                        .flat_map(|(id, layer)| {
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
            LayerIds::One(id) => entry
                .layer(id)
                .and_then(|l| l.static_property(prop_id).cloned()),
            LayerIds::Multiple(ids) => {
                let prop_map: HashMap<_, _> = ids
                    .iter()
                    .flat_map(|&id| {
                        entry.layer(id).and_then(|layer| {
                            layer
                                .static_property(prop_id)
                                .map(|p| (self.inner().get_layer_name(id), p.clone()))
                        })
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

    fn constant_edge_prop_names(&self, e: EdgeRef, layer_ids: LayerIds) -> BoxedIter<ArcStr> {
        let layer_ids = layer_ids.constrain_from_edge(e);
        let entry = self.inner().edge_entry(e.pid());
        let reverse_map = self.inner().edge_meta.constant_prop_meta().get_keys();
        match layer_ids {
            LayerIds::None => Box::new(iter::empty()),
            LayerIds::All => Box::new(
                entry
                    .layer_iter()
                    .map(|l| l.static_prop_ids())
                    .kmerge()
                    .dedup()
                    .map(move |id| reverse_map[id].clone()),
            ),
            LayerIds::One(id) => match entry.layer(id) {
                Some(l) => Box::new(
                    l.static_prop_ids()
                        .into_iter()
                        .map(move |id| reverse_map[id].clone()),
                ),
                None => Box::new(iter::empty()),
            },
            LayerIds::Multiple(ids) => Box::new(
                ids.iter()
                    .flat_map(|id| entry.layer(*id).map(|l| l.static_prop_ids()))
                    .kmerge()
                    .dedup()
                    .map(move |id| reverse_map[id].clone()),
            ),
        }
    }

    #[inline]
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

    fn temporal_edge_prop_names(&self, e: EdgeRef, layer_ids: LayerIds) -> BoxedIter<ArcStr> {
        let layer_ids = layer_ids.constrain_from_edge(e);
        let entry = self.inner().edge_entry(e.pid());
        let reverse_map = self.inner().edge_meta.temporal_prop_meta().get_keys();
        match layer_ids {
            LayerIds::None => Box::new(iter::empty()),
            LayerIds::All => Box::new(
                entry
                    .temp_prop_ids(None)
                    .into_iter()
                    .map(move |id| reverse_map[id].clone()),
            ),
            LayerIds::One(id) => Box::new(
                entry
                    .temp_prop_ids(Some(id))
                    .into_iter()
                    .map(move |id| reverse_map[id].clone()),
            ),
            LayerIds::Multiple(ids) => Box::new(
                ids.iter()
                    .map(|id| entry.temp_prop_ids(Some(*id)))
                    .kmerge()
                    .dedup()
                    .map(move |id| reverse_map[id].clone()),
            ),
        }
    }

    #[inline]
    fn core_edges(&self) -> Box<dyn Iterator<Item = ArcEntry<EdgeStore>>> {
        Box::new(self.inner().storage.edges.read_lock().into_iter())
    }

    #[inline]
    fn core_edge(&self, eid: EID) -> ArcEntry<EdgeStore> {
        self.inner().storage.edges.entry_arc(eid.into())
    }

    #[inline]
    fn core_vertices(&self) -> Box<dyn Iterator<Item = ArcEntry<VertexStore>>> {
        Box::new(self.inner().storage.nodes.read_lock().into_iter())
    }

    #[inline]
    fn core_vertex(&self, vid: VID) -> ArcEntry<VertexStore> {
        self.inner().storage.nodes.entry_arc(vid.into())
    }
}

#[cfg(test)]
mod test_edges {
    use crate::{
        core::{ArcStr, IntoPropMap},
        prelude::*,
    };
    use std::collections::HashMap;

    #[test]
    fn test_edge_properties_for_layers() {
        let g = Graph::new();

        g.add_edge(0, 1, 2, [("t", 0)], Some("layer1"))
            .unwrap()
            .add_constant_properties(
                [("layer1", "1".into_prop()), ("layer", 1.into_prop())],
                Some("layer1"),
            )
            .unwrap();
        g.add_edge(1, 1, 2, [("t", 1)], Some("layer2"))
            .unwrap()
            .add_constant_properties([("layer", 2)], Some("layer2"))
            .unwrap();

        g.add_edge(2, 1, 2, [("t2", 2)], Some("layer3"))
            .unwrap()
            .add_constant_properties([("layer", 3)], Some("layer3"))
            .unwrap();

        let e_all = g.edge(1, 2).unwrap();
        assert_eq!(
            e_all.properties().constant().as_map(),
            HashMap::from([
                (
                    ArcStr::from("layer"),
                    [("layer1", 1), ("layer2", 2), ("layer3", 3)].into_prop_map()
                ),
                (ArcStr::from("layer1"), [("layer1", "1")].into_prop_map())
            ])
        );
        assert_eq!(
            e_all.properties().temporal().get("t").unwrap().values(),
            vec![0.into(), 1.into()]
        );

        let e = g.edge(1, 2).unwrap().layer("layer1").unwrap();
        assert!(e.properties().constant().contains("layer1"));
    }
}
