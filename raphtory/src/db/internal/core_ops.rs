use crate::{
    core::{
        entities::{
            edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
            graph::tgraph::InnerTemporalGraph,
            properties::{
                graph_props::GraphProps,
                props::Meta,
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
    fn graph_meta(&self) -> &GraphProps {
        &self.inner().graph_props
    }

    #[inline]
    fn get_layer_name(&self, layer_id: usize) -> ArcStr {
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
    fn constant_prop(&self, id: usize) -> Option<Prop> {
        self.inner().get_constant_prop(id)
    }

    #[inline]
    fn temporal_prop(&self, id: usize) -> Option<LockedView<TProp>> {
        self.inner().get_temporal_prop(id)
    }

    #[inline]
    fn constant_vertex_prop(&self, v: VID, prop_id: usize) -> Option<Prop> {
        let entry = self.inner().node_entry(v);
        entry.const_prop(prop_id).cloned()
    }

    #[inline]
    fn constant_vertex_prop_ids(&self, v: VID) -> Box<dyn Iterator<Item = usize> + '_> {
        // FIXME: revisit the locking scheme so we don't have to collect the ids
        Box::new(
            self.inner()
                .node_entry(v)
                .const_prop_ids()
                .collect_vec()
                .into_iter(),
        )
    }

    #[inline]
    fn temporal_vertex_prop(&self, v: VID, prop_id: usize) -> Option<LockedView<TProp>> {
        let vertex = self.inner().vertex(v);
        vertex.temporal_property(prop_id)
    }

    #[inline]
    fn temporal_vertex_prop_ids(&self, v: VID) -> Box<dyn Iterator<Item = usize> + '_> {
        // FIXME: revisit the locking scheme so we don't have to collect the ids
        Box::new(
            self.inner()
                .node_entry(v)
                .temporal_prop_ids()
                .collect_vec()
                .into_iter(),
        )
    }

    fn get_const_edge_prop(&self, e: EdgeRef, prop_id: usize, layer_ids: LayerIds) -> Option<Prop> {
        let layer_ids = layer_ids.constrain_from_edge(e);
        let entry = self.inner().edge_entry(e.pid());
        match layer_ids {
            LayerIds::None => None,
            LayerIds::All => {
                if self.inner().num_layers() == 1 {
                    // iterator has at most 1 element
                    entry
                        .layer_iter()
                        .next()
                        .and_then(|layer| layer.const_prop(prop_id).cloned())
                } else {
                    let prop_map: HashMap<_, _> = entry
                        .layer_iter()
                        .enumerate()
                        .flat_map(|(id, layer)| {
                            layer
                                .const_prop(prop_id)
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
            LayerIds::One(id) => entry.layer(id).and_then(|l| l.const_prop(prop_id).cloned()),
            LayerIds::Multiple(ids) => {
                let prop_map: HashMap<_, _> = ids
                    .iter()
                    .flat_map(|&id| {
                        entry.layer(id).and_then(|layer| {
                            layer
                                .const_prop(prop_id)
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

    fn const_edge_prop_ids(
        &self,
        e: EdgeRef,
        layer_ids: LayerIds,
    ) -> Box<dyn Iterator<Item = usize> + '_> {
        // FIXME: revisit the locking scheme so we don't have to collect all the ids
        let layer_ids = layer_ids.constrain_from_edge(e);
        let entry = self.inner().edge_entry(e.pid());
        let ids: Vec<_> = match layer_ids {
            LayerIds::None => vec![],
            LayerIds::All => entry
                .layer_iter()
                .map(|l| l.const_prop_ids())
                .kmerge()
                .dedup()
                .collect(),
            LayerIds::One(id) => match entry.layer(id) {
                Some(l) => l.const_prop_ids().collect(),
                None => vec![],
            },
            LayerIds::Multiple(ids) => ids
                .iter()
                .flat_map(|id| entry.layer(*id).map(|l| l.const_prop_ids()))
                .kmerge()
                .dedup()
                .collect(),
        };
        Box::new(ids.into_iter())
    }

    #[inline]
    fn temporal_edge_prop(
        &self,
        e: EdgeRef,
        prop_id: usize,
        layer_ids: LayerIds,
    ) -> Option<LockedLayeredTProp> {
        let layer_ids = layer_ids.constrain_from_edge(e);
        let edge = self.inner().edge(e.pid());
        edge.temporal_property(layer_ids, prop_id)
    }

    fn temporal_edge_prop_ids(
        &self,
        e: EdgeRef,
        layer_ids: LayerIds,
    ) -> Box<dyn Iterator<Item = usize> + '_> {
        // FIXME: revisit the locking scheme so we don't have to collect the ids
        let entry = self.inner().edge_entry(e.pid());
        match layer_ids {
            LayerIds::None => Box::new(iter::empty()),
            LayerIds::All => Box::new(entry.temp_prop_ids(None).collect_vec().into_iter()),
            LayerIds::One(id) => Box::new(entry.temp_prop_ids(Some(id)).collect_vec().into_iter()),
            LayerIds::Multiple(ids) => Box::new(
                ids.iter()
                    .map(|id| entry.temp_prop_ids(Some(*id)))
                    .kmerge()
                    .dedup()
                    .collect_vec()
                    .into_iter(),
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
