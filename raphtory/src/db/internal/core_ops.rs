use crate::{
    core::{
        entities::{
            edges::{edge_ref::EdgeRef, edge_store::EdgeDataLike},
            graph::tgraph::InternalGraph,
            nodes::node_ref::NodeRef,
            properties::{graph_meta::GraphMeta, props::Meta, tprop::TProp},
            LayerIds, ELID, VID,
        },
        storage::locked_view::LockedView,
    },
    db::api::{
        storage::{
            edges::{
                edge_entry::EdgeStorageEntry, edge_owned_entry::EdgeOwnedEntry, edges::EdgesStorage,
            },
            nodes::{
                node_entry::NodeStorageEntry, node_owned_entry::NodeOwnedEntry, nodes::NodesStorage,
            },
            storage_ops::GraphStorage,
        },
        view::{internal::CoreGraphOps, BoxedIter},
    },
    prelude::Prop,
};
use itertools::Itertools;
use raphtory_api::core::storage::arc_str::ArcStr;
use std::{collections::HashMap, iter, sync::Arc};

impl CoreGraphOps for InternalGraph {
    #[inline]
    fn unfiltered_num_nodes(&self) -> usize {
        self.inner().internal_num_nodes()
    }

    fn unfiltered_num_layers(&self) -> usize {
        self.inner().num_layers()
    }

    fn core_graph(&self) -> &GraphStorage {
        todo!("this entire thing should go away")
    }
    #[inline]
    fn node_meta(&self) -> &Meta {
        &self.inner().node_meta
    }

    #[inline]
    fn edge_meta(&self) -> &Meta {
        &self.inner().edge_meta
    }

    #[inline]
    fn graph_meta(&self) -> &GraphMeta {
        &self.inner().graph_meta
    }

    #[inline]
    fn get_layer_name(&self, layer_id: usize) -> ArcStr {
        self.inner()
            .edge_meta
            .layer_meta()
            .get_name(layer_id)
            .clone()
    }

    #[inline]
    fn get_layer_id(&self, name: &str) -> Option<usize> {
        self.inner().edge_meta.get_layer_id(name)
    }

    #[inline]
    fn get_layer_names_from_ids(&self, layer_ids: &LayerIds) -> BoxedIter<ArcStr> {
        self.inner().layer_names(layer_ids)
    }

    #[inline]
    fn get_all_node_types(&self) -> Vec<ArcStr> {
        self.inner().get_all_node_types()
    }

    #[inline]
    fn node_id(&self, v: VID) -> u64 {
        self.inner().global_node_id(v)
    }

    #[inline]
    fn node_name(&self, v: VID) -> String {
        self.inner().node_name(v)
    }

    #[inline]
    fn node_type(&self, v: VID) -> Option<ArcStr> {
        self.inner().node_type(v)
    }

    #[inline]
    fn node_type_id(&self, v: VID) -> usize {
        self.inner().node_type_id(v)
    }
    #[inline]
    fn internalise_node(&self, v: NodeRef) -> Option<VID> {
        self.inner().resolve_node_ref(v)
    }

    #[inline]
    fn internalise_node_unchecked(&self, v: NodeRef) -> VID {
        match v {
            NodeRef::Internal(l) => l,
            NodeRef::External(_) => self.inner().resolve_node_ref(v).unwrap(),
            NodeRef::ExternalStr(_) => self.inner().resolve_node_ref(v).unwrap(),
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
    fn constant_node_prop(&self, v: VID, prop_id: usize) -> Option<Prop> {
        let entry = self.inner().storage.get_node(v);
        entry.const_prop(prop_id).cloned()
    }

    #[inline]
    fn constant_node_prop_ids(&self, v: VID) -> Box<dyn Iterator<Item = usize> + '_> {
        // FIXME: revisit the locking scheme so we don't have to collect the ids
        Box::new(
            self.inner()
                .storage
                .get_node(v)
                .const_prop_ids()
                .collect_vec()
                .into_iter(),
        )
    }

    #[inline]
    fn temporal_node_prop_ids(&self, v: VID) -> Box<dyn Iterator<Item = usize> + '_> {
        // FIXME: revisit the locking scheme so we don't have to collect the ids
        Box::new(
            self.inner()
                .storage
                .get_node(v)
                .temporal_prop_ids()
                .collect_vec()
                .into_iter(),
        )
    }

    #[inline]
    fn core_edges(&self) -> EdgesStorage {
        EdgesStorage::Mem(Arc::new(self.inner().storage.edges_read_lock()))
    }

    #[inline]
    fn core_nodes(&self) -> NodesStorage {
        NodesStorage::Mem(Arc::new(self.inner().storage.nodes_read_lock()))
    }

    #[inline]
    fn core_edge(&self, eid: ELID) -> EdgeStorageEntry {
        EdgeStorageEntry::Unlocked(self.inner().storage.edge_entry(eid.pid()))
    }

    #[inline]
    fn core_node_entry(&self, vid: VID) -> NodeStorageEntry {
        NodeStorageEntry::Unlocked(self.inner().storage.nodes.entry(vid))
    }

    fn core_node_arc(&self, vid: VID) -> NodeOwnedEntry {
        NodeOwnedEntry::Mem(self.inner().storage.nodes.entry_arc(vid))
    }

    fn core_edge_arc(&self, eid: ELID) -> EdgeOwnedEntry {
        EdgeOwnedEntry::Mem(self.inner().storage.get_edge_arc(eid.pid()))
    }

    #[inline]
    fn unfiltered_num_edges(&self) -> usize {
        self.inner().storage.edges_len()
    }
}

#[cfg(test)]
mod test_edges {
    use raphtory_api::core::storage::arc_str::ArcStr;
    use std::collections::HashMap;

    use crate::{core::IntoPropMap, prelude::*};

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

        let e = g.edge(1, 2).unwrap().layers("layer1").unwrap();
        assert!(e.properties().constant().contains("layer1"));
    }
}
