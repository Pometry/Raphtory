use std::borrow::Cow;

use raphtory_api::core::{
    entities::{edges::edge_ref::EdgeRef, GidRef, VID},
    Direction,
};

use crate::{
    core::{
        entities::{nodes::node_store::NodeStore, properties::tprop::TPropCell, LayerIds},
        Prop,
    },
    db::api::{
        storage::graph::{
            nodes::node_storage_ops::NodeStorageOps,
            tprop_storage_ops::{SparseTPropOps, TPropOps},
        },
        view::internal::NodeAdditions,
    },
};

use super::TColumns;

#[derive(Debug, Copy, Clone)]
pub struct NodeEntry<'a> {
    node: &'a NodeStore,
    t_props_log: &'a TColumns,
}

impl<'a> NodeEntry<'a> {
    pub fn new(node: &'a NodeStore, t_props_log: &'a TColumns) -> Self {
        Self { node, t_props_log }
    }

    pub fn node(&self) -> &'a NodeStore {
        self.node
    }

    pub fn t_prop(self, prop_id: usize) -> TPropCell<'a> {
        self.t_props_log
            .get(prop_id)
            .map(|t_prop| TPropCell::new(&self.node.timestamps().props_ts, t_prop))
            .unwrap_or(TPropCell::empty())
    }

    pub fn temporal_prop_ids(self) -> impl Iterator<Item = usize> + 'a {
        self.t_props_log
            .t_props_log
            .iter()
            .enumerate()
            .filter_map(|(id, col)| col.is_empty().then(|| id))
    }
}

impl<'a> NodeStorageOps<'a> for NodeEntry<'a> {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize {
        self.node.degree(layers, dir)
    }

    fn additions(self) -> NodeAdditions<'a> {
        NodeAdditions::Mem(self.node.timestamps())
    }

    fn tprop(self, prop_id: usize) -> impl SparseTPropOps<'a> {
        self.t_prop(prop_id)
    }

    fn prop(self, prop_id: usize) -> Option<Prop> {
        self.node.constant_property(prop_id).cloned()
    }

    fn edges_iter(self, layers: &LayerIds, dir: Direction) -> impl Iterator<Item = EdgeRef> + 'a {
        self.node.edge_tuples(layers, dir)
    }

    fn node_type_id(self) -> usize {
        self.node.node_type
    }

    fn vid(self) -> VID {
        self.node.vid
    }

    fn id(self) -> GidRef<'a> {
        (&self.node.global_id).into()
    }

    fn name(self) -> Option<Cow<'a, str>> {
        self.node.global_id.as_str().map(Cow::from)
    }

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef> {
        let eid = NodeStore::find_edge_eid(&self.node, dst, layer_ids)?;
        Some(EdgeRef::new_outgoing(eid, self.node.vid, dst))
    }
}
