use itertools::Itertools;

use crate::{
    arrow::{graph_impl::ArrowGraph, GID},
    core::{
        entities::{
            edges::edge_ref::EdgeRef,
            nodes::{input_node::InputNode, node_ref::NodeRef, node_store::NodeStore},
            properties::{
                graph_meta::GraphMeta,
                props::Meta,
                tprop::{LockedLayeredTProp, TProp},
            },
            LayerIds, EID, VID,
        },
        storage::{locked_view::LockedView, ArcEntry},
        ArcStr, Prop,
    },
    db::api::view::{
        internal::{CoreEdgeView, CoreGraphOps, EdgeUpdates, NodeAdditions},
        BoxedIter,
    },
};

use super::tprops::read_tprop_column;

impl CoreGraphOps for ArrowGraph {
    fn unfiltered_num_nodes(&self) -> usize {
        self.inner.num_nodes()
    }

    fn node_meta(&self) -> &Meta {
        &self.node_meta
    }

    fn edge_meta(&self) -> &Meta {
        &self.edge_meta
    }

    fn graph_meta(&self) -> &GraphMeta {
        &self.graph_props
    }

    fn get_layer_name(&self, layer_id: usize) -> ArcStr {
        let name = &self.inner.layer_names()[layer_id];
        ArcStr::from(name.as_str())
    }

    fn get_layer_id(&self, name: &str) -> Option<usize> {
        self.inner.find_layer_id(name)
    }

    fn get_layer_names_from_ids(&self, layer_ids: LayerIds) -> BoxedIter<ArcStr> {
        match layer_ids {
            LayerIds::None => Box::new(std::iter::empty()),
            LayerIds::All => Box::new(
                self.inner
                    .layer_names_vec()
                    .into_iter()
                    .map(|s| ArcStr::from(s)),
            ),
            LayerIds::One(id) => Box::new(
                self.inner
                    .layer_names()
                    .get(id)
                    .cloned()
                    .into_iter()
                    .map(|s| ArcStr::from(s)),
            ),
            LayerIds::Multiple(ids) => Box::new(
                ids.iter()
                    .copied()
                    .filter_map(|id| self.inner.layer_names().get(id).cloned())
                    .collect_vec()
                    .into_iter()
                    .map(|s| ArcStr::from(s)),
            ),
        }
    }

    fn get_all_node_types(&self) -> Vec<ArcStr> {
        todo!("Node types are not supported on arrow yet")
    }

    fn node_id(&self, v: VID) -> u64 {
        match self.inner.node_gid(v).unwrap() {
            GID::U64(n) => n,
            GID::I64(n) => n as u64,
            GID::Str(s) => s.id(),
        }
    }

    fn node_name(&self, v: VID) -> String {
        match self.inner.node_gid(v).unwrap() {
            GID::U64(n) => n.to_string(),
            GID::I64(n) => n.to_string(),
            GID::Str(s) => s,
        }
    }

    fn node_type(&self, _v: VID) -> Option<ArcStr> {
        todo!("Node types are not supported on arrow yet")
    }

    fn edge_additions(&self, eref: EdgeRef, layer_ids: LayerIds) -> EdgeUpdates {
        let layer_ids = layer_ids.constrain_from_edge(eref);

        let layer_id = match layer_ids {
            LayerIds::One(id) => id,
            _ => todo!("Edge views with multiple layers are not supported on arrow yet"),
        };

        let edge = self.inner.edge(eref.pid(), layer_id);
        EdgeUpdates::Col(edge.timestamps())
    }

    fn node_additions(&self, v: VID) -> NodeAdditions {
        let node = self.inner.internal_node(v, 0);
        NodeAdditions::Col(node.timestamps())
    }

    fn internalise_node(&self, v: NodeRef) -> Option<VID> {
        match v {
            NodeRef::Internal(vid) => Some(vid),
            NodeRef::External(vid) => self.inner.find_node(&GID::U64(vid)),
            NodeRef::ExternalStr(string) => self.inner.find_node(&GID::Str(string.into())),
        }
    }

    fn internalise_node_unchecked(&self, v: NodeRef) -> VID {
        self.internalise_node(v).unwrap()
    }

    fn constant_prop(&self, id: usize) -> Option<Prop> {
        self.graph_props.get_constant(id)
    }

    fn temporal_prop(&self, id: usize) -> Option<LockedView<TProp>> {
        self.graph_props.get_temporal_prop(id)
    }

    fn constant_node_prop(&self, v: VID, id: usize) -> Option<Prop> {
        match &self.inner.node_properties {
            None => None,
            Some(props) => props.const_props.prop(v, id),
        }
    }

    fn constant_node_prop_ids(&self, v: VID) -> Box<dyn Iterator<Item = usize> + '_> {
        match &self.inner.node_properties {
            None => Box::new(std::iter::empty()),
            Some(props) => Box::new(
                (0..props.const_props.num_props())
                    .filter(move |id| props.const_props.has_prop(v, *id)),
            ),
        }
    }

    fn temporal_node_prop(&self, _v: VID, _id: usize) -> Option<LockedView<TProp>> {
        None
    }

    fn temporal_node_prop_ids(&self, _v: VID) -> Box<dyn Iterator<Item = usize> + '_> {
        Box::new(std::iter::empty())
    }

    fn get_const_edge_prop(&self, _e: EdgeRef, _id: usize, _layer_ids: LayerIds) -> Option<Prop> {
        None
    }

    fn const_edge_prop_ids(
        &self,
        _e: EdgeRef,
        _layer_ids: LayerIds,
    ) -> Box<dyn Iterator<Item = usize> + '_> {
        Box::new(std::iter::empty())
    }

    fn temporal_edge_prop(
        &self,
        e: EdgeRef,
        id: usize,
        layer_ids: LayerIds,
    ) -> Option<LockedLayeredTProp> {
        let layer_ids = layer_ids.constrain_from_edge(e);

        let layer_id = match layer_ids {
            LayerIds::One(id) => id,
            _ => panic!("Only one layer is supported"),
        };

        let edge = self.inner.edge(e.pid(), layer_id);

        let prop_field = &self.inner.edges_data_type(layer_id)[id];

        let layered_t_prop = read_tprop_column(id, prop_field.clone(), edge)?;

        Some(LockedLayeredTProp::External(layered_t_prop))
    }

    fn temporal_edge_prop_ids(
        &self,
        e: EdgeRef,
        layer_ids: LayerIds,
    ) -> Box<dyn Iterator<Item = usize> + '_> {
        let layer_id = match layer_ids.constrain_from_edge(e) {
            LayerIds::One(id) => id,
            _ => panic!("Only one layer is supported"),
        };
        Box::new(1..self.inner.edges_data_type(layer_id).len())
    }

    fn core_edges(&self) -> Box<dyn Iterator<Item = CoreEdgeView<'_>>> {
        todo!()
    }

    fn core_edge(&self, eid: EID) -> CoreEdgeView<'_> {
        CoreEdgeView::Arrow(self.inner.edge(eid, 0))
    }

    fn core_node(&self, _vid: VID) -> ArcEntry<NodeStore> {
        todo!()
    }
}
