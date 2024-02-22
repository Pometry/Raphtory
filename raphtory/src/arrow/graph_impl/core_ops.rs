use itertools::Itertools;

use crate::{
    arrow::{graph_impl::Graph2, GID},
    core::{
        entities::{
            edges::edge_ref::EdgeRef,
            nodes::{node_ref::NodeRef, node_store::NodeStore},
            properties::{
                graph_props::GraphProps,
                props::Meta,
                tprop::{LockedLayeredTProp, TProp},
            },
            LayerIds, EID, VID,
        },
        storage::{locked_view::LockedView, timeindex::LockedLayeredIndex, ArcEntry},
        utils::hashing::calculate_hash,
        ArcStr, Prop,
    },
    db::api::view::{
        internal::{CoreEdgeView, CoreGraphOps, EdgeUpdates, NodeAdditions},
        BoxedIter,
    },
    prelude::TimeIndexEntry,
};

use super::tprops::read_tprop_column;

impl CoreGraphOps for Graph2 {
    fn unfiltered_num_nodes(&self) -> usize {
        self.num_nodes()
    }

    fn node_meta(&self) -> &Meta {
        &self.node_meta
    }

    fn edge_meta(&self) -> &Meta {
        &self.edge_meta
    }

    fn graph_meta(&self) -> &GraphProps {
        &self.graph_props
    }

    fn get_layer_name(&self, layer_id: usize) -> ArcStr {
        let name = &self.layer_names()[layer_id];
        ArcStr::from(name.as_str())
    }

    fn get_layer_id(&self, name: &str) -> Option<usize> {
        self.find_layer_id(name)
    }

    fn get_layer_names_from_ids(&self, layer_ids: LayerIds) -> BoxedIter<ArcStr> {
        match layer_ids {
            LayerIds::None => Box::new(std::iter::empty()),
            LayerIds::All => Box::new(self.layer_names_vec().into_iter().map(|s| ArcStr::from(s))),
            LayerIds::One(id) => Box::new(
                self.layer_names()
                    .get(id)
                    .cloned()
                    .into_iter()
                    .map(|s| ArcStr::from(s)),
            ),
            LayerIds::Multiple(ids) => Box::new(
                ids.iter()
                    .copied()
                    .filter_map(|id| self.layer_names().get(id).cloned())
                    .collect_vec()
                    .into_iter()
                    .map(|s| ArcStr::from(s)),
            ),
        }
    }

    fn get_all_node_types(&self) -> Vec<ArcStr> {
        todo!()
    }

    fn node_id(&self, v: VID) -> u64 {
        match self.node_gid(v).unwrap() {
            GID::U64(n) => n,
            GID::I64(n) => n as u64,
            GID::Str(s) => calculate_hash(&s),
        }
    }

    fn node_name(&self, v: VID) -> String {
        match self.node_gid(v).unwrap() {
            GID::U64(n) => n.to_string(),
            GID::I64(n) => n.to_string(),
            GID::Str(s) => s,
        }
    }

    fn node_type(&self, _v: VID) -> Option<ArcStr> {
        None
    }

    fn edge_additions(&self, eref: EdgeRef, layer_ids: LayerIds) -> EdgeUpdates {
        let layer_ids = layer_ids.constrain_from_edge(eref);

        let layer_id = match layer_ids {
            LayerIds::One(id) => id,
            _ => panic!("Only one layer is supported"),
        };

        let edge = self.edge(eref.pid(), layer_id);
        EdgeUpdates::Col(edge.timestamps())
    }

    fn node_additions(&self, v: VID) -> NodeAdditions {
        let node = self.internal_node(v, 0);
        NodeAdditions::Col(node.timestamps())
    }

    fn internalise_node(&self, v: NodeRef) -> Option<VID> {
        match v {
            NodeRef::Internal(vid) => Some(vid),
            NodeRef::External(vid) => self.find_node(&GID::U64(vid)),
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

    fn constant_node_prop(&self, _v: VID, _id: usize) -> Option<Prop> {
        None
    }

    fn constant_node_prop_ids(&self, _v: VID) -> Box<dyn Iterator<Item = usize> + '_> {
        Box::new(std::iter::empty())
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

        let edge = self.edge(e.pid(), layer_id);

        let prop_field = &self.edges_data_type(layer_id)[id];

        let layered_t_prop = read_tprop_column(id, prop_field.clone(), edge)?;

        Some(LockedLayeredTProp::External(layered_t_prop))
    }

    fn temporal_edge_prop_ids(
        &self,
        _e: EdgeRef,
        layer_ids: LayerIds,
    ) -> Box<dyn Iterator<Item = usize> + '_> {
        let layer_id = match layer_ids {
            LayerIds::One(id) => id,
            _ => panic!("Only one layer is supported"),
        };

        let fields = self.edges_data_type(layer_id);
        Box::new(fields.into_iter().enumerate().map(|(i, _)| i))
    }

    fn core_edges(&self) -> Box<dyn Iterator<Item = CoreEdgeView<'_>>> {
        todo!()
    }

    fn core_edge(&self, eid: EID) -> CoreEdgeView<'_> {
        CoreEdgeView::Arrow(self.edge(eid, 0))
    }

    fn core_node(&self, _vid: VID) -> ArcEntry<NodeStore> {
        todo!()
    }
}
