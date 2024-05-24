use crate::{
    arrow::{
        graph_impl::ArrowGraph,
        storage_interface::{
            edge::ArrowOwnedEdge,
            edges::ArrowEdges,
            node::{ArrowNode, ArrowOwnedNode},
            nodes::ArrowNodesOwned,
        },
    },
    core::{
        entities::{
            edges::edge_ref::EdgeRef,
            nodes::{input_node::InputNode, node_ref::NodeRef},
            properties::{graph_meta::GraphMeta, props::Meta, tprop::TProp},
            LayerIds, ELID, VID,
        },
        storage::locked_view::LockedView,
        ArcStr, Prop,
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
};
use itertools::Itertools;
use polars_arrow::datatypes::ArrowDataType;
use raphtory_arrow::{properties::Properties, GID};
use rayon::prelude::*;
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

    fn get_layer_names_from_ids(&self, layer_ids: &LayerIds) -> BoxedIter<ArcStr> {
        match layer_ids {
            LayerIds::None => Box::new(std::iter::empty()),
            LayerIds::All => Box::new(
                self.inner
                    .layer_names()
                    .iter()
                    .map(|s| ArcStr::from(s.as_str()))
                    .collect::<Vec<_>>()
                    .into_iter(),
            ),
            LayerIds::One(id) => Box::new(
                self.inner
                    .layer_names()
                    .get(*id)
                    .cloned()
                    .into_iter()
                    .map(ArcStr::from),
            ),
            LayerIds::Multiple(ids) => Box::new(
                ids.iter()
                    .copied()
                    .filter_map(|id| self.inner.layer_names().get(id).cloned())
                    .collect_vec()
                    .into_iter()
                    .map(ArcStr::from),
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
        match &self.inner.node_properties() {
            None => None,
            Some(props) => const_props(props, v, id),
        }
    }

    fn constant_node_prop_ids(&self, v: VID) -> Box<dyn Iterator<Item = usize> + '_> {
        match self.inner.node_properties() {
            None => Box::new(std::iter::empty()),
            Some(props) => Box::new(
                (0..props.const_props.num_props())
                    .filter(move |id| props.const_props.has_prop(v, *id)),
            ),
        }
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

    fn temporal_edge_prop_ids(
        &self,
        e: EdgeRef,
        layer_ids: &LayerIds,
    ) -> Box<dyn Iterator<Item = usize> + '_> {
        let layer_id = match layer_ids.constrain_from_edge(e) {
            LayerIds::One(id) => id,
            _ => panic!("Only one layer is supported"),
        };
        Box::new(1..self.inner.edges_data_type(layer_id).len())
    }

    fn core_edges(&self) -> EdgesStorage {
        EdgesStorage::Arrow(ArrowEdges::new(&self.inner))
    }

    fn unfiltered_num_layers(&self) -> usize {
        self.inner.layers().len()
    }

    fn core_graph(&self) -> GraphStorage {
        GraphStorage::Arrow(self.inner.clone())
    }

    fn core_edge(&self, eid: ELID) -> EdgeStorageEntry {
        let layer_id = eid
            .layer()
            .expect("EdgeRefs in arrow should always have layer");
        EdgeStorageEntry::Arrow(self.inner.layer(layer_id).edge(eid.pid()))
    }

    fn core_nodes(&self) -> NodesStorage {
        NodesStorage::Arrow(ArrowNodesOwned::new(&self.inner))
    }

    fn core_node_entry(&self, vid: VID) -> NodeStorageEntry {
        NodeStorageEntry::Arrow(ArrowNode::new(&self.inner, vid))
    }

    fn core_node_arc(&self, vid: VID) -> NodeOwnedEntry {
        NodeOwnedEntry::Arrow(ArrowOwnedNode::new(&self.inner, vid))
    }

    fn core_edge_arc(&self, eid: ELID) -> EdgeOwnedEntry {
        EdgeOwnedEntry::Arrow(ArrowOwnedEdge::new(&self.inner, eid))
    }

    fn unfiltered_num_edges(&self) -> usize {
        self.inner
            .layers()
            .par_iter()
            .map(|layer| layer.num_edges())
            .sum()
    }
}

pub fn const_props<Index>(props: &Properties<Index>, index: Index, id: usize) -> Option<Prop>
where
    usize: From<Index>,
{
    let dtype = props.const_props.prop_dtype(id);
    match dtype.data_type() {
        ArrowDataType::Int64 => props.const_props.prop_native(index, id).map(Prop::I64),
        ArrowDataType::Int32 => props.const_props.prop_native(index, id).map(Prop::I32),
        ArrowDataType::UInt64 => props.const_props.prop_native(index, id).map(Prop::U64),
        ArrowDataType::UInt32 => props.const_props.prop_native(index, id).map(Prop::U32),
        ArrowDataType::UInt16 => props.const_props.prop_native(index, id).map(Prop::U16),
        ArrowDataType::UInt8 => props.const_props.prop_native(index, id).map(Prop::U8),
        ArrowDataType::Float64 => props.const_props.prop_native(index, id).map(Prop::F64),
        ArrowDataType::Float32 => props.const_props.prop_native(index, id).map(Prop::F32),
        ArrowDataType::Utf8 => props
            .const_props
            .prop_str(index, id)
            .map(Into::into)
            .map(Prop::Str),
        ArrowDataType::LargeUtf8 => props
            .const_props
            .prop_str(index, id)
            .map(Into::into)
            .map(Prop::Str),
        ArrowDataType::Utf8View => props
            .const_props
            .prop_str(index, id)
            .map(Into::into)
            .map(Prop::Str),
        _ => unimplemented!(),
    }
}
