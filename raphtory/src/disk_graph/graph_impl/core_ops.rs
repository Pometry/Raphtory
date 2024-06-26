use crate::{
    core::{
        entities::{
            edges::edge_ref::EdgeRef,
            nodes::node_ref::NodeRef,
            properties::{graph_meta::GraphMeta, props::Meta, tprop::TProp},
            LayerIds, ELID, VID,
        },
        storage::locked_view::LockedView,
        Prop,
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
    disk_graph::{
        graph_impl::DiskGraph,
        storage_interface::{
            edge::DiskOwnedEdge,
            edges::DiskEdges,
            node::{DiskNode, DiskOwnedNode},
            nodes::DiskNodesOwned,
        },
    },
};
use itertools::Itertools;
use polars_arrow::datatypes::ArrowDataType;
use pometry_storage::{properties::ConstProps, GidRef, GID};
use raphtory_api::core::{input::input_node::InputNode, storage::arc_str::ArcStr};
use rayon::prelude::*;

impl CoreGraphOps for DiskGraph {
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
        self.node_meta.get_all_node_types()
    }

    fn node_id(&self, v: VID) -> u64 {
        match self.inner.node_gid(v).unwrap() {
            GidRef::U64(n) => n,
            GidRef::I64(n) => n as u64,
            GidRef::Str(s) => s.id(),
        }
    }

    fn node_name(&self, v: VID) -> String {
        match self.inner.node_gid(v).unwrap() {
            GidRef::U64(n) => n.to_string(),
            GidRef::I64(n) => n.to_string(),
            GidRef::Str(s) => s.to_owned(),
        }
    }

    fn node_type(&self, v: VID) -> Option<ArcStr> {
        let node_type_id = self.inner.node_type_id(v);
        self.node_meta.get_node_type_name_by_id(node_type_id)
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
        match &self.inner.node_properties().const_props {
            None => None,
            Some(props) => const_props(props, v, id),
        }
    }

    fn constant_node_prop_ids(&self, v: VID) -> Box<dyn Iterator<Item = usize> + '_> {
        match &self.inner.node_properties().const_props {
            None => Box::new(std::iter::empty()),
            Some(props) => {
                Box::new((0..props.num_props()).filter(move |id| props.has_prop(v, *id)))
            }
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
        EdgesStorage::Disk(DiskEdges::new(&self.inner))
    }

    fn unfiltered_num_layers(&self) -> usize {
        self.inner.layers().len()
    }

    fn core_graph(&self) -> GraphStorage {
        GraphStorage::Disk(self.inner.clone())
    }

    fn core_edge(&self, eid: ELID) -> EdgeStorageEntry {
        let layer_id = eid
            .layer()
            .expect("EdgeRefs in disk_graph should always have layer");
        EdgeStorageEntry::Disk(self.inner.layer(layer_id).edge(eid.pid()))
    }

    fn core_nodes(&self) -> NodesStorage {
        NodesStorage::Disk(DiskNodesOwned::new(self.inner.clone()))
    }

    fn core_node_entry(&self, vid: VID) -> NodeStorageEntry {
        NodeStorageEntry::Disk(DiskNode::new(&self.inner, vid))
    }

    fn core_node_arc(&self, vid: VID) -> NodeOwnedEntry {
        NodeOwnedEntry::Disk(DiskOwnedNode::new(self.inner.clone(), vid))
    }

    fn core_edge_arc(&self, eid: ELID) -> EdgeOwnedEntry {
        EdgeOwnedEntry::Disk(DiskOwnedEdge::new(&self.inner, eid))
    }

    fn unfiltered_num_edges(&self) -> usize {
        self.inner
            .layers()
            .par_iter()
            .map(|layer| layer.num_edges())
            .sum()
    }

    fn node_type_id(&self, v: VID) -> usize {
        self.inner.node_type_id(v)
    }
}

pub fn const_props<Index>(props: &ConstProps<Index>, index: Index, id: usize) -> Option<Prop>
where
    usize: From<Index>,
{
    let dtype = props.prop_dtype(id);
    match dtype.data_type() {
        ArrowDataType::Int64 => props.prop_native(index, id).map(Prop::I64),
        ArrowDataType::Int32 => props.prop_native(index, id).map(Prop::I32),
        ArrowDataType::UInt64 => props.prop_native(index, id).map(Prop::U64),
        ArrowDataType::UInt32 => props.prop_native(index, id).map(Prop::U32),
        ArrowDataType::UInt16 => props.prop_native(index, id).map(Prop::U16),
        ArrowDataType::UInt8 => props.prop_native(index, id).map(Prop::U8),
        ArrowDataType::Float64 => props.prop_native(index, id).map(Prop::F64),
        ArrowDataType::Float32 => props.prop_native(index, id).map(Prop::F32),
        ArrowDataType::Utf8 => props.prop_str(index, id).map(Into::into).map(Prop::Str),
        ArrowDataType::LargeUtf8 => props.prop_str(index, id).map(Into::into).map(Prop::Str),
        ArrowDataType::Utf8View => props.prop_str(index, id).map(Into::into).map(Prop::Str),
        _ => unimplemented!("Data type not supported"),
    }
}
