use crate::{
    core::{
        entities::{
            edges::edge_store::EdgeStore, graph::tgraph::TemporalGraph,
            nodes::node_store::NodeStore,
        },
        storage::timeindex::TimeIndexOps,
        utils::errors::GraphError,
        DocumentInput, Lifespan, Prop, PropType,
    },
    db::{
        api::{
            mutation::internal::{
                InternalAdditionOps, InternalDeletionOps, InternalPropertyAdditionOps,
            },
            storage::graph::{
                edges::edge_storage_ops::EdgeStorageOps, nodes::node_storage_ops::NodeStorageOps,
                storage_ops::GraphStorage, tprop_storage_ops::TPropOps,
            },
            view::{internal::CoreGraphOps, MaterializedGraph},
        },
        graph::views::deletion_graph::PersistentGraph,
    },
    prelude::Graph,
    serialise::{
        proto,
        proto::{
            graph_update::*, new_meta::*, new_node, new_node::Gid, prop,
            prop_type::PropType as SPropType, GraphUpdate, NewEdge, NewMeta, NewNode,
        },
    },
};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use itertools::Itertools;
use prost::Message;
use raphtory_api::core::{
    entities::{GidRef, EID, ELID, VID},
    storage::{
        arc_str::ArcStr,
        timeindex::{AsTime, TimeIndexEntry},
    },
};
use rayon::prelude::*;
use std::{borrow::Borrow, fs::File, io::Write, iter, path::Path, sync::Arc};

macro_rules! zip_tprop_updates {
    ($iter:expr) => {
        &$iter
            .map(|(key, values)| values.iter().map(move |(t, v)| (t, (key, v))))
            .kmerge_by(|(left_t, _), (right_t, _)| left_t <= right_t)
            .chunk_by(|(t, _)| *t)
    };
}

pub trait StableEncode {
    fn encode_to_proto(&self) -> proto::Graph;
    fn encode_to_vec(&self) -> Vec<u8> {
        self.encode_to_proto().encode_to_vec()
    }

    fn encode(&self, path: impl AsRef<Path>) -> Result<(), GraphError> {
        let mut file = File::create(path)?;
        let bytes = self.encode_to_vec();
        file.write_all(&bytes)?;
        Ok(())
    }
}

pub trait StableDecode: Sized {
    fn decode_from_proto(graph: &proto::Graph) -> Result<Self, GraphError>;
    fn decode_from_bytes(bytes: &[u8]) -> Result<Self, GraphError> {
        let graph = proto::Graph::decode(bytes)?;
        Self::decode_from_proto(&graph)
    }
    fn decode(path: impl AsRef<Path>) -> Result<Self, GraphError> {
        let file = File::open(path)?;
        let buf = unsafe { memmap2::MmapOptions::new().map(&file)? };
        let bytes = buf.as_ref();
        Self::decode_from_bytes(bytes)
    }
}

pub trait CacheOps: Sized {
    /// Write graph to file and append future updates to the same file.
    ///
    /// If the file already exists, it's contents are overwritten
    fn cache(&self, path: impl AsRef<Path>) -> Result<(), GraphError>;

    /// Persist the new updates by appending them to the cache file.
    fn write_updates(&self) -> Result<(), GraphError>;

    /// Load graph from file and append future updates to the same file
    fn load_cached(path: impl AsRef<Path>) -> Result<Self, GraphError>;
}

fn as_proto_prop_type(p_type: &PropType) -> SPropType {
    match p_type {
        PropType::Str => SPropType::Str,
        PropType::U8 => SPropType::U8,
        PropType::U16 => SPropType::U16,
        PropType::U32 => SPropType::U32,
        PropType::I32 => SPropType::I32,
        PropType::I64 => SPropType::I64,
        PropType::U64 => SPropType::U64,
        PropType::F32 => SPropType::F32,
        PropType::F64 => SPropType::F64,
        PropType::Bool => SPropType::Bool,
        PropType::List => SPropType::List,
        PropType::Map => SPropType::Map,
        PropType::NDTime => SPropType::NdTime,
        PropType::DTime => SPropType::DTime,
        PropType::Graph => SPropType::Graph,
        PropType::PersistentGraph => SPropType::PersistentGraph,
        PropType::Document => SPropType::Document,
        _ => unimplemented!("Empty prop types not supported!"),
    }
}

fn as_prop_type(p_type: SPropType) -> PropType {
    match p_type {
        SPropType::Str => PropType::Str,
        SPropType::U8 => PropType::U8,
        SPropType::U16 => PropType::U16,
        SPropType::U32 => PropType::U32,
        SPropType::I32 => PropType::I32,
        SPropType::I64 => PropType::I64,
        SPropType::U64 => PropType::U64,
        SPropType::F32 => PropType::F32,
        SPropType::F64 => PropType::F64,
        SPropType::Bool => PropType::Bool,
        SPropType::List => PropType::List,
        SPropType::Map => PropType::Map,
        SPropType::NdTime => PropType::NDTime,
        SPropType::DTime => PropType::DTime,
        SPropType::Graph => PropType::Graph,
        SPropType::PersistentGraph => PropType::PersistentGraph,
        SPropType::Document => PropType::Document,
    }
}

impl NewMeta {
    fn new(new_meta: Meta) -> Self {
        Self {
            meta: Some(new_meta),
        }
    }

    fn new_graph_cprop(key: &str, id: usize) -> Self {
        let inner = NewGraphCProp {
            name: key.to_string(),
            id: id as u64,
        };
        Self::new(Meta::NewGraphCprop(inner))
    }

    fn new_graph_tprop(key: &str, id: usize, dtype: &PropType) -> Self {
        let mut inner = NewGraphTProp::default();
        inner.name = key.to_string();
        inner.id = id as u64;
        inner.set_p_type(as_proto_prop_type(dtype));
        Self::new(Meta::NewGraphTprop(inner))
    }

    fn new_node_cprop(key: &str, id: usize, dtype: &PropType) -> Self {
        let mut inner = NewNodeCProp::default();
        inner.name = key.to_string();
        inner.id = id as u64;
        inner.set_p_type(as_proto_prop_type(dtype));
        Self::new(Meta::NewNodeCprop(inner))
    }

    fn new_node_tprop(key: &str, id: usize, dtype: &PropType) -> Self {
        let mut inner = NewNodeTProp::default();
        inner.name = key.to_string();
        inner.id = id as u64;
        inner.set_p_type(as_proto_prop_type(dtype));
        Self::new(Meta::NewNodeTprop(inner))
    }

    fn new_edge_cprop(key: &str, id: usize, dtype: &PropType) -> Self {
        let mut inner = NewEdgeCProp::default();
        inner.name = key.to_string();
        inner.id = id as u64;
        inner.set_p_type(as_proto_prop_type(dtype));
        Self::new(Meta::NewEdgeCprop(inner))
    }

    fn new_edge_tprop(key: &str, id: usize, dtype: &PropType) -> Self {
        let mut inner = NewEdgeTProp::default();
        inner.name = key.to_string();
        inner.id = id as u64;
        inner.set_p_type(as_proto_prop_type(dtype));
        Self::new(Meta::NewEdgeTprop(inner))
    }

    fn new_layer(layer: &str, id: usize) -> Self {
        let mut inner = NewLayer::default();
        inner.name = layer.to_string();
        inner.id = id as u64;
        Self::new(Meta::NewLayer(inner))
    }

    fn new_node_type(node_type: &str, id: usize) -> Self {
        let mut inner = NewNodeType::default();
        inner.name = node_type.to_string();
        inner.id = id as u64;
        Self::new(Meta::NewNodeType(inner))
    }
}

impl GraphUpdate {
    fn new(update: Update) -> Self {
        Self {
            update: Some(update),
        }
    }

    fn update_graph_cprops(values: impl Iterator<Item = (usize, impl Borrow<Prop>)>) -> Self {
        let inner = UpdateGraphCProps::new(values);
        Self::new(Update::UpdateGraphCprops(inner))
    }

    fn update_graph_tprops(
        time: TimeIndexEntry,
        values: impl IntoIterator<Item = (usize, impl Borrow<Prop>)>,
    ) -> Self {
        let inner = UpdateGraphTProps::new(time, values);
        Self::new(Update::UpdateGraphTprops(inner))
    }

    fn update_node_type(node_id: VID, type_id: usize) -> Self {
        let inner = UpdateNodeType {
            id: node_id.as_u64(),
            type_id: type_id as u64,
        };
        Self::new(Update::UpdateNodeType(inner))
    }

    fn update_node_cprops(
        node_id: VID,
        properties: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
    ) -> Self {
        let properties = collect_proto_props(properties);
        let inner = UpdateNodeCProps {
            id: node_id.as_u64(),
            properties,
        };
        Self::new(Update::UpdateNodeCprops(inner))
    }

    fn update_node_tprops(
        node_id: VID,
        time: TimeIndexEntry,
        properties: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
    ) -> Self {
        let properties = collect_proto_props(properties);
        let inner = UpdateNodeTProps {
            id: node_id.as_u64(),
            time: time.t(),
            secondary: time.i() as u64,
            properties,
        };
        Self::new(Update::UpdateNodeTprops(inner))
    }

    fn update_edge_tprops(
        eid: EID,
        time: TimeIndexEntry,
        layer_id: usize,
        properties: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
    ) -> Self {
        let properties = collect_proto_props(properties);
        let inner = UpdateEdgeTProps {
            eid: eid.0 as u64,
            time: time.t(),
            secondary: time.i() as u64,
            layer_id: layer_id as u64,
            properties,
        };
        Self::new(Update::UpdateEdgeTprops(inner))
    }

    fn update_edge_cprops(
        eid: EID,
        layer_id: usize,
        properties: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
    ) -> Self {
        let properties = collect_proto_props(properties);
        let inner = UpdateEdgeCProps {
            eid: eid.0 as u64,
            layer_id: layer_id as u64,
            properties,
        };
        Self::new(Update::UpdateEdgeCprops(inner))
    }

    fn del_edge(eid: EID, layer_id: usize, time: TimeIndexEntry) -> Self {
        let inner = DelEdge {
            eid: eid.as_u64(),
            time: time.t(),
            secondary: time.i() as u64,
            layer_id: layer_id as u64,
        };
        Self::new(Update::DelEdge(inner))
    }
}

impl UpdateGraphCProps {
    fn new(values: impl Iterator<Item = (usize, impl Borrow<Prop>)>) -> Self {
        let properties = collect_proto_props(values);
        UpdateGraphCProps { properties }
    }
}

impl UpdateGraphTProps {
    fn new(
        time: TimeIndexEntry,
        values: impl IntoIterator<Item = (usize, impl Borrow<Prop>)>,
    ) -> Self {
        let properties = collect_proto_props(values);
        UpdateGraphTProps {
            time: time.t(),
            secondary: time.i() as u64,
            properties,
        }
    }
}

impl PropPair {
    fn new(key: usize, value: &Prop) -> Self {
        PropPair {
            key: key as u64,
            value: Some(as_proto_prop(value)),
        }
    }
}

impl proto::Graph {
    pub fn new_edge(&mut self, src: VID, dst: VID, eid: EID) {
        let edge = NewEdge {
            src: src.as_u64(),
            dst: dst.as_u64(),
            eid: eid.as_u64(),
        };
        self.edges.push(edge);
    }

    pub fn new_node(&mut self, gid: GidRef, vid: VID, type_id: usize) {
        let type_id = type_id as u64;
        let gid = match gid {
            GidRef::U64(id) => new_node::Gid::GidU64(id),
            GidRef::Str(name) => new_node::Gid::GidStr(name.to_string()),
        };
        let node = NewNode {
            type_id,
            gid: Some(gid),
            vid: vid.as_u64(),
        };
        self.nodes.push(node);
    }

    pub fn new_graph_cprop(&mut self, key: &str, id: usize) {
        self.metas.push(NewMeta::new_graph_cprop(key, id));
    }

    pub fn new_graph_tprop(&mut self, key: &str, id: usize, dtype: &PropType) {
        self.metas.push(NewMeta::new_graph_tprop(key, id, dtype));
    }

    pub fn new_node_cprop(&mut self, key: &str, id: usize, dtype: &PropType) {
        self.metas.push(NewMeta::new_node_cprop(key, id, dtype));
    }

    pub fn new_node_tprop(&mut self, key: &str, id: usize, dtype: &PropType) {
        self.metas.push(NewMeta::new_node_tprop(key, id, dtype));
    }

    pub fn new_edge_cprop(&mut self, key: &str, id: usize, dtype: &PropType) {
        self.metas.push(NewMeta::new_edge_cprop(key, id, dtype));
    }

    pub fn new_edge_tprop(&mut self, key: &str, id: usize, dtype: &PropType) {
        self.metas.push(NewMeta::new_edge_tprop(key, id, dtype))
    }

    pub fn new_layer(&mut self, layer: &str, id: usize) {
        self.metas.push(NewMeta::new_layer(layer, id));
    }

    pub fn new_node_type(&mut self, node_type: &str, id: usize) {
        self.metas.push(NewMeta::new_node_type(node_type, id));
    }

    pub fn update_graph_cprops(
        &mut self,
        values: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
    ) {
        self.updates.push(GraphUpdate::update_graph_cprops(values));
    }

    pub fn update_graph_tprops(
        &mut self,
        time: TimeIndexEntry,
        values: impl IntoIterator<Item = (usize, impl Borrow<Prop>)>,
    ) {
        self.updates
            .push(GraphUpdate::update_graph_tprops(time, values));
    }

    pub fn update_node_type(&mut self, node_id: VID, type_id: usize) {
        self.updates
            .push(GraphUpdate::update_node_type(node_id, type_id))
    }
    pub fn update_node_cprops(
        &mut self,
        node_id: VID,
        properties: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
    ) {
        self.updates
            .push(GraphUpdate::update_node_cprops(node_id, properties));
    }

    pub fn update_node_tprops(
        &mut self,
        node_id: VID,
        time: TimeIndexEntry,
        properties: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
    ) {
        self.updates
            .push(GraphUpdate::update_node_tprops(node_id, time, properties));
    }

    pub fn update_edge_tprops(
        &mut self,
        eid: EID,
        time: TimeIndexEntry,
        layer_id: usize,
        properties: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
    ) {
        self.updates.push(GraphUpdate::update_edge_tprops(
            eid, time, layer_id, properties,
        ));
    }

    pub fn update_edge_cprops(
        &mut self,
        eid: EID,
        layer_id: usize,
        properties: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
    ) {
        self.updates
            .push(GraphUpdate::update_edge_cprops(eid, layer_id, properties));
    }

    pub fn del_edge(&mut self, eid: EID, layer_id: usize, time: TimeIndexEntry) {
        self.updates
            .push(GraphUpdate::del_edge(eid, layer_id, time))
    }
}

impl StableEncode for GraphStorage {
    fn encode_to_proto(&self) -> proto::Graph {
        #[cfg(feature = "storage")]
        if let GraphStorage::Disk(storage) = self {
            assert!(
                storage.inner.layers().len() <= 1,
                "Disk based storage not supported right now because it doesn't have aligned edges"
            );
        }

        let storage = self.lock();
        let mut graph = proto::Graph::default();

        // Graph Properties
        let graph_meta = storage.graph_meta();
        for (id, key) in graph_meta.const_prop_meta().get_keys().iter().enumerate() {
            graph.new_graph_cprop(key, id);
        }
        graph.update_graph_cprops(graph_meta.const_props());

        for (id, (key, dtype)) in graph_meta
            .temporal_prop_meta()
            .get_keys()
            .iter()
            .zip(graph_meta.temporal_prop_meta().dtypes().iter())
            .enumerate()
        {
            graph.new_graph_tprop(key, id, dtype);
        }
        for (t, group) in &graph_meta
            .temporal_props()
            .map(|(key, values)| {
                values
                    .iter()
                    .map(move |(t, v)| (t, (key, v)))
                    .collect::<Vec<_>>()
            })
            .kmerge_by(|(left_t, _), (right_t, _)| left_t <= right_t)
            .chunk_by(|(t, _)| *t)
        {
            graph.update_graph_tprops(t, group.map(|(_, v)| v));
        }

        // Layers
        for (id, layer) in storage
            .edge_meta()
            .layer_meta()
            .get_keys()
            .iter()
            .enumerate()
        {
            graph.new_layer(layer, id);
        }

        // Node Types
        for (id, node_type) in storage
            .node_meta()
            .node_type_meta()
            .get_keys()
            .iter()
            .enumerate()
        {
            graph.new_node_type(node_type, id);
        }

        // Node Properties
        let n_const_meta = self.node_meta().const_prop_meta();
        for (id, (key, dtype)) in n_const_meta
            .get_keys()
            .iter()
            .zip(n_const_meta.dtypes().iter())
            .enumerate()
        {
            graph.new_node_cprop(key, id, dtype);
        }
        let n_temporal_meta = self.node_meta().temporal_prop_meta();
        for (id, (key, dtype)) in n_temporal_meta
            .get_keys()
            .iter()
            .zip(n_temporal_meta.dtypes().iter())
            .enumerate()
        {
            graph.new_node_tprop(key, id, dtype);
        }

        // Nodes
        let nodes = storage.nodes();
        for node_id in 0..nodes.len() {
            let node = nodes.node(VID(node_id));
            graph.new_node(node.id(), node.vid(), node.node_type_id());
            for (t, group) in
                zip_tprop_updates!((0..n_temporal_meta.len()).map(|id| (id, node.tprop(id))))
            {
                graph.update_node_tprops(node.vid(), t, group.map(|(_, v)| v));
            }
            for t in node.additions().iter() {
                graph.update_node_tprops(
                    node.vid(),
                    TimeIndexEntry::start(t),
                    iter::empty::<(usize, Prop)>(),
                );
            }
            graph.update_node_cprops(
                node.vid(),
                (0..n_const_meta.len()).flat_map(|i| node.prop(i).map(|v| (i, v))),
            );
        }

        // Edge Properties
        let e_const_meta = self.edge_meta().const_prop_meta();
        for (id, (key, dtype)) in e_const_meta
            .get_keys()
            .iter()
            .zip(e_const_meta.dtypes().iter())
            .enumerate()
        {
            graph.new_edge_cprop(key, id, dtype);
        }
        let e_temporal_meta = self.edge_meta().temporal_prop_meta();
        for (id, (key, dtype)) in e_temporal_meta
            .get_keys()
            .iter()
            .zip(e_temporal_meta.dtypes().iter())
            .enumerate()
        {
            graph.new_edge_tprop(key, id, dtype);
        }

        // Edges
        let edges = storage.edges();
        for eid in 0..edges.len() {
            let eid = EID(eid);
            let edge = edges.edge(ELID::new(eid, Some(0))); // This works for DiskStorage as long as it has a single layer
            let edge = edge.as_ref();
            graph.new_edge(edge.src(), edge.dst(), eid);
            for layer_id in 0..storage.unfiltered_num_layers() {
                for (t, props) in
                    zip_tprop_updates!((0..e_temporal_meta.len())
                        .map(|i| (i, edge.temporal_prop_layer(layer_id, i))))
                {
                    graph.update_edge_tprops(eid, t, layer_id, props.map(|(_, v)| v));
                }
                for t in edge.additions(layer_id).iter() {
                    graph.update_edge_tprops(eid, t, layer_id, iter::empty::<(usize, Prop)>());
                }
                for t in edge.deletions(layer_id).iter() {
                    graph.del_edge(eid, layer_id, t);
                }
                graph.update_edge_cprops(
                    eid,
                    layer_id,
                    (0..e_const_meta.len()).filter_map(|i| {
                        edge.constant_prop_layer(layer_id, i).map(|prop| (i, prop))
                    }),
                );
            }
        }
        graph
    }
}

impl StableEncode for Graph {
    fn encode_to_proto(&self) -> proto::Graph {
        let mut graph = self.core_graph().encode_to_proto();
        graph.set_graph_type(proto::GraphType::Event);
        graph
    }
}

impl StableEncode for PersistentGraph {
    fn encode_to_proto(&self) -> proto::Graph {
        let mut graph = self.core_graph().encode_to_proto();
        graph.set_graph_type(proto::GraphType::Persistent);
        graph
    }
}

impl StableEncode for MaterializedGraph {
    fn encode_to_proto(&self) -> proto::Graph {
        match self {
            MaterializedGraph::EventGraph(graph) => graph.encode_to_proto(),
            MaterializedGraph::PersistentGraph(graph) => graph.encode_to_proto(),
        }
    }
}

impl StableDecode for TemporalGraph {
    fn decode_from_proto(graph: &proto::Graph) -> Result<Self, GraphError> {
        let storage = Self::default();
        graph.metas.par_iter().for_each(|meta| {
            if let Some(meta) = meta.meta.as_ref() {
                match meta {
                    Meta::NewNodeType(node_type) => {
                        storage
                            .node_meta
                            .node_type_meta()
                            .set_id(node_type.name.as_str(), node_type.id as usize);
                    }
                    Meta::NewNodeCprop(node_cprop) => {
                        storage.node_meta.const_prop_meta().set_id_and_dtype(
                            node_cprop.name.as_str(),
                            node_cprop.id as usize,
                            as_prop_type(node_cprop.p_type()),
                        )
                    }
                    Meta::NewNodeTprop(node_tprop) => {
                        storage.node_meta.temporal_prop_meta().set_id_and_dtype(
                            node_tprop.name.as_str(),
                            node_tprop.id as usize,
                            as_prop_type(node_tprop.p_type()),
                        )
                    }
                    Meta::NewGraphCprop(graph_cprop) => storage
                        .graph_meta
                        .const_prop_meta()
                        .set_id(graph_cprop.name.as_str(), graph_cprop.id as usize),
                    Meta::NewGraphTprop(graph_tprop) => {
                        storage.graph_meta.temporal_prop_meta().set_id_and_dtype(
                            graph_tprop.name.as_str(),
                            graph_tprop.id as usize,
                            as_prop_type(graph_tprop.p_type()),
                        )
                    }
                    Meta::NewLayer(new_layer) => storage
                        .edge_meta
                        .layer_meta()
                        .set_id(new_layer.name.as_str(), new_layer.id as usize),
                    Meta::NewEdgeCprop(edge_cprop) => {
                        storage.edge_meta.const_prop_meta().set_id_and_dtype(
                            edge_cprop.name.as_str(),
                            edge_cprop.id as usize,
                            as_prop_type(edge_cprop.p_type()),
                        )
                    }
                    Meta::NewEdgeTprop(edge_tprop) => {
                        storage.edge_meta.temporal_prop_meta().set_id_and_dtype(
                            edge_tprop.name.as_str(),
                            edge_tprop.id as usize,
                            as_prop_type(edge_tprop.p_type()),
                        )
                    }
                }
            }
        });
        graph.nodes.par_iter().try_for_each(|node| {
            let gid = match node.gid.as_ref().unwrap() {
                Gid::GidStr(name) => GidRef::Str(name),
                Gid::GidU64(gid) => GidRef::U64(*gid),
            };
            let vid = VID(node.vid as usize);
            storage.logical_to_physical.set(gid, vid)?;
            let mut node_store = NodeStore::empty(gid.to_owned());
            node_store.vid = vid;
            node_store.node_type = node.type_id as usize;
            storage.storage.nodes.set(node_store);
            Ok::<(), GraphError>(())
        })?;
        graph.edges.par_iter().for_each(|edge| {
            let eid = EID(edge.eid as usize);
            let src = VID(edge.src as usize);
            let dst = VID(edge.dst as usize);
            let mut edge = EdgeStore::new(src, dst);
            edge.eid = eid;
            storage.storage.edges.set(edge).init();
        });
        graph.updates.par_iter().try_for_each(|update| {
            if let Some(update) = update.update.as_ref() {
                match update {
                    Update::UpdateNodeCprops(props) => {
                        storage.internal_update_constant_node_properties(
                            VID(props.id as usize),
                            &collect_props(&props.properties)?,
                        )?;
                    }
                    Update::UpdateNodeTprops(props) => {
                        let time = TimeIndexEntry(props.time, props.secondary as usize);
                        let node = VID(props.id as usize);
                        let props = collect_props(&props.properties)?;
                        storage.internal_add_node(time, node, &props)?;
                    }
                    Update::UpdateGraphCprops(props) => {
                        storage.internal_update_constant_properties(&collect_props(
                            &props.properties,
                        )?)?;
                    }
                    Update::UpdateGraphTprops(props) => {
                        let time = TimeIndexEntry(props.time, props.secondary as usize);
                        storage
                            .internal_add_properties(time, &collect_props(&props.properties)?)?;
                    }
                    Update::DelEdge(del_edge) => {
                        let time = TimeIndexEntry(del_edge.time, del_edge.secondary as usize);
                        storage.internal_delete_existing_edge(
                            time,
                            EID(del_edge.eid as usize),
                            del_edge.layer_id as usize,
                        )?;
                    }
                    Update::UpdateEdgeCprops(props) => {
                        storage.internal_update_constant_edge_properties(
                            EID(props.eid as usize),
                            props.layer_id as usize,
                            &collect_props(&props.properties)?,
                        )?;
                    }
                    Update::UpdateEdgeTprops(props) => {
                        let time = TimeIndexEntry(props.time, props.secondary as usize);
                        let eid = EID(props.eid as usize);
                        storage.internal_add_edge_update(
                            time,
                            eid,
                            &collect_props(&props.properties)?,
                            props.layer_id as usize,
                        )?;
                    }
                    Update::UpdateNodeType(update) => {
                        let id = VID(update.id as usize);
                        let type_id = update.type_id as usize;
                        storage.storage.get_node_mut(id).node_type = type_id;
                    }
                }
            }
            Ok::<_, GraphError>(())
        })?;
        Ok(storage)
    }
}

impl StableDecode for GraphStorage {
    fn decode_from_proto(graph: &proto::Graph) -> Result<Self, GraphError> {
        Ok(GraphStorage::Unlocked(Arc::new(
            TemporalGraph::decode_from_proto(graph)?,
        )))
    }
}

impl StableDecode for MaterializedGraph {
    fn decode_from_proto(graph: &proto::Graph) -> Result<Self, GraphError> {
        let storage = GraphStorage::decode_from_proto(graph)?;
        let graph = match graph.graph_type() {
            proto::GraphType::Event => Self::EventGraph(Graph::from_internal_graph(storage)),
            proto::GraphType::Persistent => {
                Self::PersistentGraph(PersistentGraph::from_internal_graph(storage))
            }
        };
        Ok(graph)
    }
}

impl StableDecode for Graph {
    fn decode_from_proto(graph: &proto::Graph) -> Result<Self, GraphError> {
        match graph.graph_type() {
            proto::GraphType::Event => {
                let storage = GraphStorage::decode_from_proto(graph)?;
                Ok(Graph::from_internal_graph(storage))
            }
            proto::GraphType::Persistent => Err(GraphError::GraphLoadError),
        }
    }
}

impl StableDecode for PersistentGraph {
    fn decode_from_proto(graph: &proto::Graph) -> Result<Self, GraphError> {
        match graph.graph_type() {
            proto::GraphType::Event => Err(GraphError::GraphLoadError),
            proto::GraphType::Persistent => {
                let storage = GraphStorage::decode_from_proto(graph)?;
                Ok(PersistentGraph::from_internal_graph(storage))
            }
        }
    }
}

fn as_prop(prop_pair: &PropPair) -> Result<(usize, Prop), GraphError> {
    let PropPair { key, value } = prop_pair;
    let value = value.as_ref().expect("Missing prop value");
    let value = value.value.as_ref();
    let value = as_prop_value(value)?;

    Ok((*key as usize, value))
}

fn as_prop_value(value: Option<&prop::Value>) -> Result<Prop, GraphError> {
    let value = match value.expect("Missing prop value") {
        prop::Value::BoolValue(b) => Prop::Bool(*b),
        prop::Value::U8(u) => Prop::U8((*u).try_into().unwrap()),
        prop::Value::U16(u) => Prop::U16((*u).try_into().unwrap()),
        prop::Value::U32(u) => Prop::U32(*u),
        prop::Value::I32(i) => Prop::I32(*i),
        prop::Value::I64(i) => Prop::I64(*i),
        prop::Value::U64(u) => Prop::U64(*u),
        prop::Value::F32(f) => Prop::F32(*f),
        prop::Value::F64(f) => Prop::F64(*f),
        prop::Value::Str(s) => Prop::Str(ArcStr::from(s.as_str())),
        prop::Value::Prop(props) => Prop::List(Arc::new(
            props
                .properties
                .iter()
                .map(|prop| as_prop_value(prop.value.as_ref()))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        prop::Value::Map(dict) => Prop::Map(Arc::new(
            dict.map
                .iter()
                .map(|(k, v)| Ok((ArcStr::from(k.as_str()), as_prop_value(v.value.as_ref())?)))
                .collect::<Result<_, GraphError>>()?,
        )),
        prop::Value::NdTime(ndt) => {
            let prop::NdTime {
                year,
                month,
                day,
                hour,
                minute,
                second,
                nanos,
            } = ndt;
            let ndt = NaiveDateTime::new(
                NaiveDate::from_ymd_opt(*year as i32, *month as u32, *day as u32).unwrap(),
                NaiveTime::from_hms_nano_opt(
                    *hour as u32,
                    *minute as u32,
                    *second as u32,
                    *nanos as u32,
                )
                .unwrap(),
            );
            Prop::NDTime(ndt)
        }
        prop::Value::DTime(dt) => Prop::DTime(DateTime::parse_from_rfc3339(dt).unwrap().into()),
        prop::Value::Graph(graph_proto) => Prop::Graph(Graph::decode_from_proto(graph_proto)?),
        prop::Value::PersistentGraph(graph_proto) => {
            Prop::PersistentGraph(PersistentGraph::decode_from_proto(graph_proto)?)
        }
        prop::Value::DocumentInput(doc) => Prop::Document(DocumentInput {
            content: doc.content.clone(),
            life: doc
                .life
                .as_ref()
                .map(|l| match l.l_type {
                    Some(prop::lifespan::LType::Interval(prop::lifespan::Interval {
                        start,
                        end,
                    })) => Lifespan::Interval { start, end },
                    Some(prop::lifespan::LType::Event(prop::lifespan::Event { time })) => {
                        Lifespan::Event { time }
                    }
                    None => Lifespan::Inherited,
                })
                .unwrap_or(Lifespan::Inherited),
        }),
    };
    Ok(value)
}

fn collect_proto_props(
    iter: impl IntoIterator<Item = (usize, impl Borrow<Prop>)>,
) -> Vec<PropPair> {
    iter.into_iter()
        .map(|(key, value)| PropPair::new(key, value.borrow()))
        .collect()
}

fn collect_props<'a>(
    iter: impl IntoIterator<Item = &'a PropPair>,
) -> Result<Vec<(usize, Prop)>, GraphError> {
    iter.into_iter().map(as_prop).collect()
}

fn as_proto_prop(prop: &Prop) -> proto::Prop {
    let value: prop::Value = match prop {
        Prop::Bool(b) => prop::Value::BoolValue(*b),
        Prop::U8(u) => prop::Value::U8((*u).into()),
        Prop::U16(u) => prop::Value::U16((*u).into()),
        Prop::U32(u) => prop::Value::U32(*u),
        Prop::I32(i) => prop::Value::I32(*i),
        Prop::I64(i) => prop::Value::I64(*i),
        Prop::U64(u) => prop::Value::U64(*u),
        Prop::F32(f) => prop::Value::F32(*f),
        Prop::F64(f) => prop::Value::F64(*f),
        Prop::Str(s) => prop::Value::Str(s.to_string()),
        Prop::List(list) => {
            let properties = list.iter().map(as_proto_prop).collect();
            prop::Value::Prop(prop::Props { properties })
        }
        Prop::Map(map) => {
            let map = map
                .iter()
                .map(|(k, v)| (k.to_string(), as_proto_prop(v)))
                .collect();
            prop::Value::Map(prop::Dict { map })
        }
        Prop::NDTime(ndt) => {
            let (year, month, day) = (ndt.date().year(), ndt.date().month(), ndt.date().day());
            let (hour, minute, second, nanos) = (
                ndt.time().hour(),
                ndt.time().minute(),
                ndt.time().second(),
                ndt.time().nanosecond(),
            );

            let proto_ndt = prop::NdTime {
                year: year as u32,
                month: month as u32,
                day: day as u32,
                hour: hour as u32,
                minute: minute as u32,
                second: second as u32,
                nanos: nanos as u32,
            };
            prop::Value::NdTime(proto_ndt)
        }
        Prop::DTime(dt) => {
            prop::Value::DTime(dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
        }
        Prop::Graph(g) => prop::Value::Graph(g.encode_to_proto()),
        Prop::PersistentGraph(g) => prop::Value::PersistentGraph(g.encode_to_proto()),
        Prop::Document(doc) => {
            let life = match doc.life {
                Lifespan::Interval { start, end } => {
                    Some(prop::lifespan::LType::Interval(prop::lifespan::Interval {
                        start,
                        end,
                    }))
                }
                Lifespan::Event { time } => {
                    Some(prop::lifespan::LType::Event(prop::lifespan::Event { time }))
                }
                Lifespan::Inherited => None,
            };
            prop::Value::DocumentInput(prop::DocumentInput {
                content: doc.content.clone(),
                life: Some(prop::Lifespan { l_type: life }),
            })
        }
    };

    proto::Prop { value: Some(value) }
}

#[cfg(test)]
mod proto_test {
    use super::*;
    use crate::{
        core::DocumentInput,
        db::{
            api::{mutation::DeletionOps, properties::internal::ConstPropertiesOps},
            graph::graph::assert_graph_equal,
        },
        prelude::*,
        serialise::{proto::GraphType, ProtoGraph},
    };
    use chrono::{DateTime, NaiveDateTime};
    use raphtory_api::core::utils::logging::global_info_logger;
    use tracing::info;

    #[test]
    fn node_no_props() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let g1 = Graph::new();
        g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);
    }

    #[test]
    fn node_with_props() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let g1 = Graph::new();
        g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        g1.add_node(2, "Bob", [("age", Prop::U32(47))], None)
            .unwrap();
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);
    }

    #[test]
    fn node_with_const_props() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let g1 = Graph::new();
        g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        let n1 = g1
            .add_node(2, "Bob", [("age", Prop::U32(47))], None)
            .unwrap();

        n1.update_constant_properties([("name", Prop::Str("Bob".into()))])
            .expect("Failed to update constant properties");

        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);
    }

    #[test]
    fn edge_no_props() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let g1 = Graph::new();
        g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        g1.add_node(2, "Bob", NO_PROPS, None).unwrap();
        g1.add_edge(3, "Alice", "Bob", NO_PROPS, None).unwrap();
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);
    }

    #[test]
    fn edge_no_props_delete() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let g1 = Graph::new().persistent_graph();
        g1.add_edge(3, "Alice", "Bob", NO_PROPS, None).unwrap();
        g1.delete_edge(19, "Alice", "Bob", None).unwrap();
        g1.encode(&temp_file).unwrap();
        let g2 = PersistentGraph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);

        let edge = g2.edge("Alice", "Bob").expect("Failed to get edge");
        let deletions = edge.deletions().iter().copied().collect::<Vec<_>>();
        assert_eq!(deletions, vec![19]);
    }

    #[test]
    fn edge_t_props() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let g1 = Graph::new();
        g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        g1.add_node(2, "Bob", NO_PROPS, None).unwrap();
        g1.add_edge(3, "Alice", "Bob", [("kind", "friends")], None)
            .unwrap();
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);
    }

    #[test]
    fn edge_const_props() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let g1 = Graph::new();
        let e1 = g1.add_edge(3, "Alice", "Bob", NO_PROPS, None).unwrap();
        e1.update_constant_properties([("friends", true)], None)
            .expect("Failed to update constant properties");
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);
    }

    #[test]
    fn edge_layers() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let g1 = Graph::new();
        g1.add_edge(7, "Alice", "Bob", NO_PROPS, Some("one"))
            .unwrap();
        g1.add_edge(7, "Bob", "Charlie", [("friends", false)], Some("two"))
            .unwrap();
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);
    }

    #[test]
    fn test_all_the_t_props_on_node() {
        let mut props = vec![];
        write_props_to_vec(&mut props);

        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let g1 = Graph::new();
        g1.add_node(1, "Alice", props.clone(), None).unwrap();
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);

        let node = g2.node("Alice").expect("Failed to get node");

        assert!(props.into_iter().all(|(name, expected)| {
            node.properties()
                .temporal()
                .get(name)
                .filter(|prop_view| {
                    let (t, prop) = prop_view.iter().next().expect("Failed to get prop");
                    prop == expected && t == 1
                })
                .is_some()
        }))
    }

    #[test]
    fn test_all_the_t_props_on_edge() {
        let mut props = vec![];
        write_props_to_vec(&mut props);

        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let g1 = Graph::new();
        g1.add_edge(1, "Alice", "Bob", props.clone(), None).unwrap();
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);

        let edge = g2.edge("Alice", "Bob").expect("Failed to get edge");

        assert!(props.into_iter().all(|(name, expected)| {
            edge.properties()
                .temporal()
                .get(name)
                .filter(|prop_view| {
                    let (t, prop) = prop_view.iter().next().expect("Failed to get prop");
                    prop == expected && t == 1
                })
                .is_some()
        }))
    }

    #[test]
    fn test_all_the_const_props_on_edge() {
        let mut props = vec![];
        write_props_to_vec(&mut props);

        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let g1 = Graph::new();
        let e = g1.add_edge(1, "Alice", "Bob", NO_PROPS, Some("a")).unwrap();
        e.update_constant_properties(props.clone(), Some("a"))
            .expect("Failed to update constant properties");
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);

        let edge = g2
            .edge("Alice", "Bob")
            .expect("Failed to get edge")
            .layers("a")
            .unwrap();

        for (new, old) in edge.properties().constant().iter().zip(props.iter()) {
            assert_eq!(new.0, old.0);
            assert_eq!(new.1, old.1);
        }
    }

    #[test]
    fn test_all_the_const_props_on_node() {
        let mut props = vec![];
        write_props_to_vec(&mut props);

        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let g1 = Graph::new();
        let n = g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        n.update_constant_properties(props.clone())
            .expect("Failed to update constant properties");
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);

        let node = g2.node("Alice").expect("Failed to get node");

        assert!(props.into_iter().all(|(name, expected)| {
            node.properties()
                .constant()
                .get(name)
                .filter(|prop| prop == &expected)
                .is_some()
        }))
    }

    #[test]
    fn graph_const_properties() {
        let mut props = vec![];
        write_props_to_vec(&mut props);

        let g1 = Graph::new();
        g1.add_constant_properties(props.clone())
            .expect("Failed to add constant properties");

        let temp_file = tempfile::NamedTempFile::new().unwrap();
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);

        props.into_iter().for_each(|(name, prop)| {
            let id = g2.get_const_prop_id(name).expect("Failed to get prop id");
            assert_eq!(prop, g2.get_const_prop(id).expect("Failed to get prop"));
        });
    }

    #[test]
    fn graph_temp_properties() {
        let mut props = vec![];
        write_props_to_vec(&mut props);

        let g1 = Graph::new();
        for t in 0..props.len() {
            g1.add_properties(t as i64, (&props[t..t + 1]).to_vec())
                .expect("Failed to add constant properties");
        }

        let temp_file = tempfile::NamedTempFile::new().unwrap();
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);

        props
            .into_iter()
            .enumerate()
            .for_each(|(expected_t, (name, expected))| {
                for (t, prop) in g2
                    .properties()
                    .temporal()
                    .get(name)
                    .expect("Failed to get prop view")
                {
                    assert_eq!(prop, expected);
                    assert_eq!(t, expected_t as i64);
                }
            });
    }

    #[test]
    fn manually_test_append() {
        let mut graph1 = proto::Graph::default();
        graph1.set_graph_type(GraphType::Event);
        graph1.new_node(GidRef::Str("1"), VID(0), 0);
        graph1.new_node(GidRef::Str("2"), VID(1), 0);
        graph1.new_edge(VID(0), VID(1), EID(0));
        graph1.update_edge_tprops(
            EID(0),
            TimeIndexEntry::start(1),
            0,
            iter::empty::<(usize, Prop)>(),
        );
        let mut bytes1 = graph1.encode_to_vec();

        let mut graph2 = proto::Graph::default();
        graph2.new_node(GidRef::Str("3"), VID(2), 0);
        graph2.new_edge(VID(0), VID(2), EID(1));
        graph2.update_edge_tprops(
            EID(1),
            TimeIndexEntry::start(2),
            0,
            iter::empty::<(usize, Prop)>(),
        );
        bytes1.extend(graph2.encode_to_vec());

        let graph = Graph::decode_from_bytes(&bytes1).unwrap();
        assert_eq!(graph.nodes().name().collect_vec(), ["1", "2", "3"]);
        assert_eq!(
            graph.edges().id().collect_vec(),
            [
                (GID::Str("1".to_string()), GID::Str("2".to_string())),
                (GID::Str("1".to_string()), GID::Str("3".to_string()))
            ]
        )
    }

    #[test]
    fn test_string_interning() {
        let g = Graph::new();
        let n = g.add_node(0, 1, [("test", "test")], None).unwrap();

        n.add_updates(1, [("test", "test")]).unwrap();
        n.add_updates(2, [("test", "test")]).unwrap();

        let values = n
            .properties()
            .temporal()
            .get("test")
            .unwrap()
            .values()
            .into_iter()
            .map(|v| v.unwrap_str())
            .collect_vec();
        assert_eq!(values, ["test", "test", "test"]);
        for w in values.windows(2) {
            assert_eq!(w[0].as_ptr(), w[1].as_ptr());
        }

        let proto = g.encode_to_proto();
        let g2 = Graph::decode_from_proto(&proto).unwrap();
        let values = g2
            .node(1)
            .unwrap()
            .properties()
            .temporal()
            .get("test")
            .unwrap()
            .values()
            .into_iter()
            .map(|v| v.unwrap_str())
            .collect_vec();
        assert_eq!(values, ["test", "test", "test"]);
        for w in values.windows(2) {
            assert_eq!(w[0].as_ptr(), w[1].as_ptr());
        }
    }

    #[test]
    fn test_incremental_writing_on_graph() {
        global_info_logger();
        let g = Graph::new();
        let mut props = vec![];
        write_props_to_vec(&mut props);
        let temp_cache_file = tempfile::NamedTempFile::new().unwrap();

        g.cache(temp_cache_file.path()).unwrap();

        for t in 0..props.len() {
            g.add_properties(t as i64, (&props[t..t + 1]).to_vec())
                .expect("Failed to add constant properties");
        }
        g.write_updates().unwrap();

        g.add_constant_properties(props.clone())
            .expect("Failed to add constant properties");
        g.write_updates().unwrap();

        let n = g.add_node(1, "Alice", NO_PROPS, None).unwrap();
        n.update_constant_properties(props.clone())
            .expect("Failed to update constant properties");
        g.write_updates().unwrap();

        let e = g.add_edge(1, "Alice", "Bob", NO_PROPS, Some("a")).unwrap();
        e.update_constant_properties(props.clone(), Some("a"))
            .expect("Failed to update constant properties");
        g.write_updates().unwrap();

        g.add_edge(2, "Alice", "Bob", props.clone(), None).unwrap();
        g.add_node(1, "Charlie", props.clone(), None).unwrap();
        g.write_updates().unwrap();

        g.add_edge(7, "Alice", "Bob", NO_PROPS, Some("one"))
            .unwrap();
        g.add_edge(7, "Bob", "Charlie", [("friends", false)], Some("two"))
            .unwrap();
        g.write_updates().unwrap();
        info!("{g:?}");

        let g2 = Graph::decode(temp_cache_file.path()).unwrap();
        info!("{g2:?}");

        assert_graph_equal(&g, &g2);
    }

    #[test]
    fn test_incremental_writing_on_persistent_graph() {
        global_info_logger();
        let g = PersistentGraph::new();
        let mut props = vec![];
        write_props_to_vec(&mut props);
        let temp_cache_file = tempfile::NamedTempFile::new().unwrap();

        g.cache(temp_cache_file.path()).unwrap();

        for t in 0..props.len() {
            g.add_properties(t as i64, (&props[t..t + 1]).to_vec())
                .expect("Failed to add constant properties");
        }
        g.write_updates().unwrap();

        g.add_constant_properties(props.clone())
            .expect("Failed to add constant properties");
        g.write_updates().unwrap();

        let n = g.add_node(1, "Alice", NO_PROPS, None).unwrap();
        n.update_constant_properties(props.clone())
            .expect("Failed to update constant properties");
        g.write_updates().unwrap();

        let e = g.add_edge(1, "Alice", "Bob", NO_PROPS, Some("a")).unwrap();
        e.update_constant_properties(props.clone(), Some("a"))
            .expect("Failed to update constant properties");
        g.write_updates().unwrap();

        g.add_edge(2, "Alice", "Bob", props.clone(), None).unwrap();
        g.add_node(1, "Charlie", props.clone(), None).unwrap();
        g.write_updates().unwrap();

        g.add_edge(7, "Alice", "Bob", NO_PROPS, Some("one"))
            .unwrap();
        g.add_edge(7, "Bob", "Charlie", [("friends", false)], Some("two"))
            .unwrap();
        g.write_updates().unwrap();
        info!("{g:?}");

        let g2 = PersistentGraph::decode(temp_cache_file.path()).unwrap();
        info!("{g2:?}");

        assert_graph_equal(&g, &g2);
    }

    // we rely on this to make sure writing no updates does not actually write anything to file
    #[test]
    fn empty_proto_is_empty_bytes() {
        let proto = ProtoGraph::default();
        let bytes = proto.encode_to_vec();
        assert!(bytes.is_empty())
    }

    fn write_props_to_vec(props: &mut Vec<(&str, Prop)>) {
        props.push(("name", Prop::Str("Alice".into())));
        props.push(("age", Prop::U32(47)));
        props.push(("score", Prop::I32(27)));
        props.push(("is_adult", Prop::Bool(true)));
        props.push(("height", Prop::F32(1.75)));
        props.push(("weight", Prop::F64(75.5)));
        props.push((
            "children",
            Prop::List(Arc::new(vec![
                Prop::Str("Bob".into()),
                Prop::Str("Charlie".into()),
            ])),
        ));
        props.push((
            "properties",
            Prop::Map(Arc::new(
                props
                    .iter()
                    .map(|(k, v)| (ArcStr::from(*k), v.clone()))
                    .collect(),
            )),
        ));
        let fmt = "%Y-%m-%d %H:%M:%S";
        props.push((
            "time",
            Prop::NDTime(
                NaiveDateTime::parse_from_str("+10000-09-09 01:46:39", fmt)
                    .expect("Failed to parse time"),
            ),
        ));

        props.push((
            "dtime",
            Prop::DTime(
                DateTime::parse_from_rfc3339("2021-09-09T01:46:39Z")
                    .unwrap()
                    .into(),
            ),
        ));

        props.push((
            "doc",
            Prop::Document(DocumentInput {
                content: "Hello, World!".into(),
                life: Lifespan::Interval {
                    start: -11i64,
                    end: 100i64,
                },
            }),
        ));
        let graph = Graph::new();
        graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        props.push(("graph", Prop::Graph(graph)));

        let graph = Graph::new().persistent_graph();
        graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        graph.delete_edge(2, "a", "b", None).unwrap();
        props.push(("p_graph", Prop::PersistentGraph(graph)));
    }
}
