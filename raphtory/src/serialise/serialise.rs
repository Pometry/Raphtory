use super::{proto_ext::PropTypeExt, GraphFolder};
#[cfg(feature = "search")]
use crate::prelude::IndexMutationOps;
use crate::{
    core::entities::{graph::tgraph::TemporalGraph, LayerIds},
    db::{
        api::view::{MaterializedGraph, StaticGraphViewOps},
        graph::views::deletion_graph::PersistentGraph,
    },
    errors::GraphError,
    prelude::{AdditionOps, Graph},
    serialise::{
        proto::{self, graph_update::*, new_meta::*, new_node::Gid},
        proto_ext,
    },
};
use itertools::Itertools;
use prost::Message;
use raphtory_api::core::{
    entities::{
        properties::{
            meta::PropMapper,
            prop::{unify_types, Prop, PropType},
            tprop::TPropOps,
        },
        GidRef, EID, VID,
    },
    storage::timeindex::{TimeIndexEntry, TimeIndexOps},
    Direction,
};
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::{
        edges::edge_storage_ops::EdgeStorageOps, graph::GraphStorage,
        nodes::node_storage_ops::NodeStorageOps,
    },
    mutation::{
        addition_ops::InternalAdditionOps, property_addition_ops::InternalPropertyAdditionOps,
    },
};
use rayon::prelude::*;
use std::{iter, ops::Deref, sync::Arc};

macro_rules! zip_tprop_updates {
    ($iter:expr) => {
        &$iter
            .map(|(key, values)| values.iter().map(move |(t, v)| (t, (key, v))))
            .kmerge_by(|(left_t, _), (right_t, _)| left_t <= right_t)
            .chunk_by(|(t, _)| *t)
    };
}

pub trait StableEncode: StaticGraphViewOps + AdditionOps {
    fn encode_to_proto(&self) -> proto::Graph;
    fn encode_to_vec(&self) -> Vec<u8> {
        self.encode_to_proto().encode_to_vec()
    }

    fn encode(&self, path: impl Into<GraphFolder>) -> Result<(), GraphError> {
        let folder = path.into();
        folder.write_graph(self)
    }
}

pub trait StableDecode: InternalStableDecode + StaticGraphViewOps + AdditionOps {
    fn decode(path: impl Into<GraphFolder>) -> Result<Self, GraphError> {
        let folder = path.into();
        let graph = Self::decode_from_path(&folder)?;

        #[cfg(feature = "search")]
        graph.load_index(&folder)?;

        Ok(graph)
    }
}

impl<T: InternalStableDecode + StaticGraphViewOps + AdditionOps> StableDecode for T {}

pub trait InternalStableDecode: Sized {
    fn decode_from_proto(graph: &proto::Graph) -> Result<Self, GraphError>;

    fn decode_from_bytes(bytes: &[u8]) -> Result<Self, GraphError> {
        let graph = proto::Graph::decode(bytes)?;
        Self::decode_from_proto(&graph)
    }

    fn decode_from_path(path: &GraphFolder) -> Result<Self, GraphError> {
        let bytes = path.read_graph()?;
        let graph = Self::decode_from_bytes(bytes.as_ref())?;
        Ok(graph)
    }
}

pub trait CacheOps: Sized {
    /// Write graph to file and append future updates to the same file.
    ///
    /// If the file already exists, it's contents are overwritten
    fn cache(&self, path: impl Into<GraphFolder>) -> Result<(), GraphError>;

    /// Persist the new updates by appending them to the cache file.
    fn write_updates(&self) -> Result<(), GraphError>;

    /// Load graph from file and append future updates to the same file
    fn load_cached(path: impl Into<GraphFolder>) -> Result<Self, GraphError>;
}

impl StableEncode for GraphStorage {
    fn encode_to_proto(&self) -> proto::Graph {
        let storage = self.lock();
        let mut graph = proto::Graph::default();

        // Graph Properties
        let graph_meta = storage.graph_meta();
        for (id, key) in graph_meta.metadata_mapper().get_keys().iter().enumerate() {
            graph.new_graph_cprop(key, id);
        }
        graph.update_graph_cprops(graph_meta.metadata());

        for (id, (key, dtype)) in graph_meta
            .temporal_mapper()
            .get_keys()
            .iter()
            .zip(graph_meta.temporal_mapper().dtypes().iter())
            .enumerate()
        {
            graph.new_graph_tprop(key, id, dtype);
        }
        for (t, group) in &graph_meta
            .temporal_props()
            .map(|(key, values)| {
                values
                    .deref()
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
        let n_const_meta = self.node_meta().metadata_mapper();
        for (id, (key, dtype)) in n_const_meta
            .get_keys()
            .iter()
            .zip(n_const_meta.dtypes().iter())
            .enumerate()
        {
            graph.new_node_cprop(key, id, dtype);
        }
        let n_temporal_meta = self.node_meta().temporal_prop_mapper();
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

            for (time, row) in node.temp_prop_rows() {
                graph.update_node_tprops(
                    node.vid(),
                    time,
                    row.into_iter().filter_map(|(id, prop)| Some((id, prop?))),
                );
            }

            graph.update_node_cprops(
                node.vid(),
                (0..n_const_meta.len()).flat_map(|i| node.prop(i).map(|v| (i, v))),
            );
        }

        // Edge Properties
        let e_const_meta = self.edge_meta().metadata_mapper();
        for (id, (key, dtype)) in e_const_meta
            .get_keys()
            .iter()
            .zip(e_const_meta.dtypes().iter())
            .enumerate()
        {
            graph.new_edge_cprop(key, id, dtype);
        }
        let e_temporal_meta = self.edge_meta().temporal_prop_mapper();
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
            let edge = edges.edge(eid);
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
                    (0..e_const_meta.len())
                        .filter_map(|i| edge.metadata_layer(layer_id, i).map(|prop| (i, prop))),
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

impl InternalStableDecode for TemporalGraph {
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
                        let p_type = node_cprop.prop_type();
                        storage.node_meta.metadata_mapper().set_id_and_dtype(
                            node_cprop.name.as_str(),
                            node_cprop.id as usize,
                            p_type,
                        )
                    }
                    Meta::NewNodeTprop(node_tprop) => {
                        let p_type = node_tprop.prop_type();
                        storage.node_meta.temporal_prop_mapper().set_id_and_dtype(
                            node_tprop.name.as_str(),
                            node_tprop.id as usize,
                            p_type,
                        )
                    }
                    Meta::NewGraphCprop(graph_cprop) => storage
                        .graph_meta
                        .metadata_mapper()
                        .set_id(graph_cprop.name.as_str(), graph_cprop.id as usize),
                    Meta::NewGraphTprop(graph_tprop) => {
                        let p_type = graph_tprop.prop_type();
                        storage.graph_meta.temporal_mapper().set_id_and_dtype(
                            graph_tprop.name.as_str(),
                            graph_tprop.id as usize,
                            p_type,
                        )
                    }
                    Meta::NewLayer(new_layer) => storage
                        .edge_meta
                        .layer_meta()
                        .set_id(new_layer.name.as_str(), new_layer.id as usize),
                    Meta::NewEdgeCprop(edge_cprop) => {
                        let p_type = edge_cprop.prop_type();
                        storage.edge_meta.metadata_mapper().set_id_and_dtype(
                            edge_cprop.name.as_str(),
                            edge_cprop.id as usize,
                            p_type,
                        )
                    }
                    Meta::NewEdgeTprop(edge_tprop) => {
                        let p_type = edge_tprop.prop_type();
                        storage.edge_meta.temporal_prop_mapper().set_id_and_dtype(
                            edge_tprop.name.as_str(),
                            edge_tprop.id as usize,
                            p_type,
                        )
                    }
                }
            }
        });

        let new_edge_property_types = storage
            .write_lock_edges()?
            .into_par_iter_mut()
            .map(|mut shard| {
                let mut metadata_types =
                    vec![PropType::Empty; storage.edge_meta.metadata_mapper().len()];
                let mut temporal_prop_types =
                    vec![PropType::Empty; storage.edge_meta.temporal_prop_mapper().len()];

                for edge in graph.edges.iter() {
                    if let Some(mut new_edge) = shard.get_mut(edge.eid()) {
                        let edge_store = new_edge.edge_store_mut();
                        edge_store.src = edge.src();
                        edge_store.dst = edge.dst();
                        edge_store.eid = edge.eid();
                    }
                }
                for update in graph.updates.iter() {
                    if let Some(update) = update.update.as_ref() {
                        match update {
                            Update::DelEdge(del_edge) => {
                                if let Some(mut edge_mut) = shard.get_mut(del_edge.eid()) {
                                    edge_mut
                                        .deletions_mut(del_edge.layer_id())
                                        .insert(del_edge.time());
                                    storage.update_time(del_edge.time());
                                }
                            }
                            Update::UpdateEdgeCprops(update) => {
                                if let Some(mut edge_mut) = shard.get_mut(update.eid()) {
                                    let edge_layer = edge_mut.layer_mut(update.layer_id());
                                    for prop_update in update.props() {
                                        let (id, prop) = prop_update?;
                                        let prop = storage.process_prop_value(&prop);
                                        if let Ok(new_type) = unify_types(
                                            &metadata_types[id],
                                            &prop.dtype(),
                                            &mut false,
                                        ) {
                                            metadata_types[id] = new_type; // the original types saved in protos are now incomplete we need to update them
                                        }
                                        edge_layer.update_metadata(id, prop)?;
                                    }
                                }
                            }
                            Update::UpdateEdgeTprops(update) => {
                                if let Some(mut edge_mut) = shard.get_mut(update.eid()) {
                                    edge_mut
                                        .additions_mut(update.layer_id())
                                        .insert(update.time());
                                    if update.has_props() {
                                        let edge_layer = edge_mut.layer_mut(update.layer_id());
                                        for prop_update in update.props() {
                                            let (id, prop) = prop_update?;
                                            let prop = storage.process_prop_value(&prop);
                                            if let Ok(new_type) = unify_types(
                                                &temporal_prop_types[id],
                                                &prop.dtype(),
                                                &mut false,
                                            ) {
                                                temporal_prop_types[id] = new_type;
                                                // the original types saved in protos are now incomplete we need to update them
                                            }
                                            edge_layer.add_prop(update.time(), id, prop)?;
                                        }
                                    }
                                    storage.update_time(update.time())
                                }
                            }
                            _ => {}
                        }
                    }
                }
                Ok::<_, GraphError>((metadata_types, temporal_prop_types))
            })
            .try_reduce_with(|(l_const, l_temp), (r_const, r_temp)| {
                unify_property_types(&l_const, &r_const, &l_temp, &r_temp)
            })
            .transpose()?;

        if let Some((metadata_types, temp_prop_types)) = new_edge_property_types {
            update_meta(
                metadata_types,
                temp_prop_types,
                storage.edge_meta.metadata_mapper(),
                storage.edge_meta.temporal_prop_mapper(),
            );
        }

        let new_nodes_property_types = storage
            .write_lock_nodes()?
            .into_par_iter_mut()
            .map(|mut shard| {
                let mut metadata_types =
                    vec![PropType::Empty; storage.node_meta.metadata_mapper().len()];
                let mut temporal_prop_types =
                    vec![PropType::Empty; storage.node_meta.temporal_prop_mapper().len()];

                for node in graph.nodes.iter() {
                    let vid = VID(node.vid as usize);
                    let gid = match node.gid.as_ref().unwrap() {
                        Gid::GidStr(name) => GidRef::Str(name),
                        Gid::GidU64(gid) => GidRef::U64(*gid),
                    };
                    if let Some(mut node_store) = shard.set(vid, gid) {
                        storage.logical_to_physical.set(gid, vid)?;
                        node_store.node_store_mut().node_type = node.type_id as usize;
                    }
                }
                let edges = storage.storage.edges.read_lock();
                for edge in edges.iter() {
                    if let Some(src) = shard.get_mut(edge.src()) {
                        for layer in edge.layer_ids_iter(&LayerIds::All) {
                            src.add_edge(edge.dst(), Direction::OUT, layer, edge.eid());
                            for t in edge.additions(layer).iter() {
                                src.update_time(t, edge.eid().with_layer(layer));
                            }
                            for t in edge.deletions(layer).iter() {
                                src.update_time(t, edge.eid().with_layer_deletion(layer));
                            }
                        }
                    }
                    if let Some(dst) = shard.get_mut(edge.dst()) {
                        for layer in edge.layer_ids_iter(&LayerIds::All) {
                            dst.add_edge(edge.src(), Direction::IN, layer, edge.eid());
                            for t in edge.additions(layer).iter() {
                                dst.update_time(t, edge.eid().with_layer(layer));
                            }
                            for t in edge.deletions(layer).iter() {
                                dst.update_time(t, edge.eid().with_layer_deletion(layer));
                            }
                        }
                    }
                }
                for update in graph.updates.iter() {
                    if let Some(update) = update.update.as_ref() {
                        match update {
                            Update::UpdateNodeCprops(update) => {
                                if let Some(node) = shard.get_mut(update.vid()) {
                                    for prop_update in update.props() {
                                        let (id, prop) = prop_update?;
                                        let prop = storage.process_prop_value(&prop);
                                        if let Ok(new_type) = unify_types(
                                            &metadata_types[id],
                                            &prop.dtype(),
                                            &mut false,
                                        ) {
                                            metadata_types[id] = new_type; // the original types saved in protos are now incomplete we need to update them
                                        }
                                        node.update_metadata(id, prop)?;
                                    }
                                }
                            }
                            Update::UpdateNodeTprops(update) => {
                                if let Some(mut node) = shard.get_mut_entry(update.vid()) {
                                    let mut props = vec![];
                                    for prop_update in update.props() {
                                        let (id, prop) = prop_update?;
                                        let prop = storage.process_prop_value(&prop);
                                        if let Ok(new_type) = unify_types(
                                            &temporal_prop_types[id],
                                            &prop.dtype(),
                                            &mut false,
                                        ) {
                                            temporal_prop_types[id] = new_type; // the original types saved in protos are now incomplete we need to update them
                                        }
                                        props.push((id, prop));
                                    }

                                    if props.is_empty() {
                                        node.node_store_mut()
                                            .update_t_prop_time(update.time(), None);
                                    } else {
                                        let prop_offset = node.t_props_log_mut().push(props)?;
                                        node.node_store_mut()
                                            .update_t_prop_time(update.time(), prop_offset);
                                    }

                                    storage.update_time(update.time())
                                }
                            }
                            Update::UpdateNodeType(update) => {
                                if let Some(node) = shard.get_mut(update.vid()) {
                                    node.node_type = update.type_id();
                                }
                            }
                            _ => {}
                        }
                    }
                }
                Ok::<_, GraphError>((metadata_types, temporal_prop_types))
            })
            .try_reduce_with(|(l_const, l_temp), (r_const, r_temp)| {
                unify_property_types(&l_const, &r_const, &l_temp, &r_temp)
            })
            .transpose()?;

        if let Some((metadata_types, temp_prop_types)) = new_nodes_property_types {
            update_meta(
                metadata_types,
                temp_prop_types,
                storage.node_meta.metadata_mapper(),
                storage.node_meta.temporal_prop_mapper(),
            );
        }

        let graph_prop_new_types = graph
            .updates
            .par_iter()
            .map(|update| {
                let mut metadata_types =
                    vec![PropType::Empty; storage.graph_meta.metadata_mapper().len()];
                let mut graph_prop_types =
                    vec![PropType::Empty; storage.graph_meta.temporal_mapper().len()];

                if let Some(update) = update.update.as_ref() {
                    match update {
                        Update::UpdateGraphCprops(props) => {
                            let c_props = proto_ext::collect_props(&props.properties)?;
                            for (id, prop) in &c_props {
                                metadata_types[*id] = prop.dtype();
                            }
                            storage.internal_update_metadata(&c_props)?;
                        }
                        Update::UpdateGraphTprops(props) => {
                            let time = TimeIndexEntry(props.time, props.secondary as usize);
                            let t_props = proto_ext::collect_props(&props.properties)?;
                            for (id, prop) in &t_props {
                                graph_prop_types[*id] = prop.dtype();
                            }
                            storage.internal_add_properties(time, &t_props)?;
                        }
                        _ => {}
                    }
                }
                Ok::<_, GraphError>((metadata_types, graph_prop_types))
            })
            .try_reduce_with(|(l_const, l_temp), (r_const, r_temp)| {
                unify_property_types(&l_const, &r_const, &l_temp, &r_temp)
            })
            .transpose()?;

        if let Some((metadata_types, temp_prop_types)) = graph_prop_new_types {
            update_meta(
                metadata_types,
                temp_prop_types,
                &PropMapper::default(),
                storage.graph_meta.temporal_mapper(),
            );
        }
        Ok(storage)
    }
}

fn update_meta(
    metadata_types: Vec<PropType>,
    temp_prop_types: Vec<PropType>,
    const_meta: &PropMapper,
    temp_meta: &PropMapper,
) {
    let keys = { const_meta.get_keys().iter().cloned().collect::<Vec<_>>() };
    for ((id, prop_type), key) in metadata_types.into_iter().enumerate().zip(keys) {
        const_meta.set_id_and_dtype(key, id, prop_type);
    }
    let keys = { temp_meta.get_keys().iter().cloned().collect::<Vec<_>>() };

    for ((id, prop_type), key) in temp_prop_types.into_iter().enumerate().zip(keys) {
        temp_meta.set_id_and_dtype(key, id, prop_type);
    }
}

fn unify_property_types(
    l_const: &[PropType],
    r_const: &[PropType],
    l_temp: &[PropType],
    r_temp: &[PropType],
) -> Result<(Vec<PropType>, Vec<PropType>), GraphError> {
    let const_pt = l_const
        .iter()
        .zip(r_const)
        .map(|(l, r)| unify_types(l, r, &mut false))
        .collect::<Result<Vec<PropType>, _>>()?;
    let temp_pt = l_temp
        .iter()
        .zip(r_temp)
        .map(|(l, r)| unify_types(l, r, &mut false))
        .collect::<Result<Vec<PropType>, _>>()?;
    Ok((const_pt, temp_pt))
}

impl InternalStableDecode for GraphStorage {
    fn decode_from_proto(graph: &proto::Graph) -> Result<Self, GraphError> {
        Ok(GraphStorage::Unlocked(Arc::new(
            TemporalGraph::decode_from_proto(graph)?,
        )))
    }
}

impl InternalStableDecode for MaterializedGraph {
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

impl InternalStableDecode for Graph {
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

impl InternalStableDecode for PersistentGraph {
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

#[cfg(test)]
mod proto_test {
    use std::{collections::HashMap, path::PathBuf};

    use arrow_array::types::{Int32Type, UInt8Type};
    use tempfile::TempDir;

    use super::*;
    use crate::{
        db::{
            api::{mutation::DeletionOps, properties::internal::InternalMetadataOps},
            graph::graph::assert_graph_equal,
        },
        prelude::*,
        serialise::{metadata::assert_metadata_correct, proto::GraphType, ProtoGraph},
        test_utils::{build_edge_list, build_graph_from_edge_list},
    };
    use chrono::{DateTime, NaiveDateTime};
    use proptest::proptest;
    use raphtory_api::core::storage::arc_str::ArcStr;

    #[test]
    fn prev_proto_str() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(|p| p.join("raphtory/resources/test/old_proto/str"))
            .unwrap();

        let graph = Graph::decode(path).unwrap();

        let nodes_props = graph
            .nodes()
            .properties()
            .into_iter()
            .flat_map(|(_, props)| props.into_iter())
            .collect::<Vec<_>>();
        assert_eq!(
            nodes_props,
            vec![("a".into(), Some("a".into())), ("a".into(), None)]
        );

        // TODO: Revisit this test after metadata handling is finalised.
        //       Refer to the `test_metadata_props` test for context.
        // let nodes_metadata = graph
        //     .nodes()
        //     .metadata()
        //     .into_iter()
        //     .flat_map(|(_, props)| props.into_iter())
        //     .collect::<Vec<_>>();
        // assert_eq!(nodes_metadata, vec![("z".into(), Some("a".into())),]);
    }
    #[test]
    fn can_read_previous_proto() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(|p| p.join("raphtory/resources/test/old_proto/all_props"))
            .unwrap();

        let graph = Graph::decode(path).unwrap();

        let actual: HashMap<_, _> = graph
            .node_meta()
            .get_all_property_names(false)
            .into_iter()
            .map(|key| {
                let props = graph
                    .nodes()
                    .iter()
                    .map(|n| n.properties().get(&key).or_else(|| n.metadata().get(&key)))
                    .collect::<Vec<_>>();
                (key, props)
            })
            .collect();

        let expected: HashMap<ArcStr, Vec<Option<Prop>>> = [
            (
                "name".into(),
                vec![
                    Some("Alice".into()),
                    Some("Alice".into()),
                    Some("Alice".into()),
                ],
            ),
            (
                "age".into(),
                vec![
                    Some(Prop::U32(47)),
                    Some(Prop::U32(47)),
                    Some(Prop::U32(47)),
                ],
            ),
            ("doc".into(), vec![None, None, None]),
            (
                "dtime".into(),
                vec![
                    Some(Prop::DTime(
                        DateTime::parse_from_rfc3339("2021-09-09T01:46:39Z")
                            .unwrap()
                            .into(),
                    )),
                    Some(Prop::DTime(
                        DateTime::parse_from_rfc3339("2021-09-09T01:46:39Z")
                            .unwrap()
                            .into(),
                    )),
                    Some(Prop::DTime(
                        DateTime::parse_from_rfc3339("2021-09-09T01:46:39Z")
                            .unwrap()
                            .into(),
                    )),
                ],
            ),
            (
                "score".into(),
                vec![
                    Some(Prop::I32(27)),
                    Some(Prop::I32(27)),
                    Some(Prop::I32(27)),
                ],
            ),
            ("graph".into(), vec![None, None, None]),
            ("p_graph".into(), vec![None, None, None]),
            (
                "time".into(),
                vec![
                    Some(Prop::NDTime(
                        NaiveDateTime::parse_from_str("+10000-09-09 01:46:39", "%Y-%m-%d %H:%M:%S")
                            .expect("Failed to parse time"),
                    )),
                    Some(Prop::NDTime(
                        NaiveDateTime::parse_from_str("+10000-09-09 01:46:39", "%Y-%m-%d %H:%M:%S")
                            .expect("Failed to parse time"),
                    )),
                    Some(Prop::NDTime(
                        NaiveDateTime::parse_from_str("+10000-09-09 01:46:39", "%Y-%m-%d %H:%M:%S")
                            .expect("Failed to parse time"),
                    )),
                ],
            ),
            (
                "is_adult".into(),
                vec![
                    Some(Prop::Bool(true)),
                    Some(Prop::Bool(true)),
                    Some(Prop::Bool(true)),
                ],
            ),
            (
                "height".into(),
                vec![
                    Some(Prop::F32(1.75)),
                    Some(Prop::F32(1.75)),
                    Some(Prop::F32(1.75)),
                ],
            ),
            (
                "weight".into(),
                vec![
                    Some(Prop::F64(75.5)),
                    Some(Prop::F64(75.5)),
                    Some(Prop::F64(75.5)),
                ],
            ),
            (
                "children".into(),
                vec![
                    Some(Prop::List(
                        vec![Prop::str("Bob"), Prop::str("Charlie")].into(),
                    )),
                    Some(Prop::List(
                        vec![Prop::str("Bob"), Prop::str("Charlie")].into(),
                    )),
                    Some(Prop::List(
                        vec![Prop::str("Bob"), Prop::str("Charlie")].into(),
                    )),
                ],
            ),
            (
                "properties".into(),
                vec![
                    Some(Prop::map(vec![
                        ("is_adult", Prop::Bool(true)),
                        ("weight", Prop::F64(75.5)),
                        (
                            "children",
                            Prop::List(vec![Prop::str("Bob"), Prop::str("Charlie")].into()),
                        ),
                        ("height", Prop::F32(1.75)),
                        ("name", Prop::str("Alice")),
                        ("age", Prop::U32(47)),
                        ("score", Prop::I32(27)),
                    ])),
                    Some(Prop::map(vec![
                        ("is_adult", Prop::Bool(true)),
                        ("age", Prop::U32(47)),
                        ("name", Prop::str("Alice")),
                        ("score", Prop::I32(27)),
                        ("height", Prop::F32(1.75)),
                        (
                            "children",
                            Prop::List(vec![Prop::str("Bob"), Prop::str("Charlie")].into()),
                        ),
                        ("weight", Prop::F64(75.5)),
                    ])),
                    Some(Prop::map(vec![
                        ("weight", Prop::F64(75.5)),
                        ("name", Prop::str("Alice")),
                        ("age", Prop::U32(47)),
                        ("height", Prop::F32(1.75)),
                        ("score", Prop::I32(27)),
                        (
                            "children",
                            Prop::List(vec![Prop::str("Bob"), Prop::str("Charlie")].into()),
                        ),
                        ("is_adult", Prop::Bool(true)),
                    ])),
                ],
            ),
        ]
        .into_iter()
        .collect();

        let check_prop_mapper = |pm: &PropMapper| {
            assert_eq!(
                pm.get_id("properties").and_then(|id| pm.get_dtype(id)),
                Some(PropType::map([
                    ("is_adult", PropType::Bool),
                    ("weight", PropType::F64),
                    ("children", PropType::List(Box::new(PropType::Str))),
                    ("height", PropType::F32),
                    ("name", PropType::Str),
                    ("age", PropType::U32),
                    ("score", PropType::I32),
                ]))
            );
            assert_eq!(
                pm.get_id("children").and_then(|id| pm.get_dtype(id)),
                Some(PropType::List(Box::new(PropType::Str)))
            );
        };

        let pm = graph.node_meta().metadata_mapper();
        check_prop_mapper(pm);

        let pm = graph.edge_meta().temporal_prop_mapper();
        check_prop_mapper(pm);

        let pm = graph.graph_meta().temporal_mapper();
        check_prop_mapper(pm);

        let mut vec1 = actual.keys().collect::<Vec<_>>();
        let mut vec2 = expected.keys().collect::<Vec<_>>();
        vec1.sort();
        vec2.sort();
        assert_eq!(vec1, vec2);
        for (key, actual_props) in actual.iter() {
            let expected_props = expected.get(key).unwrap();
            assert_eq!(actual_props, expected_props, "Key: {}", key);
        }
    }

    #[test]
    fn node_no_props() {
        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        let g1 = Graph::new();
        g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);
    }

    #[test]
    fn node_with_props() {
        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        let g1 = Graph::new();
        g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        g1.add_node(2, "Bob", [("age", Prop::U32(47))], None)
            .unwrap();
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);
    }

    #[cfg(feature = "search")]
    #[test]
    fn test_node_name() {
        let g = Graph::new();
        g.add_edge(1, "ben", "hamza", NO_PROPS, None).unwrap();
        g.add_edge(2, "haaroon", "hamza", NO_PROPS, None).unwrap();
        g.add_edge(3, "ben", "haaroon", NO_PROPS, None).unwrap();
        let temp_file = TempDir::new().unwrap();

        g.encode(&temp_file).unwrap();
        let g2 = MaterializedGraph::load_cached(&temp_file).unwrap();
        assert_eq!(g2.nodes().name().collect_vec(), ["ben", "hamza", "haaroon"]);
        let node_names: Vec<_> = g2.nodes().iter().map(|n| n.name()).collect();
        assert_eq!(node_names, ["ben", "hamza", "haaroon"]);
        let g2_m = g2.materialize().unwrap();
        assert_eq!(
            g2_m.nodes().name().collect_vec(),
            ["ben", "hamza", "haaroon"]
        );
        let g3 = g.materialize().unwrap();
        assert_eq!(g3.nodes().name().collect_vec(), ["ben", "hamza", "haaroon"]);
        let node_names: Vec<_> = g3.nodes().iter().map(|n| n.name()).collect();
        assert_eq!(node_names, ["ben", "hamza", "haaroon"]);

        let temp_file = TempDir::new().unwrap();
        g3.encode(&temp_file).unwrap();
        let g4 = MaterializedGraph::decode(&temp_file).unwrap();
        assert_eq!(g4.nodes().name().collect_vec(), ["ben", "hamza", "haaroon"]);
        let node_names: Vec<_> = g4.nodes().iter().map(|n| n.name()).collect();
        assert_eq!(node_names, ["ben", "hamza", "haaroon"]);
    }

    #[test]
    fn node_with_metadata() {
        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        let g1 = Graph::new();
        g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        let n1 = g1
            .add_node(2, "Bob", [("age", Prop::U32(47))], None)
            .unwrap();

        n1.update_metadata([("name", Prop::Str("Bob".into()))])
            .expect("Failed to update metadata");

        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);
    }

    #[test]
    fn edge_no_props() {
        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
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
        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        let g1 = Graph::new().persistent_graph();
        g1.add_edge(3, "Alice", "Bob", NO_PROPS, None).unwrap();
        g1.delete_edge(19, "Alice", "Bob", None).unwrap();
        g1.encode(&temp_file).unwrap();
        let g2 = PersistentGraph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);

        let edge = g2.edge("Alice", "Bob").expect("Failed to get edge");
        let deletions = edge.deletions().to_vec();
        assert_eq!(deletions, vec![19]);
    }

    #[test]
    fn edge_t_props() {
        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        let g1 = Graph::new();
        g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        g1.add_node(2, "Bob", NO_PROPS, None).unwrap();
        g1.add_edge(3, "Alice", "Bob", [("kind", "friends")], None)
            .unwrap();

        g1.add_edge(
            3,
            "Alice",
            "Bob",
            [("image", Prop::from_arr::<Int32Type>(vec![3i32, 5]))],
            None,
        )
        .unwrap();
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);
    }

    #[test]
    fn edge_metadata() {
        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        let g1 = Graph::new();
        let e1 = g1.add_edge(3, "Alice", "Bob", NO_PROPS, None).unwrap();
        e1.update_metadata([("friends", true)], None)
            .expect("Failed to update metadata");
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);
    }

    #[test]
    fn edge_layers() {
        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
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

        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
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

        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
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
    fn test_all_the_metadata_on_edge() {
        let mut props = vec![];
        write_props_to_vec(&mut props);

        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        let g1 = Graph::new();
        let e = g1.add_edge(1, "Alice", "Bob", NO_PROPS, Some("a")).unwrap();
        e.update_metadata(props.clone(), Some("a"))
            .expect("Failed to update metadata");
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);

        let edge = g2
            .edge("Alice", "Bob")
            .expect("Failed to get edge")
            .layers("a")
            .unwrap();

        for (new, old) in edge.metadata().iter_filtered().zip(props.iter()) {
            assert_eq!(new.0, old.0);
            assert_eq!(new.1, old.1);
        }
    }

    #[test]
    fn test_all_the_metadata_on_node() {
        let mut props = vec![];
        write_props_to_vec(&mut props);

        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        let g1 = Graph::new();
        let n = g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        n.update_metadata(props.clone())
            .expect("Failed to update metadata");
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);

        let node = g2.node("Alice").expect("Failed to get node");

        assert!(props.into_iter().all(|(name, expected)| {
            node.metadata()
                .get(name)
                .filter(|prop| prop == &expected)
                .is_some()
        }))
    }

    #[test]
    fn graph_metadata() {
        let mut props = vec![];
        write_props_to_vec(&mut props);

        let g1 = Graph::new();
        g1.add_metadata(props.clone())
            .expect("Failed to add metadata");

        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);

        props.into_iter().for_each(|(name, prop)| {
            let id = g2.get_metadata_id(name).expect("Failed to get prop id");
            assert_eq!(prop, g2.get_metadata(id).expect("Failed to get prop"));
        });
    }

    #[test]
    fn graph_temp_properties() {
        let mut props = vec![];
        write_props_to_vec(&mut props);

        let g1 = Graph::new();
        for t in 0..props.len() {
            g1.add_properties(t as i64, props[t..t + 1].to_vec())
                .expect("Failed to add metadata");
        }

        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
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
            .map(|v| v.unwrap_str())
            .collect_vec();
        assert_eq!(values, ["test", "test", "test"]);
        for w in values.windows(2) {
            assert_eq!(w[0].as_ptr(), w[1].as_ptr());
        }

        let proto = g.encode_to_proto();
        let g2 = Graph::decode_from_proto(&proto).unwrap();
        let node_view = g2.node(1).unwrap();

        let values = node_view
            .properties()
            .temporal()
            .get("test")
            .unwrap()
            .values()
            .map(|v| v.unwrap_str())
            .collect_vec();
        assert_eq!(values, ["test", "test", "test"]);
        for w in values.windows(2) {
            assert_eq!(w[0].as_ptr(), w[1].as_ptr());
        }
    }

    #[test]
    fn test_incremental_writing_on_graph() {
        let g = Graph::new();
        let mut props = vec![];
        write_props_to_vec(&mut props);
        let temp_cache_file = tempfile::tempdir().unwrap();
        let folder = GraphFolder::from(&temp_cache_file);

        g.cache(&temp_cache_file).unwrap();

        assert_metadata_correct(&folder, &g);

        for t in 0..props.len() {
            g.add_properties(t as i64, props[t..t + 1].to_vec())
                .expect("Failed to add metadata");
        }
        g.write_updates().unwrap();

        g.add_metadata(props.clone())
            .expect("Failed to add metadata");
        g.write_updates().unwrap();

        let n = g.add_node(1, "Alice", NO_PROPS, None).unwrap();
        n.update_metadata(props.clone())
            .expect("Failed to update metadata");
        g.write_updates().unwrap();

        let e = g.add_edge(1, "Alice", "Bob", NO_PROPS, Some("a")).unwrap();
        e.update_metadata(props.clone(), Some("a"))
            .expect("Failed to update metadata");
        g.write_updates().unwrap();

        assert_metadata_correct(&folder, &g);

        g.add_edge(2, "Alice", "Bob", props.clone(), None).unwrap();
        g.add_node(1, "Charlie", props.clone(), None).unwrap();
        g.write_updates().unwrap();

        g.add_edge(7, "Alice", "Bob", NO_PROPS, Some("one"))
            .unwrap();
        g.add_edge(7, "Bob", "Charlie", [("friends", false)], Some("two"))
            .unwrap();
        g.write_updates().unwrap();
        let g2 = Graph::decode(&temp_cache_file).unwrap();
        assert_graph_equal(&g, &g2);

        assert_metadata_correct(&folder, &g);
    }

    #[test]
    fn test_incremental_writing_on_persistent_graph() {
        let g = PersistentGraph::new();
        let mut props = vec![];
        write_props_to_vec(&mut props);
        let temp_cache_file = tempfile::tempdir().unwrap();
        let folder = GraphFolder::from(&temp_cache_file);

        g.cache(&temp_cache_file).unwrap();

        for t in 0..props.len() {
            g.add_properties(t as i64, props[t..t + 1].to_vec())
                .expect("Failed to add metadata");
        }
        g.write_updates().unwrap();

        g.add_metadata(props.clone())
            .expect("Failed to add metadata");
        g.write_updates().unwrap();

        let n = g.add_node(1, "Alice", NO_PROPS, None).unwrap();
        n.update_metadata(props.clone())
            .expect("Failed to update metadata");
        g.write_updates().unwrap();

        let e = g.add_edge(1, "Alice", "Bob", NO_PROPS, Some("a")).unwrap();
        e.update_metadata(props.clone(), Some("a"))
            .expect("Failed to update metadata");
        g.write_updates().unwrap();

        assert_metadata_correct(&folder, &g);

        g.add_edge(2, "Alice", "Bob", props.clone(), None).unwrap();
        g.add_node(1, "Charlie", props.clone(), None).unwrap();
        g.write_updates().unwrap();

        g.add_edge(7, "Alice", "Bob", NO_PROPS, Some("one"))
            .unwrap();
        g.add_edge(7, "Bob", "Charlie", [("friends", false)], Some("two"))
            .unwrap();
        g.write_updates().unwrap();

        let g2 = PersistentGraph::decode(&temp_cache_file).unwrap();

        assert_graph_equal(&g, &g2);

        assert_metadata_correct(&folder, &g);
    }

    // we rely on this to make sure writing no updates does not actually write anything to file
    #[test]
    fn empty_proto_is_empty_bytes() {
        let proto = ProtoGraph::default();
        let bytes = proto.encode_to_vec();
        assert!(bytes.is_empty())
    }

    #[test]
    fn encode_decode_prop_test() {
        proptest!(|(edges in build_edge_list(100, 100))| {
            let g = build_graph_from_edge_list(&edges);
            let bytes = g.encode_to_vec();
            let g2 = Graph::decode_from_bytes(&bytes).unwrap();
            assert_graph_equal(&g, &g2);
        })
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
            Prop::map(props.iter().map(|(k, v)| (ArcStr::from(*k), v.clone()))),
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
            "array",
            Prop::from_arr::<UInt8Type>(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
        ));
    }
}
