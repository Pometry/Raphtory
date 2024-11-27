use crate::{
    core::{
        entities::{graph::tgraph::TemporalGraph, LayerIds},
        utils::errors::GraphError,
        Prop,
    },
    db::{
        api::{
            mutation::internal::{InternalAdditionOps, InternalPropertyAdditionOps},
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
        proto::{graph_update::*, new_meta::*, new_node::Gid},
        proto_ext,
    },
};
use itertools::Itertools;
use prost::Message;
use raphtory_api::core::{
    entities::{GidRef, EID, VID},
    storage::timeindex::TimeIndexEntry,
    Direction,
};
use rayon::prelude::*;
use std::{iter, sync::Arc};

use super::GraphFolder;

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

    fn encode(&self, path: impl Into<GraphFolder>) -> Result<(), GraphError> {
        let bytes = self.encode_to_vec();
        let result = path.into().write_graph(&bytes);
        result?;
        Ok(())
    }
}

pub trait StableDecode: Sized {
    fn decode_from_proto(graph: &proto::Graph) -> Result<Self, GraphError>;
    fn decode_from_bytes(bytes: &[u8]) -> Result<Self, GraphError> {
        let graph = proto::Graph::decode(bytes)?;
        Self::decode_from_proto(&graph)
    }
    fn decode(path: impl Into<GraphFolder>) -> Result<Self, GraphError> {
        let bytes = path.into().read_graph()?;
        Self::decode_from_bytes(bytes.as_ref())
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
                            proto_ext::as_prop_type(node_cprop.p_type()),
                        )
                    }
                    Meta::NewNodeTprop(node_tprop) => {
                        storage.node_meta.temporal_prop_meta().set_id_and_dtype(
                            node_tprop.name.as_str(),
                            node_tprop.id as usize,
                            proto_ext::as_prop_type(node_tprop.p_type()),
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
                            proto_ext::as_prop_type(graph_tprop.p_type()),
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
                            proto_ext::as_prop_type(edge_cprop.p_type()),
                        )
                    }
                    Meta::NewEdgeTprop(edge_tprop) => {
                        storage.edge_meta.temporal_prop_meta().set_id_and_dtype(
                            edge_tprop.name.as_str(),
                            edge_tprop.id as usize,
                            proto_ext::as_prop_type(edge_tprop.p_type()),
                        )
                    }
                }
            }
        });
        storage
            .write_lock_edges()?
            .into_par_iter_mut()
            .try_for_each(|mut shard| {
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
                                        edge_layer.update_constant_prop(id, prop)?;
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
                Ok::<(), GraphError>(())
            })?;
        storage
            .write_lock_nodes()?
            .into_par_iter_mut()
            .try_for_each(|mut shard| {
                for node in graph.nodes.iter() {
                    let vid = VID(node.vid as usize);
                    let gid = match node.gid.as_ref().unwrap() {
                        Gid::GidStr(name) => GidRef::Str(name),
                        Gid::GidU64(gid) => GidRef::U64(*gid),
                    };
                    if let Some(mut node_store) = shard.set(vid, gid) {
                        storage.logical_to_physical.set(gid, vid)?;
                        node_store.get_mut().node_type = node.type_id as usize;
                    }
                }
                let edges = storage.storage.edges.read_lock();
                for edge in edges.iter() {
                    if let Some(src) = shard.get_mut(edge.src()) {
                        for layer in edge.layer_ids_iter(&LayerIds::All) {
                            src.add_edge(edge.dst(), Direction::OUT, layer, edge.eid());
                            for t in edge.additions(layer).iter() {
                                src.update_time(t, edge.eid());
                            }
                            for t in edge.deletions(layer).iter() {
                                src.update_time(t, edge.eid());
                            }
                        }
                    }
                    if let Some(dst) = shard.get_mut(edge.dst()) {
                        for layer in edge.layer_ids_iter(&LayerIds::All) {
                            dst.add_edge(edge.src(), Direction::IN, layer, edge.eid());
                            for t in edge.additions(layer).iter() {
                                dst.update_time(t, edge.eid());
                            }
                            for t in edge.deletions(layer).iter() {
                                dst.update_time(t, edge.eid());
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
                                        node.update_constant_prop(id, prop)?;
                                    }
                                }
                            }
                            Update::UpdateNodeTprops(update) => {
                                if let Some(mut node) = shard.get_mut_entry(update.vid()) {
                                    let mut props = vec![];
                                    for prop_update in update.props() {
                                        let (id, prop) = prop_update?;
                                        let prop = storage.process_prop_value(&prop);
                                        props.push((id, prop));
                                    }

                                    if props.is_empty() {
                                        node.get_mut().update_t_prop_time(update.time(), None);
                                    } else {
                                        let prop_offset = node.t_props_log_mut().push(props)?;
                                        node.get_mut()
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
                Ok::<(), GraphError>(())
            })?;

        graph.updates.par_iter().try_for_each(|update| {
            if let Some(update) = update.update.as_ref() {
                match update {
                    Update::UpdateGraphCprops(props) => {
                        storage.internal_update_constant_properties(&proto_ext::collect_props(
                            &props.properties,
                        )?)?;
                    }
                    Update::UpdateGraphTprops(props) => {
                        let time = TimeIndexEntry(props.time, props.secondary as usize);
                        storage.internal_add_properties(
                            time,
                            &proto_ext::collect_props(&props.properties)?,
                        )?;
                    }
                    _ => {}
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

#[cfg(test)]
mod proto_test {
    use tempfile::TempDir;

    use super::*;
    use crate::{
        core::{DocumentInput, Lifespan},
        db::{
            api::{mutation::DeletionOps, properties::internal::ConstPropertiesOps},
            graph::graph::assert_graph_equal,
        },
        prelude::*,
        serialise::{proto::GraphType, ProtoGraph},
        test_utils::{build_edge_list, build_graph_from_edge_list},
    };
    use chrono::{DateTime, NaiveDateTime};
    use proptest::proptest;
    use raphtory_api::core::storage::arc_str::ArcStr;

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
    fn node_with_const_props() {
        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
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
        let deletions = edge.deletions().iter().copied().collect::<Vec<_>>();
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
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file).unwrap();
        assert_graph_equal(&g1, &g2);
    }

    #[test]
    fn edge_const_props() {
        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
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
    fn test_all_the_const_props_on_edge() {
        let mut props = vec![];
        write_props_to_vec(&mut props);

        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
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

        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
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

        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
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
            .into_iter()
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
        let g = Graph::new();
        let mut props = vec![];
        write_props_to_vec(&mut props);
        let temp_cache_file = tempfile::tempdir().unwrap();

        g.cache(&temp_cache_file).unwrap();

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
        let g2 = Graph::decode(&temp_cache_file).unwrap();
        assert_graph_equal(&g, &g2);
    }

    #[test]
    fn test_incremental_writing_on_persistent_graph() {
        let g = PersistentGraph::new();
        let mut props = vec![];
        write_props_to_vec(&mut props);
        let temp_cache_file = tempfile::tempdir().unwrap();

        g.cache(&temp_cache_file).unwrap();

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

        let g2 = PersistentGraph::decode(&temp_cache_file).unwrap();

        assert_graph_equal(&g, &g2);
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
