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
        graph.load_index(&folder.root_folder)?;

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

            for (time, _, row) in node.temp_prop_rows() {
                graph.update_node_tprops(node.vid(), time, row.into_iter());
            }

            graph.update_node_cprops(
                node.vid(),
                (0..n_const_meta.len())
                    .flat_map(|i| node.constant_prop_layer(0, i).map(|v| (i, v))),
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
            for layer_id in storage.unfiltered_layer_ids() {
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

impl InternalStableDecode for TemporalGraph {
    fn decode_from_proto(graph: &proto::Graph) -> Result<Self, GraphError> {
        todo!("remove this stuff!")
    }
}

fn update_meta(
    const_prop_types: Vec<PropType>,
    temp_prop_types: Vec<PropType>,
    const_meta: &PropMapper,
    temp_meta: &PropMapper,
) {
    let keys = { const_meta.get_keys().iter().cloned().collect::<Vec<_>>() };
    for ((id, prop_type), key) in const_prop_types.into_iter().enumerate().zip(keys) {
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
        todo!("remove this stuff!")
    }
}

impl InternalStableDecode for MaterializedGraph {
    fn decode_from_proto(graph: &proto::Graph) -> Result<Self, GraphError> {
        todo!("remove this stuff!")
    }
}

impl InternalStableDecode for Graph {
    fn decode_from_proto(graph: &proto::Graph) -> Result<Self, GraphError> {
        todo!("remove this stuff!")
    }
}

impl InternalStableDecode for PersistentGraph {
    fn decode_from_proto(graph: &proto::Graph) -> Result<Self, GraphError> {
        todo!("remove this stuff!")
    }
}
