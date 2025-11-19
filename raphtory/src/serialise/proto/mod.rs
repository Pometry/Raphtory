use crate::{
    core::entities::LayerIds,
    db::{
        api::{
            properties::internal::{InternalMetadataOps, InternalTemporalPropertyViewOps},
            view::MaterializedGraph,
        },
        graph::views::deletion_graph::PersistentGraph,
    },
    errors::GraphError,
    prelude::Graph,
};

// Load the generated protobuf code from the build directory
pub mod proto_generated {
    include!(concat!(env!("OUT_DIR"), "/serialise.rs"));
}

use itertools::Itertools;
use raphtory_api::core::{
    entities::{
        properties::{prop::Prop, tprop::TPropOps},
        VID,
    },
    storage::timeindex::TimeIndexOps,
};
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::{
        edges::edge_storage_ops::EdgeStorageOps, graph::GraphStorage,
        nodes::node_storage_ops::NodeStorageOps,
    },
};
use std::{io::Cursor, iter, ops::Deref};

pub mod ext;

/// Trait for encoding a graph to protobuf format
pub trait ProtoEncoder {
    fn encode_to_proto(&self) -> proto_generated::Graph;
}

/// Trait for decoding a graph from protobuf format
pub trait ProtoDecoder: Sized {
    fn decode_from_proto(graph: &proto_generated::Graph) -> Result<Self, GraphError>;
}

macro_rules! zip_tprop_updates {
    ($iter:expr) => {
        &$iter
            .map(|(key, values)| values.iter().map(move |(t, v)| (t, (key, v))))
            .kmerge_by(|(left_t, _), (right_t, _)| left_t <= right_t)
            .chunk_by(|(t, _)| *t)
    };
}

impl ProtoEncoder for GraphStorage {
    fn encode_to_proto(&self) -> proto_generated::Graph {
        let storage = self.lock();
        let mut graph = proto_generated::Graph::default();

        // Graph Properties
        let graph_meta = storage.graph_meta();
        for (id, key) in graph_meta.metadata_mapper().read().iter_ids() {
            graph.new_graph_cprop(key, id);
        }
        graph.update_graph_cprops(
            storage
                .metadata_ids()
                .filter_map(|id| Some((id, storage.get_metadata(id)?))),
        );

        for (id, key, dtype) in graph_meta
            .temporal_prop_mapper()
            .locked()
            .iter_ids_and_types()
        {
            graph.new_graph_tprop(key, id, dtype);
        }

        let t_props = graph_meta
            .temporal_prop_mapper()
            .locked()
            .iter_ids_and_types()
            .map(|(id, _, _)| storage.temporal_iter(id).map(move |(t, v)| (t, (id, v))))
            .kmerge_by(|(left_t, _), (right_t, _)| left_t <= right_t)
            .chunk_by(|(t, _)| *t);

        for (t, group) in t_props.into_iter() {
            graph.update_graph_tprops(t, group.map(|(_, v)| v));
        }

        // Layers
        for (id, layer) in storage.edge_meta().layer_meta().read().iter_ids() {
            graph.new_layer(layer, id);
        }

        // Node Types
        for (id, node_type) in storage.node_meta().node_type_meta().read().iter_ids() {
            graph.new_node_type(node_type, id);
        }

        // Node Properties
        let n_const_meta = self.node_meta().metadata_mapper();
        for (id, key, dtype) in n_const_meta.locked().iter_ids_and_types() {
            graph.new_node_cprop(key, id, dtype);
        }
        let n_temporal_meta = self.node_meta().temporal_prop_mapper();
        for (id, key, dtype) in n_temporal_meta.locked().iter_ids_and_types() {
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
                n_const_meta
                    .ids()
                    .flat_map(|i| node.constant_prop_layer(0, i).map(|v| (i, v))),
            );
        }

        // Edge Properties
        let e_const_meta = self.edge_meta().metadata_mapper();
        for (id, key, dtype) in e_const_meta.locked().iter_ids_and_types() {
            graph.new_edge_cprop(key, id, dtype);
        }
        let e_temporal_meta = self.edge_meta().temporal_prop_mapper();
        for (id, key, dtype) in e_temporal_meta.locked().iter_ids_and_types() {
            graph.new_edge_tprop(key, id, dtype);
        }

        // Edges
        let edges = storage.edges();
        for edge in edges.iter(&LayerIds::All) {
            let eid = edge.eid();
            let edge = edge.as_ref();
            graph.new_edge(edge.src(), edge.dst(), eid);
            for layer_id in storage.unfiltered_layer_ids() {
                for (t, props) in zip_tprop_updates!(e_temporal_meta
                    .ids()
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
                    e_const_meta
                        .ids()
                        .filter_map(|i| edge.metadata_layer(layer_id, i).map(|prop| (i, prop))),
                );
            }
        }
        graph
    }
}

impl ProtoEncoder for Graph {
    fn encode_to_proto(&self) -> proto_generated::Graph {
        let mut graph = self.core_graph().encode_to_proto();
        graph.set_graph_type(proto_generated::GraphType::Event);
        graph
    }
}

impl ProtoEncoder for PersistentGraph {
    fn encode_to_proto(&self) -> proto_generated::Graph {
        let mut graph = self.core_graph().encode_to_proto();
        graph.set_graph_type(proto_generated::GraphType::Persistent);
        graph
    }
}

impl ProtoEncoder for MaterializedGraph {
    fn encode_to_proto(&self) -> proto_generated::Graph {
        match self {
            MaterializedGraph::EventGraph(graph) => graph.encode_to_proto(),
            MaterializedGraph::PersistentGraph(graph) => graph.encode_to_proto(),
        }
    }
}

impl ProtoDecoder for GraphStorage {
    fn decode_from_proto(graph: &proto_generated::Graph) -> Result<Self, GraphError> {
        todo!("implement this")
    }
}

impl ProtoDecoder for Graph {
    fn decode_from_proto(graph: &proto_generated::Graph) -> Result<Self, GraphError> {
        todo!("implement this")
    }
}

impl ProtoDecoder for PersistentGraph {
    fn decode_from_proto(graph: &proto_generated::Graph) -> Result<Self, GraphError> {
        match graph.graph_type() {
            proto_generated::GraphType::Event => Err(GraphError::GraphLoadError),
            proto_generated::GraphType::Persistent => {
                let storage = GraphStorage::decode_from_proto(graph)?;
                Ok(PersistentGraph::from_internal_graph(storage))
            }
        }
    }
}

impl ProtoDecoder for MaterializedGraph {
    fn decode_from_proto(graph: &proto_generated::Graph) -> Result<Self, GraphError> {
        todo!("implement this")
    }
}
