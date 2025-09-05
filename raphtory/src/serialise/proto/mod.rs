use crate::{
    core::entities::LayerIds,
    db::{
        api::view::{MaterializedGraph, StaticGraphViewOps},
        graph::views::deletion_graph::PersistentGraph,
    },
    errors::GraphError,
    prelude::{AdditionOps, Graph},
};

// Load the generated protobuf code from the build directory
pub mod proto_generated {
    include!(concat!(env!("OUT_DIR"), "/serialise.rs"));
}

use raphtory_api::core::{
    entities::{
        properties::{
            prop::Prop,
            tprop::TPropOps,
        },
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
use std::{iter, ops::Deref};
use itertools::Itertools;

pub mod ext;

/// Trait for encoding a graph to protobuf format
pub trait ProtoEncoder: StaticGraphViewOps + AdditionOps {
    fn encode_to_proto(&self) -> proto_generated::Graph;
}

/// Trait for decoding a graph from protobuf format
pub trait ProtoDecoder: StaticGraphViewOps + AdditionOps + Sized {
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
        graph.update_graph_cprops(graph_meta.metadata());

        for (id, key, dtype) in graph_meta.temporal_mapper().locked().iter_ids_and_types() {
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

#[cfg(test)]
mod proto_test {
    use std::{collections::HashMap, path::PathBuf};

    use arrow_array::types::{Int32Type, UInt8Type};
    use prost::Message;
    use tempfile::TempDir;

    use super::*;
    use proto_generated::GraphType;
    use crate::{
        db::{
            api::{mutation::DeletionOps, properties::internal::InternalMetadataOps},
            graph::graph::assert_graph_equal,
        },
        prelude::*,
        serialise::{
            graph_folder::GraphFolder,
            metadata::assert_metadata_correct,
            ProtoGraph,
            {StableEncode, StableDecode, CacheOps},
        },
        test_utils::{build_edge_list, build_graph_from_edge_list},
    };
    use chrono::{DateTime, NaiveDateTime};
    use proptest::proptest;
    use raphtory_api::core::{
        entities::{
            EID, GID, GidRef,
            properties::{
                meta::PropMapper,
                prop::PropType,
            },
        },
        storage::{
            arc_str::ArcStr,
            timeindex::TimeIndexEntry,
        },
    };
    use std::{iter, sync::Arc};

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
        let mut graph1 = proto_generated::Graph::default();
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

        let mut graph2 = proto_generated::Graph::default();
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
            let bytes = g.encode_to_bytes();
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
