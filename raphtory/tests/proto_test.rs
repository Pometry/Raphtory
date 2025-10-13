#[cfg(test)]
#[cfg(feature = "proto")]
mod proto_test {
    use prost::Message;
    use chrono::{DateTime, NaiveDateTime};
    use itertools::Itertools;
    use proptest::proptest;
    use raphtory::{
        db::{
            api::{mutation::DeletionOps, properties::internal::InternalMetadataOps},
            graph::{graph::assert_graph_equal, views::deletion_graph::PersistentGraph},
        },
        prelude::*,
        serialise::{metadata::assert_metadata_correct, GraphFolder},
        serialise::{proto::{ProtoDecoder, ProtoEncoder, proto_generated::{GraphType}}, ProtoGraph},
    };
    use raphtory_api::core::{
        entities::properties::{meta::PropMapper, prop::PropType},
        storage::arc_str::ArcStr,
    };
    use raphtory_storage::core_ops::CoreGraphOps;
    use std::{collections::HashMap, io::Cursor, iter, path::PathBuf, sync::Arc};
    use tempfile::TempDir;
    use raphtory_core::entities::{GidRef, EID, VID};
    use raphtory_core::storage::timeindex::TimeIndexEntry;

    #[cfg(feature = "arrow")]
    use arrow::array::types::{Int32Type, UInt8Type};
    use raphtory::test_utils::{build_edge_list, build_graph_from_edge_list};

    #[test]
    fn prev_proto_str() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(|p| p.join("raphtory/resources/test/old_proto/str/graph"))
            .unwrap();

        let bytes = std::fs::read(path).unwrap();
        let proto_graph = ProtoGraph::decode(Cursor::new(bytes)).unwrap();
        let graph = Graph::decode_from_proto(&proto_graph).unwrap();
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
            .map(|p| p.join("raphtory/resources/test/old_proto/all_props/graph"))
            .unwrap();

        let bytes = std::fs::read(path).unwrap();
        let proto_graph = ProtoGraph::decode(Cursor::new(bytes)).unwrap();
        let graph = Graph::decode_from_proto(&proto_graph).unwrap();
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
    fn manually_test_append() {
        let mut graph1 = ProtoGraph::default();

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
        let mut graph2 = ProtoGraph::default();

        graph2.new_node(GidRef::Str("3"), VID(2), 0);
        graph2.new_edge(VID(0), VID(2), EID(1));
        graph2.update_edge_tprops(
            EID(1),
            TimeIndexEntry::start(2),
            0,
            iter::empty::<(usize, Prop)>(),
        );
        bytes1.extend(graph2.encode_to_vec());

        let buf = bytes1.as_slice();
        let proto_graph = ProtoGraph::decode(buf).unwrap();
        let graph = Graph::decode_from_proto(&proto_graph).unwrap();

        assert_eq!(graph.nodes().name().collect_vec(), ["1", "2", "3"]);
        assert_eq!(
            graph.edges().id().collect_vec(),
            [
                (GID::Str("1".to_string()), GID::Str("2".to_string())),
                (GID::Str("1".to_string()), GID::Str("3".to_string()))
            ]
        )
    }

    // we rely on this to make sure writing no updates does not actually write anything to file
    #[test]
    fn empty_proto_is_empty_bytes() {
        let proto = ProtoGraph::default();
        let bytes = proto.encode_to_vec();
        assert!(bytes.is_empty())
    }
}
