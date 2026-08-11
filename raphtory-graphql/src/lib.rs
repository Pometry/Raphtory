#![recursion_limit = "256"]

pub use crate::{
    auth::{require_jwt_write_access_dynamic, Access, Roles, TokenClaimValues},
    model::graph::{filtering::GraphAccessFilter, property::Value},
    server::GraphServer,
};
use crate::{data::InsertionError, paths::PathValidationError};
pub use raphtory::db::graph::views::{PropertyRedactedGraph, PropertyRedaction};
use raphtory::errors::GraphError;
use std::sync::Arc;

mod auth;
pub mod auth_policy;

pub use auth::{KeyResolver, StaticKeyResolver};
pub mod cache;
pub mod cli;
pub mod client;
pub mod config;
pub mod data;
mod graph;
pub mod model;
pub mod observability;
mod paths;
pub mod rayon;
mod routes;
pub mod server;
pub mod url_encode;

#[cfg(feature = "python")]
pub mod python;

pub mod plugin;
#[cfg(test)]
pub(crate) mod test_support;

#[derive(thiserror::Error, Debug)]
pub enum GQLError {
    #[error(transparent)]
    GraphError(#[from] GraphError),
    #[error(transparent)]
    Validation(#[from] PathValidationError),
    #[error(transparent)]
    Insertion(#[from] InsertionError),
    #[error(transparent)]
    Arc(#[from] Arc<Self>),
}

#[cfg(test)]
mod graphql_test {
    use crate::{
        auth::Access,
        auth_policy::{auth_policy_tests::FakePolicy, GraphPermission, NamespacePermission},
        config::app_config::AppConfig,
        data::{data_tests::save_graphs_to_work_dir, Data},
        model::App,
        test_support::{
            assert_is_namespace_dir, run_mutation, run_mutation_as_user, setup_with_graphs,
            setup_with_policy,
        },
        url_encode::{url_decode_graph_at, url_encode_graph},
    };
    use async_graphql::{dynamic::Schema, UploadValue};
    use dynamic_graphql::{Request, Variables};
    use itertools::Itertools;
    use raphtory::{
        db::{
            api::{
                storage::storage::Config,
                view::{IntoDynamic, MaterializedGraph},
            },
            graph::views::deletion_graph::PersistentGraph,
        },
        prelude::*,
    };
    use raphtory_api::core::{
        entities::GID,
        storage::{arc_str::ArcStr, graph_folder::GraphFolder},
    };
    use serde_json::{json, Value};
    use std::{
        collections::{HashMap, HashSet},
        fs,
        sync::Arc,
    };
    use tempfile::{tempdir, TempDir};

    #[tokio::test]
    async fn test_copy_graph() {
        let graph = Graph::new();
        graph.add_node(1, "test", NO_PROPS, None, None).unwrap();
        let tmp_dir = tempdir().unwrap();
        let data = Data::new(tmp_dir.path(), &AppConfig::default(), Config::default());
        let namespace = tmp_dir.path().join("test");
        fs::create_dir(&namespace).unwrap();
        graph.encode(namespace.join("g3")).unwrap();
        let schema = App::create_schema().data(data).finish().unwrap();
        let query = r#"mutation {
            copyGraph(
                path: "test/g3",
                newPath: "test/g4",
            )
        }"#;

        let req = Request::new(query).data(Access::Rw);
        let res = schema.execute(req).await;
        assert_eq!(res.errors, []);
    }

    #[tokio::test]
    async fn basic_query() {
        let graph = PersistentGraph::new();
        graph
            .add_node(0, 11, NO_PROPS, None, None)
            .expect("Could not add node!");
        graph.add_metadata([("name", "lotr")]).unwrap();

        let graph: MaterializedGraph = graph.into();
        let graphs = HashMap::from([("lotr".to_string(), graph)]);
        let tmp_dir = tempdir().unwrap();
        let data = Data::new(tmp_dir.path(), &AppConfig::default(), Config::default());
        save_graphs_to_work_dir(&data, &graphs).await.unwrap();

        let schema = App::create_schema().data(data).finish().unwrap();

        let query = r#"
        {
          graph(path: "lotr") {
            nodes {
              list {
                id
              }
            }
          }
        }
        "#;
        let req = Request::new(query);
        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();

        assert_eq!(
            data,
            json!({
                "graph": {
                    "nodes": {
                        "list": [
                            {
                                "id": 11
                            }
                        ]
                    }
                }
            }),
        );
    }

    #[tokio::test]
    async fn test_graph_properties_schema() {
        let graph = Graph::new();
        graph
            .add_node(
                0,
                1,
                [
                    ("type", Prop::Str(ArcStr::from("wallet"))),
                    ("cost", Prop::F32(99.5)),
                ],
                Some("a"),
                None,
            )
            .unwrap();
        graph
            .add_node(
                1,
                2,
                [
                    ("type", Prop::Str(ArcStr::from("wallet"))),
                    ("cost", Prop::F32(10.0)),
                ],
                Some("a"),
                None,
            )
            .unwrap();
        graph
            .add_node(
                5,
                3,
                [
                    ("type", Prop::Str(ArcStr::from("wallet"))),
                    ("cost", Prop::F32(76.0)),
                ],
                Some("a"),
                None,
            )
            .unwrap();
        graph
            .node(1)
            .unwrap()
            .add_metadata([("lol", "smile")])
            .unwrap();

        let edges = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];
        for e in &edges {
            graph
                .add_edge(
                    e.0,
                    e.1,
                    e.2,
                    [
                        ("prop1", Prop::I32(1)),
                        ("prop2", Prop::F32(9.8)),
                        ("prop3", Prop::Str(ArcStr::from("test"))),
                    ],
                    None,
                )
                .unwrap();
        }
        graph
            .edge(edges[0].1, edges[0].2)
            .unwrap()
            .add_metadata([("static", "test")], None)
            .unwrap();
        let graph: MaterializedGraph = graph.into();

        let graphs = HashMap::from([("graph".to_string(), graph)]);
        let tmp_dir = tempdir().unwrap();
        let data = Data::new(tmp_dir.path(), &AppConfig::default(), Config::default());
        save_graphs_to_work_dir(&data, &graphs).await.unwrap();

        let schema = App::create_schema().data(data).finish().unwrap();
        let prop_has_key_filter = r#"
        {
          graph(path: "graph") {
            schema {
              layers {
                edges {
                  properties {
                    key
                    propertyType
                    variants
                  }
                  metadata {
                    key
                    propertyType
                    variants
                  }
                }
              }
              nodes {
                properties {
                    key
                    propertyType
                    variants
                }
                metadata {
                    key
                    propertyType
                    variants
                }
              }
            }
          }
        }
        "#;

        let req = Request::new(prop_has_key_filter);
        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);

        fn sort_properties(properties: &mut Vec<Value>) {
            properties.sort_by(|a, b| {
                let a_type = a["propertyType"].as_str().unwrap_or("");
                let b_type = b["propertyType"].as_str().unwrap_or("");
                a_type.cmp(b_type)
            });
        }

        if let Value::Array(mut node_properties) =
            data["graph"]["schema"]["nodes"][1]["properties"].clone()
        {
            sort_properties(&mut node_properties);

            assert_eq!(node_properties[0]["propertyType"].as_str().unwrap(), "F32");
            assert_eq!(node_properties[1]["propertyType"].as_str().unwrap(), "Str");
        }

        if let Value::Array(mut node_metadata) =
            data["graph"]["schema"]["nodes"][1]["metadata"].clone()
        {
            sort_properties(&mut node_metadata);

            assert_eq!(node_metadata[0]["propertyType"].as_str().unwrap(), "Str");
        }

        if let Value::Array(mut edge_properties) =
            data["graph"]["schema"]["layers"][0]["edges"][0]["properties"].clone()
        {
            sort_properties(&mut edge_properties);

            assert_eq!(edge_properties[0]["propertyType"].as_str().unwrap(), "F32");
            assert_eq!(edge_properties[1]["propertyType"].as_str().unwrap(), "I32");
            assert_eq!(edge_properties[2]["propertyType"].as_str().unwrap(), "Str");
        }

        if let Value::Array(mut edge_metadata) =
            data["graph"]["schema"]["layers"][0]["edges"][0]["metadata"].clone()
        {
            sort_properties(&mut edge_metadata);

            assert_eq!(edge_metadata[0]["propertyType"].as_str().unwrap(), "Str");
        }
    }

    #[tokio::test]
    async fn query_nodefilter() {
        let graph = Graph::new();
        graph
            .add_node(0, 1, [("pgraph", Prop::I32(0))], None, None)
            .unwrap();
        let graph: MaterializedGraph = graph.into();

        let graphs = HashMap::from([("graph".to_string(), graph)]);
        let tmp_dir = tempdir().unwrap();
        let data = Data::new(tmp_dir.path(), &AppConfig::default(), Config::default());
        save_graphs_to_work_dir(&data, &graphs).await.unwrap();

        let schema = App::create_schema().data(data).finish().unwrap();
        let prop_has_key_filter = r#"
        {
          graph(path: "graph") {
            nodes{
              list {
                name
                properties{
                    contains(key:"pgraph")
                }
              }
            }
          }
        }
        "#;

        let req = Request::new(prop_has_key_filter);
        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();
        assert_eq!(
            data,
            json!({
                "graph": {
                    "nodes": {
                        "list": [
                            { "name": "1",
                              "properties":{
                                "contains":true
                            }},
                        ]
                    }
                }
            }),
        );
    }

    fn degree_graph_with_add_node_and_add_edge() -> Graph {
        let graph = degree_graph_with_add_edge_only();
        let add_nodes = [
            (0, "1", Some("layer_a")),
            (0, "7", None),
            (0, "8", None),
            (3, "9", Some("layer_a")),
            (4, "9", Some("layer_c")),
            (5, "10", Some("layer_b")),
            (6, "10", Some("layer_e")),
            (7, "11", Some("layer_d")),
            (8, "12", Some("layer_f")),
            (9, "12", Some("layer_c")),
        ];
        for (t, id, layer) in add_nodes {
            graph.add_node(t, id, NO_PROPS, None, layer).unwrap();
        }
        graph
    }

    fn degree_graph_with_add_edge_only() -> Graph {
        let graph = Graph::new();

        let edges = [
            (1, "1", "2", "layer_a"),
            (1, "1", "3", "layer_b"),
            (1, "1", "4", "layer_a"),
            (1, "1", "5", "layer_b"),
            (1, "1", "6", "layer_a"),
            (2, "2", "1", "layer_b"),
            (2, "2", "3", "layer_a"),
            (2, "2", "4", "layer_b"),
            (2, "2", "5", "layer_a"),
            (3, "3", "1", "layer_a"),
            (3, "3", "4", "layer_b"),
            (3, "3", "5", "layer_a"),
            (4, "4", "1", "layer_b"),
            (4, "4", "2", "layer_a"),
            (5, "5", "1", "layer_b"),
            (6, "6", "1", "layer_a"),
            (6, "4", "3", "layer_b"),
            (6, "5", "2", "layer_a"),
            (6, "6", "2", "layer_b"),
            (6, "5", "3", "layer_a"),
            (7, "2", "6", "layer_c"),
            (7, "3", "6", "layer_d"),
            (7, "6", "4", "layer_e"),
            (7, "1", "5", "layer_f"),
            (8, "3", "2", "layer_c"),
            (8, "4", "6", "layer_d"),
            (8, "2", "5", "layer_e"),
            (8, "6", "3", "layer_f"),
            (9, "5", "4", "layer_c"),
            (9, "4", "5", "layer_d"),
            (9, "2", "4", "layer_e"),
            (9, "3", "1", "layer_f"),
        ];
        for (t, src, dst, layer) in edges {
            graph.add_edge(t, src, dst, NO_PROPS, Some(layer)).unwrap();
        }

        graph
    }

    pub(crate) fn single_component_test_graph() -> MaterializedGraph {
        let graph = Graph::new();
        // chain a -> b -> c -> d
        for (src, dst) in [("a", "b"), ("b", "c"), ("c", "d")] {
            graph.add_edge(1, src, dst, NO_PROPS, None).unwrap();
        }
        graph.into()
    }

    pub(crate) fn star_test_graph() -> MaterializedGraph {
        let graph = Graph::new();
        // star out of a: a -> b, a -> c, a -> d
        for (src, dst) in [("a", "b"), ("a", "c"), ("a", "d")] {
            graph.add_edge(1, src, dst, NO_PROPS, None).unwrap();
        }
        graph.into()
    }

    pub(crate) fn components_test_graph() -> MaterializedGraph {
        let graph = Graph::new();
        // cycle a -> b -> c -> a (one SCC), plus d -> a (d reaches the cycle but not vice versa)
        for (src, dst) in [("a", "b"), ("b", "c"), ("c", "a"), ("d", "a")] {
            graph.add_edge(1, src, dst, NO_PROPS, None).unwrap();
        }
        graph.into()
    }

    pub(crate) fn community_test_graph() -> MaterializedGraph {
        let graph = Graph::new();
        // two triangles joined by a single bridge edge (c -> d)
        for (src, dst) in [
            ("a", "b"),
            ("b", "c"),
            ("c", "a"),
            ("d", "e"),
            ("e", "f"),
            ("f", "d"),
            ("c", "d"),
        ] {
            graph.add_edge(1, src, dst, NO_PROPS, None).unwrap();
        }
        graph.into()
    }

    pub(crate) fn scalar_metrics_test_graph() -> MaterializedGraph {
        let graph = Graph::new();
        // a <-> b reciprocated, b -> c -> a forming a triangle with a-b, and c -> d as a pendant edge,
        // so density/reciprocity/clustering/degree are all non-trivial
        for (src, dst) in [("a", "b"), ("b", "a"), ("b", "c"), ("c", "a"), ("c", "d")] {
            graph.add_edge(1, src, dst, NO_PROPS, None).unwrap();
        }
        graph.into()
    }

    pub(crate) fn centrality_test_graph() -> MaterializedGraph {
        let graph = Graph::new();
        // path a -> b -> c -> d so nodes get distinct centrality scores
        graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        graph.add_edge(2, "b", "c", NO_PROPS, None).unwrap();
        graph.add_edge(3, "c", "d", NO_PROPS, None).unwrap();
        graph.into()
    }

    #[tokio::test]
    async fn test_algorithm_scalar_metrics() {
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", scalar_metrics_test_graph())], tmp_dir.path()).await;

        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              globalClusteringCoefficient
              directedGraphDensity
              globalReciprocity
              averageDegree
              maxDegree
              minDegree
              maxOutDegree
              maxInDegree
              minOutDegree
              minInDegree
              tripletCount
              triangleCount
            }
          }
        }
        "#;

        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": { "algorithm": {
                    "globalClusteringCoefficient": 0.6,
                    "directedGraphDensity": 0.4166666666666667,
                    "globalReciprocity": 0.4,
                    "averageDegree": 2.0,
                    "maxDegree": 3,
                    "minDegree": 1,
                    "maxOutDegree": 2,
                    "maxInDegree": 2,
                    "minOutDegree": 0,
                    "minInDegree": 1,
                    "tripletCount": 5,
                    "triangleCount": 1
                } }
            })
        );
    }

    #[tokio::test]
    async fn test_degree_filter_nodes_and_select_gql() {
        let graph: MaterializedGraph = degree_graph_with_add_node_and_add_edge().into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        let query = r#"
        {
          graph(path: "g") {
            filterNodes: filter(
                expr: { node: { degree: { direction: BOTH, where: { gt: { u64: 0 } } } } }
            ) {
              nodes {
                list {
                  name
                }
              }
            }
            nodes {
              select(expr: { nodes: { degree: { direction: BOTH, where: { gt: { u64: 0 } } } } }) {
                list {
                  name
                }
              }
            }
          }
        }
        "#;

        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);

        let data = json_sort_by_name(res.data.into_json().unwrap());

        assert_eq!(
            data,
            json!({
                "graph": {
                    "filterNodes": {
                        "nodes": {
                            "list": [
                                { "name": "1" },
                                { "name": "2" },
                                { "name": "3" },
                                { "name": "4" },
                                { "name": "5" },
                                { "name": "6" }
                            ]
                        }
                    },
                    "nodes": {
                        "select": {
                            "list": [
                                { "name": "1" },
                                { "name": "2" },
                                { "name": "3" },
                                { "name": "4" },
                                { "name": "5" },
                                { "name": "6" }
                            ]
                        }
                    }
                }
            })
        );
    }

    #[tokio::test]
    async fn test_unique_temporal_properties() {
        let g = Graph::new();
        g.add_metadata([("name", "graph")]).unwrap();
        g.add_properties(1, [("state", "abc")]).unwrap();
        g.add_properties(2, [("state", "abc")]).unwrap();
        g.add_properties(3, [("state", "xyz")]).unwrap();
        g.add_properties(4, [("state", "abc")]).unwrap();
        g.add_edge(1, 1, 2, [("status", "open")], None).unwrap();
        g.add_edge(2, 1, 2, [("status", "open")], None).unwrap();
        g.add_edge(3, 1, 2, [("status", "review")], None).unwrap();
        g.add_edge(4, 1, 2, [("status", "open")], None).unwrap();
        g.add_edge(5, 1, 2, [("status", "in-progress")], None)
            .unwrap();
        g.add_edge(10, 1, 2, [("status", "in-progress")], None)
            .unwrap();
        g.add_edge(9, 1, 2, [("state", true)], None).unwrap();
        g.add_edge(10, 1, 2, [("state", false)], None).unwrap();
        g.add_edge(6, 1, 2, NO_PROPS, None).unwrap();
        g.add_node(11, 3, [("name", "phone")], None, None).unwrap();
        g.add_node(12, 3, [("name", "fax")], None, None).unwrap();
        g.add_node(13, 3, [("name", "fax")], None, None).unwrap();

        let graph: MaterializedGraph = g.into();
        let graphs = HashMap::from([("graph".to_string(), graph)]);
        let tmp_dir = tempdir().unwrap();
        let data = Data::new(tmp_dir.path(), &AppConfig::default(), Config::default());
        save_graphs_to_work_dir(&data, &graphs).await.unwrap();

        let schema = App::create_schema().data(data).finish().unwrap();

        // Query each `unique` by key so we can assert the typed element shape
        // (strings for string props, bools for bool props — not stringified).
        let query = r#"
        {
          graph(path: "graph") {
            properties {
              temporal {
                get(key: "state") { unique }
              }
            }
            node(name: "3") {
              properties {
                temporal {
                  get(key: "name") { unique }
                }
              }
            }
            edge(src: "1", dst: "2") {
              properties {
                temporal {
                  status: get(key: "status") { unique }
                  state:  get(key: "state")  { unique }
                }
              }
            }
          }
        }
        "#;

        let req = Request::new(query);
        let res = schema.execute(req).await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();

        fn sorted_unique<'a>(v: &'a Value) -> Vec<&'a Value> {
            let mut out: Vec<&Value> = v["unique"].as_array().unwrap().iter().collect();
            // serde_json::Value has a deterministic total order for same-typed values
            // and groups by type for mixed inputs — fine for this test.
            out.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
            out
        }

        // graph-level `state` is a string property
        let state = sorted_unique(&data["graph"]["properties"]["temporal"]["get"]);
        assert_eq!(state, vec![&json!("abc"), &json!("xyz")]);

        // node-level `name` is a string property
        let name = sorted_unique(&data["graph"]["node"]["properties"]["temporal"]["get"]);
        assert_eq!(name, vec![&json!("fax"), &json!("phone")]);

        // edge-level `status` is a string property
        let status = sorted_unique(&data["graph"]["edge"]["properties"]["temporal"]["status"]);
        assert_eq!(
            status,
            vec![&json!("in-progress"), &json!("open"), &json!("review")]
        );

        // edge-level `state` is a bool property — must come back as JSON bools,
        // not strings "true" / "false".
        let edge_state = sorted_unique(&data["graph"]["edge"]["properties"]["temporal"]["state"]);
        assert_eq!(edge_state, vec![&json!(false), &json!(true)]);
    }

    #[tokio::test]
    async fn test_ordered_dedupe_temporal_properties() {
        let g = Graph::new();
        g.add_metadata([("name", "graph")]).unwrap();
        g.add_properties(1, [("state", "abc")]).unwrap();
        g.add_properties(2, [("state", "abc")]).unwrap();
        g.add_properties(3, [("state", "xyz")]).unwrap();
        g.add_properties(4, [("state", "abc")]).unwrap();
        g.add_edge(1, 1, 2, [("status", "open")], None).unwrap();
        g.add_edge(2, 1, 2, [("status", "open")], None).unwrap();
        g.add_edge(3, 1, 2, [("status", "review")], None).unwrap();
        g.add_edge(4, 1, 2, [("status", "open")], None).unwrap();
        g.add_edge(5, 1, 2, [("status", "in-progress")], None)
            .unwrap();
        g.add_edge(10, 1, 2, [("status", "in-progress")], None)
            .unwrap();
        g.add_edge(9, 1, 2, [("state", true)], None).unwrap();
        g.add_edge(10, 1, 2, [("state", false)], None).unwrap();
        g.add_edge(6, 1, 2, NO_PROPS, None).unwrap();
        g.add_node(11, 3, [("name", "phone")], None, None).unwrap();
        g.add_node(12, 3, [("name", "fax")], None, None).unwrap();
        g.add_node(13, 3, [("name", "fax")], None, None).unwrap();

        let g = g.into();
        let graphs = HashMap::from([("graph".to_string(), g)]);
        let tmp_dir = tempdir().unwrap();
        let data = Data::new(tmp_dir.path(), &AppConfig::default(), Config::default());
        save_graphs_to_work_dir(&data, &graphs).await.unwrap();

        let schema = App::create_schema().data(data).finish().unwrap();

        let prop_has_key_filter = r#"
        {
          graph(path: "graph") {
            properties {
              temporal {
                values {
                  od1: orderedDedupe(latestTime: true) {
                    time {
                      timestamp eventId
                    }
                    value
                  },
                  od2: orderedDedupe(latestTime: false) {
                    time {
                      timestamp eventId
                    }
                    value
                  }
                }
              }
            }
            node(name: "3") {
              properties {
                temporal {
                  values {
                    od1: orderedDedupe(latestTime: true) {
                      time {
                        timestamp eventId
                      }
                      value
                    },
                    od2: orderedDedupe(latestTime: false) {
                      time {
                        timestamp eventId
                      }
                      value
                    }
                  }
                }
              }
            }
            edge(
              src: "1",
              dst: "2"
            ) {
              properties{
                temporal{
                  values{
                    od1: orderedDedupe(latestTime: true) {
                      time {
                        timestamp eventId
                      }
                      value
                    },
                    od2: orderedDedupe(latestTime: false) {
                      time {
                        timestamp eventId
                      }
                      value
                    }
                  }
                }
              }
            }
          }
        }
        "#;

        let req = Request::new(prop_has_key_filter);
        let res = schema.execute(req).await;
        let actual_data = res.data.into_json().unwrap();
        let expected = json!({
            "graph": {
              "properties": {
                "temporal": {
                  "values": [
                    {
                      "od1": [
                        {
                          "time": {
                            "timestamp": 2,
                            "eventId": 1
                          },
                          "value": "abc"
                        },
                        {
                          "time": {
                            "timestamp": 3,
                            "eventId": 2
                          },
                          "value": "xyz"
                        },
                        {
                          "time": {
                            "timestamp": 4,
                            "eventId": 3
                          },
                          "value": "abc"
                        }
                      ],
                      "od2": [
                        {
                          "time": {
                            "timestamp": 1,
                            "eventId": 0
                          },
                          "value": "abc"
                        },
                        {
                          "time": {
                            "timestamp": 3,
                            "eventId": 2
                          },
                          "value": "xyz"
                        },
                        {
                          "time": {
                            "timestamp": 4,
                            "eventId": 3
                          },
                          "value": "abc"
                        }
                      ]
                    }
                  ]
                }
              },
              "node": {
                "properties": {
                  "temporal": {
                    "values": [
                      {
                        "od1": [
                          {
                            "time": {
                              "timestamp": 11,
                              "eventId": 13
                            },
                            "value": "phone"
                          },
                          {
                            "time": {
                              "timestamp": 13,
                              "eventId": 15
                            },
                            "value": "fax"
                          }
                        ],
                        "od2": [
                          {
                            "time": {
                              "timestamp": 11,
                              "eventId": 13
                            },
                            "value": "phone"
                          },
                          {
                            "time": {
                              "timestamp": 12,
                              "eventId": 14
                            },
                            "value": "fax"
                          }
                        ]
                      }
                    ]
                  }
                }
              },
              "edge": {
                "properties": {
                  "temporal": {
                    "values": [
                      {
                        "od1": [
                          {
                            "time": {
                              "timestamp": 2,
                              "eventId": 5
                            },
                            "value": "open"
                          },
                          {
                            "time": {
                              "timestamp": 3,
                              "eventId": 6
                            },
                            "value": "review"
                          },
                          {
                            "time": {
                              "timestamp": 4,
                              "eventId": 7
                            },
                            "value": "open"
                          },
                          {
                            "time": {
                              "timestamp": 10,
                              "eventId": 9
                            },
                            "value": "in-progress"
                          }
                        ],
                        "od2": [
                          {
                            "time": {
                              "timestamp": 1,
                              "eventId": 4
                            },
                            "value": "open"
                          },
                          {
                            "time": {
                              "timestamp": 3,
                              "eventId": 6
                            },
                            "value": "review"
                          },
                          {
                            "time": {
                              "timestamp": 4,
                              "eventId": 7
                            },
                            "value": "open"
                          },
                          {
                            "time": {
                              "timestamp": 5,
                              "eventId": 8
                            },
                            "value": "in-progress"
                          }
                        ]
                      },
                      {
                        "od1": [
                          {
                            "time": {
                              "timestamp": 9,
                              "eventId": 10
                            },
                            "value": true
                          },
                          {
                            "time": {
                              "timestamp": 10,
                              "eventId": 11
                            },
                            "value": false
                          }
                        ],
                        "od2": [
                          {
                            "time": {
                              "timestamp": 9,
                              "eventId": 10
                            },
                            "value": true
                          },
                          {
                            "time": {
                              "timestamp": 10,
                              "eventId": 11
                            },
                            "value": false
                          }
                        ]
                      }
                    ]
                  }
                }
              }
            }
        });

        assert_eq!(
            actual_data["graph"]["properties"]["temporal"]["values"][0]["od1"],
            expected["graph"]["properties"]["temporal"]["values"][0]["od1"]
        );

        assert_eq!(
            actual_data["graph"]["properties"]["temporal"]["values"][0]["od2"],
            expected["graph"]["properties"]["temporal"]["values"][0]["od2"]
        );

        assert_eq!(
            actual_data["graph"]["node"]["properties"]["temporal"]["values"][0]["od1"],
            expected["graph"]["node"]["properties"]["temporal"]["values"][0]["od1"]
        );

        assert_eq!(
            actual_data["graph"]["node"]["properties"]["temporal"]["values"][0]["od2"],
            expected["graph"]["node"]["properties"]["temporal"]["values"][0]["od2"]
        );

        assert_eq!(
            actual_data["graph"]["edge"]["properties"]["temporal"]["values"][0]["od1"],
            expected["graph"]["edge"]["properties"]["temporal"]["values"][0]["od1"]
        );

        assert_eq!(
            actual_data["graph"]["edge"]["properties"]["temporal"]["values"][0]["od2"],
            expected["graph"]["edge"]["properties"]["temporal"]["values"][0]["od2"]
        );
    }

    #[tokio::test]
    async fn query_properties() {
        let graph = Graph::new();
        graph
            .add_node(0, 1, [("pgraph", Prop::I32(0))], None, None)
            .unwrap();

        let graph = graph.into();
        let graphs = HashMap::from([("graph".to_string(), graph)]);
        let tmp_dir = tempdir().unwrap();
        let data = Data::new(tmp_dir.path(), &AppConfig::default(), Config::default());
        save_graphs_to_work_dir(&data, &graphs).await.unwrap();

        let schema = App::create_schema().data(data).finish().unwrap();
        let prop_has_key_filter = r#"
        {
          graph(path: "graph") {
            nodes{
              list {
                name
                properties{
                    contains(key:"pgraph")
                }
              }
            }
          }
        }
        "#;

        let req = Request::new(prop_has_key_filter);
        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();
        assert_eq!(
            data,
            json!({
                "graph": {
                    "nodes": {
                        "list": [
                            { "name": "1",
                              "properties":{
                                "contains":true
                            }},
                        ]
                    }
                }
            }),
        );
    }

    #[tokio::test]
    async fn test_graph_injection() {
        let g = PersistentGraph::new();
        g.add_node(0, 1, NO_PROPS, None, None).unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let zip_path = tmp_dir.path().join("graph.zip");
        g.encode(GraphFolder::new_as_zip(&zip_path)).unwrap();
        let file = fs::File::open(&zip_path).unwrap();
        let upload_val = UploadValue {
            filename: "test".into(),
            content_type: Some("application/octet-stream".into()),
            content: file,
        };

        let tmp_dir = tempdir().unwrap();
        let data = Data::new(tmp_dir.path(), &AppConfig::default(), Config::default());
        let schema = App::create_schema().data(data).finish().unwrap();

        let query = r##"
        mutation($file: Upload!, $overwrite: Boolean!) {
            uploadGraph(path: "test", graph: $file, overwrite: $overwrite)
        }
        "##;

        let variables = json!({ "file": null, "overwrite": false });
        let mut req = Request::new(query)
            .variables(Variables::from_json(variables))
            .data(Access::Rw);
        req.set_upload("variables.file", upload_val);
        let res = schema.execute(req).await;
        assert_eq!(res.errors, vec![]);
        let res_json = res.data.into_json().unwrap();
        assert_eq!(res_json, json!({"uploadGraph": "test"}));

        let list_nodes = r#"
        query {
            graph(path: "test") {
                nodes {
                  list {
                    id
                  }
                }
            }
        }
        "#;

        let req = Request::new(list_nodes);
        let res = schema.execute(req).await;
        assert_eq!(res.errors, []);
        let res_json = res.data.into_json().unwrap();
        assert_eq!(res_json, json!({"graph": {"nodes": {"list": [{"id": 1}]}}}));
    }

    #[tokio::test]
    async fn test_graph_send_receive_base64() {
        let g = PersistentGraph::new();
        g.add_node(0, 1, NO_PROPS, None, None).unwrap();

        let graph_str = url_encode_graph(g.clone()).unwrap();

        let tmp_dir = tempdir().unwrap();
        let data = Data::new(tmp_dir.path(), &AppConfig::default(), Config::default());
        let schema = App::create_schema().data(data).finish().unwrap();

        let query = r#"
        mutation($graph: String!, $overwrite: Boolean!) {
            sendGraph(path: "test", graph: $graph, overwrite: $overwrite)
        }
        "#;
        let req = Request::new(query)
            .variables(Variables::from_json(
                json!({ "graph": graph_str, "overwrite": false }),
            ))
            .data(Access::Rw);

        let res = schema.execute(req).await;
        assert_eq!(res.errors, []);
        let res_json = res.data.into_json().unwrap();
        assert_eq!(res_json, json!({"sendGraph": "test"}));

        let list_nodes = r#"
        query {
            graph(path: "test") {
                nodes {
                  list {
                    id
                  }
                }
            }
        }
        "#;

        let req = Request::new(list_nodes);
        let res = schema.execute(req).await;
        assert_eq!(res.errors.len(), 0);
        let res_json = res.data.into_json().unwrap();
        assert_eq!(res_json, json!({"graph": {"nodes": {"list": [{"id": 1}]}}}));

        let receive_graph = r#"
        query {
            receiveGraph(path: "test")
        }
        "#;

        let req = Request::new(receive_graph);
        let res = schema.execute(req).await;
        assert_eq!(res.errors.len(), 0);
        let res_json = res.data.into_json().unwrap();
        let graph_encoded = res_json.get("receiveGraph").unwrap().as_str().unwrap();
        let temp_dir = tempdir().unwrap();
        let graph_roundtrip =
            url_decode_graph_at(graph_encoded, temp_dir.path(), Config::default())
                .unwrap()
                .into_dynamic();
        assert_eq!(g, graph_roundtrip);
    }

    #[tokio::test]
    async fn test_type_filter() {
        let graph = Graph::new();
        graph.add_metadata([("name", "graph")]).unwrap();
        graph.add_node(1, 1, NO_PROPS, Some("a"), None).unwrap();
        graph.add_node(1, 2, NO_PROPS, Some("b"), None).unwrap();
        graph.add_node(1, 3, NO_PROPS, Some("b"), None).unwrap();
        graph.add_node(1, 4, NO_PROPS, Some("a"), None).unwrap();
        graph.add_node(1, 5, NO_PROPS, Some("c"), None).unwrap();
        graph.add_node(1, 6, NO_PROPS, Some("e"), None).unwrap();
        graph.add_edge(2, 1, 2, NO_PROPS, Some("a")).unwrap();
        graph.add_edge(2, 3, 2, NO_PROPS, Some("a")).unwrap();
        graph.add_edge(2, 2, 4, NO_PROPS, Some("a")).unwrap();
        graph.add_edge(2, 4, 5, NO_PROPS, Some("a")).unwrap();
        graph.add_edge(2, 4, 5, NO_PROPS, Some("a")).unwrap();
        graph.add_edge(2, 5, 6, NO_PROPS, Some("a")).unwrap();
        graph.add_edge(2, 3, 6, NO_PROPS, Some("a")).unwrap();

        let graph = graph.into();
        let graphs = HashMap::from([("graph".to_string(), graph)]);
        let tmp_dir = tempdir().unwrap();
        let data = Data::new(tmp_dir.path(), &AppConfig::default(), Config::default());
        save_graphs_to_work_dir(&data, &graphs).await.unwrap();

        let schema = App::create_schema().data(data).finish().unwrap();

        let req = r#"
        {
          graph(path: "graph") {
            nodes {
              typeFilter(nodeTypes: ["a"]) {
                list {
                  name
                }
              }
            }
          }
        }
        "#;

        let req = Request::new(req);
        let res = schema.execute(req).await;
        let data = json_sort_by_name(res.data.into_json().unwrap());
        assert_eq!(
            data,
            json!({
                "graph": {
                  "nodes": {
                    "typeFilter": {
                      "list": [
                        {
                          "name": "1"
                        },
                        {
                          "name": "4"
                        }
                      ]
                    }
                  }
                }
            }),
        );

        let req = r#"
        {
          graph(path: "graph") {
            nodes {
              typeFilter(nodeTypes: ["a"]) {
                list {
                  name
                  neighbours {
                    list {
                      name
                    }
                  }
                }
              }
            }
          }
        }
        "#;

        let req = Request::new(req);
        let res = schema.execute(req).await;
        let data = json_sort_by_name(res.data.into_json().unwrap());
        assert_eq!(
            data,
            json!({
                "graph": {
                  "nodes": {
                    "typeFilter": {
                      "list": [
                        {
                            "name": "1",
                            "neighbours": {
                            "list": [
                              {
                                "name": "2"
                              }
                            ]
                          }
                        },
                        {
                            "name": "4",
                            "neighbours": {
                            "list": [
                              {
                                "name": "2"
                              },
                              {
                                "name": "5"
                              }
                            ]
                          }
                        }
                      ]
                    }
                  }
                }
            }),
        );
    }

    #[tokio::test]
    async fn test_paging() {
        let graph1 = Graph::new();
        graph1.add_metadata([("name", "graph1")]).unwrap();
        graph1.add_node(1, 1, NO_PROPS, Some("a"), None).unwrap();
        graph1.add_node(1, 2, NO_PROPS, Some("b"), None).unwrap();
        graph1.add_node(1, 3, NO_PROPS, Some("b"), None).unwrap();
        graph1.add_node(1, 4, NO_PROPS, Some("a"), None).unwrap();
        graph1.add_node(1, 5, NO_PROPS, Some("c"), None).unwrap();
        graph1.add_node(1, 6, NO_PROPS, Some("e"), None).unwrap();
        graph1.add_edge(2, 1, 2, NO_PROPS, Some("a")).unwrap();
        graph1.add_edge(2, 3, 2, NO_PROPS, Some("a")).unwrap();
        graph1.add_edge(2, 2, 4, NO_PROPS, Some("a")).unwrap();
        graph1.add_edge(2, 4, 5, NO_PROPS, Some("a")).unwrap();
        graph1.add_edge(2, 4, 6, NO_PROPS, Some("a")).unwrap();
        graph1.add_edge(2, 5, 6, NO_PROPS, Some("a")).unwrap();
        graph1.add_edge(2, 3, 6, NO_PROPS, Some("a")).unwrap();

        let all_nodes: Vec<_> = graph1.nodes().name().into_iter_values().collect();

        // make sure we have the correct nodes
        assert_eq!(
            all_nodes.iter().sorted().collect_vec(),
            ["1", "2", "3", "4", "5", "6"]
        );
        let all_edges: Vec<_> = graph1
            .edges()
            .id()
            .map(|(src, dst)| {
                let src = match src {
                    GID::U64(u) => u,
                    GID::Str(_) => unreachable!("integer-indexed graph"),
                };
                let dst = match dst {
                    GID::U64(u) => u,
                    GID::Str(_) => unreachable!("integer-indexed graph"),
                };
                (src, dst)
            })
            .collect();

        // make sure we have the correct edges
        assert_eq!(
            all_edges.iter().cloned().sorted().collect_vec(),
            [(1, 2), (2, 4), (3, 2), (3, 6), (4, 5), (4, 6), (5, 6),]
        );
        let graph2 = Graph::new();
        graph2.add_metadata([("name", "graph2")]).unwrap();
        graph2.add_node(1, 1, NO_PROPS, Some("a"), None).unwrap();
        let graph3 = Graph::new();
        graph3.add_metadata([("name", "graph3")]).unwrap();
        graph3.add_node(1, 1, NO_PROPS, Some("a"), None).unwrap();
        let graph4 = Graph::new();
        graph4.add_metadata([("name", "graph4")]).unwrap();
        graph4.add_node(1, 1, NO_PROPS, Some("a"), None).unwrap();
        let graph5 = Graph::new();
        graph5.add_metadata([("name", "graph5")]).unwrap();
        graph5.add_node(1, 1, NO_PROPS, Some("a"), None).unwrap();
        let graph6 = Graph::new();
        graph6.add_metadata([("name", "graph6")]).unwrap();
        graph6.add_node(1, 1, NO_PROPS, Some("a"), None).unwrap();

        let graphs = HashMap::from([
            ("graph1".to_string(), graph1.into()),
            ("graph2".to_string(), graph2.into()),
            ("graph3".to_string(), graph3.into()),
            ("graph4".to_string(), graph4.into()),
            ("graph5".to_string(), graph5.into()),
            ("graph6".to_string(), graph6.into()),
        ]);
        let tmp_dir = tempdir().unwrap();
        let data = Data::new(tmp_dir.path(), &AppConfig::default(), Config::default());
        save_graphs_to_work_dir(&data, &graphs).await.unwrap();
        let schema = App::create_schema().data(data).finish().unwrap();

        let all = r#"{
            graph(path: "graph1") {
                nodes {
                    list {
                        name
                    }
                }
                edges {
                    list {
                        id
                    }
                }
            }
        }"#;

        let res = schema.execute(Request::new(all)).await;
        let data = res.data.into_json().unwrap();

        let all_nodes: Vec<_> = data
            .get("graph")
            .unwrap()
            .get("nodes")
            .unwrap()
            .get("list")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v.get("name").unwrap().as_str())
            .collect();

        let all_edges: Vec<(_, _)> = data
            .get("graph")
            .unwrap()
            .get("edges")
            .unwrap()
            .get("list")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v.get("id").unwrap().as_array())
            .filter_map(|ids| ids.iter().filter_map(|v| v.as_u64()).collect_tuple())
            .collect();

        // make sure we have the correct edges
        assert_eq!(
            all_edges.iter().cloned().sorted().collect_vec(),
            [(1, 2), (2, 4), (3, 2), (3, 6), (4, 5), (4, 6), (5, 6),]
        );

        // make sure we have the correct nodes
        assert_eq!(
            all_nodes.iter().copied().sorted().collect_vec(),
            ["1", "2", "3", "4", "5", "6"]
        );

        let req = r#"
        {
            graph(path: "graph1") {
                nodes {
                    page(limit: 3, offset: 1) {
                        name
                    }
                }
            }
        }
        "#;

        let req = Request::new(req);
        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();
        let expected_page: Vec<_> = all_nodes[1..4]
            .iter()
            .map(|node| json!({"name": node}))
            .collect();
        assert_eq!(
            data,
            json!({
                "graph": {
                    "nodes": {
                        "page": expected_page
                    }
                }
            }),
        );

        let req = r#"
        {
            graph(path: "graph1") {
                nodes {
                    page(limit: 3, offset: 999) {
                        name
                    }
                }
            }
        }
        "#;

        let req = Request::new(req);
        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();
        assert_eq!(
            data,
            json!({
                "graph": {
                    "nodes": {
                        "page": []
                    }
                }
            }),
        );

        let req = r#"
        {
            graph(path: "graph1") {
                nodes {
                    page(limit: 2, pageIndex: 1) {
                        name
                    }
                }
            }
        }
        "#;

        let req = Request::new(req);
        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();
        let expected_page: Vec<_> = all_nodes[2..4]
            .iter()
            .map(|node| json!({"name": node}))
            .collect();
        assert_eq!(
            data,
            json!({
                "graph": {
                    "nodes": {
                        "page": expected_page
                    }
                }
            }),
        );

        let req = r#"
        {
            graph(path: "graph1") {
                edges {
                    page(limit: 2, pageIndex: 1, offset: 3) {
                        id
                    }
                }
            }
        }
        "#;

        let req = Request::new(req);
        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();
        let expected_page: Vec<_> = all_edges[5..7]
            .iter()
            .map(|edge| json!({"id": edge}))
            .collect();
        assert_eq!(
            data,
            json!({
                "graph": {
                    "edges": {
                        "page": expected_page
                    }
                }
            }),
        );

        let req = r#"
        {
            graph(path: "graph1") {
                edges {
                    page(limit: 3, pageIndex: 2) {
                        id
                    }
                }
            }
        }
        "#;

        let req = Request::new(req);
        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();
        let expected_page: Vec<_> = all_edges[6..]
            .iter()
            .map(|edge| json!({"id": edge}))
            .collect();
        assert_eq!(
            data,
            json!({
                "graph": {
                    "edges": {
                        "page": expected_page
                    }
                }
            }),
        );

        let req = r#"
        {
            root {
                graphs {
                    page(limit: 4, offset: 3) {
                        name
                    }
                }
            }
        }
        "#;

        let req = Request::new(req);
        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();
        assert_eq!(
            data,
            json!({
                "root": {
                    "graphs": {
                        "page": [
                            {
                                "name": "graph4"
                            },
                            {
                                "name": "graph5"
                            },
                            {
                                "name": "graph6"
                            }
                        ]
                    }
                }
            }),
        );
    }

    #[tokio::test]
    async fn test_query_namespace() {
        let graph = Graph::new();
        graph.add_metadata([("name", "graph")]).unwrap();
        graph.add_node(1, 1, NO_PROPS, Some("a"), None).unwrap();
        graph.add_node(1, 2, NO_PROPS, Some("b"), None).unwrap();
        graph.add_node(1, 3, NO_PROPS, Some("b"), None).unwrap();
        graph.add_node(1, 4, NO_PROPS, Some("a"), None).unwrap();
        graph.add_node(1, 5, NO_PROPS, Some("c"), None).unwrap();
        graph.add_node(1, 6, NO_PROPS, Some("e"), None).unwrap();
        graph.add_edge(2, 1, 2, NO_PROPS, Some("a")).unwrap();
        graph.add_edge(2, 3, 2, NO_PROPS, Some("a")).unwrap();
        graph.add_edge(2, 2, 4, NO_PROPS, Some("a")).unwrap();
        graph.add_edge(2, 4, 5, NO_PROPS, Some("a")).unwrap();
        graph.add_edge(2, 4, 5, NO_PROPS, Some("a")).unwrap();
        graph.add_edge(2, 5, 6, NO_PROPS, Some("a")).unwrap();
        graph.add_edge(2, 3, 6, NO_PROPS, Some("a")).unwrap();

        let graph = graph.into();
        let graphs = HashMap::from([("graph".to_string(), graph)]);
        let tmp_dir = tempdir().unwrap();
        let data = Data::new(tmp_dir.path(), &AppConfig::default(), Config::default());
        save_graphs_to_work_dir(&data, &graphs).await.unwrap();
        let schema = App::create_schema().data(data).finish().unwrap();

        let req = r#"
        {
  namespace(path: "") {
    path
    graphs {
      list {
        path
        name
        nodeCount
        edgeCount
        metadata {
          key
          value
        }
      }
    }
    children {
      list {
        path
      }
    }
    parent {
      path
    }
  }
}
"#;

        let req = Request::new(req);
        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();
        assert_eq!(res.errors, vec![]);
        assert_eq!(
            data,
            json!({
                "namespace": {
                    "path": "",
                    "graphs": {"list":[
                        {
                            "path": "graph",
                            "name": "graph",
                            "nodeCount": 6,
                            "edgeCount": 6,
                            "metadata": [
                                {
                                    "key": "name",
                                    "value": "graph"
                                },
                            ]
                        },
                    ]},
                    "children":{"list":[]},
                    "parent": null
                },
            }),
        );

        let req = r#"
        mutation CreateGraph2 {
          createSubgraph(parentPath: "graph", newPath: "graph2", nodes: ["1", "2"], overwrite: false)
        }
        "#;
        let req = Request::new(req).data(Access::Rw);
        let res = schema.execute(req).await;
        assert_eq!(res.errors, vec![]);
        let req = r#"
        mutation CreateNamespace1Graph3 {
          createSubgraph(parentPath: "graph", newPath: "namespace1/graph3", nodes: ["2", "3", "4"], overwrite: false)
        }
        "#;
        let req = Request::new(req).data(Access::Rw);
        let res = schema.execute(req).await;
        assert_eq!(res.errors, vec![]);

        let req = r#"
        {
  namespace(path: "") {
    path
    graphs {
      list {
        path
        name
          nodeCount
          edgeCount
          metadata {
            key
            value
          }
      }
    }
    children {
      list {
        path
      }
    }
    parent {
      path
    }
    items {
      list {
        __typename
        ... on Namespace {
          path
        }
        ... on MetaGraph {
          path
        }
      }
      page(limit: 2, offset: 1) {
        __typename
        ... on Namespace {
          path
        }
        ... on MetaGraph {
          path
        }
      }
    }
  }
}
        "#;

        let req = Request::new(req);
        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();
        assert_eq!(res.errors, vec![]);
        assert_eq!(
            data,
            json!({
                "namespace": {
                    "path": "",
                    "graphs": {"list":[
                        {
                            "path": "graph",
                            "name": "graph",
                            "nodeCount": 6,
                            "edgeCount": 6,
                            "metadata": [
                                {
                                    "key": "name",
                                    "value": "graph"
                                },
                            ]
                        },
                        {
                            "path": "graph2",
                            "name": "graph2",
                            "nodeCount": 2,
                            "edgeCount": 1,
                            "metadata": [
                                {
                                    "key": "name",
                                    "value": "graph"
                                },
                            ]
                        },
                    ]},
                    "children": {
                        "list": [
                            {
                                "path": "namespace1"
                            }
                        ]
                    },
                    "parent": null,
                    "items": {
                        "list": [
                            {
                                "__typename": "Namespace",
                                "path": "namespace1",
                            },
                            {
                                "__typename": "MetaGraph",
                                "path": "graph",
                            },
                            {
                                "__typename": "MetaGraph",
                                "path": "graph2",
                            }
                        ],
                        "page": [
                            {
                                "__typename": "MetaGraph",
                                "path": "graph",
                            },
                            {
                                "__typename": "MetaGraph",
                                "path": "graph2",
                            }
                        ]
                    }
                },
            }),
        );

        let req = r#"
        {
          namespace(path: "namespace1") {
            graphs {
              list {
                path
              }
            }
            parent {
              path
            }
            items {
              list {
                __typename
                ... on Namespace {
                  path
                }
                ... on MetaGraph {
                  path
                }
              }
            }
          }
        }
        "#;

        let req = Request::new(req);
        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();
        assert_eq!(
            data,
            json!({
                "namespace": {
                    "graphs": {
                        "list": [
                            {
                                "path": "namespace1/graph3",
                            },
                        ]
                    },
                    "parent": {
                        "path": ""
                    },
                    "items": {
                        "list": [
                            {
                                "__typename": "MetaGraph",
                                "path": "namespace1/graph3",
                            },
                        ],
                    }
                },
            }),
        );
    }

    async fn test_new_graph(schema: &Schema, path: &str, should_work: bool) {
        let req = Request::new(format!(
            r#"mutation {{ newGraph(path: "{path}", graphType: EVENT) }}"#,
        ))
        .data(Access::Rw);
        let res = schema.execute(req).await;

        if should_work {
            assert_eq!(res.errors, vec![], "expected no errors for path: {path}");
            assert_eq!(
                res.data.into_json().unwrap(),
                json!({"newGraph": true}),
                "expected newGraph to return true for path: {path}",
            );
        } else {
            assert!(!res.errors.is_empty(), "expected errors for path: {path}",);
        }
    }

    async fn assert_namespace_graphs(
        schema: &Schema,
        namespace_path: &str,
        expected_graphs: Vec<&str>,
        expected_children: Vec<&str>,
    ) {
        let req = Request::new(format!(
            r#"
            {{
              namespace(path: "{namespace_path}") {{
                graphs {{
                  list {{
                    path
                  }}
                }}
                children {{
                  list {{
                    path
                  }}
                }}
              }}
            }}
            "#,
        ));
        let res = schema.execute(req).await;
        let into_paths = |v: Vec<&str>| v.iter().map(|p| json!({ "path": *p })).collect::<Vec<_>>();
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "namespace": {
                    "graphs": { "list": into_paths(expected_graphs) },
                    "children": { "list": into_paths(expected_children) },
                }
            }),
        );
    }

    #[tokio::test]
    async fn test_new_graph_rejects_hidden_path_components() {
        let tmp_dir = tempdir().unwrap();
        let data = Data::new(tmp_dir.path(), &AppConfig::default(), Config::default());
        let schema = App::create_schema().data(data).finish().unwrap();

        // Valid paths
        let should_work = true;
        test_new_graph(&schema, "valid_graph-1", should_work).await;
        test_new_graph(&schema, "some.graph", should_work).await;
        test_new_graph(&schema, "some-namespace/graph", should_work).await;

        // Hidden paths should be rejected
        let should_work = false;
        test_new_graph(&schema, ".graph", should_work).await;
        test_new_graph(&schema, "some-namespace/.some-hidden/graph", should_work).await;
        test_new_graph(&schema, "..hidden", should_work).await;

        assert_namespace_graphs(
            &schema,
            "",
            vec!["some.graph", "valid_graph-1"],
            vec!["some-namespace"],
        )
        .await;
        assert_namespace_graphs(
            &schema,
            "some-namespace",
            vec!["some-namespace/graph"],
            vec![],
        )
        .await;
    }

    #[tokio::test]
    async fn test_node_types() {
        // Ensure node types are returned correctly by the server.
        let node_types = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon"];
        let graph = Graph::new();

        for (node_id, node_type) in node_types.iter().enumerate() {
            graph
                .add_node(0, node_id as u64, NO_PROPS, Some(node_type), None)
                .expect("add_node");
        }

        let tmp_dir = tempdir().unwrap();
        let graph_name = "graph_with_node_types";
        let graphs = HashMap::from([(graph_name.to_string(), graph.into())]);
        let data = Data::new(tmp_dir.path(), &AppConfig::default(), Config::default());

        save_graphs_to_work_dir(&data, &graphs).await.unwrap();

        // Drop and reload data to mimic server restart.
        drop(data);

        let data = Data::new(tmp_dir.path(), &AppConfig::default(), Config::default());
        let schema = App::create_schema().data(data).finish().unwrap();

        let query = format!(
            r#"
        query {{
          graph(path: "{graph_name}", graphType: EVENT) {{
            nodes {{
              list {{
                nodeType
              }}
            }}
          }}
        }}
      "#
        );

        let res = schema.execute(Request::new(query).data(Access::Rw)).await;

        assert_eq!(res.errors, vec![], "{:?}", res.errors);

        let gql_data = res.data.into_json().unwrap();

        let list = gql_data
            .get("graph")
            .and_then(|g| g.get("nodes"))
            .and_then(|n| n.get("list"))
            .unwrap();

        let Value::Array(nodes) = list else {
            panic!("graph.nodes.list should be an array, got {list:?}");
        };

        assert_eq!(nodes.len(), 5, "expected 5 nodes, got {:?}", nodes.len());

        let retrieved: HashSet<String> = nodes
            .iter()
            .map(|node| {
                node.get("nodeType")
                    .and_then(|v| v.as_str())
                    .unwrap_or_else(|| panic!("nodeType missing or not a string: {node:?}"))
                    .to_owned()
            })
            .collect();

        let expected: HashSet<String> = node_types.iter().map(|s| (*s).to_string()).collect();

        assert_eq!(
            retrieved, expected,
            "node types returned by GraphQL should match those set on ingest"
        );
    }

    #[derive(PartialEq, Eq, PartialOrd, Ord)]
    pub enum NameSortKey<'a> {
        Node(&'a str),
        Edge(&'a str, &'a str),
    }

    #[tokio::test]
    async fn test_create_namespace_at_root() {
        let work_dir = TempDir::new().unwrap();
        {
            let setup = setup_with_graphs(&[], work_dir.path()).await;

            let res = run_mutation(
                &setup.schema,
                r#"mutation { createNamespace(path: "foo") }"#,
            )
            .await;

            assert_eq!(res.errors, vec![]);
            assert_eq!(
                res.data.into_json().unwrap(),
                json!({ "createNamespace": "foo" }),
            );

            let foo = work_dir.path().join("foo");
            assert_is_namespace_dir(&foo);

            let req = Request::new(
                r#"{ namespace(path: "") { items { list { __typename ... on Namespace { path } ... on MetaGraph { path } } } } }"#,
            );
            let res = setup.schema.execute(req).await;
            assert_eq!(res.errors, vec![]);
            assert_eq!(
                res.data.into_json().unwrap(),
                json!({
                    "namespace": {
                        "items": {
                            "list": [
                                { "__typename": "Namespace", "path": "foo" }
                            ]
                        }
                    }
                }),
            );
        }
    }

    #[tokio::test]
    async fn test_create_namespace_nested() {
        let work_dir = TempDir::new().unwrap();
        {
            let setup = setup_with_graphs(&[], work_dir.path()).await;

            let res = run_mutation(
                &setup.schema,
                r#"mutation { createNamespace(path: "a/b/c") }"#,
            )
            .await;
            assert_eq!(res.errors, vec![]);
            assert_eq!(
                res.data.into_json().unwrap(),
                json!({ "createNamespace": "a/b/c" }),
            );

            for rel in ["a", "a/b", "a/b/c"] {
                let p = work_dir.path().join(rel);
                assert_is_namespace_dir(&p);
            }

            let req = Request::new(r#"{ namespace(path: "a/b") { children { list { path } } } }"#);
            let res = setup.schema.execute(req).await;
            assert_eq!(res.errors, vec![]);
            assert_eq!(
                res.data.into_json().unwrap(),
                json!({
                    "namespace": {
                        "children": { "list": [ { "path": "a/b/c" } ] }
                    }
                }),
            );
        }
    }

    #[tokio::test]
    async fn test_create_namespace_rejects_existing_graph() {
        let work_dir = TempDir::new().unwrap();
        {
            let g = Graph::new();
            g.add_node(0, 1, NO_PROPS, None, None).unwrap();
            let g: MaterializedGraph = g.into();
            let setup = setup_with_graphs(&[("g", g)], work_dir.path()).await;

            let res =
                run_mutation(&setup.schema, r#"mutation { createNamespace(path: "g") }"#).await;
            assert!(!res.errors.is_empty(), "expected error, got {:?}", res);

            assert!(setup.data.get_graph_for_test("g").await.is_ok());
        }
    }

    #[tokio::test]
    async fn test_create_namespace_rejects_existing_namespace() {
        let work_dir = TempDir::new().unwrap();
        {
            let setup = setup_with_graphs(&[], work_dir.path()).await;

            let res =
                run_mutation(&setup.schema, r#"mutation { createNamespace(path: "ns") }"#).await;
            assert_eq!(res.errors, vec![]);

            let res =
                run_mutation(&setup.schema, r#"mutation { createNamespace(path: "ns") }"#).await;
            assert!(!res.errors.is_empty(), "expected error, got {:?}", res);
            assert!(
                res.errors[0].message.contains("Namespace"),
                "unexpected error message: {}",
                res.errors[0].message,
            );
        }
    }

    #[tokio::test]
    async fn test_create_namespace_rejects_invalid_paths() {
        let work_dir = TempDir::new().unwrap();
        {
            let setup = setup_with_graphs(&[], work_dir.path()).await;

            let cases = ["", ".hidden/x", "x/.hidden", "../escape", "a//b"];

            let snapshot_before = fs::read_dir(work_dir.path())
                .unwrap()
                .map(|e| e.unwrap().file_name())
                .collect::<HashSet<_>>();

            for path in cases {
                let query = format!(
                    r#"mutation {{ createNamespace(path: "{}") }}"#,
                    path.replace('"', r#"\""#),
                );
                let res = run_mutation(&setup.schema, &query).await;
                assert!(
                    !res.errors.is_empty(),
                    "expected error for path {:?}, got {:?}",
                    path,
                    res,
                );
            }

            let snapshot_after = fs::read_dir(work_dir.path())
                .unwrap()
                .map(|e| e.unwrap().file_name())
                .collect::<HashSet<_>>();
            assert_eq!(
                snapshot_before, snapshot_after,
                "work_dir contents changed after rejected creates",
            );
        }
    }

    #[tokio::test]
    async fn test_create_namespace_denied_without_parent_write() {
        let work_dir = TempDir::new().unwrap();
        {
            let policy =
                Arc::new(FakePolicy::default().with_namespace("", NamespacePermission::Read));
            let setup = setup_with_policy(&[], work_dir.path(), policy).await;

            let res = run_mutation_as_user(
                &setup.schema,
                r#"mutation { createNamespace(path: "foo") }"#,
            )
            .await;
            assert!(!res.errors.is_empty(), "expected error, got {:?}", res);
            assert!(
                res.errors[0]
                    .message
                    .contains("WRITE required on namespace"),
                "unexpected error message: {}",
                res.errors[0].message,
            );

            assert!(!work_dir.path().join("foo").exists());
        }
    }

    #[tokio::test]
    async fn test_delete_namespace_empty() {
        let work_dir = TempDir::new().unwrap();
        {
            let setup = setup_with_graphs(&[], work_dir.path()).await;

            let res =
                run_mutation(&setup.schema, r#"mutation { createNamespace(path: "ns") }"#).await;
            assert_eq!(res.errors, vec![]);
            assert!(work_dir.path().join("ns").is_dir());

            let res =
                run_mutation(&setup.schema, r#"mutation { deleteNamespace(path: "ns") }"#).await;
            assert_eq!(res.errors, vec![]);
            assert_eq!(
                res.data.into_json().unwrap(),
                json!({ "deleteNamespace": true }),
            );
            assert!(!work_dir.path().join("ns").exists());
            assert_is_namespace_dir(work_dir.path());
        }
    }

    #[tokio::test]
    async fn test_delete_namespace_with_children() {
        let work_dir = TempDir::new().unwrap();
        {
            let g1 = Graph::new();
            g1.add_node(0, 1, NO_PROPS, None, None).unwrap();
            let g1: MaterializedGraph = g1.into();
            let g2 = Graph::new();
            g2.add_node(0, 2, NO_PROPS, None, None).unwrap();
            let g2: MaterializedGraph = g2.into();
            let setup =
                setup_with_graphs(&[("ns/g1", g1), ("ns/sub/g2", g2)], work_dir.path()).await;

            let res = run_mutation(
                &setup.schema,
                r#"mutation { createNamespace(path: "ns/empty") }"#,
            )
            .await;
            assert_eq!(res.errors, vec![]);
            assert!(work_dir.path().join("ns/empty").is_dir());

            let res =
                run_mutation(&setup.schema, r#"mutation { deleteNamespace(path: "ns") }"#).await;
            assert_eq!(res.errors, vec![]);
            assert_eq!(
                res.data.into_json().unwrap(),
                json!({ "deleteNamespace": true }),
            );
            assert!(!work_dir.path().join("ns").exists());

            let req = Request::new(r#"{ namespace(path: "") { children { list { path } } } }"#);
            let res = setup.schema.execute(req).await;
            assert_eq!(res.errors, vec![]);
            assert_eq!(
                res.data.into_json().unwrap(),
                json!({ "namespace": { "children": { "list": [] } } }),
            );
        }
    }

    #[tokio::test]
    async fn test_delete_namespace_rejects_empty_path() {
        let work_dir = TempDir::new().unwrap();
        {
            let setup = setup_with_graphs(&[], work_dir.path()).await;

            let res =
                run_mutation(&setup.schema, r#"mutation { deleteNamespace(path: "") }"#).await;
            assert!(!res.errors.is_empty(), "expected error, got {:?}", res);
        }
    }

    #[tokio::test]
    async fn test_delete_namespace_rejects_nonexistent() {
        let work_dir = TempDir::new().unwrap();
        {
            let setup = setup_with_graphs(&[], work_dir.path()).await;

            let res = run_mutation(
                &setup.schema,
                r#"mutation { deleteNamespace(path: "noexist") }"#,
            )
            .await;
            assert!(!res.errors.is_empty(), "expected error, got {:?}", res);
        }
    }

    #[tokio::test]
    async fn test_delete_namespace_denied_when_descendant_unwritable() {
        let work_dir = TempDir::new().unwrap();
        {
            let g1 = Graph::new();
            g1.add_node(0, 1, NO_PROPS, None, None).unwrap();
            let g1: MaterializedGraph = g1.into();
            let g2 = Graph::new();
            g2.add_node(0, 2, NO_PROPS, None, None).unwrap();
            let g2: MaterializedGraph = g2.into();

            let policy = Arc::new(
                FakePolicy::default()
                    .with_namespace("", NamespacePermission::Write)
                    .with_namespace("ns", NamespacePermission::Write)
                    .with_graph("ns/g1", GraphPermission::Write)
                    .with_graph("ns/g2", GraphPermission::Read { filter: None }),
            );
            let setup =
                setup_with_policy(&[("ns/g1", g1), ("ns/g2", g2)], work_dir.path(), policy).await;

            let res =
                run_mutation_as_user(&setup.schema, r#"mutation { deleteNamespace(path: "ns") }"#)
                    .await;
            assert!(!res.errors.is_empty(), "expected error, got {:?}", res);
            // Substring is from `require_graph_write` in raphtory-graphql/src/model/mod.rs.
            assert!(
                res.errors[0]
                    .message
                    .contains("WRITE permission required for graph"),
                "unexpected error message: {}",
                res.errors[0].message,
            );

            assert!(work_dir.path().join("ns").is_dir());
            assert!(setup.data.get_graph_for_test("ns/g1").await.is_ok());
            assert!(setup.data.get_graph_for_test("ns/g2").await.is_ok());
        }
    }

    #[tokio::test]
    async fn test_delete_namespace_denied_without_parent_write() {
        let work_dir = TempDir::new().unwrap();
        {
            let g = Graph::new();
            g.add_node(0, 1, NO_PROPS, None, None).unwrap();
            let g: MaterializedGraph = g.into();

            let policy = Arc::new(
                FakePolicy::default()
                    .with_namespace("", NamespacePermission::Read)
                    .with_namespace("ns", NamespacePermission::Write)
                    .with_graph("ns/g", GraphPermission::Write),
            );
            let setup = setup_with_policy(&[("ns/g", g)], work_dir.path(), policy).await;

            let res =
                run_mutation_as_user(&setup.schema, r#"mutation { deleteNamespace(path: "ns") }"#)
                    .await;
            assert!(!res.errors.is_empty(), "expected error, got {:?}", res);
            assert!(
                res.errors[0]
                    .message
                    .contains("WRITE required on namespace"),
                "unexpected error message: {}",
                res.errors[0].message,
            );

            assert!(work_dir.path().join("ns").is_dir());
            assert!(setup.data.get_graph_for_test("ns/g").await.is_ok());
        }
    }

    #[tokio::test]
    async fn test_delete_namespace_denied_without_own_write() {
        let work_dir = TempDir::new().unwrap();
        {
            let g = Graph::new();
            g.add_node(0, 1, NO_PROPS, None, None).unwrap();
            let g: MaterializedGraph = g.into();

            let policy = Arc::new(
                FakePolicy::default()
                    .with_namespace("", NamespacePermission::Write)
                    .with_namespace("ns", NamespacePermission::Read)
                    .with_graph("ns/g", GraphPermission::Write),
            );
            let setup = setup_with_policy(&[("ns/g", g)], work_dir.path(), policy).await;

            let res =
                run_mutation_as_user(&setup.schema, r#"mutation { deleteNamespace(path: "ns") }"#)
                    .await;
            assert!(!res.errors.is_empty(), "expected error, got {:?}", res);
            assert!(
                res.errors[0]
                    .message
                    .contains("WRITE required on namespace"),
                "unexpected error message: {}",
                res.errors[0].message,
            );

            assert!(work_dir.path().join("ns").is_dir());
            assert!(setup.data.get_graph_for_test("ns/g").await.is_ok());
        }
    }

    #[tokio::test]
    async fn test_delete_namespace_denied_without_descendant_namespace_write() {
        let work_dir = TempDir::new().unwrap();
        {
            let g = Graph::new();
            g.add_node(0, 1, NO_PROPS, None, None).unwrap();
            let g: MaterializedGraph = g.into();

            let policy = Arc::new(
                FakePolicy::default()
                    .with_namespace("", NamespacePermission::Write)
                    .with_namespace("ns", NamespacePermission::Write)
                    .with_namespace("ns/sub", NamespacePermission::Read)
                    .with_graph("ns/sub/g", GraphPermission::Write),
            );
            let setup = setup_with_policy(&[("ns/sub/g", g)], work_dir.path(), policy).await;

            let res =
                run_mutation_as_user(&setup.schema, r#"mutation { deleteNamespace(path: "ns") }"#)
                    .await;
            assert!(!res.errors.is_empty(), "expected error, got {:?}", res);
            assert!(
                res.errors[0]
                    .message
                    .contains("WRITE required on namespace"),
                "unexpected error message: {}",
                res.errors[0].message,
            );

            assert!(work_dir.path().join("ns").is_dir());
            assert!(work_dir.path().join("ns/sub").is_dir());
            assert!(setup.data.get_graph_for_test("ns/sub/g").await.is_ok());
        }
    }

    #[tokio::test]
    async fn test_delete_namespace_invalidates_cache() {
        let work_dir = TempDir::new().unwrap();
        {
            let g = Graph::new();
            g.add_node(0, 1, NO_PROPS, None, None).unwrap();
            let g: MaterializedGraph = g.into();
            let setup = setup_with_graphs(&[("ns/g", g)], work_dir.path()).await;

            setup.data.get_graph_for_test("ns/g").await.unwrap();
            assert!(setup.data.get_cached_graph("ns/g").await.is_some());

            let res =
                run_mutation(&setup.schema, r#"mutation { deleteNamespace(path: "ns") }"#).await;
            assert_eq!(res.errors, vec![]);
            assert_eq!(
                res.data.into_json().unwrap(),
                json!({ "deleteNamespace": true }),
            );

            assert!(setup.data.get_cached_graph("ns/g").await.is_none());
        }
    }

    pub fn json_sort_by_name(value: Value) -> Value {
        match value {
            Value::Array(inner) => Value::Array(
                inner
                    .into_iter()
                    .sorted_by(|l, r| name_sort_key(l).cmp(&name_sort_key(r)))
                    .map(|inner_value| json_sort_by_name(inner_value))
                    .collect(),
            ),
            Value::Object(inner) => Value::Object(
                inner
                    .into_iter()
                    .map(|(key, value)| (key, json_sort_by_name(value)))
                    .collect(),
            ),
            value => value,
        }
    }

    fn name_sort_key(value: &Value) -> Option<NameSortKey<'_>> {
        match value {
            Value::Object(inner) => inner
                .get("name")
                .and_then(|name| Some(NameSortKey::Node(name.as_str()?)))
                .or_else(|| {
                    inner.get("id").and_then(|id| match id {
                        Value::String(node) => Some(NameSortKey::Node(node)),
                        Value::Array(edge) => {
                            let (src, dst) =
                                edge.iter().map(|e| e.as_str().unwrap()).next_tuple()?;
                            Some(NameSortKey::Edge(src, dst))
                        }
                        _ => None,
                    })
                }),
            _ => None,
        }
    }

    #[tokio::test]
    async fn test_load_nodes_from_parquet() {
        use crate::config::app_config::AppConfigBuilder;
        use arrow::{
            array::{Int64Array, StringArray},
            datatypes::{DataType, Field, Schema as ArrowSchema},
            record_batch::RecordBatch,
        };
        use parquet::arrow::ArrowWriter;
        use std::{fs::File, sync::Arc};

        let graph_dir = tempdir().unwrap();
        let tmp_dir = tempdir().unwrap();

        // Write a minimal parquet file: two nodes "a" and "b" at t=1 with a weight property.
        let parquet_path = tmp_dir.path().join("nodes.parquet");
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("time", DataType::Int64, false),
            Field::new("weight", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(Int64Array::from(vec![10, 20])),
            ],
        )
        .unwrap();
        let file = File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Build an AppConfig that permits the temp dir as a parquet source.
        let app_config = AppConfigBuilder::new()
            .with_allowed_parquet_paths(vec![tmp_dir.path().to_path_buf()])
            .build();

        let data = Data::new(graph_dir.path(), &app_config, Config::default());
        let folder = data
            .work_dir_write()
            .await
            .validate_path_for_insert("mygraph", false)
            .unwrap();
        data.insert_graph(folder, Graph::new().into())
            .await
            .unwrap();

        let schema = App::create_schema().data(data).finish().unwrap();

        let parquet_path_str = parquet_path.to_str().unwrap().replace('\\', "/");
        let mutation = format!(
            r#"mutation {{
                loadNodes(
                    graphPath: "mygraph",
                    dataPath: "{}",
                    time: "time",
                    id: "id",
                    properties: ["weight"]
                )
            }}"#,
            parquet_path_str
        );
        let res = run_mutation(&schema, &mutation).await;
        assert_eq!(res.errors, vec![], "loadNodes mutation returned errors");
        assert_eq!(res.data.into_json().unwrap(), json!({"loadNodes": true}));

        // Query the loaded nodes back via GraphQL to confirm they landed.
        let query = r#"{
            graph(path: "mygraph") {
                nodes {
                    list { name }
                }
            }
        }"#;
        let res = schema.execute(Request::new(query).data(Access::Rw)).await;
        assert_eq!(res.errors, vec![], "node query returned errors");
        let mut names: Vec<String> = res.data.into_json().unwrap()["graph"]["nodes"]["list"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["name"].as_str().unwrap().to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[tokio::test]
    async fn test_load_edges_from_parquet() {
        use crate::config::app_config::AppConfigBuilder;
        use arrow::{
            array::{Int64Array, StringArray},
            datatypes::{DataType, Field, Schema as ArrowSchema},
            record_batch::RecordBatch,
        };
        use parquet::arrow::ArrowWriter;
        use std::{fs::File, sync::Arc};

        let graph_dir = tempdir().unwrap();
        let tmp_dir = tempdir().unwrap();

        // Write a minimal parquet file: two edges a→b and b→c at t=1 with a weight property.
        let parquet_path = tmp_dir.path().join("edges.parquet");
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("src", DataType::Utf8, false),
            Field::new("dst", DataType::Utf8, false),
            Field::new("time", DataType::Int64, false),
            Field::new("weight", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(StringArray::from(vec!["b", "c"])),
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(Int64Array::from(vec![10, 20])),
            ],
        )
        .unwrap();
        let file = File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Build an AppConfig that permits the temp dir as a parquet source.
        let app_config = AppConfigBuilder::new()
            .with_allowed_parquet_paths(vec![tmp_dir.path().to_path_buf()])
            .build();

        let data = Data::new(graph_dir.path(), &app_config, Config::default());
        let folder = data
            .work_dir_write()
            .await
            .validate_path_for_insert("mygraph", false)
            .unwrap();
        data.insert_graph(folder, Graph::new().into())
            .await
            .unwrap();

        let schema = App::create_schema().data(data).finish().unwrap();

        let parquet_path_str = parquet_path.to_str().unwrap().replace('\\', "/");
        let mutation = format!(
            r#"mutation {{
                loadEdges(
                    graphPath: "mygraph",
                    dataPath: "{}",
                    time: "time",
                    src: "src",
                    dst: "dst",
                    properties: ["weight"]
                )
            }}"#,
            parquet_path_str
        );
        let res = run_mutation(&schema, &mutation).await;
        assert_eq!(res.errors, vec![], "loadEdges mutation returned errors");
        assert_eq!(res.data.into_json().unwrap(), json!({"loadEdges": true}));

        // Query the loaded edges back via GraphQL to confirm they landed.
        let query = r#"{
            graph(path: "mygraph") {
                edges {
                    list { src { name } dst { name } }
                }
            }
        }"#;
        let res = schema.execute(Request::new(query).data(Access::Rw)).await;
        assert_eq!(res.errors, vec![], "edge query returned errors");
        let mut edges: Vec<(String, String)> = res.data.into_json().unwrap()["graph"]["edges"]
            ["list"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| {
                (
                    e["src"]["name"].as_str().unwrap().to_string(),
                    e["dst"]["name"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        edges.sort();
        assert_eq!(
            edges,
            vec![
                ("a".to_string(), "b".to_string()),
                ("b".to_string(), "c".to_string())
            ]
        );
    }

    // ── allowed-paths tests ──────────────────────────────────────────────────

    /// Write a one-row nodes parquet file into `dir` and return its path.
    fn write_nodes_parquet(dir: &std::path::Path) -> std::path::PathBuf {
        use arrow::{
            array::{Int64Array, StringArray},
            datatypes::{DataType, Field, Schema as ArrowSchema},
            record_batch::RecordBatch,
        };
        use parquet::arrow::ArrowWriter;
        use std::{fs::File, sync::Arc};

        let path = dir.join("nodes.parquet");
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("time", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["x"])),
                Arc::new(Int64Array::from(vec![1i64])),
            ],
        )
        .unwrap();
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    }

    #[tokio::test]
    async fn test_parquet_allowed_path_accepted() {
        use crate::config::app_config::AppConfigBuilder;

        let graph_dir = tempdir().unwrap();
        let allowed_dir = tempdir().unwrap();
        let parquet_path = write_nodes_parquet(allowed_dir.path());

        let app_config = AppConfigBuilder::new()
            .with_allowed_parquet_paths(vec![allowed_dir.path().to_path_buf()])
            .build();
        let data = Data::new(graph_dir.path(), &app_config, Config::default());
        let folder = data
            .work_dir_write()
            .await
            .validate_path_for_insert("g", false)
            .unwrap();
        data.insert_graph(folder, Graph::new().into())
            .await
            .unwrap();

        let schema = App::create_schema().data(data).finish().unwrap();
        let parquet_path_str = parquet_path.to_str().unwrap().replace('\\', "/");
        let mutation = format!(
            r#"mutation {{ loadNodes(graphPath: "g", dataPath: "{}", time: "time", id: "id") }}"#,
            parquet_path_str
        );
        let res = run_mutation(&schema, &mutation).await;
        assert_eq!(
            res.errors,
            vec![],
            "path inside allowlist and outside the work dir should be accepted"
        );
    }

    #[tokio::test]
    async fn test_parquet_path_within_work_dir_rejected() {
        use crate::config::app_config::AppConfigBuilder;

        let graph_dir = tempdir().unwrap();
        let parquet_path = write_nodes_parquet(graph_dir.path());

        // Even though the work dir is allowlisted, paths within it must be rejected.
        let app_config = AppConfigBuilder::new()
            .with_allowed_parquet_paths(vec![graph_dir.path().to_path_buf()])
            .build();
        let data = Data::new(graph_dir.path(), &app_config, Config::default());
        let folder = data
            .work_dir_write()
            .await
            .validate_path_for_insert("g", false)
            .unwrap();
        data.insert_graph(folder, Graph::new().into())
            .await
            .unwrap();

        let schema = App::create_schema().data(data).finish().unwrap();
        let parquet_path_str = parquet_path.to_str().unwrap().replace('\\', "/");
        let mutation = format!(
            r#"mutation {{ loadNodes(graphPath: "g", dataPath: "{}", time: "time", id: "id") }}"#,
            parquet_path_str
        );
        let res = run_mutation(&schema, &mutation).await;
        assert!(
            !res.errors.is_empty(),
            "path within the work dir should be rejected"
        );
        assert!(
            res.errors[0]
                .message
                .contains("working directory are not permitted"),
            "unexpected error: {}",
            res.errors[0].message
        );
    }

    #[tokio::test]
    async fn test_parquet_disallowed_path_rejected() {
        use crate::config::app_config::AppConfigBuilder;

        let allowed_dir = tempdir().unwrap();
        let other_dir = tempdir().unwrap();
        let parquet_path = write_nodes_parquet(other_dir.path());

        let app_config = AppConfigBuilder::new()
            .with_allowed_parquet_paths(vec![allowed_dir.path().to_path_buf()])
            .build();
        let data = Data::new(allowed_dir.path(), &app_config, Config::default());
        let folder = data
            .work_dir_write()
            .await
            .validate_path_for_insert("g", false)
            .unwrap();
        data.insert_graph(folder, Graph::new().into())
            .await
            .unwrap();

        let schema = App::create_schema().data(data).finish().unwrap();
        let parquet_path_str = parquet_path.to_str().unwrap().replace('\\', "/");
        let mutation = format!(
            r#"mutation {{ loadNodes(graphPath: "g", dataPath: "{}", time: "time", id: "id") }}"#,
            parquet_path_str
        );
        let res = run_mutation(&schema, &mutation).await;
        assert!(
            !res.errors.is_empty(),
            "path outside allowlist should be rejected"
        );
        assert!(
            res.errors[0]
                .message
                .contains("not in the list of allowed paths"),
            "unexpected error: {}",
            res.errors[0].message
        );
    }

    #[tokio::test]
    async fn test_parquet_empty_allowlist_denies_any_path() {
        use crate::config::app_config::AppConfigBuilder;

        let graph_dir = tempdir().unwrap();
        let other_dir = tempdir().unwrap();
        let parquet_path = write_nodes_parquet(other_dir.path());

        // No allowed paths configured → nothing is permitted.
        let app_config = AppConfigBuilder::new()
            .with_allowed_parquet_paths(vec![])
            .build();
        let data = Data::new(graph_dir.path(), &app_config, Config::default());
        let folder = data
            .work_dir_write()
            .await
            .validate_path_for_insert("g", false)
            .unwrap();
        data.insert_graph(folder, Graph::new().into())
            .await
            .unwrap();

        let schema = App::create_schema().data(data).finish().unwrap();
        let parquet_path_str = parquet_path.to_str().unwrap().replace('\\', "/");
        let mutation = format!(
            r#"mutation {{ loadNodes(graphPath: "g", dataPath: "{}", time: "time", id: "id") }}"#,
            parquet_path_str
        );
        let res = run_mutation(&schema, &mutation).await;
        assert!(
            !res.errors.is_empty(),
            "empty allowlist should deny any path"
        );
        assert!(
            res.errors[0]
                .message
                .contains("not in the list of allowed paths"),
            "unexpected error: {}",
            res.errors[0].message
        );
    }

    #[tokio::test]
    async fn test_load_nodes_schema_parameter() {
        use crate::config::app_config::AppConfigBuilder;
        use arrow::{
            array::{Float32Array, Int64Array, StringArray},
            datatypes::{DataType, Field, Schema as ArrowSchema},
            record_batch::RecordBatch,
        };
        use parquet::arrow::ArrowWriter;
        use std::{fs::File, sync::Arc};

        let graph_dir = tempdir().unwrap();
        let tmp_dir = tempdir().unwrap();

        // Write a parquet file where 'score' is stored as Float32.
        let parquet_path = tmp_dir.path().join("nodes.parquet");
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("time", DataType::Int64, false),
            Field::new("score", DataType::Float32, true),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(Float32Array::from(vec![1.5f32, 2.5f32])),
            ],
        )
        .unwrap();
        let file = File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let app_config = AppConfigBuilder::new()
            .with_allowed_parquet_paths(vec![tmp_dir.path().to_path_buf()])
            .build();
        let data = Data::new(graph_dir.path(), &app_config, Config::default());
        let folder = data
            .work_dir_write()
            .await
            .validate_path_for_insert("g", false)
            .unwrap();
        data.insert_graph(folder, Graph::new().into())
            .await
            .unwrap();

        let schema = App::create_schema().data(data).finish().unwrap();
        let parquet_path_str = parquet_path.to_str().unwrap().replace('\\', "/");
        // Cast 'score' from Float32 (on-disk type) to Float64 via the schema parameter.
        let mutation = format!(
            r#"mutation {{
                loadNodes(
                    graphPath: "g",
                    dataPath: "{}",
                    time: "time",
                    id: "id",
                    properties: ["score"],
                    schema: "{{\"score\": \"Float64\"}}"
                )
            }}"#,
            parquet_path_str
        );
        let res = run_mutation(&schema, &mutation).await;
        assert_eq!(
            res.errors,
            vec![],
            "schema parameter cast Float32 → Float64 should succeed"
        );
        assert_eq!(res.data.into_json().unwrap(), json!({"loadNodes": true}));

        // Confirm both nodes loaded.
        let query = r#"{
            graph(path: "g") {
                nodes { list { name } }
            }
        }"#;
        let res = schema.execute(Request::new(query).data(Access::Rw)).await;
        assert_eq!(res.errors, vec![], "node query returned errors");
        let mut names: Vec<String> = res.data.into_json().unwrap()["graph"]["nodes"]["list"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["name"].as_str().unwrap().to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[tokio::test]
    async fn test_load_nodes_nested_directory_within_allowed_path() {
        use crate::config::app_config::AppConfigBuilder;

        let graph_dir = tempdir().unwrap();
        let tmp_dir = tempdir().unwrap();
        // Create a subdirectory inside the allowed root.
        let sub_dir = tmp_dir.path().join("subdir");
        fs::create_dir_all(&sub_dir).unwrap();
        let parquet_path = write_nodes_parquet(&sub_dir);

        // The allowlist only contains the top-level directory, not subdir directly.
        let app_config = AppConfigBuilder::new()
            .with_allowed_parquet_paths(vec![tmp_dir.path().to_path_buf()])
            .build();
        let data = Data::new(graph_dir.path(), &app_config, Config::default());
        let folder = data
            .work_dir_write()
            .await
            .validate_path_for_insert("g", false)
            .unwrap();
        data.insert_graph(folder, Graph::new().into())
            .await
            .unwrap();

        let schema = App::create_schema().data(data).finish().unwrap();
        let parquet_path_str = parquet_path.to_str().unwrap().replace('\\', "/");
        let mutation = format!(
            r#"mutation {{ loadNodes(graphPath: "g", dataPath: "{}", time: "time", id: "id") }}"#,
            parquet_path_str
        );
        let res = run_mutation(&schema, &mutation).await;
        assert_eq!(
            res.errors,
            vec![],
            "parquet nested inside an allowed path should be accepted"
        );
    }

    #[tokio::test]
    async fn test_flush() {
        let tmp_dir = tempdir().unwrap();
        let data = Data::new(tmp_dir.path(), &AppConfig::default(), Config::default());
        let folder = data
            .work_dir_write()
            .await
            .validate_path_for_insert("g", false)
            .unwrap();
        data.insert_graph(folder, Graph::new().into())
            .await
            .unwrap();

        let schema = App::create_schema().data(data).finish().unwrap();
        let res = run_mutation(&schema, r#"mutation { flush(graphPath: "g") }"#).await;
        assert_eq!(res.errors, vec![], "flush mutation returned errors");
        assert_eq!(res.data.into_json().unwrap(), json!({"flush": true}));
    }

    /// End-to-end reproduction of the stale namespace-listing counts bug:
    /// create a graph, populate it, and flush — all over GraphQL — then read
    /// the listing from a cold-cache session (as after a server restart),
    /// which resolves `nodeCount`/`edgeCount` from the persisted sidecar.
    /// Before the fix, `updateGraph{ flush }` never rewrote the sidecar, so
    /// this reported 0/0.
    #[tokio::test]
    async fn test_namespace_listing_counts_after_flush() {
        use crate::test_support::{run_mutation, setup_with_graphs};

        let work_dir = tempdir().unwrap();

        let session = setup_with_graphs(&[], work_dir.path()).await;

        // Graph lives inside the `people` namespace so we can list it below.
        let created = run_mutation(
            &session.schema,
            r#"mutation { newGraph(path: "people/g", graphType: EVENT) }"#,
        )
        .await;
        assert_eq!(created.errors, vec![], "newGraph errored");

        // `updateGraph` is a side-effecting field on the query root.
        // `addEdge` implicitly creates both endpoints: 2 nodes, 1 edge.
        let written = run_mutation(
            &session.schema,
            r#"query { updateGraph(path: "people/g") { addEdge(time: 0, src: "a", dst: "b") { success } } }"#,
        )
        .await;
        assert_eq!(written.errors, vec![], "addEdge errored");

        // Separate request so `flush` is ordered after the writes.
        let flushed = run_mutation(
            &session.schema,
            r#"query { updateGraph(path: "people/g") { flush } }"#,
        )
        .await;
        assert_eq!(flushed.errors, vec![], "flush errored");

        // Fresh session over the same work dir → cold cache, so the listing
        // reads counts from the persisted sidecar (the bug surface).
        let restarted = setup_with_graphs(&[], work_dir.path()).await;
        let listed = restarted
            .schema
            .execute(Request::new(
                r#"query { namespace(path: "people") { graphs { list { nodeCount edgeCount } } } }"#,
            ))
            .await;
        assert_eq!(listed.errors, vec![], "namespace listing errored");

        let json = listed.data.into_json().unwrap();
        let row = &json["namespace"]["graphs"]["list"][0];
        assert_eq!(row["nodeCount"], 2, "listing nodeCount stale after flush");
        assert_eq!(row["edgeCount"], 1, "listing edgeCount stale after flush");

        // Keep session 1 alive past the assertion: its `Drop` runs
        // `flush_and_clear`, which would rewrite the sidecar and mask the bug.
        drop(session);
    }

    #[tokio::test]
    async fn test_nodes_sorted_by_type_then_name() {
        let g = Graph::new();
        g.add_node(1, "b", NO_PROPS, Some("Person"), None).unwrap();
        g.add_node(1, "a", NO_PROPS, Some("Person"), None).unwrap();
        g.add_node(1, "c", NO_PROPS, Some("Company"), None).unwrap();
        g.add_node(1, "d", NO_PROPS, None, None).unwrap(); // untyped

        let graph: MaterializedGraph = g.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        // type ascending: None < "Company" < "Person"; then name ascending.
        let query = r#"
        {
          graph(path: "g") {
            nodes {
              sorted(sortBys: [{ type: true }, { name: true }]) {
                list { name }
              }
            }
          }
        }
        "#;
        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        let data = res.data.into_json().unwrap();
        assert_eq!(
            data,
            json!({ "graph": { "nodes": { "sorted": { "list": [
                { "name": "d" }, { "name": "c" }, { "name": "a" }, { "name": "b" }
            ] } } } })
        );
    }

    #[tokio::test]
    async fn test_nodes_sorted_by_type_reverse() {
        let g = Graph::new();
        g.add_node(1, "b", NO_PROPS, Some("Person"), None).unwrap();
        g.add_node(1, "a", NO_PROPS, Some("Person"), None).unwrap();
        g.add_node(1, "c", NO_PROPS, Some("Company"), None).unwrap();
        g.add_node(1, "d", NO_PROPS, None, None).unwrap(); // untyped

        let graph: MaterializedGraph = g.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        // type descending (reverse: true): "Person" > "Company" > None; then name ascending.
        let query = r#"
        {
          graph(path: "g") {
            nodes {
              sorted(sortBys: [{ type: true, reverse: true }, { name: true }]) {
                list { name }
              }
            }
          }
        }
        "#;
        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        let data = res.data.into_json().unwrap();
        assert_eq!(
            data,
            json!({ "graph": { "nodes": { "sorted": { "list": [
                { "name": "a" }, { "name": "b" }, { "name": "c" }, { "name": "d" }
            ] } } } })
        );
    }

    #[tokio::test]
    async fn test_nodes_sorted_by_id_regression() {
        let g = Graph::new();
        g.add_node(1, "c", NO_PROPS, None, None).unwrap();
        g.add_node(1, "a", NO_PROPS, None, None).unwrap();
        g.add_node(1, "b", NO_PROPS, None, None).unwrap();

        let graph: MaterializedGraph = g.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        let query = r#"
        {
          graph(path: "g") {
            nodes { sorted(sortBys: [{ id: true }]) { list { name } } }
          }
        }
        "#;
        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        let data = res.data.into_json().unwrap();
        assert_eq!(
            data,
            json!({ "graph": { "nodes": { "sorted": { "list": [
                { "name": "a" }, { "name": "b" }, { "name": "c" }
            ] } } } })
        );
    }

    async fn neighbour_sort_setup(tmp: &std::path::Path) -> crate::test_support::TestSetup {
        // Anchor "hub" with edges in BOTH directions to typed + untyped neighbours.
        let g = Graph::new();
        g.add_node(1, "hub", NO_PROPS, None, None).unwrap();
        g.add_node(1, "x", [("score", 3i64)], Some("B"), None)
            .unwrap();
        g.add_node(1, "y", [("score", 1i64)], Some("A"), None)
            .unwrap();
        g.add_node(1, "w", [("score", 2i64)], Some("A"), None)
            .unwrap();
        g.add_node(1, "z", [("score", 4i64)], None, None).unwrap(); // untyped
        g.add_edge(10, "hub", "x", NO_PROPS, None).unwrap(); // out -> nbr x (B)
        g.add_edge(11, "y", "hub", NO_PROPS, None).unwrap(); // in  -> nbr y (A)
        g.add_edge(12, "w", "hub", NO_PROPS, None).unwrap(); // in  -> nbr w (A)
        g.add_edge(13, "hub", "z", NO_PROPS, None).unwrap(); // out -> nbr z (untyped)

        let graph: MaterializedGraph = g.into();
        setup_with_graphs(&[("g", graph)], tmp).await
    }

    #[tokio::test]
    async fn test_edges_sorted_by_neighbour_type_then_name() {
        let tmp_dir = tempdir().unwrap();
        let setup = neighbour_sort_setup(tmp_dir.path()).await;

        // type asc (None < "A" < "B"), then name asc: z(None), w(A), y(A), x(B)
        let query = r#"
        {
          graph(path: "g") {
            node(name: "hub") {
              edges {
                explodeLayers {
                  sorted(sortBys: [
                    { neighbour: { type: true } },
                    { neighbour: { name: true } },
                    { time: LATEST }
                  ]) {
                    count
                    page(limit: 10) { nbr { name nodeType } }
                  }
                }
              }
            }
          }
        }
        "#;
        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        let data = res.data.into_json().unwrap();
        assert_eq!(
            data,
            json!({ "graph": { "node": { "edges": { "explodeLayers": { "sorted": {
                "count": 4,
                "page": [
                    { "nbr": { "name": "z", "nodeType": null } },
                    { "nbr": { "name": "w", "nodeType": "A" } },
                    { "nbr": { "name": "y", "nodeType": "A" } },
                    { "nbr": { "name": "x", "nodeType": "B" } }
                ]
            } } } } } })
        );
    }

    #[tokio::test]
    async fn test_edges_sorted_by_neighbour_property() {
        let tmp_dir = tempdir().unwrap();
        let setup = neighbour_sort_setup(tmp_dir.path()).await;

        // neighbour "score" ascending: y(1), w(2), x(3), z(4)
        let query = r#"
        {
          graph(path: "g") {
            node(name: "hub") {
              edges {
                explodeLayers {
                  sorted(sortBys: [{ neighbour: { property: "score" } }]) {
                    page(limit: 10) { nbr { name } }
                  }
                }
              }
            }
          }
        }
        "#;
        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        let data = res.data.into_json().unwrap();
        assert_eq!(
            data,
            json!({ "graph": { "node": { "edges": { "explodeLayers": { "sorted": {
                "page": [
                    { "nbr": { "name": "y" } }, { "nbr": { "name": "w" } },
                    { "nbr": { "name": "x" } }, { "nbr": { "name": "z" } }
                ]
            } } } } } })
        );
    }

    #[tokio::test]
    async fn test_graph_edges_neighbour_id_equals_dst() {
        // At graph level every ref is Dir::Out, so neighbour == dst.
        let tmp_dir = tempdir().unwrap();
        let setup = neighbour_sort_setup(tmp_dir.path()).await;

        let query = r#"
        {
          graph(path: "g") {
            edges {
              sorted(sortBys: [{ neighbour: { id: true } }]) {
                page(limit: 10) { dst { name } }
              }
            }
          }
        }
        "#;
        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        let data = res.data.into_json().unwrap();
        // dst ids ascending: hub, hub, x, z
        assert_eq!(
            data,
            json!({ "graph": { "edges": { "sorted": {
                "page": [
                    { "dst": { "name": "hub" } }, { "dst": { "name": "hub" } },
                    { "dst": { "name": "x" } }, { "dst": { "name": "z" } }
                ]
            } } } })
        );
    }

    #[tokio::test]
    async fn test_edges_sorted_by_neighbour_name_reverse() {
        let tmp_dir = tempdir().unwrap();
        let setup = neighbour_sort_setup(tmp_dir.path()).await;

        // Neighbour name DESCENDING via the nested `reverse`; the outer
        // `reverse: true` must be ignored (the neighbour arm early-returns).
        // hub's neighbours are w, x, y, z -> descending: z, y, x, w.
        let query = r#"
        {
          graph(path: "g") {
            node(name: "hub") {
              edges {
                explodeLayers {
                  sorted(sortBys: [{ reverse: true, neighbour: { name: true, reverse: true } }]) {
                    page(limit: 10) { nbr { name } }
                  }
                }
              }
            }
          }
        }
        "#;
        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        let data = res.data.into_json().unwrap();
        assert_eq!(
            data,
            json!({ "graph": { "node": { "edges": { "explodeLayers": { "sorted": {
                "page": [
                    { "nbr": { "name": "z" } }, { "nbr": { "name": "y" } },
                    { "nbr": { "name": "x" } }, { "nbr": { "name": "w" } }
                ]
            } } } } } })
        );
    }

    #[tokio::test]
    async fn test_edges_sorted_by_neighbour_self_loop() {
        // Self-loop hub->hub must resolve nbr to the anchor itself, sorting
        // under its own type ("M"), not treated specially.
        let g = Graph::new();
        g.add_node(1, "hub", NO_PROPS, Some("M"), None).unwrap();
        g.add_node(1, "a", NO_PROPS, Some("A"), None).unwrap();
        g.add_edge(10, "hub", "a", NO_PROPS, None).unwrap();
        g.add_edge(11, "hub", "hub", NO_PROPS, None).unwrap();

        let graph: MaterializedGraph = g.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        let query = r#"
        {
          graph(path: "g") {
            node(name: "hub") {
              edges {
                explodeLayers {
                  sorted(sortBys: [{ neighbour: { type: true } }]) {
                    count
                    page(limit: 10) { nbr { name nodeType } }
                  }
                }
              }
            }
          }
        }
        "#;
        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        let data = res.data.into_json().unwrap();
        assert_eq!(
            data,
            json!({ "graph": { "node": { "edges": { "explodeLayers": { "sorted": {
                "count": 2,
                "page": [
                    { "nbr": { "name": "a", "nodeType": "A" } },
                    { "nbr": { "name": "hub", "nodeType": "M" } }
                ]
            } } } } } })
        );
    }

    #[tokio::test]
    async fn test_edges_sorted_by_neighbour_type_reverse_untyped_last() {
        let tmp_dir = tempdir().unwrap();
        let setup = neighbour_sort_setup(tmp_dir.path()).await;

        // neighbour type DESCENDING (untyped sorts last, not first), then
        // neighbour name ascending to break the "A" tie: x(B), w(A), y(A), z(None)
        let query = r#"
        {
          graph(path: "g") {
            node(name: "hub") {
              edges {
                explodeLayers {
                  sorted(sortBys: [
                      { neighbour: { type: true, reverse: true } },
                      { neighbour: { name: true } }
                  ]) {
                    page(limit: 10) { nbr { name nodeType } }
                  }
                }
              }
            }
          }
        }
        "#;
        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        let data = res.data.into_json().unwrap();
        assert_eq!(
            data,
            json!({ "graph": { "node": { "edges": { "explodeLayers": { "sorted": {
                "page": [
                    { "nbr": { "name": "x", "nodeType": "B" } },
                    { "nbr": { "name": "w", "nodeType": "A" } },
                    { "nbr": { "name": "y", "nodeType": "A" } },
                    { "nbr": { "name": "z", "nodeType": null } }
                ]
            } } } } } })
        );
    }

    #[tokio::test]
    async fn test_graph_edges_sorted_by_src_node() {
        // Sort a graph-level edge collection by the src node (type, then name).
        // Enabled by making `src` take a NodeSortBy; a bare id flag could not
        // express this.
        let g = Graph::new();
        g.add_node(1, "a", NO_PROPS, Some("Z"), None).unwrap();
        g.add_node(1, "b", NO_PROPS, Some("A"), None).unwrap();
        g.add_node(1, "c", NO_PROPS, None, None).unwrap(); // untyped
        g.add_node(1, "x", NO_PROPS, None, None).unwrap();
        g.add_edge(10, "a", "x", NO_PROPS, None).unwrap();
        g.add_edge(11, "b", "x", NO_PROPS, None).unwrap();
        g.add_edge(12, "c", "x", NO_PROPS, None).unwrap();

        let graph: MaterializedGraph = g.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        // src type ascending, untyped first: None(c) < "A"(b) < "Z"(a)
        let query = r#"
        {
          graph(path: "g") {
            edges {
              sorted(sortBys: [{ src: { type: true } }, { src: { name: true } }]) {
                page(limit: 10) { src { name nodeType } }
              }
            }
          }
        }
        "#;
        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        let data = res.data.into_json().unwrap();
        assert_eq!(
            data,
            json!({ "graph": { "edges": { "sorted": {
                "page": [
                    { "src": { "name": "c", "nodeType": null } },
                    { "src": { "name": "b", "nodeType": "A" } },
                    { "src": { "name": "a", "nodeType": "Z" } }
                ]
            } } } })
        );
    }
}
