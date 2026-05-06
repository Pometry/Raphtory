pub use crate::{
    auth::{require_jwt_write_access_dynamic, Access},
    model::graph::filtering::GraphAccessFilter,
    server::GraphServer,
};
use crate::{data::InsertionError, paths::PathValidationError};
use raphtory::errors::GraphError;
use std::sync::Arc;

mod auth;
pub mod auth_policy;
pub mod client;
pub mod data;
mod graph;
pub mod model;
pub mod observability;
mod paths;
mod routes;
pub mod server;
pub mod url_encode;

pub mod cli;
pub mod config;
#[cfg(feature = "python")]
pub mod python;
pub mod rayon;

#[derive(thiserror::Error, Debug)]
pub enum GQLError {
    #[error(transparent)]
    GraphError(#[from] GraphError),
    #[error(transparent)]
    Validation(#[from] PathValidationError),
    #[error("Insertion failed for Graph {graph}: {error}")]
    Insertion {
        graph: String,
        error: InsertionError,
    },
    #[error(transparent)]
    Arc(#[from] Arc<Self>),
}

#[cfg(test)]
mod graphql_test {
    #[cfg(feature = "search")]
    use crate::config::app_config::AppConfigBuilder;
    use crate::{
        auth::Access,
        config::app_config::AppConfig,
        data::{data_tests::save_graphs_to_work_dir, Data},
        model::App,
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
        serialise::GraphFolder,
        test_utils::json_sort_by_name,
    };
    use raphtory_api::core::{entities::GID, storage::arc_str::ArcStr};
    use serde_json::{json, Value};
    use std::{
        collections::{HashMap, HashSet},
        fs,
    };
    use tempfile::tempdir;

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
    #[cfg(feature = "search")]
    async fn test_search_nodes_gql() {
        let graph = Graph::new();

        let nodes = vec![
            (6, "N1", vec![("p1", Prop::U64(2u64))]),
            (7, "N1", vec![("p1", Prop::U64(1u64))]),
            (6, "N2", vec![("p1", Prop::U64(1u64))]),
            (7, "N2", vec![("p1", Prop::U64(2u64))]),
            (8, "N3", vec![("p1", Prop::U64(1u64))]),
            (9, "N4", vec![("p1", Prop::U64(1u64))]),
            (5, "N5", vec![("p1", Prop::U64(1u64))]),
            (6, "N5", vec![("p1", Prop::U64(2u64))]),
            (5, "N6", vec![("p1", Prop::U64(1u64))]),
            (6, "N6", vec![("p1", Prop::U64(1u64))]),
            (3, "N7", vec![("p1", Prop::U64(1u64))]),
            (5, "N7", vec![("p1", Prop::U64(1u64))]),
            (3, "N8", vec![("p1", Prop::U64(1u64))]),
            (4, "N8", vec![("p1", Prop::U64(2u64))]),
            (2, "N9", vec![("p1", Prop::U64(2u64))]),
            (2, "N10", vec![("q1", Prop::U64(0u64))]),
            (2, "N10", vec![("p1", Prop::U64(3u64))]),
            (2, "N11", vec![("p1", Prop::U64(3u64))]),
            (2, "N11", vec![("q1", Prop::U64(0u64))]),
            (2, "N12", vec![("q1", Prop::U64(0u64))]),
            (3, "N12", vec![("p1", Prop::U64(3u64))]),
            (2, "N13", vec![("q1", Prop::U64(0u64))]),
            (3, "N13", vec![("p1", Prop::U64(3u64))]),
            (2, "N14", vec![("q1", Prop::U64(0u64))]),
            (2, "N15", vec![]),
        ];

        for (id, name, props) in nodes {
            graph.add_node(id, name, props, None, None).unwrap();
        }

        let metadata = vec![
            ("N1", vec![("p1", Prop::U64(1u64))]),
            ("N4", vec![("p1", Prop::U64(2u64))]),
            ("N9", vec![("p1", Prop::U64(1u64))]),
            ("N10", vec![("p1", Prop::U64(1u64))]),
            ("N11", vec![("p1", Prop::U64(1u64))]),
            ("N12", vec![("p1", Prop::U64(1u64))]),
            ("N13", vec![("p1", Prop::U64(1u64))]),
            ("N14", vec![("p1", Prop::U64(1u64))]),
            ("N15", vec![("p1", Prop::U64(1u64))]),
        ];

        for (name, props) in metadata {
            graph.node(name).unwrap().add_metadata(props).unwrap();
        }

        let graph: MaterializedGraph = graph.into();

        let graphs = HashMap::from([("master".to_string(), graph)]);
        let tmp_dir = tempdir().unwrap();
        let config = AppConfigBuilder::new().with_create_index(true).build();
        let data = Data::new(tmp_dir.path(), &config, Config::default());
        save_graphs_to_work_dir(&data, &graphs).await.unwrap();

        let schema = App::create_schema().data(data).finish().unwrap();

        let query = r#"
            {
              graph(path: "master") {
                searchNodes(
                    filter: {
                      or: [
                        {
                          property: {
                            name: "p1",
                            where: {
                              gt: {
                                u64: 2
                              }
                            }
                          }
                        },
                        {
                          and: [
                        {
                          node: {
                                field: NODE_NAME,
                            		where: {
                                  eq: {
                                    str: "N1"
                                  }
                                }
                            }
                        },
                        {
                          node: {
                            field: NODE_TYPE,
                            where: {
                              ne: {
                                str: "air_nomads"
                              }
                            }
                          }
                        },
                        {
                          property: {
                            name: "p1",
                            where: {
                              lt: {
                                u64: 5
                              }
                            }
                          }
                        }
                      ]
                        }
                      ]


                    },
                  limit: 20,
                  offset: 0
                ) {
                  name
                }
              }
            }
        "#;
        let req = Request::new(query);
        let res = schema.execute(req).await;
        assert_eq!(res.errors, []);
        let mut data = res.data.into_json().unwrap();

        if let Some(nodes) = data["graph"]["searchNodes"].as_array_mut() {
            nodes.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        }

        assert_eq!(
            data,
            json!({
                "graph": {
                    "searchNodes": [
                        { "name": "N1" },
                        { "name": "N10" },
                        { "name": "N11" },
                        { "name": "N12" },
                        { "name": "N13" }
                    ]
                }
            }),
        );
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
        let tmp_dir = tempfile::TempDir::new().unwrap();
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
}
