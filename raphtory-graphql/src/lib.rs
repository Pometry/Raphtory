pub use crate::server::GraphServer;
mod auth;
pub mod data;
mod embeddings;
mod graph;
pub mod model;
pub mod observability;
mod paths;
mod routes;
pub mod server;
pub mod url_encode;

pub mod config;
#[cfg(feature = "python")]
pub mod python;

#[cfg(test)]
mod graphql_test {
    use crate::{
        config::app_config::AppConfig,
        data::{data_tests::save_graphs_to_work_dir, Data},
        model::App,
        url_encode::{url_decode_graph, url_encode_graph},
    };
    use arrow_array::types::UInt8Type;
    use async_graphql::UploadValue;
    use serde_json::Value;

    use dynamic_graphql::{Request, Variables};
    #[cfg(feature = "storage")]
    use raphtory::disk_graph::DiskGraphStorage;
    use raphtory::{
        db::{
            api::view::{IntoDynamic, MaterializedGraph},
            graph::views::deletion_graph::PersistentGraph,
        },
        prelude::*,
        serialise::GraphFolder,
    };
    use raphtory_api::core::storage::arc_str::ArcStr;
    use serde_json::json;
    use std::{
        collections::{HashMap, HashSet},
        fs,
    };
    use tempfile::tempdir;

    #[tokio::test]
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
            graph.add_node(id, name, props, None).unwrap();
        }

        let constant_props = vec![
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

        for (name, props) in constant_props {
            graph
                .node(name)
                .unwrap()
                .add_constant_properties(props)
                .unwrap();
        }

        let graph: MaterializedGraph = graph.into();

        let graphs = HashMap::from([("master".to_string(), graph)]);
        let tmp_dir = tempdir().unwrap();
        save_graphs_to_work_dir(tmp_dir.path(), &graphs).unwrap();

        let data = Data::new(tmp_dir.path(), &AppConfig::default());

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
                            operator: GREATER_THAN,
                            value: {
                              u64: 2
                            }
                          }
                        },
                        {
                          and: [
                        {
                          node: {
                                field: NODE_NAME,
                                operator: EQUAL,
                                value: {
                                  str: "N1"
                                }
                            }
                        },
                        {
                          node: {
                            field: NODE_TYPE,
                            operator: NOT_EQUAL,
                            value: {
                              str: "air_nomads"
                            }
                          }
                        },
                        {
                          property: {
                            name: "p1",
                            operator: LESS_THAN,
                            value: {
                              u64: 5
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
            .add_node(0, 11, NO_PROPS, None)
            .expect("Could not add node!");
        graph.add_constant_properties([("name", "lotr")]).unwrap();

        let graph: MaterializedGraph = graph.into();
        let graphs = HashMap::from([("lotr".to_string(), graph)]);
        let tmp_dir = tempdir().unwrap();
        save_graphs_to_work_dir(tmp_dir.path(), &graphs).unwrap();

        let data = Data::new(tmp_dir.path(), &AppConfig::default());

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
                                "id": "11"
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
            )
            .unwrap();
        graph
            .node(1)
            .unwrap()
            .add_constant_properties([("lol", "smile")])
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
            .add_constant_properties([("static", "test")], None)
            .unwrap();
        let graph: MaterializedGraph = graph.into();

        let graphs = HashMap::from([("graph".to_string(), graph)]);
        let tmp_dir = tempdir().unwrap();
        save_graphs_to_work_dir(tmp_dir.path(), &graphs).unwrap();

        let data = Data::new(tmp_dir.path(), &AppConfig::default());
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
                }
              }
              nodes {
                properties {
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

        println!("{data:?}");

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
            assert_eq!(node_properties[2]["propertyType"].as_str().unwrap(), "Str");
        }

        if let Value::Array(mut edge_properties) =
            data["graph"]["schema"]["layers"][0]["edges"][0]["properties"].clone()
        {
            sort_properties(&mut edge_properties);

            assert_eq!(edge_properties[0]["propertyType"].as_str().unwrap(), "F32");
            assert_eq!(edge_properties[1]["propertyType"].as_str().unwrap(), "I32");
            assert_eq!(edge_properties[2]["propertyType"].as_str().unwrap(), "Str");
            assert_eq!(edge_properties[3]["propertyType"].as_str().unwrap(), "Str");
        }
    }

    #[tokio::test]
    async fn query_nodefilter() {
        let graph = Graph::new();
        graph
            .add_node(
                0,
                1,
                [("pgraph", Prop::from_arr::<UInt8Type>(vec![3u8]))],
                None,
            )
            .unwrap();
        let graph: MaterializedGraph = graph.into();

        let graphs = HashMap::from([("graph".to_string(), graph)]);
        let tmp_dir = tempdir().unwrap();
        save_graphs_to_work_dir(tmp_dir.path(), &graphs).unwrap();

        let data = Data::new(tmp_dir.path(), &AppConfig::default());
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
        g.add_constant_properties([("name", "graph")]).unwrap();
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
        g.add_node(11, 3, [("name", "phone")], None).unwrap();
        g.add_node(12, 3, [("name", "fax")], None).unwrap();
        g.add_node(13, 3, [("name", "fax")], None).unwrap();

        let graph: MaterializedGraph = g.into();
        let graphs = HashMap::from([("graph".to_string(), graph)]);
        let tmp_dir = tempdir().unwrap();
        save_graphs_to_work_dir(tmp_dir.path(), &graphs).unwrap();

        let expected = json!({
            "graph": {
              "properties": {
                "temporal": {
                  "values": [
                    {
                      "unique": [
                        "xyz",
                        "abc"
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
                        "unique": [
                          "fax",
                          "phone"
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
                        "unique": [
                          "open",
                          "review",
                          "in-progress"
                        ]
                      },
                      {
                        "unique": [
                          "false",
                          "true"
                        ]
                      }
                    ]
                  }
                }
              }
            }
        });

        let mut actual_graph_props = HashSet::new();
        let mut actual_node_props = HashSet::new();
        let mut actual_edge_props = HashSet::new();

        let graph_props = &expected["graph"]["properties"]["temporal"]["values"];
        for value in graph_props.as_array().unwrap().iter() {
            let unique_values: HashSet<_> = value["unique"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect();
            actual_graph_props.extend(unique_values);
        }

        let node_props = &expected["graph"]["node"]["properties"]["temporal"]["values"];
        for value in node_props.as_array().unwrap().iter() {
            let unique_values: HashSet<_> = value["unique"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect();
            actual_node_props.extend(unique_values);
        }

        let edge_props = &expected["graph"]["edge"]["properties"]["temporal"]["values"];
        for value in edge_props.as_array().unwrap().iter() {
            let unique_values: HashSet<_> = value["unique"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect();
            actual_edge_props.extend(unique_values);
        }

        assert_eq!(
            actual_graph_props,
            expected["graph"]["properties"]["temporal"]["values"][0]["unique"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect::<HashSet<_>>()
        );
        assert_eq!(
            actual_node_props,
            expected["graph"]["node"]["properties"]["temporal"]["values"][0]["unique"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect::<HashSet<_>>()
        );
        assert_eq!(
            actual_edge_props,
            expected["graph"]["edge"]["properties"]["temporal"]["values"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value["unique"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_str().unwrap()))
                .flatten()
                .collect::<HashSet<_>>()
        );
    }

    #[tokio::test]
    async fn test_ordered_dedupe_temporal_properties() {
        let g = Graph::new();
        g.add_constant_properties([("name", "graph")]).unwrap();
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
        g.add_node(11, 3, [("name", "phone")], None).unwrap();
        g.add_node(12, 3, [("name", "fax")], None).unwrap();
        g.add_node(13, 3, [("name", "fax")], None).unwrap();

        let g = g.into();
        let graphs = HashMap::from([("graph".to_string(), g)]);
        let tmp_dir = tempdir().unwrap();
        save_graphs_to_work_dir(tmp_dir.path(), &graphs).unwrap();

        let data = Data::new(tmp_dir.path(), &AppConfig::default());
        let schema = App::create_schema().data(data).finish().unwrap();

        let prop_has_key_filter = r#"
        {
          graph(path: "graph") {
            properties {
              temporal {
                values {
                  od1: orderedDedupe(latestTime: true) {
                    time
                    value
                  },
                  od2: orderedDedupe(latestTime: false) {
                    time
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
                      time
                      value
                    },
                    od2: orderedDedupe(latestTime: false) {
                      time
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
                      time
                      value
                    },
                    od2: orderedDedupe(latestTime: false) {
                      time
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
                          "time": 2,
                          "value": "abc"
                        },
                        {
                          "time": 3,
                          "value": "xyz"
                        },
                        {
                          "time": 4,
                          "value": "abc"
                        }
                      ],
                      "od2": [
                        {
                          "time": 1,
                          "value": "abc"
                        },
                        {
                          "time": 3,
                          "value": "xyz"
                        },
                        {
                          "time": 4,
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
                            "time": 11,
                            "value": "phone"
                          },
                          {
                            "time": 13,
                            "value": "fax"
                          }
                        ],
                        "od2": [
                          {
                            "time": 11,
                            "value": "phone"
                          },
                          {
                            "time": 12,
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
                            "time": 2,
                            "value": "open"
                          },
                          {
                            "time": 3,
                            "value": "review"
                          },
                          {
                            "time": 4,
                            "value": "open"
                          },
                          {
                            "time": 10,
                            "value": "in-progress"
                          }
                        ],
                        "od2": [
                          {
                            "time": 1,
                            "value": "open"
                          },
                          {
                            "time": 3,
                            "value": "review"
                          },
                          {
                            "time": 4,
                            "value": "open"
                          },
                          {
                            "time": 5,
                            "value": "in-progress"
                          }
                        ]
                      },
                      {
                        "od1": [
                          {
                            "time": 9,
                            "value": true
                          },
                          {
                            "time": 10,
                            "value": false
                          }
                        ],
                        "od2": [
                          {
                            "time": 9,
                            "value": true
                          },
                          {
                            "time": 10,
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
            .add_node(
                0,
                1,
                [("pgraph", Prop::from_arr::<UInt8Type>(vec![3u8]))],
                None,
            )
            .unwrap();

        let graph = graph.into();
        let graphs = HashMap::from([("graph".to_string(), graph)]);
        let tmp_dir = tempdir().unwrap();
        save_graphs_to_work_dir(tmp_dir.path(), &graphs).unwrap();

        let data = Data::new(tmp_dir.path(), &AppConfig::default());
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
        g.add_node(0, 1, NO_PROPS, None).unwrap();
        let tmp_file = tempfile::NamedTempFile::new().unwrap();
        g.encode(GraphFolder::new_as_zip(tmp_file.path())).unwrap();
        let file = fs::File::open(&tmp_file).unwrap();
        let upload_val = UploadValue {
            filename: "test".into(),
            content_type: Some("application/octet-stream".into()),
            content: file,
        };

        let tmp_dir = tempdir().unwrap();
        let data = Data::new(tmp_dir.path(), &AppConfig::default());
        let schema = App::create_schema().data(data).finish().unwrap();

        let query = r##"
        mutation($file: Upload!, $overwrite: Boolean!) {
            uploadGraph(path: "test", graph: $file, overwrite: $overwrite)
        }
        "##;

        let variables = json!({ "file": null, "overwrite": false });
        let mut req = Request::new(query).variables(Variables::from_json(variables));
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
        assert_eq!(res.errors.len(), 0);
        let res_json = res.data.into_json().unwrap();
        assert_eq!(
            res_json,
            json!({"graph": {"nodes": {"list": [{"id": "1"}]}}})
        );
    }

    #[tokio::test]
    async fn test_graph_send_receive_base64() {
        let g = PersistentGraph::new();
        g.add_node(0, 1, NO_PROPS, None).unwrap();

        let graph_str = url_encode_graph(g.clone()).unwrap();

        let tmp_dir = tempdir().unwrap();
        let data = Data::new(tmp_dir.path(), &AppConfig::default());
        let schema = App::create_schema().data(data).finish().unwrap();

        let query = r#"
        mutation($graph: String!, $overwrite: Boolean!) {
            sendGraph(path: "test", graph: $graph, overwrite: $overwrite)
        }
        "#;
        let req = Request::new(query).variables(Variables::from_json(
            json!({ "graph": graph_str, "overwrite": false }),
        ));

        let res = schema.execute(req).await;
        assert_eq!(res.errors.len(), 0);
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
        assert_eq!(
            res_json,
            json!({"graph": {"nodes": {"list": [{"id": "1"}]}}})
        );

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
        let graph_roundtrip = url_decode_graph(graph_encoded).unwrap().into_dynamic();
        assert_eq!(g, graph_roundtrip);
    }

    #[tokio::test]
    async fn test_type_filter() {
        let graph = Graph::new();
        graph.add_constant_properties([("name", "graph")]).unwrap();
        graph.add_node(1, 1, NO_PROPS, Some("a")).unwrap();
        graph.add_node(1, 2, NO_PROPS, Some("b")).unwrap();
        graph.add_node(1, 3, NO_PROPS, Some("b")).unwrap();
        graph.add_node(1, 4, NO_PROPS, Some("a")).unwrap();
        graph.add_node(1, 5, NO_PROPS, Some("c")).unwrap();
        graph.add_node(1, 6, NO_PROPS, Some("e")).unwrap();
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
        save_graphs_to_work_dir(tmp_dir.path(), &graphs).unwrap();

        let data = Data::new(tmp_dir.path(), &AppConfig::default());
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
        let data = res.data.into_json().unwrap();
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
        let data = res.data.into_json().unwrap();
        assert_eq!(
            data,
            json!({
                "graph": {
                  "nodes": {
                    "typeFilter": {
                      "list": [
                        {
                          "neighbours": {
                            "list": [
                              {
                                "name": "2"
                              }
                            ]
                          }
                        },
                        {
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

    #[cfg(feature = "storage")]
    #[tokio::test]
    async fn test_disk_graph() {
        let graph = Graph::new();
        graph.add_constant_properties([("name", "graph")]).unwrap();
        graph.add_node(1, 1, NO_PROPS, Some("a")).unwrap();
        graph.add_node(1, 2, NO_PROPS, Some("b")).unwrap();
        graph.add_node(1, 3, NO_PROPS, Some("b")).unwrap();
        graph.add_node(1, 4, NO_PROPS, Some("a")).unwrap();
        graph.add_node(1, 5, NO_PROPS, Some("c")).unwrap();
        graph.add_node(1, 6, NO_PROPS, Some("e")).unwrap();
        graph.add_edge(22, 1, 2, NO_PROPS, Some("a")).unwrap();
        graph.add_edge(22, 3, 2, NO_PROPS, Some("a")).unwrap();
        graph.add_edge(22, 2, 4, NO_PROPS, Some("a")).unwrap();
        graph.add_edge(22, 4, 5, NO_PROPS, Some("a")).unwrap();
        graph.add_edge(22, 4, 5, NO_PROPS, Some("a")).unwrap();
        graph.add_edge(22, 5, 6, NO_PROPS, Some("a")).unwrap();
        graph.add_edge(22, 3, 6, NO_PROPS, Some("a")).unwrap();

        let tmp_work_dir = tempdir().unwrap();
        let tmp_work_dir = tmp_work_dir.path();

        let disk_graph_path = tmp_work_dir.join("graph");
        fs::create_dir(&disk_graph_path).unwrap();
        fs::File::create(disk_graph_path.join(".raph")).unwrap();
        let _ = DiskGraphStorage::from_graph(&graph, disk_graph_path.join("graph")).unwrap();

        let data = Data::new(&tmp_work_dir, &AppConfig::default());
        let schema = App::create_schema().data(data).finish().unwrap();

        let req = r#"
        {
          graph(path: "graph") {
            nodes {
              list {
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
                      "list": [
                        {
                          "name": "1"
                        },
                        {
                          "name": "2"
                        },
                        {
                          "name": "3"
                        },
                        {
                          "name": "4"
                        },
                        {
                          "name": "5"
                        },
                        {
                          "name": "6"
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
        graph.add_constant_properties([("name", "graph")]).unwrap();
        graph.add_node(1, 1, NO_PROPS, Some("a")).unwrap();
        graph.add_node(1, 2, NO_PROPS, Some("b")).unwrap();
        graph.add_node(1, 3, NO_PROPS, Some("b")).unwrap();
        graph.add_node(1, 4, NO_PROPS, Some("a")).unwrap();
        graph.add_node(1, 5, NO_PROPS, Some("c")).unwrap();
        graph.add_node(1, 6, NO_PROPS, Some("e")).unwrap();
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
        save_graphs_to_work_dir(tmp_dir.path(), &graphs).unwrap();

        let data = Data::new(tmp_dir.path(), &AppConfig::default());
        let schema = App::create_schema().data(data).finish().unwrap();

        let req = r#"
        {
  namespace(path: "") {
    path
    graphs {
      list {
        path
        name
        metadata {
          nodeCount
          edgeCount
          properties {
            key
            value
          }
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
        assert_eq!(
            data,
            json!({
                "namespace": {
                    "path": "",
                    "graphs": {"list":[
                        {
                            "path": "graph",
                            "name": "graph",
                            "metadata": {
                                "nodeCount": 6,
                                "edgeCount": 6,
                                "properties": [
                                    {
                                        "key": "name",
                                        "value": "graph"
                                    },
                                ]
                            }
                        },
                    ]},
                    "children":{"list":[]},
                    "parent": null
                },
            }),
        );

        let req = r#"
        mutation CreateSubgraph {
          createSubgraph(parentPath: "graph", newPath: "graph2", nodes: ["1", "2"], overwrite: false)
        }
        "#;
        let req = Request::new(req);
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
        metadata {
          nodeCount
          edgeCount
          properties {
            key
            value
          }
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
        assert_eq!(
            data,
            json!({
                "namespace": {
                    "path": "",
                    "graphs": {"list":[
                        {
                            "path": "graph",
                            "name": "graph",
                            "metadata": {
                                "nodeCount": 6,
                                "edgeCount": 6,
                                "properties": [
                                    {
                                        "key": "name",
                                        "value": "graph"
                                    },
                                ]
                            }
                        },
                        {
                            "path": "graph2",
                            "name": "graph2",
                            "metadata": {
                                "nodeCount": 2,
                                "edgeCount": 1,
                                "properties": [
                                    {
                                        "key": "name",
                                        "value": "graph"
                                    },
                                ]
                            }
                        },
                    ]},
                    "children":{"list":[]},
                    "parent": null
                },
            }),
        );
    }
}
