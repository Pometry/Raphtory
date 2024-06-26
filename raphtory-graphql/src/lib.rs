pub use crate::server::RaphtoryServer;
use base64::{prelude::BASE64_URL_SAFE_NO_PAD, DecodeError, Engine};
use raphtory::{core::utils::errors::GraphError, db::api::view::MaterializedGraph};

pub mod model;
mod observability;
mod routes;
pub mod server;

pub mod azure_auth;

mod data;

#[derive(thiserror::Error, Debug)]
pub enum UrlDecodeError {
    #[error("Bincode operation failed")]
    BincodeError {
        #[from]
        source: Box<bincode::ErrorKind>,
    },
    #[error("Base64 decoding failed")]
    DecodeError {
        #[from]
        source: DecodeError,
    },
}

pub fn url_encode_graph<G: Into<MaterializedGraph>>(graph: G) -> Result<String, GraphError> {
    let g: MaterializedGraph = graph.into();
    Ok(BASE64_URL_SAFE_NO_PAD.encode(bincode::serialize(&g)?))
}

pub fn url_decode_graph<T: AsRef<[u8]>>(graph: T) -> Result<MaterializedGraph, UrlDecodeError> {
    Ok(bincode::deserialize(
        &BASE64_URL_SAFE_NO_PAD.decode(graph)?,
    )?)
}

#[cfg(test)]
mod graphql_test {
    use super::*;
    use crate::{data::Data, model::App};
    use async_graphql::UploadValue;
    use dynamic_graphql::{Request, Variables};
    #[cfg(feature = "storage")]
    use raphtory::disk_graph::graph_impl::DiskGraph;
    use raphtory::{
        db::{api::view::IntoDynamic, graph::views::deletion_graph::PersistentGraph},
        prelude::*,
    };
    use serde_json::json;
    use std::collections::{HashMap, HashSet};
    use tempfile::{tempdir, TempDir};

    #[tokio::test]
    async fn search_for_gandalf_query() {
        let graph = PersistentGraph::new();
        graph
            .add_node(
                0,
                "Gandalf",
                [("kind".to_string(), Prop::str("wizard"))],
                None,
            )
            .expect("Could not add node!");
        graph
            .add_node(
                0,
                "Frodo",
                [("kind".to_string(), Prop::str("Hobbit"))],
                None,
            )
            .expect("Could not add node!");

        let graph: MaterializedGraph = graph.into();
        let graphs = HashMap::from([("lotr".to_string(), graph)]);
        let data = Data::from_map(graphs);
        let schema = App::create_schema().data(data).finish().unwrap();

        let query = r#"
        {
          graph(name: "lotr") {
            searchNodes(query: "kind:wizard", limit: 10, offset: 0) {
              name
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
                    "searchNodes": [
                        {
                            "name": "Gandalf"
                        }
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

        let graph: MaterializedGraph = graph.into();
        let graphs = HashMap::from([("lotr".to_string(), graph)]);
        let data = Data::from_map(graphs);

        let schema = App::create_schema().data(data).finish().unwrap();

        let query = r#"
        {
          graph(name: "lotr") {
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
    async fn query_nodefilter() {
        let graph = Graph::new();
        graph
            .add_node(
                0,
                1,
                [("pgraph", Prop::PersistentGraph(PersistentGraph::new()))],
                None,
            )
            .unwrap();
        let graph: MaterializedGraph = graph.into();

        let graphs = HashMap::from([("graph".to_string(), graph)]);
        let data = Data::from_map(graphs);
        let schema = App::create_schema().data(data).finish().unwrap();
        let prop_has_key_filter = r#"
        {
          graph(name: "graph") {
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

        let graphs = HashMap::from([("graph".to_string(), g)]);
        let data = Data::from_map(graphs);
        let schema = App::create_schema().data(data).finish().unwrap();

        let prop_has_key_filter = r#"
        {
          graph(name: "graph") {
            properties {
              temporal {
                values {
                  unique
                }
              }
            }
            node(name: "3") {
              properties {
                temporal {
                  values {
                    unique
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
                    unique
                  }
                }
              }
            }
          }
        }
        "#;

        let req = Request::new(prop_has_key_filter);
        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();
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

        let graphs = HashMap::from([("graph".to_string(), g)]);
        let data = Data::from_map(graphs);
        let schema = App::create_schema().data(data).finish().unwrap();

        let prop_has_key_filter = r#"
        {
          graph(name: "graph") {
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
                [("pgraph", Prop::PersistentGraph(PersistentGraph::new()))],
                None,
            )
            .unwrap();

        let graphs = HashMap::from([("graph".to_string(), graph)]);
        let data = Data::from_map(graphs);
        let schema = App::create_schema().data(data).finish().unwrap();
        let prop_has_key_filter = r#"
        {
          graph(name: "graph") {
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
    async fn test_mutation() {
        let test_dir = tempdir().unwrap();
        let g0 = PersistentGraph::new();
        let test_dir_path = test_dir.path().to_str().unwrap().replace(r#"\"#, r#"\\"#);
        let f0 = &test_dir.path().join("g0");
        let f1 = &test_dir.path().join("g1");
        g0.save_to_file(f0).unwrap();

        let g1 = PersistentGraph::new();
        g1.add_node(0, 1, [("name", "1")], None).unwrap();

        let g2 = PersistentGraph::new();
        g2.add_node(0, 2, [("name", "2")], None).unwrap();

        let data = Data::default();
        let schema = App::create_schema().data(data).finish().unwrap();

        let list_graphs = r#"
        {
          graphs {
            name
          }
        }"#;

        let list_nodes = |name: &str| {
            format!(
                r#"{{
                  graph(name: "{}") {{
                    nodes {{
                      list {{
                        id
                      }}
                    }}
                  }}
                }}"#,
                name
            )
        };

        let load_all = &format!(
            r#"mutation {{
              loadGraphsFromPath(path: "{}")
            }}"#,
            test_dir_path
        );

        let load_new = &format!(
            r#"mutation {{
              loadNewGraphsFromPath(path: "{}")
            }}"#,
            test_dir_path
        );

        let save_graph = |parent_name: &str, nodes: &str| {
            format!(
                r#"mutation {{
                  saveGraph(
                    parentGraphName: "{parent_name}",
                    graphName: "{parent_name}",
                    newGraphName: "g2",
                    props: "{{}}",
                    isArchive: 0,
                    graphNodes: {nodes},
                  )
              }}"#
            )
        };

        // only g0 which is empty
        let req = Request::new(load_all);
        let res = schema.execute(req).await;
        let res_json = res.data.into_json().unwrap();
        assert_eq!(res_json, json!({"loadGraphsFromPath": ["g0"]}));

        let req = Request::new(list_graphs);
        let res = schema.execute(req).await;
        let res_json = res.data.into_json().unwrap();
        assert_eq!(res_json, json!({"graphs": [{"name": "g0"}]}));

        let req = Request::new(list_nodes("g0"));
        let res = schema.execute(req).await;
        let res_json = res.data.into_json().unwrap();
        assert_eq!(res_json, json!({"graph": {"nodes": {"list": []}}}));

        // add g1 to folder and replace g0 with g2 and load new graphs
        g1.save_to_file(f1).unwrap();
        g2.save_to_file(f0).unwrap();
        let req = Request::new(load_new);
        let res = schema.execute(req).await;
        let res_json = res.data.into_json().unwrap();
        assert_eq!(res_json, json!({"loadNewGraphsFromPath": ["g1"]}));

        // g0 is still empty
        let req = Request::new(list_nodes("g0"));
        let res = schema.execute(req).await;
        let res_json = res.data.into_json().unwrap();
        assert_eq!(res_json, json!({"graph": {"nodes": {"list": []}}}));

        // g1 has node 1
        let req = Request::new(list_nodes("g1"));
        let res = schema.execute(req).await;
        let res_json = res.data.into_json().unwrap();
        assert_eq!(
            res_json,
            json!({"graph": {"nodes": {"list": [{"id": "1"}]}}})
        );

        // reload all graphs from folder
        let req = Request::new(load_all);
        schema.execute(req).await;

        // g0 now has node 2
        let req = Request::new(list_nodes("g0"));
        let res = schema.execute(req).await;
        let res_json = res.data.into_json().unwrap();
        assert_eq!(
            res_json,
            json!({"graph": {"nodes": {"list": [{"id": "2"}]}}})
        );

        // g1 still has node 1
        let req = Request::new(list_nodes("g1"));
        let res = schema.execute(req).await;
        let res_json = res.data.into_json().unwrap();
        assert_eq!(
            res_json,
            json!({"graph": {"nodes": {"list": [{"id": "1"}]}}})
        );

        // test save graph
        let req = Request::new(save_graph("g0", r#""{ \"2\": {} }""#));
        let res = schema.execute(req).await;
        println!("{:?}", res.errors);
        assert!(res.errors.is_empty());
        let req = Request::new(list_nodes("g2"));
        let res = schema.execute(req).await;
        let res_json = res.data.into_json().unwrap();
        assert_eq!(
            res_json,
            json!({"graph": {"nodes": {"list": [{"id": "2"}]}}})
        );

        // test save graph overwrite
        let req = Request::new(save_graph("g1", r#""{ \"1\": {} }""#));
        let res = schema.execute(req).await;
        println!("{:?}", res.errors);
        assert!(res.errors.is_empty());
        let req = Request::new(list_nodes("g2"));
        let res = schema.execute(req).await;
        println!("{:?}", res);
        let res_json = res.data.into_json().unwrap();
        assert_eq!(
            res_json,
            json!({"graph": {"nodes": {"list": [{"id": "1"}]}}})
        );

        // reload all graphs from folder
        let req = Request::new(load_all);
        schema.execute(req).await;
        // g2 is still the last version
        let req = Request::new(list_nodes("g2"));
        let res = schema.execute(req).await;
        println!("{:?}", res);
        let res_json = res.data.into_json().unwrap();
        assert_eq!(
            res_json,
            json!({"graph": {"nodes": {"list": [{"id": "1"}]}}})
        );
    }

    #[tokio::test]
    async fn test_graph_injection() {
        let g = PersistentGraph::new();
        g.add_node(0, 1, NO_PROPS, None).unwrap();
        let tmp_file = tempfile::NamedTempFile::new().unwrap();
        let path = tmp_file.path();
        g.save_to_file(path).unwrap();
        let file = std::fs::File::open(path).unwrap();
        let upload_val = UploadValue {
            filename: "test".into(),
            content_type: Some("application/octet-stream".into()),
            content: file,
        };

        let data = Data::default();
        let schema = App::create_schema().data(data).finish().unwrap();

        let query = r##"
        mutation($file: Upload!) {
            uploadGraph(name: "test", graph: $file)
        }
        "##;

        let variables = json!({ "file": null });
        let mut req = Request::new(query).variables(Variables::from_json(variables));
        req.set_upload("variables.file", upload_val);
        let res = schema.execute(req).await;
        println!("{:?}", res);
        assert_eq!(res.errors.len(), 0);
        let res_json = res.data.into_json().unwrap();
        assert_eq!(res_json, json!({"uploadGraph": "test"}));

        let list_nodes = r#"
        query {
            graph(name: "test") {
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

        let data = Data::default();
        let schema = App::create_schema().data(data).finish().unwrap();

        let query = r#"
        mutation($graph: String!) {
            sendGraph(name: "test", graph: $graph)
        }
        "#;
        let req =
            Request::new(query).variables(Variables::from_json(json!({ "graph": graph_str })));

        let res = schema.execute(req).await;
        assert_eq!(res.errors.len(), 0);
        let res_json = res.data.into_json().unwrap();
        assert_eq!(res_json, json!({"sendGraph": "test"}));

        let list_nodes = r#"
        query {
            graph(name: "test") {
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
            receiveGraph(name: "test")
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

        let graphs = HashMap::from([("graph".to_string(), graph)]);
        let data = Data::from_map(graphs);
        let schema = App::create_schema().data(data).finish().unwrap();

        let req = r#"
        {
          graph(name: "graph") {
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
          graph(name: "graph") {
            nodes {
              typeFilter(nodeTypes: ["a"]) {
                list{
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

        let test_dir = TempDir::new().unwrap();
        let disk_graph = DiskGraph::from_graph(&graph, test_dir.path()).unwrap();
        let graph: MaterializedGraph = disk_graph.into();

        let graphs = HashMap::from([("graph".to_string(), graph)]);
        let data = Data::from_map(graphs);
        let schema = App::create_schema().data(data).finish().unwrap();

        let req = r#"
        {
          graph(name: "graph") {
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

        let req = &format!(
            r#"mutation {{
              updateGraphLastOpened(graphName: "{}")
            }}"#,
            "graph"
        );

        let req = Request::new(req);
        let res = schema.execute(req).await;
        let data = res.errors;
        let error_message = &data[0].message;
        let expected_error_message = "Disk Graph is immutable";
        assert_eq!(error_message, expected_error_message);
    }
}
