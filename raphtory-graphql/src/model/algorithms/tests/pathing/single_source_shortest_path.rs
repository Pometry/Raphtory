use crate::test_support::setup_with_graphs;
use async_graphql::Request;
use raphtory::{
    db::api::view::MaterializedGraph,
    prelude::{AdditionOps, Graph, NO_PROPS},
};
use serde_json::json;
use tempfile::tempdir;

#[tokio::test]
async fn test_algorithm_single_source_shortest_path() {
    let graph = Graph::new();
    // simple chain a -> b -> c
    graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    graph.add_edge(2, "b", "c", NO_PROPS, None).unwrap();
    let graph: MaterializedGraph = graph.into();
    let tmp_dir = tempdir().unwrap();
    let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

    // The `path` column holds Nodes, not a Prop
    let query = r#"
        {
          graph(path: "g") {
            algorithm {
              singleSourceShortestPath(source: "a") {
                columnNames
                rows {
                  node { id }
                  entries {
                    columnName
                    value {
                      __typename
                      ... on Nodes { list { id } }
                    }
                  }
                }
                min(column: "path") { value }
                mean(column: "path")
              }
            }
          }
        }
        "#;

    let res = setup.schema.execute(Request::new(query)).await;
    assert_eq!(res.errors, vec![], "{:?}", res.errors);
    // row order is not guaranteed
    let mut data = res.data.into_json().unwrap();
    data["graph"]["algorithm"]["singleSourceShortestPath"]["rows"]
        .as_array_mut()
        .unwrap()
        .sort_by_key(|row| row["node"]["id"].as_str().unwrap().to_string());
    assert_eq!(
        data,
        json!({
            "graph": {
                "algorithm": {
                    "singleSourceShortestPath": {
                        "columnNames": ["path"],
                        "rows": [
                            {
                                "node": { "id": "a" },
                                "entries": [{
                                    "columnName": "path",
                                    "value": {
                                        "__typename": "Nodes",
                                        "list": [{ "id": "a" }]
                                    }
                                }]
                            },
                            {
                                "node": { "id": "b" },
                                "entries": [{
                                    "columnName": "path",
                                    "value": {
                                        "__typename": "Nodes",
                                        "list": [{ "id": "a" }, { "id": "b" }]
                                    }
                                }]
                            },
                            {
                                "node": { "id": "c" },
                                "entries": [{
                                    "columnName": "path",
                                    "value": {
                                        "__typename": "Nodes",
                                        "list": [{ "id": "a" }, { "id": "b" }, { "id": "c" }]
                                    }
                                }]
                            }
                        ],
                        // node-valued column: numeric aggregates return null
                        "min": null,
                        "mean": null
                    }
                }
            }
        })
    );
}
