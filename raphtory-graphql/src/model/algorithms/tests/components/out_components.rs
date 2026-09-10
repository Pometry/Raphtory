use crate::test_support::setup_with_graphs;
use async_graphql::Request;
use raphtory::{
    db::api::view::MaterializedGraph,
    prelude::{AdditionOps, Graph, NO_PROPS},
};
use serde_json::json;
use tempfile::tempdir;

#[tokio::test]
async fn test_algorithm_out_components() {
    let graph = Graph::new();
    // chain a -> b -> c
    graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    graph.add_edge(2, "b", "c", NO_PROPS, None).unwrap();
    let graph: MaterializedGraph = graph.into();
    let tmp_dir = tempdir().unwrap();
    let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

    // The `out_components` column holds Nodes
    let query = r#"
        {
          graph(path: "g") {
            algorithm {
              outComponents {
                nodes { list { id } }
                columns {
                  name
                  values {
                    __typename
                    ... on Nodes { ids }
                  }
                }
              }
            }
          }
        }
        "#;

    let res = setup.schema.execute(Request::new(query)).await;
    assert_eq!(res.errors, vec![], "{:?}", res.errors);
    // component node order is not guaranteed (backed by a HashSet), so sort each set
    let mut data = res.data.into_json().unwrap();
    for col in data["graph"]["algorithm"]["outComponents"]["columns"]
        .as_array_mut()
        .unwrap()
    {
        for value in col["values"].as_array_mut().unwrap() {
            if let Some(ids) = value["ids"].as_array_mut() {
                ids.sort_by_key(|id| id.as_str().unwrap().to_string());
            }
        }
    }
    // values are row-aligned with nodes: a -> {b,c}, b -> {c}, c -> {}
    assert_eq!(
        data,
        json!({
            "graph": {
                "algorithm": {
                    "outComponents": {
                        "nodes": { "list": [{ "id": "a" }, { "id": "b" }, { "id": "c" }] },
                        "columns": [{
                            "name": "out_components",
                            "values": [
                                { "__typename": "Nodes", "ids": ["b", "c"] },
                                { "__typename": "Nodes", "ids": ["c"] },
                                { "__typename": "Nodes", "ids": [] }
                            ]
                        }]
                    }
                }
            }
        })
    );
}
