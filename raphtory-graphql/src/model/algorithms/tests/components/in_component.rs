use crate::{graphql_test, test_support::setup_with_graphs};
use async_graphql::Request;
use serde_json::json;
use tempfile::tempdir;

#[tokio::test]
async fn test_algorithm_in_component() {
    let tmp_dir = tempdir().unwrap();
    let setup = setup_with_graphs(
        &[("g", graphql_test::single_component_test_graph())],
        tmp_dir.path(),
    )
    .await;

    // in component of d: nodes that can reach it, keyed by distance
    let query = r#"
        {
          graph(path: "g") {
            algorithm {
              inComponent(node: "d") {
                nodes { list { id } }
                columns {
                  name
                  values { ... on NodeStateProp { prop } }
                }
              }
            }
          }
        }
        "#;
    let res = setup.schema.execute(Request::new(query)).await;
    assert_eq!(res.errors, vec![], "{:?}", res.errors);
    // a (3), b (2), c (1) can reach d; row order is not guaranteed
    let data = res.data.into_json().unwrap();
    let result = &data["graph"]["algorithm"]["inComponent"];
    assert_eq!(result["columns"][0]["name"], "distance");
    let mut rows: Vec<_> = result["nodes"]["list"]
        .as_array()
        .unwrap()
        .iter()
        .zip(result["columns"][0]["values"].as_array().unwrap())
        .map(|(node, distance)| {
            (
                node["id"].as_str().unwrap(),
                distance["prop"].as_i64().unwrap(),
            )
        })
        .collect();
    rows.sort();
    assert_eq!(rows, vec![("a", 3), ("b", 2), ("c", 1)]);
}
