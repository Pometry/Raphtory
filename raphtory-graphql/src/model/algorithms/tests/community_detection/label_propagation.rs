use crate::{graphql_test, test_support::setup_with_graphs};
use async_graphql::Request;
use std::collections::BTreeMap;
use tempfile::tempdir;

#[tokio::test]
async fn test_algorithm_label_propagation() {
    let tmp_dir = tempdir().unwrap();
    let setup = setup_with_graphs(
        &[("g", graphql_test::community_test_graph())],
        tmp_dir.path(),
    )
    .await;

    // threads: 1 for deterministic output (multi-threaded label propagation output is non-deterministic)
    let query = r#"
        {
          graph(path: "g") {
            algorithm {
              labelPropagation(threads: 1) {
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
    // Two triangles -> two communities: {a,b,c} and {d,e,f}. Each community's id
    // is an arbitrary representative derived from a node index, and both the ids
    // and the row order depend on the storage backend, so assert the partition
    // rather than specific label values or ordering.
    let actual = res.data.into_json().unwrap();
    let result = &actual["graph"]["algorithm"]["labelPropagation"];
    assert_eq!(result["columns"][0]["name"], "community_id");

    let ids: Vec<&str> = result["nodes"]["list"]
        .as_array()
        .expect("node list should be an array")
        .iter()
        .map(|node| node["id"].as_str().expect("node id should be a string"))
        .collect();
    let labels: Vec<i64> = result["columns"][0]["values"]
        .as_array()
        .expect("values should be an array")
        .iter()
        .map(|value| {
            value["prop"]
                .as_i64()
                .expect("community id should be an int")
        })
        .collect();
    assert_eq!(ids.len(), labels.len(), "one label per node");

    let mut communities: BTreeMap<i64, Vec<&str>> = BTreeMap::new();
    for (id, label) in ids.iter().zip(&labels) {
        communities.entry(*label).or_default().push(id);
    }
    let mut partition: Vec<Vec<&str>> = communities
        .into_values()
        .map(|mut members| {
            members.sort();
            members
        })
        .collect();
    partition.sort();
    assert_eq!(partition, vec![vec!["a", "b", "c"], vec!["d", "e", "f"]]);
}
