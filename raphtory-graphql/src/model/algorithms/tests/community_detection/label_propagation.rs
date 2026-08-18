use crate::{graphql_test, test_support::setup_with_graphs};
use async_graphql::Request;
use std::collections::{BTreeMap, BTreeSet};
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
    // Row order and the community labels both derive from storage-internal node
    // ids, so neither can be pinned literally. The contract is the partition:
    // two triangles -> two communities, {a, b, c} and {d, e, f}.
    let data = res.data.into_json().unwrap();
    let result = &data["graph"]["algorithm"]["labelPropagation"];
    let ids = result["nodes"]["list"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["id"].as_str().unwrap());
    assert_eq!(result["columns"][0]["name"], "community_id");
    let labels = result["columns"][0]["values"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["prop"].as_i64().unwrap());
    let mut communities: BTreeMap<i64, BTreeSet<&str>> = BTreeMap::new();
    for (id, label) in ids.zip(labels) {
        communities.entry(label).or_default().insert(id);
    }
    let partition: BTreeSet<BTreeSet<&str>> = communities.into_values().collect();
    let expected: BTreeSet<BTreeSet<&str>> = [
        BTreeSet::from(["a", "b", "c"]),
        BTreeSet::from(["d", "e", "f"]),
    ]
    .into();
    assert_eq!(partition, expected);
}
