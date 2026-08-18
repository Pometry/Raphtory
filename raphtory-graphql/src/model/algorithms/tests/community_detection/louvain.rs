use crate::{graphql_test, test_support::setup_with_graphs};
use async_graphql::Request;
use serde_json::json;
use tempfile::tempdir;

#[tokio::test]
async fn test_algorithm_louvain() {
    let tmp_dir = tempdir().unwrap();
    let setup = setup_with_graphs(
        &[("g", graphql_test::community_test_graph())],
        tmp_dir.path(),
    )
    .await;

    // fixed rng_seed for deterministic output
    let query = r#"
        {
          graph(path: "g") {
            algorithm {
              louvain(rngSeed: 42) {
                rows {
                  node { id }
                  entries {
                    columnName
                    value { ... on NodeStateProp { prop } }
                  }
                }
              }
            }
          }
        }
        "#;

    let res = setup.schema.execute(Request::new(query)).await;
    assert_eq!(res.errors, vec![], "{:?}", res.errors);
    // two triangles -> two communities: {a,b,c} and {d,e,f}. Row order follows
    // storage-internal node order, so rows are sorted by id before comparing.
    let entry = |id: &str, community| {
        json!({
            "node": { "id": id },
            "entries": [{ "columnName": "community_id", "value": { "prop": community } }]
        })
    };
    let mut data = res.data.into_json().unwrap();
    let rows = data["graph"]["algorithm"]["louvain"]["rows"]
        .as_array_mut()
        .unwrap();
    rows.sort_by_key(|row| row["node"]["id"].as_str().unwrap().to_string());
    assert_eq!(
        *rows,
        vec![
            entry("a", 0),
            entry("b", 0),
            entry("c", 0),
            entry("d", 1),
            entry("e", 1),
            entry("f", 1),
        ]
    );
}
