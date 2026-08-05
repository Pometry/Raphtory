#[cfg(test)]
mod graphql_test {
    use crate::{graphql_test, test_support::setup_with_graphs};
    use async_graphql::Request;
    use serde_json::json;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_algorithm_out_component() {
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(
            &[("g", graphql_test::single_component_test_graph())],
            tmp_dir.path(),
        )
        .await;

        // out component of a: nodes reachable following out-edges, keyed by distance
        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              outComponent(node: "a") {
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
        // a reaches b (1), c (2), d (3); source itself is not included
        let entry = |id: &str, distance| {
            json!({
                "node": { "id": id },
                "entries": [{ "columnName": "distance", "value": { "prop": distance } }]
            })
        };
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": { "algorithm": { "outComponent": { "rows": [
                    entry("b", 1),
                    entry("c", 2),
                    entry("d", 3),
                ] } } }
            })
        );
    }
}
