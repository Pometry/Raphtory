#[cfg(test)]
mod graphql_test {
    use crate::{graphql_test, test_support::setup_with_graphs};
    use async_graphql::Request;
    use serde_json::json;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_algorithm_strongly_connected_components() {
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(
            &[("g", graphql_test::components_test_graph())],
            tmp_dir.path(),
        )
        .await;

        // {a,b,c} form one SCC (the cycle); d is its own
        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              stronglyConnectedComponents {
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
        let entry = |id: &str, component| {
            json!({
                "node": { "id": id },
                "entries": [{ "columnName": "component_id", "value": { "prop": component } }]
            })
        };
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": { "algorithm": { "stronglyConnectedComponents": { "rows": [
                    entry("a", 0),
                    entry("b", 0),
                    entry("c", 0),
                    entry("d", 1),
                ] } } }
            })
        );
    }
}
