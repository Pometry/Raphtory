#[cfg(test)]
mod graphql_test {
    use crate::{graphql_test, test_support::setup_with_graphs};
    use async_graphql::Request;
    use serde_json::json;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_algorithm_hits() {
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(
            &[("g", graphql_test::centrality_test_graph())],
            tmp_dir.path(),
        )
        .await;

        // hits has two columns (hub_score, auth_score)
        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              hits(iterCount: 20) {
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
        // source has no auth, sink has no hub
        let s = 0.3333333333333333;
        let row = |id: &str, hub, auth| {
            json!({
                "node": { "id": id },
                "entries": [
                    { "columnName": "hub_score", "value": { "prop": hub } },
                    { "columnName": "auth_score", "value": { "prop": auth } }
                ]
            })
        };
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": { "algorithm": { "hits": { "rows": [
                    row("a", s, 0.0),
                    row("b", s, s),
                    row("c", s, s),
                    row("d", 0.0, s),
                ] } } }
            })
        );
    }
}
