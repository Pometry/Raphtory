#[cfg(test)]
mod graphql_test {
    use crate::{graphql_test, test_support::setup_with_graphs};
    use async_graphql::Request;
    use serde_json::json;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_algorithm_betweenness_centrality() {
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(
            &[("g", graphql_test::centrality_test_graph())],
            tmp_dir.path(),
        )
        .await;

        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              betweennessCentrality {
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
        // endpoints lie on no shortest path (0.0); middle nodes b,c each on one (1/3 normalized)
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": { "algorithm": { "betweennessCentrality": {
                    "nodes": { "list": [{ "id": "a" }, { "id": "b" }, { "id": "c" }, { "id": "d" }] },
                    "columns": [{
                        "name": "betweenness_centrality",
                        "values": [
                            { "prop": 0.0 },
                            { "prop": 0.3333333333333333 },
                            { "prop": 0.3333333333333333 },
                            { "prop": 0.0 }
                        ]
                    }]
                } } }
            })
        );
    }
}
