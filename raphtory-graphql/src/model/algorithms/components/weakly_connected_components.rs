#[cfg(test)]
mod graphql_test {
    use crate::{graphql_test, test_support::setup_with_graphs};
    use async_graphql::Request;
    use serde_json::json;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_algorithm_weakly_connected_components() {
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(
            &[("g", graphql_test::components_test_graph())],
            tmp_dir.path(),
        )
        .await;

        // whole graph is weakly connected -> all nodes share one component
        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              weaklyConnectedComponents {
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
        // all four nodes are weakly connected -> one component
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": { "algorithm": { "weaklyConnectedComponents": {
                    "nodes": { "list": [
                        { "id": "a" }, { "id": "b" }, { "id": "c" }, { "id": "d" }
                    ] },
                    "columns": [{
                        "name": "component_id",
                        "values": [{ "prop": 0 }, { "prop": 0 }, { "prop": 0 }, { "prop": 0 }]
                    }]
                } } }
            })
        );
    }
}
