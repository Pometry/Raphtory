#[cfg(test)]
mod graphql_test {
    use crate::{graphql_test, test_support::setup_with_graphs};
    use async_graphql::Request;
    use serde_json::json;
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
        // two triangles -> two communities; ids derive from node index
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": { "algorithm": { "labelPropagation": {
                    "nodes": { "list": [
                        { "id": "a" }, { "id": "b" }, { "id": "c" },
                        { "id": "d" }, { "id": "e" }, { "id": "f" }
                    ] },
                    "columns": [{
                        "name": "community_id",
                        "values": [
                            { "prop": 2 }, { "prop": 2 }, { "prop": 2 },
                            { "prop": 600002 }, { "prop": 600002 }, { "prop": 600002 }
                        ]
                    }]
                } } }
            })
        );
    }
}
