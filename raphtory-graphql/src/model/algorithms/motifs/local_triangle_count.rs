#[cfg(test)]
mod graphql_test {
    use crate::{graphql_test, test_support::setup_with_graphs};
    use async_graphql::Request;
    use serde_json::json;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_algorithm_local_triangle_count() {
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(
            &[("g", graphql_test::scalar_metrics_test_graph())],
            tmp_dir.path(),
        )
        .await;

        // a is in the a-b-c triangle; d is a pendant with degree 1
        // only a missing node yields null
        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              inTriangle: localTriangleCount(node: "a")
              pendant: localTriangleCount(node: "d")
              missing: localTriangleCount(node: "not-a-node")
            }
          }
        }
        "#;

        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": { "algorithm": {
                    "inTriangle": 1,
                    "pendant": 0,
                    "missing": null
                } }
            })
        );
    }

    #[tokio::test]
    async fn test_algorithm_local_triangle_count_filtered() {
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(
            &[("g", graphql_test::scalar_metrics_test_graph())],
            tmp_dir.path(),
        )
        .await;

        // filtering out c breaks the a-b-c triangle, so a's local triangle count drops
        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              localTriangleCount(node: "a", filter: { nodes: { node: { field: NODE_NAME, where: { ne: { str: "c" } } } } })
            }
          }
        }
        "#;

        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({ "graph": { "algorithm": { "localTriangleCount": 0 } } })
        );
    }
}
