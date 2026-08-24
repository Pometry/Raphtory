use crate::{graphql_test, test_support::setup_with_graphs};
use async_graphql::Request;
use serde_json::json;
use tempfile::tempdir;

#[tokio::test]
async fn test_algorithm_min_in_degree() {
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
              minInDegree
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
                "minInDegree": 1,
            } }
        })
    );
}
