#[cfg(test)]
mod graphql_test {
    use crate::test_support::setup_with_graphs;
    use async_graphql::Request;
    use raphtory::{
        db::api::view::MaterializedGraph,
        prelude::{AdditionOps, Graph, NO_PROPS},
    };
    use serde_json::json;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_algorithm_local_clustering_coefficient_batch() {
        let graph = Graph::new();
        // triangle a-b-c
        graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        graph.add_edge(2, "b", "c", NO_PROPS, None).unwrap();
        graph.add_edge(3, "c", "a", NO_PROPS, None).unwrap();
        let graph: MaterializedGraph = graph.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              localClusteringCoefficientBatch(nodes: ["a", "b"]) {
                rows {
                  node { id }
                  entries { columnName value { ... on NodeStateProp { prop } } }
                }
              }
            }
          }
        }
        "#;

        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        // only the queried nodes are present; each is in a triangle -> coefficient 1.0
        let entry = |id: &str| {
            json!({
                "node": { "id": id },
                "entries": [{ "columnName": "lcc", "value": { "prop": 1.0 } }]
            })
        };
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": { "algorithm": { "localClusteringCoefficientBatch": { "rows": [
                    entry("a"),
                    entry("b"),
                ] } } }
            })
        );
    }
}
