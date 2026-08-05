use crate::{
    graphql_test,
    model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState},
    test_support::setup_with_graphs,
};
use async_graphql::Request;
use raphtory::{
    algorithms::centrality::degree_centrality::degree_centrality, db::api::view::DynamicGraph,
    errors::GraphError,
};
use serde_json::json;
use tempfile::tempdir;

/// Degree centrality, see [`degree_centrality`].
pub(crate) struct GqlDegreeCentrality;

pub(crate) struct GqlDegreeCentralityArgs;

impl GqlExecutableAlgorithm for GqlDegreeCentrality {
    type Args = GqlDegreeCentralityArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, _args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(degree_centrality(graph).into())
    }
}

#[cfg(test)]
mod graphql_test {
    use crate::{graphql_test, test_support::setup_with_graphs};
    use async_graphql::Request;
    use serde_json::json;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_algorithm_degree_centrality() {
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
              degreeCentrality {
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
        // degree/max_degree: endpoints 0.5, middle nodes 1.0
        let entry = |id: &str, prop| {
            json!({
                "node": { "id": id },
                "entries": [{ "columnName": "degree_centrality", "value": { "prop": prop } }]
            })
        };
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": { "algorithm": { "degreeCentrality": { "rows": [
                    entry("a", 0.5),
                    entry("b", 1.0),
                    entry("c", 1.0),
                    entry("d", 0.5),
                ] } } }
            })
        );
    }
}
