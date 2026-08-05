use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::alternating_mask::alternating_mask, db::api::view::DynamicGraph, errors::GraphError,
};

/// Alternating boolean mask over the nodes, see [`alternating_mask`].
pub(crate) struct GqlAlternatingMask;

pub(crate) struct GqlAlternatingMaskArgs;

impl GqlExecutableAlgorithm for GqlAlternatingMask {
    type Args = GqlAlternatingMaskArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, _args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(alternating_mask(graph).into())
    }
}

#[cfg(test)]
mod graphql_test {
    use crate::{graphql_test, test_support::setup_with_graphs};
    use async_graphql::Request;
    use serde_json::json;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_algorithm_alternating_mask() {
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(
            &[("g", graphql_test::scalar_metrics_test_graph())],
            tmp_dir.path(),
        )
        .await;

        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              alternatingMask {
                nodes { ids }
                columns { name values { ... on NodeStateProp { prop } } }
              }
            }
          }
        }
        "#;

        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        // the mask alternates over the nodes in order
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": { "algorithm": { "alternatingMask": {
                    "nodes": { "ids": ["a", "b", "c", "d"] },
                    "columns": [{
                        "name": "bool_col",
                        "values": [
                            { "prop": false },
                            { "prop": true },
                            { "prop": false },
                            { "prop": true }
                        ]
                    }]
                } } }
            })
        );
    }
}
