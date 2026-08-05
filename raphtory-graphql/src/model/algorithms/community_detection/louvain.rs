use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::community_detection::{louvain::louvain, modularity::ModularityUnDir},
    db::api::view::DynamicGraph,
    errors::GraphError,
};

/// Louvain community detection, see [`louvain`].
pub(crate) struct GqlLouvain;

pub(crate) struct GqlLouvainArgs {
    pub(crate) resolution: f64,
    pub(crate) weight_prop: Option<String>,
    pub(crate) tol: Option<f64>,
    pub(crate) rng_seed: Option<u64>,
}

impl GqlExecutableAlgorithm for GqlLouvain {
    type Args = GqlLouvainArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let state = louvain::<ModularityUnDir, _>(
            graph,
            args.resolution,
            args.weight_prop.as_deref(),
            args.tol,
            args.rng_seed,
        );
        Ok(state.into())
    }
}

#[cfg(test)]
mod graphql_test {
    use crate::{graphql_test, test_support::setup_with_graphs};
    use async_graphql::Request;
    use serde_json::json;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_algorithm_louvain() {
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(
            &[("g", graphql_test::community_test_graph())],
            tmp_dir.path(),
        )
        .await;

        // fixed rng_seed for deterministic output
        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              louvain(rngSeed: 42) {
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
        // two triangles -> two communities: {a,b,c} and {d,e,f}
        let entry = |id: &str, community| {
            json!({
                "node": { "id": id },
                "entries": [{ "columnName": "community_id", "value": { "prop": community } }]
            })
        };
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": { "algorithm": { "louvain": { "rows": [
                    entry("a", 0),
                    entry("b", 0),
                    entry("c", 0),
                    entry("d", 1),
                    entry("e", 1),
                    entry("f", 1),
                ] } } }
            })
        );
    }
}
