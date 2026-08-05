use crate::model::{
    algorithms::{filtered_view, GqlExecutableAlgorithm},
    graph::{filtering::GqlViewFilter, node_id::GqlNodeId},
};
use raphtory::{
    algorithms::metrics::clustering_coefficient::local_clustering_coefficient::local_clustering_coefficient,
    db::api::view::DynamicGraph, errors::GraphError,
};

/// Local clustering coefficient of a single node, see [`local_clustering_coefficient`].
pub(crate) struct GqlLocalClusteringCoefficient;

pub(crate) struct GqlLocalClusteringCoefficientArgs {
    pub(crate) node: GqlNodeId,
    pub(crate) filter: Option<GqlViewFilter>,
}

impl GqlExecutableAlgorithm for GqlLocalClusteringCoefficient {
    type Args = GqlLocalClusteringCoefficientArgs;
    type Output = Option<f64>;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let view = filtered_view(graph, args.filter)?;
        Ok(local_clustering_coefficient(&view, args.node))
    }
}

#[cfg(test)]
mod graphql_test {
    use crate::{graphql_test, test_support::setup_with_graphs};
    use async_graphql::Request;
    use serde_json::json;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_algorithm_local_clustering_coefficient() {
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
              inTriangle: localClusteringCoefficient(node: "a")
              pendant: localClusteringCoefficient(node: "d")
              missing: localClusteringCoefficient(node: "not-a-node")
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
                    "inTriangle": 1.0,
                    "pendant": 0.0,
                    "missing": null
                } }
            })
        );
    }
}
