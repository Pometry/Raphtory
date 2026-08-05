use crate::model::{
    algorithms::{filtered_view, GqlExecutableAlgorithm},
    graph::{filtering::GqlViewFilter, node_id::GqlNodeId},
};
use raphtory::{
    algorithms::motifs::local_triangle_count::local_triangle_count, db::api::view::DynamicGraph,
    errors::GraphError,
};

/// Local triangle count of a single node, see [`local_triangle_count`].
pub(crate) struct GqlLocalTriangleCount;

pub(crate) struct GqlLocalTriangleCountArgs {
    pub(crate) node: GqlNodeId,
    pub(crate) filter: Option<GqlViewFilter>,
}

impl GqlExecutableAlgorithm for GqlLocalTriangleCount {
    type Args = GqlLocalTriangleCountArgs;
    type Output = Option<usize>;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let view = filtered_view(graph, args.filter)?;
        Ok(local_triangle_count(&view, args.node))
    }
}

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
