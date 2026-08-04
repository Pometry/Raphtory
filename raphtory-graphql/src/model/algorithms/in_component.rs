use crate::model::{
    algorithms::{filtered_view, GqlExecutableAlgorithm},
    graph::{filtering::GqlViewFilter, node_id::GqlNodeId, node_state::GqlNodeState},
};
use raphtory::{
    algorithms::components::in_component, db::api::view::DynamicGraph, errors::GraphError,
    prelude::*,
};

/// In component of a single node, see [`in_component`].
pub(crate) struct GqlInComponent;

pub(crate) struct GqlInComponentArgs {
    pub(crate) node: GqlNodeId,
    pub(crate) filter: Option<GqlViewFilter>,
}

impl GqlExecutableAlgorithm for GqlInComponent {
    type Args = GqlInComponentArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let view = filtered_view(graph, args.filter)?;
        let node = view
            .node(args.node.clone())
            .ok_or_else(|| GraphError::NodeMissingError(args.node.into()))?;
        Ok(in_component(node).into())
    }
}

#[cfg(test)]
mod graphql_test {
    use crate::{graphql_test, test_support::setup_with_graphs};
    use async_graphql::Request;
    use serde_json::json;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_algorithm_in_component() {
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(
            &[("g", graphql_test::single_component_test_graph())],
            tmp_dir.path(),
        )
        .await;

        // in component of d: nodes that can reach it, keyed by distance
        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              inComponent(node: "d") {
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
        // a (3), b (2), c (1) can reach d; row order follows the key index (a, b, c)
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": { "algorithm": { "inComponent": {
                    "nodes": { "list": [{ "id": "a" }, { "id": "b" }, { "id": "c" }] },
                    "columns": [{
                        "name": "distance",
                        "values": [{ "prop": 3 }, { "prop": 2 }, { "prop": 1 }]
                    }]
                } } }
            })
        );
    }
}
