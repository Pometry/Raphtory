use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::metrics::reciprocity::all_local_reciprocity, db::api::view::DynamicGraph,
    errors::GraphError,
};

/// Local reciprocity of every node, see [`all_local_reciprocity`].
pub(crate) struct GqlAllLocalReciprocity;

pub(crate) struct GqlAllLocalReciprocityArgs;

impl GqlExecutableAlgorithm for GqlAllLocalReciprocity {
    type Args = GqlAllLocalReciprocityArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, _args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(all_local_reciprocity(graph).into())
    }
}

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
    async fn test_algorithm_all_local_reciprocity() {
        let graph = Graph::new();
        // a<->b reciprocated, a->c not
        graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        graph.add_edge(2, "b", "a", NO_PROPS, None).unwrap();
        graph.add_edge(3, "a", "c", NO_PROPS, None).unwrap();
        let graph: MaterializedGraph = graph.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              allLocalReciprocity {
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
        // a: 2 of 3 edges reciprocated; b: fully reciprocated; c: none
        let entry = |id: &str, reciprocity| {
            json!({
                "node": { "id": id },
                "entries": [{ "columnName": "reciprocity", "value": { "prop": reciprocity } }]
            })
        };
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": { "algorithm": { "allLocalReciprocity": { "rows": [
                    entry("a", 0.6666666666666666),
                    entry("b", 1.0),
                    entry("c", 0.0),
                ] } } }
            })
        );
    }
}
