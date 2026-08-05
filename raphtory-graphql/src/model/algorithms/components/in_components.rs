use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::components::in_components, db::api::view::DynamicGraph, errors::GraphError,
};

/// In components, see [`in_components`].
pub(crate) struct GqlInComponents;

pub(crate) struct GqlInComponentsArgs {
    pub(crate) threads: Option<usize>,
}

impl GqlExecutableAlgorithm for GqlInComponents {
    type Args = GqlInComponentsArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let state = in_components(graph, args.threads);
        Ok(state.into())
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
    async fn test_algorithm_in_components() {
        let graph = Graph::new();
        // chain a -> b -> c
        graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        graph.add_edge(2, "b", "c", NO_PROPS, None).unwrap();
        let graph: MaterializedGraph = graph.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        // The `in_components` column holds Nodes
        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              inComponents {
                rows {
                  node { id }
                  entries {
                    columnName
                    value {
                      __typename
                      ... on Nodes { ids }
                    }
                  }
                }
              }
            }
          }
        }
        "#;

        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        // component node order is not guaranteed (backed by a HashSet), so sort each set
        let mut data = res.data.into_json().unwrap();
        for row in data["graph"]["algorithm"]["inComponents"]["rows"]
            .as_array_mut()
            .unwrap()
        {
            for entry in row["entries"].as_array_mut().unwrap() {
                if let Some(ids) = entry["value"]["ids"].as_array_mut() {
                    ids.sort_by_key(|id| id.as_str().unwrap().to_string());
                }
            }
        }
        // in_components: a <- {}, b <- {a}, c <- {a,b}
        assert_eq!(
            data,
            json!({
                "graph": {
                    "algorithm": {
                        "inComponents": {
                            "rows": [
                                {
                                    "node": { "id": "a" },
                                    "entries": [{
                                        "columnName": "in_components",
                                        "value": { "__typename": "Nodes", "ids": [] }
                                    }]
                                },
                                {
                                    "node": { "id": "b" },
                                    "entries": [{
                                        "columnName": "in_components",
                                        "value": { "__typename": "Nodes", "ids": ["a"] }
                                    }]
                                },
                                {
                                    "node": { "id": "c" },
                                    "entries": [{
                                        "columnName": "in_components",
                                        "value": { "__typename": "Nodes", "ids": ["a", "b"] }
                                    }]
                                }
                            ]
                        }
                    }
                }
            })
        );
    }
}
