use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::pathing::single_source_shortest_path::single_source_shortest_path,
    db::api::view::DynamicGraph, errors::GraphError,
};

/// Single source shortest path (unweighted BFS), see [`single_source_shortest_path`].
pub(crate) struct GqlSingleSourceShortestPath;

pub(crate) struct GqlSingleSourceShortestPathArgs {
    pub(crate) source: String,
    pub(crate) cutoff: Option<usize>,
}

impl GqlExecutableAlgorithm for GqlSingleSourceShortestPath {
    type Args = GqlSingleSourceShortestPathArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let state = single_source_shortest_path(graph, args.source, args.cutoff);
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
    async fn test_algorithm_single_source_shortest_path() {
        let graph = Graph::new();
        // simple chain a -> b -> c
        graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        graph.add_edge(2, "b", "c", NO_PROPS, None).unwrap();
        let graph: MaterializedGraph = graph.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        // The `path` column holds Nodes, not a Prop
        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              singleSourceShortestPath(source: "a") {
                columnNames
                rows {
                  node { id }
                  entries {
                    columnName
                    value {
                      __typename
                      ... on Nodes { list { id } }
                    }
                  }
                }
                min(column: "path") { value }
                mean(column: "path")
              }
            }
          }
        }
        "#;

        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": {
                    "algorithm": {
                        "singleSourceShortestPath": {
                            "columnNames": ["path"],
                            "rows": [
                                {
                                    "node": { "id": "a" },
                                    "entries": [{
                                        "columnName": "path",
                                        "value": {
                                            "__typename": "Nodes",
                                            "list": [{ "id": "a" }]
                                        }
                                    }]
                                },
                                {
                                    "node": { "id": "b" },
                                    "entries": [{
                                        "columnName": "path",
                                        "value": {
                                            "__typename": "Nodes",
                                            "list": [{ "id": "a" }, { "id": "b" }]
                                        }
                                    }]
                                },
                                {
                                    "node": { "id": "c" },
                                    "entries": [{
                                        "columnName": "path",
                                        "value": {
                                            "__typename": "Nodes",
                                            "list": [{ "id": "a" }, { "id": "b" }, { "id": "c" }]
                                        }
                                    }]
                                }
                            ],
                            // node-valued column: numeric aggregates return null
                            "min": null,
                            "mean": null
                        }
                    }
                }
            })
        );
    }
}
