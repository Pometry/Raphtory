use crate::model::{
    algorithms::{GqlDirection, GqlExecutableAlgorithm},
    graph::node_state::GqlNodeState,
};
use raphtory::{
    algorithms::pathing::dijkstra::dijkstra_single_source_shortest_paths,
    db::api::view::DynamicGraph, errors::GraphError,
};

/// Weighted single source shortest paths (Dijkstra), see [`dijkstra_single_source_shortest_paths`].
pub(crate) struct GqlDijkstra;

pub(crate) struct GqlDijkstraArgs {
    pub(crate) source: String,
    pub(crate) targets: Vec<String>,
    pub(crate) weight: Option<String>,
    pub(crate) direction: GqlDirection,
}

impl GqlExecutableAlgorithm for GqlDijkstra {
    type Args = GqlDijkstraArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let state = dijkstra_single_source_shortest_paths(
            graph,
            args.source,
            args.targets,
            args.weight.as_deref(),
            args.direction.into(),
        )?;
        Ok(state.into())
    }
}

#[cfg(test)]
mod graphql_test {
    use crate::test_support::setup_with_graphs;
    use async_graphql::Request;
    use raphtory::{
        db::api::view::MaterializedGraph,
        prelude::{AdditionOps, Graph},
    };
    use serde_json::json;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_algorithm_dijkstra() {
        let graph = Graph::new();
        // weighted chain a -> b -> c
        graph
            .add_edge(1, "a", "b", [("weight", 2.0)], None)
            .unwrap();
        graph
            .add_edge(2, "b", "c", [("weight", 3.0)], None)
            .unwrap();
        let graph: MaterializedGraph = graph.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        // Mixed columns: `distance` is a Prop, `path` is Nodes
        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              dijkstra(source: "a", targets: ["c"], weight: "weight", direction: OUT) {
                nodes { list { id } }
                columns {
                  name
                  values {
                    __typename
                    ... on NodeStateProp { prop }
                    ... on Nodes { ids }
                  }
                }
              }
            }
          }
        }
        "#;

        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        // one row (target c): distance 2+3=5, path a -> b -> c
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": {
                    "algorithm": {
                        "dijkstra": {
                            "nodes": { "list": [{ "id": "c" }] },
                            "columns": [
                                {
                                    "name": "distance",
                                    "values": [{ "__typename": "NodeStateProp", "prop": 5.0 }]
                                },
                                {
                                    "name": "path",
                                    "values": [{ "__typename": "Nodes", "ids": ["a", "b", "c"] }]
                                }
                            ]
                        }
                    }
                }
            })
        );
    }
}
