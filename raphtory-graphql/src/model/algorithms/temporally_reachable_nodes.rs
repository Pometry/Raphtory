use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::pathing::temporal_reachability::temporally_reachable_nodes,
    db::api::view::DynamicGraph, errors::GraphError,
};

/// Temporally reachable nodes, see [`temporally_reachable_nodes`].
pub(crate) struct GqlTemporallyReachableNodes;

pub(crate) struct GqlTemporallyReachableNodesArgs {
    pub(crate) max_hops: usize,
    pub(crate) start_time: i64,
    pub(crate) seed_nodes: Vec<String>,
    pub(crate) stop_nodes: Option<Vec<String>>,
    pub(crate) threads: Option<usize>,
}

impl GqlExecutableAlgorithm for GqlTemporallyReachableNodes {
    type Args = GqlTemporallyReachableNodesArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let state = temporally_reachable_nodes(
            graph,
            args.threads,
            args.max_hops,
            args.start_time,
            args.seed_nodes,
            args.stop_nodes,
        );
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
    async fn test_algorithm_temporally_reachable_nodes() {
        let graph = Graph::new();
        graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        graph.add_edge(2, "b", "c", NO_PROPS, None).unwrap();
        let graph: MaterializedGraph = graph.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              temporallyReachableNodes(maxHops: 5, startTime: 0, seedNodes: ["a"], threads: 1) {
                columnNames
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
        // each node is tainted by (time, source); tuples serialize as {"0": time, "1": source}
        let row = |id: &str, taint: serde_json::Value| {
            json!({
                "node": { "id": id },
                "entries": [{ "columnName": "reachable_nodes", "value": { "prop": [taint] } }]
            })
        };
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": { "algorithm": { "temporallyReachableNodes": {
                    "columnNames": ["reachable_nodes"],
                    "rows": [
                        row("a", json!({ "0": 0, "1": "start" })),
                        row("b", json!({ "0": 1, "1": "a" })),
                        row("c", json!({ "0": 2, "1": "b" })),
                    ]
                } } }
            })
        );
    }
}
