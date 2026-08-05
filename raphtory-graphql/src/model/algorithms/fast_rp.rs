use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::embeddings::fast_rp::fast_rp, db::api::view::DynamicGraph, errors::GraphError,
};

/// FastRP node embeddings, see [`fast_rp`].
pub(crate) struct GqlFastRp;

pub(crate) struct GqlFastRpArgs {
    pub(crate) embedding_dim: usize,
    pub(crate) normalization_strength: f64,
    pub(crate) iter_weights: Vec<f64>,
    pub(crate) seed: Option<u64>,
    pub(crate) threads: Option<usize>,
}

impl GqlExecutableAlgorithm for GqlFastRp {
    type Args = GqlFastRpArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let state = fast_rp(
            graph,
            args.embedding_dim,
            args.normalization_strength,
            args.iter_weights,
            args.seed,
            args.threads,
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
    async fn test_algorithm_fast_rp() {
        let graph = Graph::new();
        graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        graph.add_edge(2, "b", "c", NO_PROPS, None).unwrap();
        graph.add_edge(3, "c", "a", NO_PROPS, None).unwrap();
        let graph: MaterializedGraph = graph.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        let query = r#"
    {
      graph(path: "g") {
        algorithm {
          fastRp(embeddingDim: 4, normalizationStrength: 1.0, iterWeights: [1.0, 1.0], seed: 42, threads: 1) {
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
        // each embedding is a 4d vector (embeddingDim); values are deterministic given the seed
        let row = |id: &str, embedding: [f64; 4]| {
            json!({
                "node": { "id": id },
                "entries": [{ "columnName": "embedding_state", "value": { "prop": embedding } }]
            })
        };
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": { "algorithm": { "fastRp": {
                    "columnNames": ["embedding_state"],
                    "rows": [
                        row("a", [-0.9870555097143693, 0.3290185032381231, -1.6450925161906156, 0.0]),
                        row("b", [0.9870555097143693, 0.3290185032381231, -1.6450925161906156, -0.9870555097143693]),
                        row("c", [0.0, 1.3160740129524924, -0.6580370064762462, 0.9870555097143693]),
                    ]
                } } }
            })
        );
    }
}
