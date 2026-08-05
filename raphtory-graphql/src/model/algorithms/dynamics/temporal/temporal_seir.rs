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
    async fn test_algorithm_temporal_seir() {
        let graph = Graph::new();
        // a chain so the infection can spread forward in time
        graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        graph.add_edge(2, "b", "c", NO_PROPS, None).unwrap();
        graph.add_edge(3, "c", "d", NO_PROPS, None).unwrap();
        let graph: MaterializedGraph = graph.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        // seeding an explicit node with certain infection spreads along the chain;
        // rngSeed keeps the run reproducible
        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              temporalSeir(
                seeds: { nodes: ["a"] }
                infectionProb: 1.0
                initialInfection: 0
                rngSeed: 42
              ) {
                nodes { ids }
                columnNames
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
                "graph": { "algorithm": { "temporalSeir": {
                    "nodes": { "ids": ["a", "b", "c", "d"] },
                    "columnNames": ["infected", "active", "recovered"]
                } } }
            })
        );
    }

    #[tokio::test]
    async fn test_algorithm_temporal_seir_seed_variants() {
        let graph = Graph::new();
        graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        graph.add_edge(2, "b", "c", NO_PROPS, None).unwrap();
        graph.add_edge(3, "c", "d", NO_PROPS, None).unwrap();
        let graph: MaterializedGraph = graph.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        // all three Seeds variants are accepted; number/probability pick nodes at random
        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              byNumber: temporalSeir(
                seeds: { number: 2 }
                infectionProb: 0.0
                initialInfection: 0
                rngSeed: 7
              ) { count }
              byProbability: temporalSeir(
                seeds: { probability: 0.5 }
                infectionProb: 0.0
                initialInfection: 0
                rngSeed: 7
              ) { count }
            }
          }
        }
        "#;

        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        // With no onward infection only the seeds appear. `number` samples exactly that
        // many nodes; `probability` seeds each node independently
        let data = res.data.into_json().unwrap();
        assert_eq!(data["graph"]["algorithm"]["byNumber"]["count"], 2);
        let by_probability = data["graph"]["algorithm"]["byProbability"]["count"]
            .as_u64()
            .unwrap();
        assert!(
            by_probability <= 4,
            "expected at most every node to be seeded, got {by_probability}"
        );
    }
}
