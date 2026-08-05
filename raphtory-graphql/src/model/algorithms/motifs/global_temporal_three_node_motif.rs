#[cfg(test)]
mod graphql_test {
    use crate::test_support::setup_with_graphs;
    use async_graphql::Request;
    use raphtory::{
        db::api::view::MaterializedGraph,
        prelude::{AdditionOps, Graph, NO_PROPS},
    };
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_algorithm_global_temporal_three_node_motif() {
        let graph = Graph::new();
        // a -> b -> c -> a, each edge at a distinct time, so triangle motifs are counted
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
              single: globalTemporalThreeNodeMotif(delta: 10)
              multi: globalTemporalThreeNodeMotifMulti(deltas: [10, 1]) { delta counts }
            }
          }
        }
        "#;

        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        let data = res.data.into_json().unwrap();
        let single = data["graph"]["algorithm"]["single"].as_array().unwrap();
        let multi = data["graph"]["algorithm"]["multi"].as_array().unwrap();

        // 40 counts: 8 two-node + 24 star + 8 triangle
        assert_eq!(single.len(), 40);
        // one row per delta, and the first row is the same as the single-delta call
        assert_eq!(multi.len(), 2);
        assert!(multi
            .iter()
            .all(|row| row["counts"].as_array().unwrap().len() == 40));
        assert_eq!(multi[0]["delta"], 10);
        assert_eq!(multi[1]["delta"], 1);
        assert_eq!(multi[0]["counts"].as_array().unwrap(), single);
        // delta 10 spans the whole triangle so it finds motifs delta 1 does not
        assert!(
            single.iter().any(|c| c.as_u64().unwrap() > 0),
            "expected some motifs at delta 10, got {single:?}"
        );
        assert_ne!(multi[0]["counts"], multi[1]["counts"]);
    }
}
