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
    async fn test_algorithm_temporal_rich_club_coefficient() {
        let graph = Graph::new();
        // a triangle a-b-c repeated at every time step, so it persists across
        // every snapshot, plus a pendant d that never joins the club
        for t in 1..=4 {
            for (src, dst) in [("a", "b"), ("b", "c"), ("c", "a")] {
                graph.add_edge(t, src, dst, NO_PROPS, None).unwrap();
            }
        }
        graph.add_edge(1, "c", "d", NO_PROPS, None).unwrap();
        let graph: MaterializedGraph = graph.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        // one snapshot per time step; the triangle persists over every pair of them
        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              temporalRichClubCoefficient(
                k: 2
                windowSize: 2
                rollingWindow: { epoch: 1 }
              )
            }
          }
        }
        "#;

        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        // the a-b-c triangle is fully connected and persists, so the coefficient is 1
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({ "graph": { "algorithm": { "temporalRichClubCoefficient": 1.0 } } })
        );
    }
}
