use crate::model::{algorithms::GqlExecutableAlgorithm, graph::WindowDuration};
use raphtory::{
    algorithms::motifs::temporal_rich_club_coefficient::temporal_rich_club_coefficient,
    core::utils::time::TryIntoInterval,
    db::api::view::{DynamicGraph, TimeOps},
    errors::GraphError,
};

/// Temporal rich club coefficient, see [`temporal_rich_club_coefficient`].
pub(crate) struct GqlTemporalRichClubCoefficient;

pub(crate) struct GqlTemporalRichClubCoefficientArgs {
    pub(crate) k: usize,
    pub(crate) window_size: usize,
    pub(crate) rolling_window: WindowDuration,
    pub(crate) rolling_step: Option<WindowDuration>,
}

impl GqlExecutableAlgorithm for GqlTemporalRichClubCoefficient {
    type Args = GqlTemporalRichClubCoefficientArgs;
    type Output = f64;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let rolling_window = args.rolling_window.try_into_interval()?;
        let rolling_step = args
            .rolling_step
            .map(|step| step.try_into_interval())
            .transpose()?;
        let views: Vec<_> = graph
            .rolling(rolling_window, rolling_step)?
            .into_iter()
            .collect();
        Ok(temporal_rich_club_coefficient(
            graph,
            views,
            args.k,
            args.window_size,
        ))
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
