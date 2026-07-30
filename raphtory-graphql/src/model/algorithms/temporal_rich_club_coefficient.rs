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
