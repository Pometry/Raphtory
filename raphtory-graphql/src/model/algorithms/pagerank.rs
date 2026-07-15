use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::centrality::pagerank::page_rank, db::api::view::DynamicGraph, errors::GraphError,
};
use raphtory_api::core::storage::arc_str::OptionAsStr;

/// PageRank, see [`page_rank`].
pub(crate) struct GqlPagerank;

pub(crate) struct GqlPagerankArgs {
    pub(crate) iter_count: Option<usize>,
    pub(crate) threads: Option<usize>,
    pub(crate) tol: Option<f64>,
    pub(crate) damping_factor: Option<f64>,
    pub(crate) weight: Option<String>,
}

impl GqlExecutableAlgorithm for GqlPagerank {
    type Args = GqlPagerankArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let state = page_rank(
            graph,
            args.weight.as_str(),
            args.iter_count,
            args.threads,
            args.tol,
            true,
            args.damping_factor,
        );
        Ok(state.into())
    }
}
