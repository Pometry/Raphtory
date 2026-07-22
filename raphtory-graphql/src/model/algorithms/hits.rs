use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::centrality::hits::hits, db::api::view::DynamicGraph, errors::GraphError,
};

/// HITS (hub and authority scores), see [`hits`].
pub(crate) struct GqlHits;

pub(crate) struct GqlHitsArgs {
    pub(crate) iter_count: usize,
    pub(crate) threads: Option<usize>,
}

impl GqlExecutableAlgorithm for GqlHits {
    type Args = GqlHitsArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(hits(graph, args.iter_count, args.threads).into())
    }
}
