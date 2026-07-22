use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::centrality::betweenness::betweenness_centrality, db::api::view::DynamicGraph,
    errors::GraphError,
};

/// Betweenness centrality, see [`betweenness_centrality`].
pub(crate) struct GqlBetweennessCentrality;

pub(crate) struct GqlBetweennessCentralityArgs {
    pub(crate) k: Option<usize>,
    pub(crate) normalized: bool,
}

impl GqlExecutableAlgorithm for GqlBetweennessCentrality {
    type Args = GqlBetweennessCentralityArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(betweenness_centrality(graph, args.k, args.normalized).into())
    }
}
