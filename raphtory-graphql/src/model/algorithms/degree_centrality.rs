use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::centrality::degree_centrality::degree_centrality, db::api::view::DynamicGraph,
    errors::GraphError,
};

/// Degree centrality, see [`degree_centrality`].
pub(crate) struct GqlDegreeCentrality;

pub(crate) struct GqlDegreeCentralityArgs;

impl GqlExecutableAlgorithm for GqlDegreeCentrality {
    type Args = GqlDegreeCentralityArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, _args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(degree_centrality(graph).into())
    }
}
