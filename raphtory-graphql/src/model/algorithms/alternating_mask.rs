use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::alternating_mask::alternating_mask, db::api::view::DynamicGraph, errors::GraphError,
};

/// Alternating boolean mask over the nodes, see [`alternating_mask`].
pub(crate) struct GqlAlternatingMask;

pub(crate) struct GqlAlternatingMaskArgs;

impl GqlExecutableAlgorithm for GqlAlternatingMask {
    type Args = GqlAlternatingMaskArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, _args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(alternating_mask(graph).into())
    }
}
