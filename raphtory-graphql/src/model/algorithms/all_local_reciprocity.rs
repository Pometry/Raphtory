use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::metrics::reciprocity::all_local_reciprocity, db::api::view::DynamicGraph,
    errors::GraphError,
};

/// Local reciprocity of every node, see [`all_local_reciprocity`].
pub(crate) struct GqlAllLocalReciprocity;

pub(crate) struct GqlAllLocalReciprocityArgs;

impl GqlExecutableAlgorithm for GqlAllLocalReciprocity {
    type Args = GqlAllLocalReciprocityArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, _args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(all_local_reciprocity(graph).into())
    }
}
