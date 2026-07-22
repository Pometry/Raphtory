use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::components::strongly_connected_components, db::api::view::DynamicGraph,
    errors::GraphError,
};

/// Strongly connected components, see [`strongly_connected_components`].
pub(crate) struct GqlStronglyConnectedComponents;

pub(crate) struct GqlStronglyConnectedComponentsArgs;

impl GqlExecutableAlgorithm for GqlStronglyConnectedComponents {
    type Args = GqlStronglyConnectedComponentsArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, _args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(strongly_connected_components(graph).into())
    }
}
