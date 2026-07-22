use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::components::weakly_connected_components, db::api::view::DynamicGraph,
    errors::GraphError,
};

/// Weakly connected components, see [`weakly_connected_components`].
pub(crate) struct GqlWeaklyConnectedComponents;

pub(crate) struct GqlWeaklyConnectedComponentsArgs;

impl GqlExecutableAlgorithm for GqlWeaklyConnectedComponents {
    type Args = GqlWeaklyConnectedComponentsArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, _args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(weakly_connected_components(graph).into())
    }
}
