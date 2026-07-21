use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::components::in_components, db::api::view::DynamicGraph, errors::GraphError,
};

/// In components, see [`in_components`].
pub(crate) struct GqlInComponents;

pub(crate) struct GqlInComponentsArgs {
    pub(crate) threads: Option<usize>,
}

impl GqlExecutableAlgorithm for GqlInComponents {
    type Args = GqlInComponentsArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let state = in_components(graph, args.threads);
        Ok(state.into())
    }
}
