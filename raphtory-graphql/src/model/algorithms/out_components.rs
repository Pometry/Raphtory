use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::components::out_components, db::api::view::DynamicGraph, errors::GraphError,
};

/// Out components, see [`out_components`].
pub(crate) struct GqlOutComponents;

pub(crate) struct GqlOutComponentsArgs {
    pub(crate) threads: Option<usize>,
}

impl GqlExecutableAlgorithm for GqlOutComponents {
    type Args = GqlOutComponentsArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let state = out_components(graph, args.threads);
        Ok(state.into())
    }
}
