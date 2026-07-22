use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::community_detection::label_propagation::label_propagation,
    db::api::view::DynamicGraph, errors::GraphError,
};

/// Label propagation community detection, see [`label_propagation`].
pub(crate) struct GqlLabelPropagation;

pub(crate) struct GqlLabelPropagationArgs {
    pub(crate) iter_count: usize,
    pub(crate) threads: Option<usize>,
}

impl GqlExecutableAlgorithm for GqlLabelPropagation {
    type Args = GqlLabelPropagationArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let state = label_propagation(graph, args.iter_count, None, args.threads);
        Ok(state.into())
    }
}
