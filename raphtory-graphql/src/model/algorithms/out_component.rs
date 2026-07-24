use crate::model::{
    algorithms::{filtered_view, GqlExecutableAlgorithm},
    graph::{filtering::GqlViewFilter, node_id::GqlNodeId, node_state::GqlNodeState},
};
use raphtory::{
    algorithms::components::out_component, db::api::view::DynamicGraph, errors::GraphError,
    prelude::*,
};

/// Out component of a single node, see [`out_component`].
pub(crate) struct GqlOutComponent;

pub(crate) struct GqlOutComponentArgs {
    pub(crate) node: GqlNodeId,
    pub(crate) filter: Option<GqlViewFilter>,
}

impl GqlExecutableAlgorithm for GqlOutComponent {
    type Args = GqlOutComponentArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let view = filtered_view(graph, args.filter)?;
        let node = view
            .node(args.node.clone())
            .ok_or_else(|| GraphError::NodeMissingError(args.node.into()))?;
        Ok(out_component(node).into())
    }
}
