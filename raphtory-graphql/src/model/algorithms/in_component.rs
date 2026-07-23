use crate::model::{
    algorithms::{filtered_view, GqlExecutableAlgorithm},
    graph::{filtering::GqlNodeFilter, node_id::GqlNodeId, node_state::GqlNodeState},
};
use raphtory::{
    algorithms::components::in_component, db::api::view::DynamicGraph, errors::GraphError,
    prelude::*,
};

/// In component of a single node, see [`in_component`].
pub(crate) struct GqlInComponent;

pub(crate) struct GqlInComponentArgs {
    pub(crate) node: GqlNodeId,
    pub(crate) filter: Option<GqlNodeFilter>,
}

impl GqlExecutableAlgorithm for GqlInComponent {
    type Args = GqlInComponentArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let view = filtered_view(graph, args.filter)?;
        let node = view
            .node(args.node.clone())
            .ok_or_else(|| GraphError::NodeMissingError(args.node.into()))?;
        Ok(in_component(node).into())
    }
}
