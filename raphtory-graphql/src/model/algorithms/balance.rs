use crate::model::{
    algorithms::{GqlDirection, GqlExecutableAlgorithm},
    graph::node_state::GqlNodeState,
};
use raphtory::{
    algorithms::metrics::balance::balance, db::api::view::DynamicGraph, errors::GraphError,
};

/// Net sum of edge weights per node, see [`balance`].
pub(crate) struct GqlBalance;

pub(crate) struct GqlBalanceArgs {
    pub(crate) name: String,
    pub(crate) direction: GqlDirection,
}

impl GqlExecutableAlgorithm for GqlBalance {
    type Args = GqlBalanceArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let state = balance(graph, args.name, args.direction.into())?;
        Ok(state.into())
    }
}
