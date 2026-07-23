use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::pathing::temporal_reachability::temporally_reachable_nodes,
    db::api::view::DynamicGraph, errors::GraphError,
};

/// Temporally reachable nodes, see [`temporally_reachable_nodes`].
pub(crate) struct GqlTemporallyReachableNodes;

pub(crate) struct GqlTemporallyReachableNodesArgs {
    pub(crate) max_hops: usize,
    pub(crate) start_time: i64,
    pub(crate) seed_nodes: Vec<String>,
    pub(crate) stop_nodes: Option<Vec<String>>,
    pub(crate) threads: Option<usize>,
}

impl GqlExecutableAlgorithm for GqlTemporallyReachableNodes {
    type Args = GqlTemporallyReachableNodesArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let state = temporally_reachable_nodes(
            graph,
            args.threads,
            args.max_hops,
            args.start_time,
            args.seed_nodes,
            args.stop_nodes,
        );
        Ok(state.into())
    }
}
