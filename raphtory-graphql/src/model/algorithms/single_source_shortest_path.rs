use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::pathing::single_source_shortest_path::single_source_shortest_path,
    db::api::view::DynamicGraph, errors::GraphError,
};

/// Single source shortest path (unweighted BFS), see [`single_source_shortest_path`].
pub(crate) struct GqlSingleSourceShortestPath;

pub(crate) struct GqlSingleSourceShortestPathArgs {
    pub(crate) source: String,
    pub(crate) cutoff: Option<usize>,
}

impl GqlExecutableAlgorithm for GqlSingleSourceShortestPath {
    type Args = GqlSingleSourceShortestPathArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let state = single_source_shortest_path(graph, args.source, args.cutoff);
        Ok(state.into())
    }
}
