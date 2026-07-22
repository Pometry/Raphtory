use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::pathing::dijkstra::dijkstra_single_source_shortest_paths,
    db::api::view::DynamicGraph, errors::GraphError,
};
use crate::model::algorithms::GqlDirection;

/// Weighted single source shortest paths (Dijkstra), see [`dijkstra_single_source_shortest_paths`].
pub(crate) struct GqlDijkstra;

pub(crate) struct GqlDijkstraArgs {
    pub(crate) source: String,
    pub(crate) targets: Vec<String>,
    pub(crate) weight: Option<String>,
    pub(crate) direction: GqlDirection,
}

impl GqlExecutableAlgorithm for GqlDijkstra {
    type Args = GqlDijkstraArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let state = dijkstra_single_source_shortest_paths(
            graph,
            args.source,
            args.targets,
            args.weight.as_deref(),
            args.direction.into(),
        )?;
        Ok(state.into())
    }
}
