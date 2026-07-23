use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::layout::cohesive_fruchterman_reingold::cohesive_fruchterman_reingold,
    db::api::view::DynamicGraph, errors::GraphError,
};

/// Cohesive Fruchterman-Reingold layout, see [`cohesive_fruchterman_reingold`].
pub(crate) struct GqlCohesiveFruchtermanReingold;

pub(crate) struct GqlCohesiveFruchtermanReingoldArgs {
    pub(crate) iter_count: u64,
    pub(crate) scale: f32,
    pub(crate) node_start_size: f32,
    pub(crate) cooloff_factor: f32,
    pub(crate) dt: f32,
}

impl GqlExecutableAlgorithm for GqlCohesiveFruchtermanReingold {
    type Args = GqlCohesiveFruchtermanReingoldArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let state = cohesive_fruchterman_reingold(
            graph,
            args.iter_count,
            args.scale,
            args.node_start_size,
            args.cooloff_factor,
            args.dt,
        );
        Ok(state.into())
    }
}
