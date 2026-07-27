use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::layout::fruchterman_reingold::fruchterman_reingold_unbounded,
    db::api::view::DynamicGraph, errors::GraphError,
};

/// Fruchterman-Reingold layout, see [`fruchterman_reingold_unbounded`].
pub(crate) struct GqlFruchtermanReingold;

pub(crate) struct GqlFruchtermanReingoldArgs {
    pub(crate) iter_count: u64,
    pub(crate) scale: f64,
    pub(crate) node_start_size: f64,
    pub(crate) cooloff_factor: f64,
    pub(crate) dt: f64,
}

impl GqlExecutableAlgorithm for GqlFruchtermanReingold {
    type Args = GqlFruchtermanReingoldArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let state = fruchterman_reingold_unbounded(
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
