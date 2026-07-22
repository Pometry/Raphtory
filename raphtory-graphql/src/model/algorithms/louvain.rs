use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::community_detection::{louvain::louvain, modularity::ModularityUnDir},
    db::api::view::DynamicGraph,
    errors::GraphError,
};

/// Louvain community detection, see [`louvain`].
pub(crate) struct GqlLouvain;

pub(crate) struct GqlLouvainArgs {
    pub(crate) resolution: f64,
    pub(crate) weight_prop: Option<String>,
    pub(crate) tol: Option<f64>,
    pub(crate) rng_seed: Option<u64>,
}

impl GqlExecutableAlgorithm for GqlLouvain {
    type Args = GqlLouvainArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let state = louvain::<ModularityUnDir, _>(
            graph,
            args.resolution,
            args.weight_prop.as_deref(),
            args.tol,
            args.rng_seed,
        );
        Ok(state.into())
    }
}
