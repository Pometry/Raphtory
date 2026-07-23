use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::embeddings::fast_rp::fast_rp, db::api::view::DynamicGraph, errors::GraphError,
};

/// FastRP node embeddings, see [`fast_rp`].
pub(crate) struct GqlFastRp;

pub(crate) struct GqlFastRpArgs {
    pub(crate) embedding_dim: usize,
    pub(crate) normalization_strength: f64,
    pub(crate) iter_weights: Vec<f64>,
    pub(crate) seed: Option<u64>,
    pub(crate) threads: Option<usize>,
}

impl GqlExecutableAlgorithm for GqlFastRp {
    type Args = GqlFastRpArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let state = fast_rp(
            graph,
            args.embedding_dim,
            args.normalization_strength,
            args.iter_weights,
            args.seed,
            args.threads,
        );
        Ok(state.into())
    }
}
