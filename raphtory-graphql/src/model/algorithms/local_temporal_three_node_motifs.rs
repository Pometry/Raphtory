use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::motifs::local_temporal_three_node_motifs::temporal_three_node_motif,
    db::api::view::DynamicGraph, errors::GraphError,
};

/// Local temporal three-node motif counts, see [`temporal_three_node_motif`].
pub(crate) struct GqlLocalTemporalThreeNodeMotifs;

pub(crate) struct GqlLocalTemporalThreeNodeMotifsArgs {
    pub(crate) delta: i64,
    pub(crate) threads: Option<usize>,
}

impl GqlExecutableAlgorithm for GqlLocalTemporalThreeNodeMotifs {
    type Args = GqlLocalTemporalThreeNodeMotifsArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let state = temporal_three_node_motif(graph, args.delta, args.threads);
        Ok(state.into())
    }
}
