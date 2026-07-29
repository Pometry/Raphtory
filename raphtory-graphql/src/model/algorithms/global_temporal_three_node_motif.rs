use crate::model::algorithms::GqlExecutableAlgorithm;
use raphtory::{
    algorithms::motifs::global_temporal_three_node_motifs::global_temporal_three_node_motif,
    db::api::view::DynamicGraph, errors::GraphError,
};

/// Global temporal three-node motif counts, see [`global_temporal_three_node_motif`].
pub(crate) struct GqlGlobalTemporalThreeNodeMotif;

pub(crate) struct GqlGlobalTemporalThreeNodeMotifArgs {
    pub(crate) delta: i64,
    pub(crate) threads: Option<usize>,
}

impl GqlExecutableAlgorithm for GqlGlobalTemporalThreeNodeMotif {
    type Args = GqlGlobalTemporalThreeNodeMotifArgs;
    /// The 40 motif counts, positionally ordered (see the core docs).
    type Output = Vec<usize>;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let counts = global_temporal_three_node_motif(graph, args.delta, args.threads);
        Ok(counts.to_vec())
    }
}
