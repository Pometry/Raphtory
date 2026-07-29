use crate::model::algorithms::GqlExecutableAlgorithm;
use dynamic_graphql::SimpleObject;
use raphtory::{
    algorithms::motifs::global_temporal_three_node_motifs::temporal_three_node_motif_multi,
    db::api::view::DynamicGraph, errors::GraphError,
};

/// The motif counts for a single delta. Wraps the counts in an object because
/// the schema builder does not support nested lists of scalars.
#[derive(SimpleObject)]
#[graphql(name = "MotifCounts")]
pub(crate) struct GqlMotifCounts {
    /// The delta these counts were computed for.
    delta: i64,
    /// The 40 motif counts, positionally ordered (see the core docs).
    counts: Vec<usize>,
}

/// Global temporal three-node motif counts for several deltas, see
/// [`temporal_three_node_motif_multi`].
pub(crate) struct GqlGlobalTemporalThreeNodeMotifMulti;

pub(crate) struct GqlGlobalTemporalThreeNodeMotifMultiArgs {
    pub(crate) deltas: Vec<i64>,
    pub(crate) threads: Option<usize>,
}

impl GqlExecutableAlgorithm for GqlGlobalTemporalThreeNodeMotifMulti {
    type Args = GqlGlobalTemporalThreeNodeMotifMultiArgs;
    /// One entry per delta, in the order the deltas were given.
    type Output = Vec<GqlMotifCounts>;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let deltas = args.deltas.clone();
        let counts = temporal_three_node_motif_multi(graph, args.deltas, args.threads);
        Ok(counts
            .into_iter()
            .zip(deltas)
            .map(|(counts, delta)| GqlMotifCounts {
                delta,
                counts: counts.to_vec(),
            })
            .collect())
    }
}
