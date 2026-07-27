use crate::model::algorithms::GqlExecutableAlgorithm;
use raphtory::{
    algorithms::motifs::triplet_count::triplet_count, db::api::view::DynamicGraph,
    errors::GraphError,
};

/// Triplet count, see [`triplet_count`].
pub(crate) struct GqlTripletCount;

pub(crate) struct GqlTripletCountArgs {
    pub(crate) threads: Option<usize>,
}

impl GqlExecutableAlgorithm for GqlTripletCount {
    type Args = GqlTripletCountArgs;
    type Output = usize;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(triplet_count(graph, args.threads))
    }
}
