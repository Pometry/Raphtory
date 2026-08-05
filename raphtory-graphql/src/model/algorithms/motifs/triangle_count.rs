use crate::model::algorithms::GqlExecutableAlgorithm;
use raphtory::{
    algorithms::motifs::triangle_count::triangle_count, db::api::view::DynamicGraph,
    errors::GraphError,
};

/// Triangle count, see [`triangle_count`].
pub(crate) struct GqlTriangleCount;

pub(crate) struct GqlTriangleCountArgs {
    pub(crate) threads: Option<usize>,
}

impl GqlExecutableAlgorithm for GqlTriangleCount {
    type Args = GqlTriangleCountArgs;
    type Output = usize;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(triangle_count(graph, args.threads))
    }
}
