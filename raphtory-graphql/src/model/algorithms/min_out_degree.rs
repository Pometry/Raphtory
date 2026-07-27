use crate::model::algorithms::GqlExecutableAlgorithm;
use raphtory::{
    algorithms::metrics::degree::min_out_degree, db::api::view::DynamicGraph, errors::GraphError,
};

/// Minimum node out-degree, see [`min_out_degree`].
pub(crate) struct GqlMinOutDegree;

pub(crate) struct GqlMinOutDegreeArgs;

impl GqlExecutableAlgorithm for GqlMinOutDegree {
    type Args = GqlMinOutDegreeArgs;
    type Output = usize;

    fn execute(graph: &DynamicGraph, _args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(min_out_degree(graph))
    }
}
