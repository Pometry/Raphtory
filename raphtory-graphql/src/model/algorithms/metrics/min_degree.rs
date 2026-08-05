use crate::model::algorithms::GqlExecutableAlgorithm;
use raphtory::{
    algorithms::metrics::degree::min_degree, db::api::view::DynamicGraph, errors::GraphError,
};

/// Minimum node degree, see [`min_degree`].
pub(crate) struct GqlMinDegree;

pub(crate) struct GqlMinDegreeArgs;

impl GqlExecutableAlgorithm for GqlMinDegree {
    type Args = GqlMinDegreeArgs;
    type Output = usize;

    fn execute(graph: &DynamicGraph, _args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(min_degree(graph))
    }
}
