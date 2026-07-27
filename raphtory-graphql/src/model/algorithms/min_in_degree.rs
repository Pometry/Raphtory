use crate::model::algorithms::GqlExecutableAlgorithm;
use raphtory::{
    algorithms::metrics::degree::min_in_degree, db::api::view::DynamicGraph, errors::GraphError,
};

/// Minimum node in-degree, see [`min_in_degree`].
pub(crate) struct GqlMinInDegree;

pub(crate) struct GqlMinInDegreeArgs;

impl GqlExecutableAlgorithm for GqlMinInDegree {
    type Args = GqlMinInDegreeArgs;
    type Output = usize;

    fn execute(graph: &DynamicGraph, _args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(min_in_degree(graph))
    }
}
