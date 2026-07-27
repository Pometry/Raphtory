use crate::model::algorithms::GqlExecutableAlgorithm;
use raphtory::{
    algorithms::metrics::degree::max_in_degree, db::api::view::DynamicGraph, errors::GraphError,
};

/// Maximum node in-degree, see [`max_in_degree`].
pub(crate) struct GqlMaxInDegree;

pub(crate) struct GqlMaxInDegreeArgs;

impl GqlExecutableAlgorithm for GqlMaxInDegree {
    type Args = GqlMaxInDegreeArgs;
    type Output = usize;

    fn execute(graph: &DynamicGraph, _args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(max_in_degree(graph))
    }
}
