use crate::model::algorithms::GqlExecutableAlgorithm;
use raphtory::{
    algorithms::metrics::degree::max_degree, db::api::view::DynamicGraph, errors::GraphError,
};

/// Maximum node degree, see [`max_degree`].
pub(crate) struct GqlMaxDegree;

pub(crate) struct GqlMaxDegreeArgs;

impl GqlExecutableAlgorithm for GqlMaxDegree {
    type Args = GqlMaxDegreeArgs;
    type Output = usize;

    fn execute(graph: &DynamicGraph, _args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(max_degree(graph))
    }
}
