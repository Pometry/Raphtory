use crate::model::algorithms::GqlExecutableAlgorithm;
use raphtory::{
    algorithms::metrics::degree::max_out_degree, db::api::view::DynamicGraph, errors::GraphError,
};

/// Maximum node out-degree, see [`max_out_degree`].
pub(crate) struct GqlMaxOutDegree;

pub(crate) struct GqlMaxOutDegreeArgs;

impl GqlExecutableAlgorithm for GqlMaxOutDegree {
    type Args = GqlMaxOutDegreeArgs;
    type Output = usize;

    fn execute(graph: &DynamicGraph, _args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(max_out_degree(graph))
    }
}
