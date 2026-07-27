use crate::model::algorithms::GqlExecutableAlgorithm;
use raphtory::{
    algorithms::metrics::reciprocity::global_reciprocity, db::api::view::DynamicGraph,
    errors::GraphError,
};

/// Global reciprocity, see [`global_reciprocity`].
pub(crate) struct GqlGlobalReciprocity;

pub(crate) struct GqlGlobalReciprocityArgs;

impl GqlExecutableAlgorithm for GqlGlobalReciprocity {
    type Args = GqlGlobalReciprocityArgs;
    type Output = f64;

    fn execute(graph: &DynamicGraph, _args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(global_reciprocity(graph))
    }
}
