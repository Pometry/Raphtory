use crate::model::algorithms::GqlExecutableAlgorithm;
use raphtory::{
    algorithms::metrics::degree::average_degree, db::api::view::DynamicGraph, errors::GraphError,
};

/// Average node degree, see [`average_degree`].
pub(crate) struct GqlAverageDegree;

pub(crate) struct GqlAverageDegreeArgs;

impl GqlExecutableAlgorithm for GqlAverageDegree {
    type Args = GqlAverageDegreeArgs;
    type Output = f64;

    fn execute(graph: &DynamicGraph, _args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(average_degree(graph))
    }
}
