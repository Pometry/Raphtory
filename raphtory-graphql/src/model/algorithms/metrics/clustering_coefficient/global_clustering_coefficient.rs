use crate::model::algorithms::GqlExecutableAlgorithm;
use raphtory::{
    algorithms::metrics::clustering_coefficient::global_clustering_coefficient::global_clustering_coefficient,
    db::api::view::DynamicGraph, errors::GraphError,
};

/// Global clustering coefficient, see [`global_clustering_coefficient`].
pub(crate) struct GqlGlobalClusteringCoefficient;

pub(crate) struct GqlGlobalClusteringCoefficientArgs;

impl GqlExecutableAlgorithm for GqlGlobalClusteringCoefficient {
    type Args = GqlGlobalClusteringCoefficientArgs;
    type Output = f64;

    fn execute(graph: &DynamicGraph, _args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(global_clustering_coefficient(graph))
    }
}
