use crate::model::algorithms::GqlExecutableAlgorithm;
use raphtory::{
    algorithms::metrics::directed_graph_density::directed_graph_density,
    db::api::view::DynamicGraph, errors::GraphError,
};

/// Directed graph density, see [`directed_graph_density`].
pub(crate) struct GqlDirectedGraphDensity;

pub(crate) struct GqlDirectedGraphDensityArgs;

impl GqlExecutableAlgorithm for GqlDirectedGraphDensity {
    type Args = GqlDirectedGraphDensityArgs;
    type Output = f64;

    fn execute(graph: &DynamicGraph, _args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(directed_graph_density(graph))
    }
}
