use crate::model::{
    algorithms::{filtered_view, GqlExecutableAlgorithm},
    graph::{filtering::GqlViewFilter, node_id::GqlNodeId},
};
use raphtory::{
    algorithms::metrics::clustering_coefficient::local_clustering_coefficient::local_clustering_coefficient,
    db::api::view::DynamicGraph, errors::GraphError,
};

/// Local clustering coefficient of a single node, see [`local_clustering_coefficient`].
pub(crate) struct GqlLocalClusteringCoefficient;

pub(crate) struct GqlLocalClusteringCoefficientArgs {
    pub(crate) node: GqlNodeId,
    pub(crate) filter: Option<GqlViewFilter>,
}

impl GqlExecutableAlgorithm for GqlLocalClusteringCoefficient {
    type Args = GqlLocalClusteringCoefficientArgs;
    type Output = Option<f64>;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let view = filtered_view(graph, args.filter)?;
        Ok(local_clustering_coefficient(&view, args.node))
    }
}
