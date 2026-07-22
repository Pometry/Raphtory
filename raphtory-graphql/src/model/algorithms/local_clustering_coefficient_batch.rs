use crate::model::{algorithms::GqlExecutableAlgorithm, graph::node_state::GqlNodeState};
use raphtory::{
    algorithms::metrics::clustering_coefficient::local_clustering_coefficient_batch::local_clustering_coefficient_batch,
    db::api::view::DynamicGraph, errors::GraphError,
};

/// Local clustering coefficient of the given nodes, see [`local_clustering_coefficient_batch`].
pub(crate) struct GqlLocalClusteringCoefficientBatch;

pub(crate) struct GqlLocalClusteringCoefficientBatchArgs {
    pub(crate) nodes: Vec<String>,
}

impl GqlExecutableAlgorithm for GqlLocalClusteringCoefficientBatch {
    type Args = GqlLocalClusteringCoefficientBatchArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        Ok(local_clustering_coefficient_batch(graph, args.nodes).into())
    }
}
