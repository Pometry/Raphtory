use crate::model::plugins::vector_algorithm_plugin::VectorAlgorithmPlugin;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::vectors::vectorised_graph::DynamicVectorisedGraph;

#[derive(ResolvedObject)]
pub(crate) struct GqlVectorisedGraph {
    graph: DynamicVectorisedGraph,
}

impl From<DynamicVectorisedGraph> for GqlVectorisedGraph {
    fn from(value: DynamicVectorisedGraph) -> Self {
        Self {
            graph: value.clone(),
        }
    }
}

#[ResolvedObjectFields]
impl GqlVectorisedGraph {
    async fn algorithms(&self) -> VectorAlgorithmPlugin {
        self.graph.clone().into()
    }
}
