use crate::model::algorithms::vector_algorithms::VectorAlgorithms;
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
    async fn algorithms(&self) -> VectorAlgorithms {
        self.graph.clone().into()
    }
}
