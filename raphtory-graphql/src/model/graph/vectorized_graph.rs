use crate::{data::DynamicVectorizedGraph, model::algorithms::vector_algorithms::VectorAlgorithms};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};

#[derive(ResolvedObject)]
pub(crate) struct GqlVectorizedGraph {
    graph: DynamicVectorizedGraph,
}

impl From<DynamicVectorizedGraph> for GqlVectorizedGraph {
    fn from(value: DynamicVectorizedGraph) -> Self {
        Self {
            graph: value.clone(),
        }
    }
}

#[ResolvedObjectFields]
impl GqlVectorizedGraph {
    async fn algorithms(&self) -> VectorAlgorithms {
        self.graph.clone().into()
    }
}
