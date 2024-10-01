use crate::model::plugins::vector_algorithm_plugin::VectorAlgorithmPlugin;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{db::api::view::MaterializedGraph, vectors::vectorised_graph::VectorisedGraph};

#[derive(ResolvedObject)]
pub(crate) struct GqlVectorisedGraph {
    graph: VectorisedGraph<MaterializedGraph>,
}

impl From<VectorisedGraph<MaterializedGraph>> for GqlVectorisedGraph {
    fn from(value: VectorisedGraph<MaterializedGraph>) -> Self {
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
