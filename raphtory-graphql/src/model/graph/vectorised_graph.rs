use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    db::api::view::MaterializedGraph,
    vectors::{vectorised_graph::VectorisedGraph, Embedding},
};

use super::vector_selection::GqlVectorSelection;

#[derive(ResolvedObject)]
pub(crate) struct GqlVectorisedGraph(VectorisedGraph<MaterializedGraph>);

impl From<VectorisedGraph<MaterializedGraph>> for GqlVectorisedGraph {
    fn from(value: VectorisedGraph<MaterializedGraph>) -> Self {
        Self(value.clone())
    }
}

#[ResolvedObjectFields]
impl GqlVectorisedGraph {
    async fn empty_selection(&self) -> GqlVectorSelection {
        self.0.empty_selection().into()
    }

    async fn entities_by_similarity(&self, query: Vec<f32>, limit: usize) -> GqlVectorSelection {
        self.0
            .entities_by_similarity(&query.into(), limit, None)
            .into()
    }

    async fn nodes_by_similarity(&self, query: Vec<f32>, limit: usize) -> GqlVectorSelection {
        self.0
            .nodes_by_similarity(&query.into(), limit, None)
            .into()
    }

    async fn edges_by_similarity(&self, query: Vec<f32>, limit: usize) -> GqlVectorSelection {
        self.0
            .edges_by_similarity(&query.into(), limit, None)
            .into()
    }
}
