use async_graphql::parser::Error;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    db::api::view::DynamicGraph,
    prelude::GraphViewOps,
    search::{into_indexed::DynamicIndexedGraph, IndexedGraph},
};

#[derive(ResolvedObject)]
pub(crate) struct GqlGraphs {
    graphs: Vec<IndexedGraph<DynamicGraph>>,
}

impl GqlGraphs {
    pub fn new<G: DynamicIndexedGraph>(graphs: Vec<G>) -> Self {
        Self {
            graphs: graphs
                .into_iter()
                .map(|g| g.into_dynamic_indexed())
                .collect(),
        }
    }
}

#[ResolvedObjectFields]
impl GqlGraphs {
    async fn names(&self) -> Result<Vec<String>, Error> {
        Ok(self
            .graphs
            .iter()
            .filter_map(|g| {
                g.properties()
                    .constant()
                    .get("name")
                    .map(|name| name.to_string())
            })
            .collect())
    }
}
