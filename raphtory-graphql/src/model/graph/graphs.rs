use crate::model::graph::property::GqlProperties;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    db::api::{properties::dyn_props::DynProperties, view::DynamicGraph},
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
    async fn names(&self) -> Vec<String> {
        self.graphs
            .iter()
            .map(|g| g.properties().constant().get("name").unwrap().to_string())
            .collect()
    }

    async fn properties(&self) -> Vec<GqlProperties> {
        self.graphs
            .iter()
            .map(|g| Into::<DynProperties>::into(g.properties()).into())
            .collect()
    }

    async fn unique_layers(&self) -> Vec<Vec<String>> {
        self.graphs
            .iter()
            .map(|g| g.unique_layers().map_into().collect())
            .collect()
    }
}
