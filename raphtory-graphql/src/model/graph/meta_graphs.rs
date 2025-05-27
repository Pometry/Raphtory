use crate::model::graph::meta_graph::MetaGraph;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use tokio::task::spawn_blocking;

#[derive(ResolvedObject, Clone)]
pub(crate) struct MetaGraphs {
    graphs: Vec<MetaGraph>,
}

impl MetaGraphs {
    pub(crate) fn new(graphs: Vec<MetaGraph>) -> Self {
        Self { graphs }
    }
}
#[ResolvedObjectFields]
impl MetaGraphs {
    async fn list(&self) -> Vec<MetaGraph> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.graphs.clone())
            .await
            .unwrap()
    }

    async fn page(&self, limit: usize, offset: usize) -> Vec<MetaGraph> {
        let self_clone = self.clone();
        spawn_blocking(move || {
            let start = offset * limit;
            self_clone
                .graphs
                .iter()
                .map(|n| n.clone())
                .skip(start)
                .take(limit)
                .collect()
        })
        .await
        .unwrap()
    }

    async fn count(&self) -> usize {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.graphs.iter().count())
            .await
            .unwrap()
    }
}
