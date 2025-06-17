use crate::model::graph::meta_graph::MetaGraph;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};

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
        self.graphs.clone()
    }

    async fn page(&self, limit: usize, offset: usize) -> Vec<MetaGraph> {
        let start = offset * limit;
        self.graphs
            .iter()
            .skip(start)
            .take(limit)
            .cloned()
            .collect()
    }

    async fn count(&self) -> usize {
        self.graphs.len()
    }
}
