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

    /// Fetch one "page" of items, optionally offset by a specified amount.
    ///
    /// * `limit` - The size of the page (number of items to fetch).
    /// * `offset` - The number of items to skip (defaults to 0).
    /// * `page_index` - The number of pages (of size `limit`) to skip (defaults to 0).
    ///
    /// e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Vec<MetaGraph> {
        let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
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
