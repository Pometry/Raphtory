use crate::model::graph::namespace::Namespace;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use tokio::task::spawn_blocking;

#[derive(ResolvedObject, Clone)]
pub(crate) struct Namespaces {
    namespaces: Vec<Namespace>,
}

impl Namespaces {
    pub(crate) fn new(namespaces: Vec<Namespace>) -> Self {
        Self { namespaces }
    }
}

#[ResolvedObjectFields]
impl Namespaces {
    async fn list(&self) -> Vec<Namespace> {
        self.namespaces.clone()
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
    ) -> Vec<Namespace> {
        let self_clone = self.clone();
        spawn_blocking(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .namespaces
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
        spawn_blocking(move || self_clone.namespaces.iter().count())
            .await
            .unwrap()
    }
}
