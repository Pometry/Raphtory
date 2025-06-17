use crate::model::graph::namespaced_item::NamespacedItem;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use tokio::task::spawn_blocking;

#[derive(ResolvedObject, Clone)]
pub(crate) struct NamespacedItems {
    pub(crate) items: Vec<NamespacedItem>,
}

#[ResolvedObjectFields]
impl NamespacedItems {
    pub(crate) async fn list(&self) -> Vec<NamespacedItem> {
        self.items.clone()
    }

    /// Fetch one "page" of items, optionally offset by a specified amount.
    ///
    /// * `limit` - The size of the page (number of items to fetch).
    /// * `offset` - The number of items to skip (defaults to 0).
    /// * `page_index` - The number of pages (of size `limit`) to skip (defaults to 0).
    ///
    /// e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    pub(crate) async fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Vec<NamespacedItem> {
        let self_clone = self.clone();
        spawn_blocking(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .items
                .iter()
                .map(|n| n.clone())
                .skip(start)
                .take(limit)
                .collect()
        })
        .await
        .unwrap()
    }
    pub(crate) async fn count(&self) -> usize {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.items.iter().count())
            .await
            .unwrap()
    }
}
