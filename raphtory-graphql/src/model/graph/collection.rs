use crate::rayon::blocking_compute;
use dynamic_graphql::{
    internal::{OutputTypeName, ResolveOwned, TypeName},
    ResolvedObject, ResolvedObjectFields,
};
use std::{borrow::Cow, sync::Arc};

#[derive(ResolvedObject, Clone)]
#[graphql(get_type_name = true)]
pub(crate) struct GqlCollection<T>
where
    T: Clone + Send + Sync,
    T: OutputTypeName + 'static,
    T: for<'a> ResolveOwned<'a>,
{
    items: Arc<[T]>,
}

impl<T> GqlCollection<T>
where
    T: Clone + Send + Sync,
    T: OutputTypeName + 'static,
    T: for<'a> ResolveOwned<'a>,
{
    pub(crate) fn new(items: Arc<[T]>) -> Self {
        Self { items }
    }
}

impl<T> TypeName for GqlCollection<T>
where
    T: Clone + Send + Sync,
    T: OutputTypeName + 'static,
    T: for<'a> ResolveOwned<'a>,
{
    fn get_type_name() -> Cow<'static, str> {
        format!("CollectionOf{}", T::get_type_name()).into()
    }
}

#[ResolvedObjectFields]
impl<T> GqlCollection<T>
where
    T: Clone + Send + Sync,
    T: OutputTypeName + 'static,
    T: for<'a> ResolveOwned<'a>,
{
    async fn list(&self) -> Vec<T> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.items.to_vec()).await
    }

    /// Fetch one "page" of items, optionally offset by a specified amount.
    ///
    /// * `limit` - The size of the page (number of items to fetch).
    /// * `offset` - The number of items to skip (defaults to 0).
    /// * `page_index` - The number of pages (of size `limit`) to skip (defaults to 0).
    ///
    /// e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page(&self, limit: usize, offset: Option<usize>, page_index: Option<usize>) -> Vec<T> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .items
                .iter()
                .skip(start)
                .take(limit)
                .cloned()
                .collect()
        })
        .await
    }

    async fn count(&self) -> usize {
        self.items.len()
    }
}
