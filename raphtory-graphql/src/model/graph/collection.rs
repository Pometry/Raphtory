use crate::{config::concurrency_config::ConcurrencyConfig, rayon::blocking_compute};
use async_graphql::{Context, Error, Result};
use dynamic_graphql::{
    internal::{OutputTypeName, ResolveOwned, TypeName},
    ResolvedObject, ResolvedObjectFields,
};
use std::{borrow::Cow, sync::Arc};

/// Returns an error when `concurrency.disable_lists` is set. Called from every `list`
/// resolver on paginated collections.
pub(crate) fn check_list_allowed(ctx: &Context<'_>) -> Result<()> {
    if ctx
        .data_opt::<ConcurrencyConfig>()
        .map(|cfg| cfg.disable_lists)
        .unwrap_or(false)
    {
        return Err(Error::new(
            "Bulk list endpoints are disabled on this server. Use `page` instead.",
        ));
    }
    Ok(())
}

/// Returns an error when `limit` exceeds `concurrency.max_page_size`. Called from every
/// `page` resolver on paginated collections.
pub(crate) fn check_page_limit(ctx: &Context<'_>, limit: usize) -> Result<()> {
    if let Some(max) = ctx
        .data_opt::<ConcurrencyConfig>()
        .and_then(|cfg| cfg.max_page_size)
    {
        if limit > max {
            return Err(Error::new(format!(
                "page limit {limit} exceeds the maximum allowed page size {max}"
            )));
        }
    }
    Ok(())
}

/// Collection of items
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
    /// Returns a list of collection objects.
    async fn list(&self, ctx: &Context<'_>) -> Result<Vec<T>> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || self_clone.items.to_vec()).await)
    }

    /// Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).
    ///
    /// For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.

    async fn page(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Maximum number of items to return on this page.")] limit: usize,
        #[graphql(desc = "Extra items to skip on top of `pageIndex` paging (default 0).")]
        offset: Option<usize>,
        #[graphql(
            desc = "Zero-based page number; multiplies `limit` to determine where to start (default 0)."
        )]
        page_index: Option<usize>,
    ) -> Result<Vec<T>> {
        check_page_limit(ctx, limit)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .items
                .iter()
                .skip(start)
                .take(limit)
                .cloned()
                .collect()
        })
        .await)
    }

    /// Returns a count of collection objects.
    async fn count(&self) -> usize {
        self.items.len()
    }
}
