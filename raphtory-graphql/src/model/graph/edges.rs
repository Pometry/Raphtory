use crate::{
    model::{
        graph::{
            edge::GqlEdge,
            filtering::EdgesViewCollection,
            windowset::GqlEdgesWindowSet,
            WindowDuration,
            WindowDuration::{Duration, Epoch},
        },
        sorting::{EdgeSortBy, SortByTime},
    },
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    db::{
        api::view::{internal::OneHopFilter, DynamicGraph},
        graph::edges::Edges,
    },
    errors::GraphError,
    prelude::{EdgeViewOps, LayerOps, NodeViewOps, TimeOps},
};
use raphtory_api::iter::IntoDynBoxed;
use std::{cmp::Ordering, sync::Arc};

#[derive(ResolvedObject, Clone)]
#[graphql(name = "Edges")]
pub(crate) struct GqlEdges {
    pub(crate) ee: Edges<'static, DynamicGraph>,
}

impl GqlEdges {
    fn update<E: Into<Edges<'static, DynamicGraph>>>(&self, edges: E) -> Self {
        Self::new(edges)
    }
}

impl GqlEdges {
    pub(crate) fn new<E: Into<Edges<'static, DynamicGraph>>>(edges: E) -> Self {
        Self { ee: edges.into() }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = GqlEdge> + '_> {
        let iter = self.ee.iter().map(GqlEdge::from_ref);
        Box::new(iter)
    }
}

#[ResolvedObjectFields]
impl GqlEdges {
    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    async fn default_layer(&self) -> Self {
        self.update(self.ee.default_layer())
    }
    async fn layers(&self, names: Vec<String>) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.ee.valid_layers(names))).await
    }

    async fn exclude_layers(&self, names: Vec<String>) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.ee.exclude_valid_layers(names))).await
    }

    async fn layer(&self, name: String) -> Self {
        self.update(self.ee.valid_layers(name))
    }

    async fn exclude_layer(&self, name: String) -> Self {
        self.update(self.ee.exclude_valid_layers(name))
    }

    async fn rolling(
        &self,
        window: WindowDuration,
        step: Option<WindowDuration>,
    ) -> Result<GqlEdgesWindowSet, GraphError> {
        match window {
            Duration(window_duration) => match step {
                Some(step) => match step {
                    Duration(step_duration) => Ok(GqlEdgesWindowSet::new(
                        self.ee.rolling(window_duration, Some(step_duration))?,
                    )),
                    Epoch(_) => Err(GraphError::MismatchedIntervalTypes),
                },
                None => Ok(GqlEdgesWindowSet::new(
                    self.ee.rolling(window_duration, None)?,
                )),
            },
            Epoch(window_duration) => match step {
                Some(step) => match step {
                    Duration(_) => Err(GraphError::MismatchedIntervalTypes),
                    Epoch(step_duration) => Ok(GqlEdgesWindowSet::new(
                        self.ee.rolling(window_duration, Some(step_duration))?,
                    )),
                },
                None => Ok(GqlEdgesWindowSet::new(
                    self.ee.rolling(window_duration, None)?,
                )),
            },
        }
    }

    async fn expanding(&self, step: WindowDuration) -> Result<GqlEdgesWindowSet, GraphError> {
        match step {
            Duration(step) => Ok(GqlEdgesWindowSet::new(self.ee.expanding(step)?)),
            Epoch(step) => Ok(GqlEdgesWindowSet::new(self.ee.expanding(step)?)),
        }
    }

    async fn window(&self, start: i64, end: i64) -> Self {
        self.update(self.ee.window(start, end))
    }

    async fn at(&self, time: i64) -> Self {
        self.update(self.ee.at(time))
    }
    async fn latest(&self) -> Self {
        self.update(self.ee.latest())
    }

    async fn snapshot_at(&self, time: i64) -> Self {
        self.update(self.ee.snapshot_at(time))
    }
    async fn snapshot_latest(&self) -> Self {
        self.update(self.ee.snapshot_latest())
    }

    async fn before(&self, time: i64) -> Self {
        self.update(self.ee.before(time))
    }

    async fn after(&self, time: i64) -> Self {
        self.update(self.ee.after(time))
    }

    async fn shrink_window(&self, start: i64, end: i64) -> Self {
        self.update(self.ee.shrink_window(start, end))
    }

    async fn shrink_start(&self, start: i64) -> Self {
        self.update(self.ee.shrink_start(start))
    }

    async fn shrink_end(&self, end: i64) -> Self {
        self.update(self.ee.shrink_end(end))
    }

    async fn apply_views(&self, views: Vec<EdgesViewCollection>) -> Result<GqlEdges, GraphError> {
        let mut return_view: GqlEdges = self.update(self.ee.clone());
        for view in views {
            return_view = match view {
                EdgesViewCollection::DefaultLayer(apply) => {
                    if apply {
                        return_view.default_layer().await
                    } else {
                        return_view
                    }
                }
                EdgesViewCollection::Latest(apply) => {
                    if apply {
                        return_view.latest().await
                    } else {
                        return_view
                    }
                }
                EdgesViewCollection::SnapshotLatest(apply) => {
                    if apply {
                        return_view.snapshot_latest().await
                    } else {
                        return_view
                    }
                }
                EdgesViewCollection::SnapshotAt(at) => return_view.snapshot_at(at).await,
                EdgesViewCollection::Layers(layers) => return_view.layers(layers).await,
                EdgesViewCollection::ExcludeLayers(layers) => {
                    return_view.exclude_layers(layers).await
                }
                EdgesViewCollection::Layer(layer) => return_view.layer(layer).await,
                EdgesViewCollection::ExcludeLayer(layer) => return_view.exclude_layer(layer).await,
                EdgesViewCollection::Window(window) => {
                    return_view.window(window.start, window.end).await
                }
                EdgesViewCollection::At(at) => return_view.at(at).await,
                EdgesViewCollection::Before(time) => return_view.before(time).await,
                EdgesViewCollection::After(time) => return_view.after(time).await,
                EdgesViewCollection::ShrinkWindow(window) => {
                    return_view.shrink_window(window.start, window.end).await
                }
                EdgesViewCollection::ShrinkStart(time) => return_view.shrink_start(time).await,
                EdgesViewCollection::ShrinkEnd(time) => return_view.shrink_end(time).await,
            }
        }

        Ok(return_view)
    }
    async fn explode(&self) -> Self {
        self.update(self.ee.explode())
    }

    async fn explode_layers(&self) -> Self {
        self.update(self.ee.explode_layers())
    }

    async fn sorted(&self, sort_bys: Vec<EdgeSortBy>) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || {
            let sorted: Arc<[_]> = self_clone
                .ee
                .iter()
                .sorted_by(|first_edge, second_edge| {
                    sort_bys.clone().into_iter().fold(
                        Ordering::Equal,
                        |current_ordering, sort_by| {
                            current_ordering.then_with(|| {
                                let ordering = if sort_by.src == Some(true) {
                                    first_edge.src().id().partial_cmp(&second_edge.src().id())
                                } else if sort_by.dst == Some(true) {
                                    first_edge.dst().id().partial_cmp(&second_edge.dst().id())
                                } else if let Some(sort_by_time) = sort_by.time {
                                    let (first_time, second_time) = match sort_by_time {
                                        SortByTime::Latest => {
                                            (first_edge.latest_time(), second_edge.latest_time())
                                        }
                                        SortByTime::Earliest => (
                                            first_edge.earliest_time(),
                                            second_edge.earliest_time(),
                                        ),
                                    };
                                    first_time.partial_cmp(&second_time)
                                } else if let Some(sort_by_property) = sort_by.property {
                                    let first_prop_maybe =
                                        first_edge.properties().get(&*sort_by_property);
                                    let second_prop_maybe =
                                        second_edge.properties().get(&*sort_by_property);
                                    first_prop_maybe.partial_cmp(&second_prop_maybe)
                                } else {
                                    None
                                };
                                if let Some(ordering) = ordering {
                                    if sort_by.reverse == Some(true) {
                                        ordering.reverse()
                                    } else {
                                        ordering
                                    }
                                } else {
                                    Ordering::Equal
                                }
                            })
                        },
                    )
                })
                .map(|edge_view| edge_view.edge)
                .collect();
            self_clone.update(Edges::new(
                self_clone.ee.current_filter().clone(),
                self_clone.ee.base_graph().clone(),
                Arc::new(move || {
                    let sorted = sorted.clone();
                    (0..sorted.len()).map(move |i| sorted[i]).into_dyn_boxed()
                }),
            ))
        })
        .await
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    async fn start(&self) -> Option<i64> {
        self.ee.start()
    }

    async fn end(&self) -> Option<i64> {
        self.ee.end()
    }

    /////////////////
    //// List ///////
    /////////////////

    async fn count(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.len()).await
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
    ) -> Vec<GqlEdge> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone.iter().skip(start).take(limit).collect()
        })
        .await
    }

    async fn list(&self) -> Vec<GqlEdge> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.iter().collect()).await
    }
}
