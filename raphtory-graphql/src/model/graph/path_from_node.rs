use crate::{
    model::graph::{
        filtering::PathFromNodeViewCollection,
        node::GqlNode,
        windowset::GqlPathFromNodeWindowSet,
        WindowDuration::{self, Duration, Epoch},
    },
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    core::utils::time::TryIntoInterval,
    db::{api::view::DynamicGraph, graph::path::PathFromNode},
    errors::GraphError,
    prelude::*,
};

#[derive(ResolvedObject, Clone)]
#[graphql(name = "PathFromNode")]
pub(crate) struct GqlPathFromNode {
    pub(crate) nn: PathFromNode<'static, DynamicGraph, DynamicGraph>,
}

impl GqlPathFromNode {
    fn update<N: Into<PathFromNode<'static, DynamicGraph, DynamicGraph>>>(&self, nodes: N) -> Self {
        GqlPathFromNode::new(nodes)
    }
}

impl GqlPathFromNode {
    pub(crate) fn new<N: Into<PathFromNode<'static, DynamicGraph, DynamicGraph>>>(
        nodes: N,
    ) -> Self {
        Self { nn: nodes.into() }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = GqlNode> + '_> {
        let iter = self.nn.iter().map(GqlNode::from);
        Box::new(iter)
    }
}

#[ResolvedObjectFields]
impl GqlPathFromNode {
    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    /// Returns a view of PathFromNode containing the specified layer, errors if the layer does not exist.
    async fn layers(&self, names: Vec<String>) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.valid_layers(names))).await
    }

    /// Return a view of PathFromNode containing all layers except the specified excluded layers, errors if any of the layers do not exist.
    async fn exclude_layers(&self, names: Vec<String>) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.exclude_valid_layers(names))).await
    }

    /// Return a view of PathFromNode containing the layer specified layer, errors if the layer does not exist.
    async fn layer(&self, name: String) -> Self {
        self.update(self.nn.valid_layers(name))
    }

    /// Return a view of PathFromNode containing all layers except the specified excluded layers, errors if any of the layers do not exist.
    async fn exclude_layer(&self, name: String) -> Self {
        self.update(self.nn.exclude_valid_layers(name))
    }

    /// Creates a WindowSet with the given window size and optional step using a rolling window.
    ///
    /// align_start defaults to true and aligns the start of the first window to the smallest unit of time passed as input.
    /// e.g. "1 month and 1 day" will align at the start of the day.
    /// Note that passing a step larger than window while align_start is true can lead to some entries appearing before
    /// the start of the first window and/or after the end of the last window (i.e. not included in any window).
    async fn rolling(
        &self,
        window: WindowDuration,
        step: Option<WindowDuration>,
        align_start: Option<bool>,
    ) -> Result<GqlPathFromNodeWindowSet, GraphError> {
        let window = window.try_into_interval()?;
        let step = step.map(|x| x.try_into_interval()).transpose()?;
        let ws = if align_start.unwrap_or(true) {
            self.nn.rolling_aligned(window, step)?
        } else {
            self.nn.rolling(window, step)?
        };
        Ok(GqlPathFromNodeWindowSet::new(ws))
    }

    /// Creates a WindowSet with the given step size using an expanding window.
    ///
    /// align_start aligns the start of the first window to the smallest unit of time passed as input.
    /// e.g. "1 month and 1 day" will align at the start of the day. Defaults to true.
    async fn expanding(
        &self,
        step: WindowDuration,
        align_start: Option<bool>,
    ) -> Result<GqlPathFromNodeWindowSet, GraphError> {
        let step = step.try_into_interval()?;
        let ws = if align_start.unwrap_or(true) {
            self.nn.expanding_aligned(step)?
        } else {
            self.nn.expanding(step)?
        };
        Ok(GqlPathFromNodeWindowSet::new(ws))
    }

    /// Create a view of the PathFromNode including all events between a specified start (inclusive) and end (exclusive).
    async fn window(&self, start: i64, end: i64) -> Self {
        self.update(self.nn.window(start, end))
    }

    /// Create a view of the PathFromNode including all events at time.
    async fn at(&self, time: i64) -> Self {
        self.update(self.nn.at(time))
    }

    /// Create a view of the PathFromNode including all events that are valid at the latest time.
    async fn snapshot_latest(&self) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.snapshot_latest())).await
    }

    /// Create a view of the PathFromNode including all events that are valid at the specified time.
    async fn snapshot_at(&self, time: i64) -> Self {
        self.update(self.nn.snapshot_at(time))
    }

    /// Create a view of the PathFromNode including all events at the latest time.
    async fn latest(&self) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.latest())).await
    }

    /// Create a view of the PathFromNode including all events before the specified end (exclusive).
    async fn before(&self, time: i64) -> Self {
        self.update(self.nn.before(time))
    }

    /// Create a view of the PathFromNode including all events after the specified start (exclusive).
    async fn after(&self, time: i64) -> Self {
        self.update(self.nn.after(time))
    }

    /// Shrink both the start and end of the window.
    async fn shrink_window(&self, start: i64, end: i64) -> Self {
        self.update(self.nn.shrink_window(start, end))
    }

    /// Set the start of the window to the larger of the specified start and self.start().
    async fn shrink_start(&self, start: i64) -> Self {
        self.update(self.nn.shrink_start(start))
    }

    /// Set the end of the window to the smaller of the specified end and self.end().
    async fn shrink_end(&self, end: i64) -> Self {
        self.update(self.nn.shrink_end(end))
    }

    /// Filter nodes by type.
    async fn type_filter(&self, node_types: Vec<String>) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.type_filter(&node_types))).await
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    /// Returns the earliest time that this PathFromNode is valid or None if the PathFromNode is valid for all times.
    async fn start(&self) -> Option<i64> {
        self.nn.start()
    }

    /// Returns the latest time that this PathFromNode is valid or None if the PathFromNode is valid for all times.
    async fn end(&self) -> Option<i64> {
        self.nn.end()
    }

    /////////////////
    //// List ///////
    /////////////////

    async fn count(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.nn.len()).await
    }

    /// Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
    /// The page_index sets the number of pages to skip (defaults to 0).
    ///
    /// For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Vec<GqlNode> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone.iter().skip(start).take(limit).collect()
        })
        .await
    }

    async fn list(&self) -> Vec<GqlNode> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.iter().collect()).await
    }

    /// Returns the node ids.
    async fn ids(&self) -> Vec<String> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.nn.name().collect()).await
    }

    /// Takes a specified selection of views and applies them in given order.
    async fn apply_views(
        &self,
        views: Vec<PathFromNodeViewCollection>,
    ) -> Result<GqlPathFromNode, GraphError> {
        let mut return_view: GqlPathFromNode = self.clone();
        for view in views {
            return_view = match view {
                PathFromNodeViewCollection::Layers(layers) => return_view.layers(layers).await,
                PathFromNodeViewCollection::ExcludeLayers(layers) => {
                    return_view.exclude_layers(layers).await
                }
                PathFromNodeViewCollection::Layer(layer) => return_view.layer(layer).await,

                PathFromNodeViewCollection::ExcludeLayer(layer) => {
                    return_view.exclude_layer(layer).await
                }
                PathFromNodeViewCollection::Window(window) => {
                    return_view.window(window.start, window.end).await
                }
                PathFromNodeViewCollection::ShrinkWindow(window) => {
                    return_view.shrink_window(window.start, window.end).await
                }
                PathFromNodeViewCollection::ShrinkStart(time) => {
                    return_view.shrink_start(time).await
                }
                PathFromNodeViewCollection::ShrinkEnd(time) => return_view.shrink_end(time).await,
                PathFromNodeViewCollection::At(time) => return_view.at(time).await,
                PathFromNodeViewCollection::SnapshotLatest(apply) => {
                    if apply {
                        return_view.snapshot_latest().await
                    } else {
                        return_view
                    }
                }
                PathFromNodeViewCollection::SnapshotAt(time) => return_view.snapshot_at(time).await,
                PathFromNodeViewCollection::Latest(apply) => {
                    if apply {
                        return_view.latest().await
                    } else {
                        return_view
                    }
                }
                PathFromNodeViewCollection::Before(time) => return_view.before(time).await,
                PathFromNodeViewCollection::After(time) => return_view.after(time).await,
            }
        }
        Ok(return_view)
    }
}
