use crate::{
    model::{
        graph::{
            filtering::{NodeFilter, NodesViewCollection},
            node::GqlNode,
            windowset::GqlNodesWindowSet,
            WindowDuration,
            WindowDuration::{Duration, Epoch},
        },
        sorting::{NodeSortBy, SortByTime},
    },
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    db::{
        api::{state::Index, view::DynamicGraph},
        graph::{nodes::Nodes, views::filter::model::node_filter::CompositeNodeFilter},
    },
    errors::GraphError,
    prelude::*,
};
use raphtory_api::core::entities::VID;
use std::cmp::Ordering;

#[derive(ResolvedObject, Clone)]
#[graphql(name = "Nodes")]
pub(crate) struct GqlNodes {
    pub(crate) nn: Nodes<'static, DynamicGraph>,
}

impl GqlNodes {
    fn update<N: Into<Nodes<'static, DynamicGraph>>>(&self, nodes: N) -> Self {
        GqlNodes::new(nodes)
    }
}

impl GqlNodes {
    pub(crate) fn new<N: Into<Nodes<'static, DynamicGraph>>>(nodes: N) -> Self {
        Self { nn: nodes.into() }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = GqlNode> + '_> {
        let iter = self.nn.iter_owned().map(GqlNode::from);
        Box::new(iter)
    }
}

#[ResolvedObjectFields]
impl GqlNodes {
    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    /// Return a view of the nodes containing only the default edge layer.
    async fn default_layer(&self) -> Self {
        self.update(self.nn.default_layer())
    }

    /// Return a view of the nodes containing all layers specified.
    async fn layers(&self, names: Vec<String>) -> Self {
        self.update(self.nn.valid_layers(names))
    }

    /// Return a view of the nodes containing all layers except those specified.
    async fn exclude_layers(&self, names: Vec<String>) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.exclude_valid_layers(names))).await
    }

    /// Return a view of the nodes containing the specified layer.
    async fn layer(&self, name: String) -> Self {
        self.update(self.nn.valid_layers(name))
    }

    /// Return a view of the nodes containing all layers except those specified.
    async fn exclude_layer(&self, name: String) -> Self {
        self.update(self.nn.exclude_valid_layers(name))
    }

    /// Creates a WindowSet with the specified window size and optional step using a rolling window.
    async fn rolling(
        &self,
        window: WindowDuration,
        step: Option<WindowDuration>,
    ) -> Result<GqlNodesWindowSet, GraphError> {
        match window {
            Duration(window_duration) => match step {
                Some(step) => match step {
                    Duration(step_duration) => Ok(GqlNodesWindowSet::new(
                        self.nn.rolling(window_duration, Some(step_duration))?,
                    )),
                    Epoch(_) => Err(GraphError::MismatchedIntervalTypes),
                },
                None => Ok(GqlNodesWindowSet::new(
                    self.nn.rolling(window_duration, None)?,
                )),
            },
            Epoch(window_duration) => match step {
                Some(step) => match step {
                    Duration(_) => Err(GraphError::MismatchedIntervalTypes),
                    Epoch(step_duration) => Ok(GqlNodesWindowSet::new(
                        self.nn.rolling(window_duration, Some(step_duration))?,
                    )),
                },
                None => Ok(GqlNodesWindowSet::new(
                    self.nn.rolling(window_duration, None)?,
                )),
            },
        }
    }

    /// Creates a WindowSet with the specified step size using an expanding window.
    async fn expanding(&self, step: WindowDuration) -> Result<GqlNodesWindowSet, GraphError> {
        match step {
            Duration(step) => Ok(GqlNodesWindowSet::new(self.nn.expanding(step)?)),
            Epoch(step) => Ok(GqlNodesWindowSet::new(self.nn.expanding(step)?)),
        }
    }

    /// Create a view of the node including all events between the specified start (inclusive) and end (exclusive).
    async fn window(&self, start: i64, end: i64) -> Self {
        self.update(self.nn.window(start, end))
    }

    /// Create a view of the nodes including all events at a specified time.
    async fn at(&self, time: i64) -> Self {
        self.update(self.nn.at(time))
    }

    /// Create a view of the nodes including all events at the latest time.
    async fn latest(&self) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.latest())).await
    }

    /// Create a view of the nodes including all events that are valid at the specified time.
    async fn snapshot_at(&self, time: i64) -> Self {
        self.update(self.nn.snapshot_at(time))
    }

    /// Create a view of the nodes including all events that are valid at the latest time.
    async fn snapshot_latest(&self) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.snapshot_latest())).await
    }

    /// Create a view of the nodes including all events before specified end time (exclusive).
    async fn before(&self, time: i64) -> Self {
        self.update(self.nn.before(time))
    }

    /// Create a view of the nodes including all events after the specified start time (exclusive).
    async fn after(&self, time: i64) -> Self {
        self.update(self.nn.after(time))
    }

    /// Shrink both the start and end of the window.
    async fn shrink_window(&self, start: i64, end: i64) -> Self {
        self.update(self.nn.shrink_window(start, end))
    }

    /// Set the start of the window to the larger of a specified start time and self.start().
    async fn shrink_start(&self, start: i64) -> Self {
        self.update(self.nn.shrink_start(start))
    }

    /// Set the end of the window to the smaller of a specified end and self.end().
    async fn shrink_end(&self, end: i64) -> Self {
        self.update(self.nn.shrink_end(end))
    }

    /// Filter nodes by node type.
    async fn type_filter(&self, node_types: Vec<String>) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.type_filter(&node_types))).await
    }

    /// Returns a view of the node types.
    async fn node_filter(&self, filter: NodeFilter) -> Result<Self, GraphError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let filter: CompositeNodeFilter = filter.try_into()?;
            let filtered_nodes = self_clone.nn.filter_nodes(filter)?;
            Ok(self_clone.update(filtered_nodes.into_dyn()))
        })
        .await
    }

    async fn apply_views(&self, views: Vec<NodesViewCollection>) -> Result<GqlNodes, GraphError> {
        let mut return_view: GqlNodes = GqlNodes::new(self.nn.clone());
        for view in views {
            return_view = match view {
                NodesViewCollection::DefaultLayer(apply) => {
                    if apply {
                        return_view.default_layer().await
                    } else {
                        return_view
                    }
                }
                NodesViewCollection::Layer(layer) => return_view.layer(layer).await,
                NodesViewCollection::ExcludeLayer(layer) => return_view.exclude_layer(layer).await,
                NodesViewCollection::Layers(layers) => return_view.layers(layers).await,
                NodesViewCollection::ExcludeLayers(layers) => {
                    return_view.exclude_layers(layers).await
                }
                NodesViewCollection::Window(window) => {
                    return_view.window(window.start, window.end).await
                }
                NodesViewCollection::At(at) => return_view.at(at).await,
                NodesViewCollection::Latest(apply) => {
                    if apply {
                        return_view.latest().await
                    } else {
                        return_view
                    }
                }
                NodesViewCollection::SnapshotLatest(apply) => {
                    if apply {
                        return_view.snapshot_latest().await
                    } else {
                        return_view
                    }
                }
                NodesViewCollection::SnapshotAt(at) => return_view.snapshot_at(at).await,
                NodesViewCollection::Before(time) => return_view.before(time).await,
                NodesViewCollection::After(time) => return_view.after(time).await,
                NodesViewCollection::ShrinkWindow(window) => {
                    return_view.shrink_window(window.start, window.end).await
                }
                NodesViewCollection::ShrinkStart(time) => return_view.shrink_start(time).await,
                NodesViewCollection::ShrinkEnd(time) => return_view.shrink_end(time).await,
                NodesViewCollection::NodeFilter(node_filter) => {
                    return_view.node_filter(node_filter).await?
                }
                NodesViewCollection::TypeFilter(types) => return_view.type_filter(types).await,
            }
        }

        Ok(return_view)
    }

    /////////////////
    //// Sorting ////
    /////////////////

    async fn sorted(&self, sort_bys: Vec<NodeSortBy>) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || {
            let sorted: Index<VID> = self_clone
                .nn
                .iter()
                .sorted_by(|first_node, second_node| {
                    sort_bys
                        .iter()
                        .fold(Ordering::Equal, |current_ordering, sort_by| {
                            current_ordering.then_with(|| {
                                let ordering = if sort_by.id == Some(true) {
                                    first_node.id().partial_cmp(&second_node.id())
                                } else if let Some(sort_by_time) = sort_by.time.as_ref() {
                                    let (first_time, second_time) = match sort_by_time {
                                        SortByTime::Latest => {
                                            (first_node.latest_time(), second_node.latest_time())
                                        }
                                        SortByTime::Earliest => (
                                            first_node.earliest_time(),
                                            second_node.earliest_time(),
                                        ),
                                    };
                                    first_time.partial_cmp(&second_time)
                                } else if let Some(sort_by_property) = sort_by.property.as_ref() {
                                    let first_prop_maybe =
                                        first_node.properties().get(sort_by_property);
                                    let second_prop_maybe =
                                        second_node.properties().get(sort_by_property);
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
                        })
                })
                .map(|node_view| node_view.node)
                .collect();
            GqlNodes::new(self_clone.nn.indexed(sorted))
        })
        .await
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    /// Returns the start time of the window. Errors if there is no window.
    async fn start(&self) -> Option<i64> {
        self.nn.start()
    }

    /// Returns the end time of the window. Errors if there is no window.
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

    /// Returns a view of the node ids.
    async fn ids(&self) -> Vec<String> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.nn.name().collect()).await
    }
}
