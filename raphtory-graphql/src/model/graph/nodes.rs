use crate::model::{
    graph::{
        filtering::{NodeFilter, NodesViewCollection},
        node::GqlNode,
        windowset::GqlNodesWindowSet,
        WindowDuration,
        WindowDuration::{Duration, Epoch},
    },
    sorting::{NodeSortBy, SortByTime},
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
use tokio::task::spawn_blocking;

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
    async fn default_layer(&self) -> Self {
        self.update(self.nn.default_layer())
    }

    async fn layers(&self, names: Vec<String>) -> Self {
        self.update(self.nn.valid_layers(names))
    }

    async fn exclude_layers(&self, names: Vec<String>) -> Self {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.update(self_clone.nn.exclude_valid_layers(names)))
            .await
            .unwrap()
    }

    async fn layer(&self, name: String) -> Self {
        self.update(self.nn.valid_layers(name))
    }

    async fn exclude_layer(&self, name: String) -> Self {
        self.update(self.nn.exclude_valid_layers(name))
    }

    async fn rolling(
        &self,
        window: WindowDuration,
        step: Option<WindowDuration>,
    ) -> Result<GqlNodesWindowSet, GraphError> {
        let self_clone = self.clone();
        spawn_blocking(move || match window {
            Duration(window_duration) => match step {
                Some(step) => match step {
                    Duration(step_duration) => Ok(GqlNodesWindowSet::new(
                        self_clone
                            .nn
                            .rolling(window_duration, Some(step_duration))?,
                    )),
                    Epoch(_) => Err(GraphError::MismatchedIntervalTypes),
                },
                None => Ok(GqlNodesWindowSet::new(
                    self_clone.nn.rolling(window_duration, None)?,
                )),
            },
            Epoch(window_duration) => match step {
                Some(step) => match step {
                    Duration(_) => Err(GraphError::MismatchedIntervalTypes),
                    Epoch(step_duration) => Ok(GqlNodesWindowSet::new(
                        self_clone
                            .nn
                            .rolling(window_duration, Some(step_duration))?,
                    )),
                },
                None => Ok(GqlNodesWindowSet::new(
                    self_clone.nn.rolling(window_duration, None)?,
                )),
            },
        })
        .await
        .unwrap()
    }

    async fn expanding(&self, step: WindowDuration) -> Result<GqlNodesWindowSet, GraphError> {
        let self_clone = self.clone();
        spawn_blocking(move || match step {
            Duration(step) => Ok(GqlNodesWindowSet::new(self_clone.nn.expanding(step)?)),
            Epoch(step) => Ok(GqlNodesWindowSet::new(self_clone.nn.expanding(step)?)),
        })
        .await
        .unwrap()
    }

    async fn window(&self, start: i64, end: i64) -> Self {
        self.update(self.nn.window(start, end))
    }

    async fn at(&self, time: i64) -> Self {
        self.update(self.nn.at(time))
    }

    async fn latest(&self) -> Self {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.update(self_clone.nn.latest()))
            .await
            .unwrap()
    }

    async fn snapshot_at(&self, time: i64) -> Self {
        self.update(self.nn.snapshot_at(time))
    }

    async fn snapshot_latest(&self) -> Self {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.update(self_clone.nn.snapshot_latest()))
            .await
            .unwrap()
    }

    async fn before(&self, time: i64) -> Self {
        self.update(self.nn.before(time))
    }

    async fn after(&self, time: i64) -> Self {
        self.update(self.nn.after(time))
    }

    async fn shrink_window(&self, start: i64, end: i64) -> Self {
        self.update(self.nn.shrink_window(start, end))
    }

    async fn shrink_start(&self, start: i64) -> Self {
        self.update(self.nn.shrink_start(start))
    }

    async fn shrink_end(&self, end: i64) -> Self {
        self.update(self.nn.shrink_end(end))
    }

    async fn type_filter(&self, node_types: Vec<String>) -> Self {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.update(self_clone.nn.type_filter(&node_types)))
            .await
            .unwrap()
    }

    async fn node_filter(&self, filter: NodeFilter) -> Result<Self, GraphError> {
        let self_clone = self.clone();
        spawn_blocking(move || {
            let filter: CompositeNodeFilter = filter.try_into()?;
            let filtered_nodes = self_clone.nn.filter_nodes(filter)?;
            Ok(self_clone.update(filtered_nodes.into_dyn()))
        })
        .await
        .unwrap()
    }

    async fn apply_views(&self, views: Vec<NodesViewCollection>) -> Result<GqlNodes, GraphError> {
        let mut return_view: GqlNodes = GqlNodes::new(self.nn.clone());
        for view in views {
            let mut count = 0;
            if let Some(_) = view.default_layer {
                count += 1;
                return_view = return_view.default_layer().await;
            }
            if let Some(layers) = view.layers {
                count += 1;
                return_view = return_view.layers(layers).await;
            }
            if let Some(layers) = view.exclude_layers {
                count += 1;
                return_view = return_view.exclude_layers(layers).await;
            }
            if let Some(layer) = view.layer {
                count += 1;
                return_view = return_view.layer(layer).await;
            }
            if let Some(layer) = view.exclude_layer {
                count += 1;
                return_view = return_view.exclude_layer(layer).await;
            }
            if let Some(window) = view.window {
                count += 1;
                return_view = return_view.window(window.start, window.end).await;
            }
            if let Some(time) = view.at {
                count += 1;
                return_view = return_view.at(time).await;
            }
            if let Some(_) = view.latest {
                count += 1;
                return_view = return_view.latest().await;
            }
            if let Some(time) = view.snapshot_at {
                count += 1;
                return_view = return_view.snapshot_at(time).await;
            }
            if let Some(_) = view.snapshot_latest {
                count += 1;
                return_view = return_view.snapshot_latest().await;
            }
            if let Some(time) = view.before {
                count += 1;
                return_view = return_view.before(time).await;
            }
            if let Some(time) = view.after {
                count += 1;
                return_view = return_view.after(time).await;
            }
            if let Some(window) = view.shrink_window {
                count += 1;
                return_view = return_view.shrink_window(window.start, window.end).await;
            }
            if let Some(time) = view.shrink_start {
                count += 1;
                return_view = return_view.shrink_start(time).await;
            }
            if let Some(time) = view.shrink_end {
                count += 1;
                return_view = return_view.shrink_end(time).await;
            }
            if let Some(types) = view.type_filter {
                count += 1;
                return_view = return_view.type_filter(types).await;
            }
            if let Some(node_filter) = view.node_filter {
                count += 1;
                return_view = return_view.node_filter(node_filter).await?;
            }

            if count > 1 {
                return Err(GraphError::TooManyViewsSet);
            }
        }

        Ok(return_view)
    }

    /////////////////
    //// Sorting ////
    /////////////////

    async fn sorted(&self, sort_bys: Vec<NodeSortBy>) -> Self {
        let self_clone = self.clone();
        spawn_blocking(move || {
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
        .unwrap()
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    async fn start(&self) -> Option<i64> {
        self.nn.start()
    }

    async fn end(&self) -> Option<i64> {
        self.nn.end()
    }

    /////////////////
    //// List ///////
    /////////////////

    async fn count(&self) -> usize {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.iter().count())
            .await
            .unwrap()
    }

    async fn page(&self, limit: usize, offset: usize) -> Vec<GqlNode> {
        let self_clone = self.clone();
        spawn_blocking(move || {
            let start = offset * limit;
            self_clone.iter().skip(start).take(limit).collect()
        })
        .await
        .unwrap()
    }

    async fn list(&self) -> Vec<GqlNode> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.iter().collect())
            .await
            .unwrap()
    }

    async fn ids(&self) -> Vec<String> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.nn.name().collect())
            .await
            .unwrap()
    }
}
