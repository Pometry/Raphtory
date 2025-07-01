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

    async fn layers(&self, names: Vec<String>) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.valid_layers(names))).await
    }

    async fn exclude_layers(&self, names: Vec<String>) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.exclude_valid_layers(names))).await
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
    ) -> Result<GqlPathFromNodeWindowSet, GraphError> {
        match window {
            Duration(window_duration) => match step {
                Some(step) => match step {
                    Duration(step_duration) => Ok(GqlPathFromNodeWindowSet::new(
                        self.nn.rolling(window_duration, Some(step_duration))?,
                    )),
                    Epoch(_) => Err(GraphError::MismatchedIntervalTypes),
                },
                None => Ok(GqlPathFromNodeWindowSet::new(
                    self.nn.rolling(window_duration, None)?,
                )),
            },
            Epoch(window_duration) => match step {
                Some(step) => match step {
                    Duration(_) => Err(GraphError::MismatchedIntervalTypes),
                    Epoch(step_duration) => Ok(GqlPathFromNodeWindowSet::new(
                        self.nn.rolling(window_duration, Some(step_duration))?,
                    )),
                },
                None => Ok(GqlPathFromNodeWindowSet::new(
                    self.nn.rolling(window_duration, None)?,
                )),
            },
        }
    }

    async fn expanding(
        &self,
        step: WindowDuration,
    ) -> Result<GqlPathFromNodeWindowSet, GraphError> {
        match step {
            Duration(step) => Ok(GqlPathFromNodeWindowSet::new(self.nn.expanding(step)?)),
            Epoch(step) => Ok(GqlPathFromNodeWindowSet::new(self.nn.expanding(step)?)),
        }
    }

    async fn window(&self, start: i64, end: i64) -> Self {
        self.update(self.nn.window(start, end))
    }

    async fn at(&self, time: i64) -> Self {
        self.update(self.nn.at(time))
    }

    async fn snapshot_latest(&self) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.snapshot_latest())).await
    }

    async fn snapshot_at(&self, time: i64) -> Self {
        self.update(self.nn.snapshot_at(time))
    }
    async fn latest(&self) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.latest())).await
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
        blocking_compute(move || self_clone.update(self_clone.nn.type_filter(&node_types))).await
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
        blocking_compute(move || self_clone.nn.len()).await
    }

    async fn page(&self, limit: usize, offset: usize) -> Vec<GqlNode> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let start = offset * limit;
            self_clone.iter().skip(start).take(limit).collect()
        })
        .await
    }

    async fn list(&self) -> Vec<GqlNode> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.iter().collect()).await
    }

    async fn ids(&self) -> Vec<String> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.nn.name().collect()).await
    }

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
