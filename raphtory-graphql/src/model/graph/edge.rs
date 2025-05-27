use crate::model::graph::{
    edges::GqlEdges,
    filtering::EdgeViewCollection,
    node::GqlNode,
    property::GqlProperties,
    windowset::GqlEdgeWindowSet,
    WindowDuration,
    WindowDuration::{Duration, Epoch},
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    core::utils::errors::{GraphError, GraphError::MismatchedIntervalTypes},
    db::{
        api::view::{DynamicGraph, EdgeViewOps, IntoDynamic, StaticGraphViewOps},
        graph::edge::EdgeView,
    },
    prelude::{LayerOps, TimeOps},
};
use tokio::task::spawn_blocking;

#[derive(ResolvedObject, Clone)]
#[graphql(name = "Edge")]
pub struct GqlEdge {
    pub(crate) ee: EdgeView<DynamicGraph>,
}

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>
    From<EdgeView<G, GH>> for GqlEdge
{
    fn from(value: EdgeView<G, GH>) -> Self {
        Self {
            ee: EdgeView {
                base_graph: value.base_graph.into_dynamic(),
                graph: value.graph.into_dynamic(),
                edge: value.edge,
            },
        }
    }
}

impl GqlEdge {
    pub(crate) fn from_ref<
        G: StaticGraphViewOps + IntoDynamic,
        GH: StaticGraphViewOps + IntoDynamic,
    >(
        value: EdgeView<&G, &GH>,
    ) -> Self {
        value.cloned().into()
    }
}

#[ResolvedObjectFields]
impl GqlEdge {
    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    async fn default_layer(&self) -> GqlEdge {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.ee.default_layer().into())
            .await
            .unwrap()
    }

    async fn layers(&self, names: Vec<String>) -> GqlEdge {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.ee.valid_layers(names).into())
            .await
            .unwrap()
    }

    async fn exclude_layers(&self, names: Vec<String>) -> GqlEdge {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.ee.exclude_valid_layers(names).into())
            .await
            .unwrap()
    }

    async fn layer(&self, name: String) -> GqlEdge {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.ee.valid_layers(name).into())
            .await
            .unwrap()
    }
    async fn exclude_layer(&self, name: String) -> GqlEdge {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.ee.exclude_valid_layers(name).into())
            .await
            .unwrap()
    }

    async fn rolling(
        &self,
        window: WindowDuration,
        step: Option<WindowDuration>,
    ) -> Result<GqlEdgeWindowSet, GraphError> {
        let self_clone = self.clone();
        spawn_blocking(move || match window {
            Duration(window_duration) => match step {
                Some(step) => match step {
                    Duration(step_duration) => Ok(GqlEdgeWindowSet::new(
                        self_clone
                            .ee
                            .rolling(window_duration, Some(step_duration))?,
                    )),
                    Epoch(_) => Err(MismatchedIntervalTypes),
                },
                None => Ok(GqlEdgeWindowSet::new(
                    self_clone.ee.rolling(window_duration, None)?,
                )),
            },
            Epoch(window_duration) => match step {
                Some(step) => match step {
                    Duration(_) => Err(MismatchedIntervalTypes),
                    Epoch(step_duration) => Ok(GqlEdgeWindowSet::new(
                        self_clone
                            .ee
                            .rolling(window_duration, Some(step_duration))?,
                    )),
                },
                None => Ok(GqlEdgeWindowSet::new(
                    self_clone.ee.rolling(window_duration, None)?,
                )),
            },
        })
        .await
        .unwrap()
    }

    async fn expanding(&self, step: WindowDuration) -> Result<GqlEdgeWindowSet, GraphError> {
        let self_clone = self.clone();
        spawn_blocking(move || match step {
            Duration(step) => Ok(GqlEdgeWindowSet::new(self_clone.ee.expanding(step)?)),
            Epoch(step) => Ok(GqlEdgeWindowSet::new(self_clone.ee.expanding(step)?)),
        })
        .await
        .unwrap()
    }

    async fn window(&self, start: i64, end: i64) -> GqlEdge {
        self.ee.window(start, end).into()
    }

    async fn at(&self, time: i64) -> GqlEdge {
        self.ee.at(time).into()
    }

    async fn latest(&self) -> GqlEdge {
        self.ee.latest().into()
    }

    async fn snapshot_at(&self, time: i64) -> GqlEdge {
        self.ee.snapshot_at(time).into()
    }

    async fn snapshot_latest(&self) -> GqlEdge {
        self.ee.snapshot_latest().into()
    }

    async fn before(&self, time: i64) -> GqlEdge {
        self.ee.before(time).into()
    }

    async fn after(&self, time: i64) -> GqlEdge {
        self.ee.after(time).into()
    }

    async fn shrink_window(&self, start: i64, end: i64) -> Self {
        self.ee.shrink_window(start, end).into()
    }

    async fn shrink_start(&self, start: i64) -> Self {
        self.ee.shrink_start(start).into()
    }

    async fn shrink_end(&self, end: i64) -> Self {
        self.ee.shrink_end(end).into()
    }

    async fn apply_views(&self, views: Vec<EdgeViewCollection>) -> Result<GqlEdge, GraphError> {
        let mut return_view: GqlEdge = self.ee.clone().into();
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
            if count > 1 {
                return Err(GraphError::TooManyViewsSet);
            }
        }
        Ok(return_view)
    }

    async fn earliest_time(&self) -> Option<i64> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.ee.earliest_time())
            .await
            .unwrap()
    }

    async fn first_update(&self) -> Option<i64> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.ee.history().first().cloned())
            .await
            .unwrap()
    }

    async fn latest_time(&self) -> Option<i64> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.ee.latest_time())
            .await
            .unwrap()
    }

    async fn last_update(&self) -> Option<i64> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.ee.history().last().cloned())
            .await
            .unwrap()
    }

    async fn time(&self) -> Result<i64, GraphError> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.ee.time().map(|x| x.into()))
            .await
            .unwrap()
    }

    async fn start(&self) -> Option<i64> {
        self.ee.start()
    }

    async fn end(&self) -> Option<i64> {
        self.ee.end()
    }

    async fn src(&self) -> GqlNode {
        self.ee.src().into()
    }

    async fn dst(&self) -> GqlNode {
        self.ee.dst().into()
    }
    async fn nbr(&self) -> GqlNode {
        self.ee.nbr().into()
    }

    async fn id(&self) -> Vec<String> {
        let (src_name, dst_name) = self.ee.id();
        vec![src_name.to_string(), dst_name.to_string()]
    }

    async fn properties(&self) -> GqlProperties {
        self.ee.properties().into()
    }

    async fn layer_names(&self) -> Vec<String> {
        let self_clone = self.clone();
        spawn_blocking(move || {
            self_clone
                .ee
                .layer_names()
                .into_iter()
                .map(|x| x.into())
                .collect()
        })
        .await
        .unwrap()
    }

    async fn layer_name(&self) -> Result<String, GraphError> {
        self.ee.layer_name().map(|x| x.into())
    }

    async fn explode(&self) -> GqlEdges {
        GqlEdges::new(self.ee.explode())
    }

    async fn explode_layers(&self) -> GqlEdges {
        GqlEdges::new(self.ee.explode_layers())
    }

    async fn history(&self) -> Vec<i64> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.ee.history())
            .await
            .unwrap()
    }

    async fn deletions(&self) -> Vec<i64> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.ee.deletions())
            .await
            .unwrap()
    }

    async fn is_valid(&self) -> bool {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.ee.is_valid())
            .await
            .unwrap()
    }

    async fn is_active(&self) -> bool {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.ee.is_active())
            .await
            .unwrap()
    }

    async fn is_deleted(&self) -> bool {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.ee.is_deleted())
            .await
            .unwrap()
    }

    async fn is_self_loop(&self) -> bool {
        self.ee.is_self_loop()
    }
}
