use crate::model::graph::{
    edges::GqlEdges, filtering::EdgeViewCollection, node::GqlNode, property::GqlProperties,
    windowset::GqlEdgeWindowSet,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    core::utils::errors::{
        GraphError,
        GraphError::{MismatchedIntervalTypes, NoIntervalProvided, WrongNumOfArgs},
    },
    db::{
        api::view::{DynamicGraph, EdgeViewOps, IntoDynamic, StaticGraphViewOps},
        graph::edge::EdgeView,
    },
    prelude::{LayerOps, TimeOps},
};

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
        self.ee.default_layer().into()
    }

    async fn layers(&self, names: Vec<String>) -> GqlEdge {
        self.ee.valid_layers(names).into()
    }

    async fn exclude_layers(&self, names: Vec<String>) -> GqlEdge {
        self.ee.exclude_valid_layers(names).into()
    }

    async fn layer(&self, name: String) -> GqlEdge {
        self.ee.valid_layers(name).into()
    }

    async fn exclude_layer(&self, name: String) -> GqlEdge {
        self.ee.exclude_valid_layers(name).into()
    }

    async fn rolling(
        &self,
        window_str: Option<String>,
        window_int: Option<i64>,
        step_str: Option<String>,
        step_int: Option<i64>,
    ) -> Result<GqlEdgeWindowSet, GraphError> {
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || match (window_str, window_int) {
            (Some(_), Some(_)) => Err(WrongNumOfArgs(
                "window_str".to_string(),
                "window_int".to_string(),
            )),
            (None, Some(window_int)) => {
                if step_str.is_some() {
                    return Err(MismatchedIntervalTypes);
                }
                Ok(GqlEdgeWindowSet::new(
                    self_clone.ee.rolling(window_int, step_int)?,
                ))
            }
            (Some(window_str), None) => {
                if step_int.is_some() {
                    return Err(MismatchedIntervalTypes);
                }
                Ok(GqlEdgeWindowSet::new(
                    self_clone.ee.rolling(window_str, step_str)?,
                ))
            }
            (None, None) => return Err(NoIntervalProvided),
        })
        .await
        .unwrap()
    }

    async fn expanding(
        &self,
        step_str: Option<String>,
        step_int: Option<i64>,
    ) -> Result<GqlEdgeWindowSet, GraphError> {
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || match (step_str, step_int) {
            (Some(_), Some(_)) => Err(WrongNumOfArgs(
                "step_str".to_string(),
                "step_int".to_string(),
            )),
            (None, Some(step_int)) => Ok(GqlEdgeWindowSet::new(self_clone.ee.expanding(step_int)?)),
            (Some(step_str), None) => Ok(GqlEdgeWindowSet::new(self_clone.ee.expanding(step_str)?)),
            (None, None) => return Err(NoIntervalProvided),
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

        tokio::task::spawn(async move {
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
        })
        .await
        .unwrap()
    }

    async fn earliest_time(&self) -> Option<i64> {
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || self_clone.ee.earliest_time())
            .await
            .unwrap()
    }

    async fn first_update(&self) -> Option<i64> {
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || self_clone.ee.history().first().cloned())
            .await
            .unwrap()
    }

    async fn latest_time(&self) -> Option<i64> {
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || self_clone.ee.latest_time())
            .await
            .unwrap()
    }

    async fn last_update(&self) -> Option<i64> {
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || self_clone.ee.history().last().cloned())
            .await
            .unwrap()
    }

    async fn time(&self) -> Result<i64, GraphError> {
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || self_clone.ee.time().map(|x| x.into()))
            .await
            .unwrap()
    }

    async fn start(&self) -> Option<i64> {
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || self_clone.ee.start())
            .await
            .unwrap()
    }

    async fn end(&self) -> Option<i64> {
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || self_clone.ee.end())
            .await
            .unwrap()
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
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || {
            let (src_name, dst_name) = self_clone.ee.id();
            vec![src_name.to_string(), dst_name.to_string()]
        })
        .await
        .unwrap()
    }

    async fn properties(&self) -> GqlProperties {
        self.ee.properties().into()
    }

    async fn layer_names(&self) -> Vec<String> {
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || {
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
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || self_clone.ee.layer_name().map(|x| x.into()))
            .await
            .unwrap()
    }

    async fn explode(&self) -> GqlEdges {
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || GqlEdges::new(self_clone.ee.explode()))
            .await
            .unwrap()
    }

    async fn explode_layers(&self) -> GqlEdges {
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || GqlEdges::new(self_clone.ee.explode_layers()))
            .await
            .unwrap()
    }

    async fn history(&self) -> Vec<i64> {
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || self_clone.ee.history())
            .await
            .unwrap()
    }

    async fn deletions(&self) -> Vec<i64> {
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || self_clone.ee.deletions())
            .await
            .unwrap()
    }

    async fn is_valid(&self) -> bool {
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || self_clone.ee.is_valid())
            .await
            .unwrap()
    }

    async fn is_active(&self) -> bool {
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || self_clone.ee.is_active())
            .await
            .unwrap()
    }

    async fn is_deleted(&self) -> bool {
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || self_clone.ee.is_deleted())
            .await
            .unwrap()
    }

    async fn is_self_loop(&self) -> bool {
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || self_clone.ee.is_self_loop())
            .await
            .unwrap()
    }
}
