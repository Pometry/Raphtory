use crate::model::graph::{node::GqlNode, windowset::GqlPathFromNodeWindowSet};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    core::utils::errors::{
        GraphError,
        GraphError::{MismatchedIntervalTypes, NoIntervalProvided, WrongNumOfArgs},
    },
    db::{api::view::DynamicGraph, graph::path::PathFromNode},
    prelude::*,
};
use tokio::task::spawn_blocking;

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
        spawn_blocking(move || self_clone.update(self_clone.nn.valid_layers(names)))
            .await
            .unwrap()
    }

    async fn exclude_layers(&self, names: Vec<String>) -> Self {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.update(self_clone.nn.exclude_valid_layers(names)))
            .await
            .unwrap()
    }

    async fn layer(&self, name: String) -> Self {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.update(self_clone.nn.valid_layers(name)))
            .await
            .unwrap()
    }

    async fn exclude_layer(&self, name: String) -> Self {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.update(self_clone.nn.exclude_valid_layers(name)))
            .await
            .unwrap()
    }

    async fn rolling(
        &self,
        window_str: Option<String>,
        window_int: Option<i64>,
        step_str: Option<String>,
        step_int: Option<i64>,
    ) -> Result<GqlPathFromNodeWindowSet, GraphError> {
        let self_clone = self.clone();
        spawn_blocking(move || match (window_str, window_int) {
            (Some(_), Some(_)) => Err(WrongNumOfArgs(
                "window_str".to_string(),
                "window_int".to_string(),
            )),
            (None, Some(window_int)) => {
                if step_str.is_some() {
                    return Err(MismatchedIntervalTypes);
                }
                Ok(GqlPathFromNodeWindowSet::new(
                    self_clone.nn.rolling(window_int, step_int)?,
                ))
            }
            (Some(window_str), None) => {
                if step_int.is_some() {
                    return Err(MismatchedIntervalTypes);
                }
                Ok(GqlPathFromNodeWindowSet::new(
                    self_clone.nn.rolling(window_str, step_str)?,
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
    ) -> Result<GqlPathFromNodeWindowSet, GraphError> {
        let self_clone = self.clone();
        spawn_blocking(move || match (step_str, step_int) {
            (Some(_), Some(_)) => Err(WrongNumOfArgs(
                "step_str".to_string(),
                "step_int".to_string(),
            )),
            (None, Some(step_int)) => Ok(GqlPathFromNodeWindowSet::new(
                self_clone.nn.expanding(step_int)?,
            )),
            (Some(step_str), None) => Ok(GqlPathFromNodeWindowSet::new(
                self_clone.nn.expanding(step_str)?,
            )),
            (None, None) => return Err(NoIntervalProvided),
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

    async fn snapshot_latest(&self) -> Self {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.update(self_clone.nn.snapshot_latest()))
            .await
            .unwrap()
    }

    async fn snapshot_at(&self, time: i64) -> Self {
        self.update(self.nn.snapshot_at(time))
    }
    async fn latest(&self) -> Self {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.update(self_clone.nn.latest()))
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
