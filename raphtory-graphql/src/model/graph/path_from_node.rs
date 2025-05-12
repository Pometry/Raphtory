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

#[derive(ResolvedObject)]
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
        self.update(self.nn.valid_layers(names))
    }

    async fn exclude_layers(&self, names: Vec<String>) -> Self {
        self.update(self.nn.exclude_valid_layers(names))
    }

    async fn layer(&self, name: String) -> Self {
        self.update(self.nn.valid_layers(name))
    }

    async fn exclude_layer(&self, name: String) -> Self {
        self.update(self.nn.exclude_valid_layers(name))
    }

    fn rolling(
        &self,
        window_str: Option<String>,
        window_int: Option<i64>,
        step_str: Option<String>,
        step_int: Option<i64>,
    ) -> Result<GqlPathFromNodeWindowSet, GraphError> {
        match (window_str, window_int) {
            (Some(_), Some(_)) => Err(WrongNumOfArgs(
                "window_str".to_string(),
                "window_int".to_string(),
            )),
            (None, Some(window_int)) => {
                if step_str.is_some() {
                    return Err(MismatchedIntervalTypes);
                }
                Ok(GqlPathFromNodeWindowSet::new(
                    self.nn.rolling(window_int, step_int)?,
                ))
            }
            (Some(window_str), None) => {
                if step_int.is_some() {
                    return Err(MismatchedIntervalTypes);
                }
                Ok(GqlPathFromNodeWindowSet::new(
                    self.nn.rolling(window_str, step_str)?,
                ))
            }
            (None, None) => return Err(NoIntervalProvided),
        }
    }

    fn expanding(
        &self,
        step_str: Option<String>,
        step_int: Option<i64>,
    ) -> Result<GqlPathFromNodeWindowSet, GraphError> {
        match (step_str, step_int) {
            (Some(_), Some(_)) => Err(WrongNumOfArgs(
                "step_str".to_string(),
                "step_int".to_string(),
            )),
            (None, Some(step_int)) => {
                Ok(GqlPathFromNodeWindowSet::new(self.nn.expanding(step_int)?))
            }
            (Some(step_str), None) => {
                Ok(GqlPathFromNodeWindowSet::new(self.nn.expanding(step_str)?))
            }
            (None, None) => return Err(NoIntervalProvided),
        }
    }

    async fn window(&self, start: i64, end: i64) -> Self {
        self.update(self.nn.window(start, end))
    }

    async fn at(&self, time: i64) -> Self {
        self.update(self.nn.at(time))
    }

    async fn snapshot_latest(&self) -> Self {
        self.update(self.nn.snapshot_latest())
    }

    async fn snapshot_at(&self, time: i64) -> Self {
        self.update(self.nn.snapshot_at(time))
    }
    async fn latest(&self) -> Self {
        self.update(self.nn.latest())
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
        self.update(self.nn.type_filter(&node_types))
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
        self.iter().count()
    }

    async fn page(&self, limit: usize, offset: usize) -> Vec<GqlNode> {
        let start = offset * limit;
        self.iter().skip(start).take(limit).collect()
    }

    async fn list(&self) -> Vec<GqlNode> {
        self.iter().collect()
    }

    async fn ids(&self) -> Vec<String> {
        self.nn.name().collect()
    }
}
