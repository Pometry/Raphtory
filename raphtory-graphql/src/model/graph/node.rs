use crate::model::graph::{
    edges::GqlEdges, filtering::NodeViewCollection, nodes::GqlNodes,
    path_from_node::GqlPathFromNode, property::GqlProperties, windowset::GqlNodeWindowSet,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    algorithms::components::{in_component, out_component},
    core::utils::errors::{
        GraphError,
        GraphError::{MismatchedIntervalTypes, NoIntervalProvided, WrongNumOfArgs},
    },
    db::{
        api::{properties::dyn_props::DynProperties, view::*},
        graph::node::NodeView,
    },
    prelude::NodeStateOps,
};
use tokio::{spawn, task::spawn_blocking};

#[derive(ResolvedObject, Clone)]
#[graphql(name = "Node")]
pub struct GqlNode {
    pub(crate) vv: NodeView<DynamicGraph>,
}

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>
    From<NodeView<G, GH>> for GqlNode
{
    fn from(value: NodeView<G, GH>) -> Self {
        Self {
            vv: NodeView {
                base_graph: value.base_graph.into_dynamic(),
                graph: value.graph.into_dynamic(),
                node: value.node,
            },
        }
    }
}

#[ResolvedObjectFields]
impl GqlNode {
    async fn id(&self) -> String {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.vv.id().to_string())
            .await
            .unwrap()
    }

    pub async fn name(&self) -> String {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.vv.name()).await.unwrap()
    }

    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////
    async fn default_layer(&self) -> GqlNode {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.vv.default_layer().into())
            .await
            .unwrap()
    }
    async fn layers(&self, names: Vec<String>) -> GqlNode {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.vv.valid_layers(names).into())
            .await
            .unwrap()
    }

    async fn exclude_layers(&self, names: Vec<String>) -> GqlNode {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.vv.exclude_valid_layers(names).into())
            .await
            .unwrap()
    }

    async fn layer(&self, name: String) -> GqlNode {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.vv.valid_layers(name).into())
            .await
            .unwrap()
    }

    async fn exclude_layer(&self, name: String) -> GqlNode {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.vv.exclude_valid_layers(name).into())
            .await
            .unwrap()
    }

    async fn rolling(
        &self,
        window_str: Option<String>,
        window_int: Option<i64>,
        step_str: Option<String>,
        step_int: Option<i64>,
    ) -> Result<GqlNodeWindowSet, GraphError> {
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
                Ok(GqlNodeWindowSet::new(
                    self_clone.vv.rolling(window_int, step_int)?,
                ))
            }
            (Some(window_str), None) => {
                if step_int.is_some() {
                    return Err(MismatchedIntervalTypes);
                }
                Ok(GqlNodeWindowSet::new(
                    self_clone.vv.rolling(window_str, step_str)?,
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
    ) -> Result<GqlNodeWindowSet, GraphError> {
        let self_clone = self.clone();
        spawn_blocking(move || match (step_str, step_int) {
            (Some(_), Some(_)) => Err(WrongNumOfArgs(
                "step_str".to_string(),
                "step_int".to_string(),
            )),
            (None, Some(step_int)) => Ok(GqlNodeWindowSet::new(self_clone.vv.expanding(step_int)?)),
            (Some(step_str), None) => Ok(GqlNodeWindowSet::new(self_clone.vv.expanding(step_str)?)),
            (None, None) => return Err(NoIntervalProvided),
        })
        .await
        .unwrap()
    }

    async fn window(&self, start: i64, end: i64) -> GqlNode {
        self.vv.window(start, end).into()
    }

    async fn at(&self, time: i64) -> GqlNode {
        self.vv.at(time).into()
    }

    async fn latest(&self) -> GqlNode {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.vv.latest().into())
            .await
            .unwrap()
    }

    async fn snapshot_at(&self, time: i64) -> GqlNode {
        self.vv.snapshot_at(time).into()
    }

    async fn snapshot_latest(&self) -> GqlNode {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.vv.snapshot_latest().into())
            .await
            .unwrap()
    }

    async fn before(&self, time: i64) -> GqlNode {
        self.vv.before(time).into()
    }

    async fn after(&self, time: i64) -> GqlNode {
        self.vv.after(time).into()
    }

    async fn shrink_window(&self, start: i64, end: i64) -> Self {
        self.vv.shrink_window(start, end).into()
    }

    async fn shrink_start(&self, start: i64) -> Self {
        self.vv.shrink_start(start).into()
    }

    async fn shrink_end(&self, end: i64) -> Self {
        self.vv.shrink_end(end).into()
    }

    async fn apply_views(&self, views: Vec<NodeViewCollection>) -> Result<GqlNode, GraphError> {
        let mut return_view: GqlNode = self.vv.clone().into();
        spawn(async move {
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

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    async fn earliest_time(&self) -> Option<i64> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.vv.earliest_time())
            .await
            .unwrap()
    }

    async fn first_update(&self) -> Option<i64> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.vv.history().first().cloned())
            .await
            .unwrap()
    }

    async fn latest_time(&self) -> Option<i64> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.vv.latest_time())
            .await
            .unwrap()
    }

    async fn last_update(&self) -> Option<i64> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.vv.history().last().cloned())
            .await
            .unwrap()
    }

    async fn start(&self) -> Option<i64> {
        self.vv.start()
    }

    async fn end(&self) -> Option<i64> {
        self.vv.end()
    }

    async fn history(&self) -> Vec<i64> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.vv.history())
            .await
            .unwrap()
    }

    async fn is_active(&self) -> bool {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.vv.is_active())
            .await
            .unwrap()
    }

    ////////////////////////
    /////// PROPERTIES /////
    ////////////////////////

    pub async fn node_type(&self) -> Option<String> {
        let self_clone = self.clone();
        spawn_blocking(move || match self_clone.vv.node_type() {
            None => None,
            str => str.map(|s| (*s).to_string()),
        })
        .await
        .unwrap()
    }

    async fn properties(&self) -> GqlProperties {
        Into::<DynProperties>::into(self.vv.properties()).into()
    }

    ////////////////////////
    //// EDGE GETTERS //////
    ////////////////////////
    /// Returns the number of edges connected to this node

    async fn degree(&self) -> usize {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.vv.degree())
            .await
            .unwrap()
    }

    /// Returns the number edges with this node as the source

    async fn out_degree(&self) -> usize {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.vv.out_degree())
            .await
            .unwrap()
    }

    /// Returns the number edges with this node as the destination

    async fn in_degree(&self) -> usize {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.vv.in_degree())
            .await
            .unwrap()
    }

    async fn in_component(&self) -> GqlNodes {
        let self_clone = self.clone();
        spawn_blocking(move || GqlNodes::new(in_component(self_clone.vv.clone()).nodes()))
            .await
            .unwrap()
    }

    async fn out_component(&self) -> GqlNodes {
        let self_clone = self.clone();
        spawn_blocking(move || GqlNodes::new(out_component(self_clone.vv.clone()).nodes()))
            .await
            .unwrap()
    }

    async fn edges(&self) -> GqlEdges {
        let self_clone = self.clone();
        spawn_blocking(move || GqlEdges::new(self_clone.vv.edges()))
            .await
            .unwrap()
    }

    async fn out_edges(&self) -> GqlEdges {
        let self_clone = self.clone();
        spawn_blocking(move || GqlEdges::new(self_clone.vv.out_edges()))
            .await
            .unwrap()
    }

    async fn in_edges(&self) -> GqlEdges {
        let self_clone = self.clone();
        spawn_blocking(move || GqlEdges::new(self_clone.vv.in_edges()))
            .await
            .unwrap()
    }

    async fn neighbours<'a>(&self) -> GqlPathFromNode {
        let self_clone = self.clone();
        spawn_blocking(move || GqlPathFromNode::new(self_clone.vv.neighbours()))
            .await
            .unwrap()
    }

    async fn in_neighbours<'a>(&self) -> GqlPathFromNode {
        let self_clone = self.clone();
        spawn_blocking(move || GqlPathFromNode::new(self_clone.vv.in_neighbours()))
            .await
            .unwrap()
    }

    async fn out_neighbours(&self) -> GqlPathFromNode {
        let self_clone = self.clone();
        spawn_blocking(move || GqlPathFromNode::new(self_clone.vv.out_neighbours()))
            .await
            .unwrap()
    }
}
