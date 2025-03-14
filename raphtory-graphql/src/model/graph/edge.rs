use crate::model::graph::{
    edges::GqlEdges, filtering::EdgeViewCollection, node::Node, property::GqlProperties,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    core::utils::errors::GraphError,
    db::{
        api::view::{DynamicGraph, EdgeViewOps, IntoDynamic, StaticGraphViewOps},
        graph::edge::EdgeView,
    },
    prelude::{LayerOps, TimeOps},
};

#[derive(ResolvedObject, Clone)]
pub struct Edge {
    pub(crate) ee: EdgeView<DynamicGraph>,
}

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>
    From<EdgeView<G, GH>> for Edge
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

impl Edge {
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
impl Edge {
    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    async fn default_layer(&self) -> Edge {
        self.ee.default_layer().into()
    }

    async fn layers(&self, names: Vec<String>) -> Edge {
        self.ee.valid_layers(names).into()
    }

    async fn exclude_layers(&self, names: Vec<String>) -> Edge {
        self.ee.exclude_valid_layers(names).into()
    }

    async fn layer(&self, name: String) -> Edge {
        self.ee.valid_layers(name).into()
    }

    async fn exclude_layer(&self, name: String) -> Edge {
        self.ee.exclude_valid_layers(name).into()
    }

    async fn window(&self, start: i64, end: i64) -> Edge {
        self.ee.window(start, end).into()
    }

    async fn at(&self, time: i64) -> Edge {
        self.ee.at(time).into()
    }

    async fn latest(&self) -> Edge {
        self.ee.latest().into()
    }

    async fn snapshot_at(&self, time: i64) -> Edge {
        self.ee.snapshot_at(time).into()
    }

    async fn snapshot_latest(&self) -> Edge {
        self.ee.snapshot_latest().into()
    }

    async fn before(&self, time: i64) -> Edge {
        self.ee.before(time).into()
    }

    async fn after(&self, time: i64) -> Edge {
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

    async fn apply_views(&self, views: Vec<EdgeViewCollection>) -> Result<Edge, GraphError> {
        let mut return_view: Edge = self.ee.clone().into();

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
        self.ee.earliest_time()
    }

    async fn first_update(&self) -> Option<i64> {
        self.ee.history().first().cloned()
    }

    async fn latest_time(&self) -> Option<i64> {
        self.ee.latest_time()
    }

    async fn last_update(&self) -> Option<i64> {
        self.ee.history().last().cloned()
    }

    async fn time(&self) -> Result<i64, GraphError> {
        self.ee.time().map(|x| x.into())
    }

    async fn start(&self) -> Option<i64> {
        self.ee.start()
    }

    async fn end(&self) -> Option<i64> {
        self.ee.end()
    }

    async fn src(&self) -> Node {
        self.ee.src().into()
    }

    async fn dst(&self) -> Node {
        self.ee.dst().into()
    }

    async fn id(&self) -> Vec<String> {
        let (src_name, dst_name) = self.ee.id();
        vec![src_name.to_string(), dst_name.to_string()]
    }

    async fn properties(&self) -> GqlProperties {
        self.ee.properties().into()
    }

    async fn layer_names(&self) -> Vec<String> {
        self.ee
            .layer_names()
            .into_iter()
            .map(|x| x.into())
            .collect()
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
        self.ee.history()
    }

    async fn deletions(&self) -> Vec<i64> {
        self.ee.deletions()
    }

    async fn is_valid(&self) -> bool {
        self.ee.is_valid()
    }

    async fn is_active(&self) -> bool {
        self.ee.is_active()
    }

    async fn is_deleted(&self) -> bool {
        self.ee.is_deleted()
    }

    async fn is_self_loop(&self) -> bool {
        self.ee.is_self_loop()
    }

    async fn nbr(&self) -> Node {
        self.ee.nbr().into()
    }
}
