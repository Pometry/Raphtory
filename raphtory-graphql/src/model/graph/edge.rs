use crate::{
    model::graph::{
        edges::GqlEdges,
        filtering::EdgeViewCollection,
        node::GqlNode,
        property::GqlProperties,
        windowset::GqlEdgeWindowSet,
        WindowDuration,
        WindowDuration::{Duration, Epoch},
    },
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    db::{
        api::view::{DynamicGraph, EdgeViewOps, IntoDynamic, StaticGraphViewOps},
        graph::edge::EdgeView,
    },
    errors::GraphError,
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
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.valid_layers(names).into()).await
    }

    async fn exclude_layers(&self, names: Vec<String>) -> GqlEdge {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.exclude_valid_layers(names).into()).await
    }

    async fn layer(&self, name: String) -> GqlEdge {
        self.ee.valid_layers(name).into()
    }
    async fn exclude_layer(&self, name: String) -> GqlEdge {
        self.ee.exclude_valid_layers(name).into()
    }

    async fn rolling(
        &self,
        window: WindowDuration,
        step: Option<WindowDuration>,
    ) -> Result<GqlEdgeWindowSet, GraphError> {
        match window {
            Duration(window_duration) => match step {
                Some(step) => match step {
                    Duration(step_duration) => Ok(GqlEdgeWindowSet::new(
                        self.ee.rolling(window_duration, Some(step_duration))?,
                    )),
                    Epoch(_) => Err(GraphError::MismatchedIntervalTypes),
                },
                None => Ok(GqlEdgeWindowSet::new(
                    self.ee.rolling(window_duration, None)?,
                )),
            },
            Epoch(window_duration) => match step {
                Some(step) => match step {
                    Duration(_) => Err(GraphError::MismatchedIntervalTypes),
                    Epoch(step_duration) => Ok(GqlEdgeWindowSet::new(
                        self.ee.rolling(window_duration, Some(step_duration))?,
                    )),
                },
                None => Ok(GqlEdgeWindowSet::new(
                    self.ee.rolling(window_duration, None)?,
                )),
            },
        }
    }

    async fn expanding(&self, step: WindowDuration) -> Result<GqlEdgeWindowSet, GraphError> {
        match step {
            Duration(step) => Ok(GqlEdgeWindowSet::new(self.ee.expanding(step)?)),
            Epoch(step) => Ok(GqlEdgeWindowSet::new(self.ee.expanding(step)?)),
        }
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
            return_view = match view {
                EdgeViewCollection::DefaultLayer(apply) => {
                    if apply {
                        return_view.default_layer().await
                    } else {
                        return_view
                    }
                }
                EdgeViewCollection::Layers(layers) => return_view.layers(layers).await,

                EdgeViewCollection::ExcludeLayers(layers) => {
                    return_view.exclude_layers(layers).await
                }
                EdgeViewCollection::Layer(layer) => return_view.layer(layer).await,

                EdgeViewCollection::ExcludeLayer(layer) => return_view.exclude_layer(layer).await,

                EdgeViewCollection::Latest(apply) => {
                    if apply {
                        return_view.latest().await
                    } else {
                        return_view
                    }
                }
                EdgeViewCollection::SnapshotLatest(apply) => {
                    if apply {
                        return_view.snapshot_latest().await
                    } else {
                        return_view
                    }
                }
                EdgeViewCollection::SnapshotAt(at) => return_view.snapshot_at(at).await,
                EdgeViewCollection::Window(window) => {
                    return_view.window(window.start, window.end).await
                }
                EdgeViewCollection::At(at) => return_view.at(at).await,
                EdgeViewCollection::Before(time) => return_view.before(time).await,
                EdgeViewCollection::After(time) => return_view.after(time).await,
                EdgeViewCollection::ShrinkWindow(window) => {
                    return_view.shrink_window(window.start, window.end).await
                }
                EdgeViewCollection::ShrinkStart(time) => return_view.shrink_start(time).await,
                EdgeViewCollection::ShrinkEnd(time) => return_view.shrink_end(time).await,
            }
        }
        Ok(return_view)
    }

    async fn earliest_time(&self) -> Option<i64> {
        self.ee.earliest_time()
    }

    async fn first_update(&self) -> Option<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.history().first().cloned()).await
    }

    async fn latest_time(&self) -> Option<i64> {
        self.ee.latest_time()
    }

    async fn last_update(&self) -> Option<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.history().last().cloned()).await
    }

    async fn time(&self) -> Result<i64, GraphError> {
        self.ee.time()
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
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.history()).await
    }

    async fn deletions(&self) -> Vec<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.deletions()).await
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
}
