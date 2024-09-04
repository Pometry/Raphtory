use crate::model::graph::{node::Node, property::GqlProperties};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    core::utils::errors::GraphError,
    db::{
        api::view::{DynamicGraph, EdgeViewOps, IntoDynamic, StaticGraphViewOps},
        graph::edge::EdgeView,
    },
    prelude::{LayerOps, TimeOps},
};
use tracing::instrument;

#[derive(ResolvedObject)]
pub(crate) struct Edge {
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

#[ResolvedObjectFields]
impl Edge {
    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    #[instrument(skip(self))]
    async fn layers(&self, names: Vec<String>) -> Edge {
        self.ee.valid_layers(names).into()
    }

    #[instrument(skip(self))]
    async fn exclude_layers(&self, names: Vec<String>) -> Edge {
        self.ee.exclude_valid_layers(names).into()
    }

    #[instrument(skip(self))]
    async fn layer(&self, name: String) -> Edge {
        self.ee.valid_layers(name).into()
    }

    #[instrument(skip(self))]
    async fn exclude_layer(&self, name: String) -> Edge {
        self.ee.exclude_valid_layers(name).into()
    }

    #[instrument(skip(self))]
    async fn window(&self, start: i64, end: i64) -> Edge {
        self.ee.window(start, end).into()
    }

    #[instrument(skip(self))]
    async fn at(&self, time: i64) -> Edge {
        self.ee.at(time).into()
    }

    #[instrument(skip(self))]
    async fn before(&self, time: i64) -> Edge {
        self.ee.before(time).into()
    }

    #[instrument(skip(self))]
    async fn after(&self, time: i64) -> Edge {
        self.ee.after(time).into()
    }

    #[instrument(skip(self))]
    async fn shrink_window(&self, start: i64, end: i64) -> Self {
        self.ee.shrink_window(start, end).into()
    }

    #[instrument(skip(self))]
    async fn shrink_start(&self, start: i64) -> Self {
        self.ee.shrink_start(start).into()
    }

    #[instrument(skip(self))]
    async fn shrink_end(&self, end: i64) -> Self {
        self.ee.shrink_end(end).into()
    }

    #[instrument(skip(self))]
    async fn earliest_time(&self) -> Option<i64> {
        self.ee.earliest_time()
    }

    #[instrument(skip(self))]
    async fn first_update(&self) -> Option<i64> {
        self.ee.history().first().cloned()
    }

    #[instrument(skip(self))]
    async fn latest_time(&self) -> Option<i64> {
        self.ee.latest_time()
    }

    #[instrument(skip(self))]
    async fn last_update(&self) -> Option<i64> {
        self.ee.history().last().cloned()
    }

    #[instrument(skip(self))]
    async fn time(&self) -> Result<i64, GraphError> {
        self.ee.time().map(|x| x.into())
    }

    #[instrument(skip(self))]
    async fn start(&self) -> Option<i64> {
        self.ee.start()
    }

    #[instrument(skip(self))]
    async fn end(&self) -> Option<i64> {
        self.ee.end()
    }

    #[instrument(skip(self))]
    async fn src(&self) -> Node {
        self.ee.src().into()
    }

    #[instrument(skip(self))]
    async fn dst(&self) -> Node {
        self.ee.dst().into()
    }

    #[instrument(skip(self))]
    async fn properties(&self) -> GqlProperties {
        self.ee.properties().into()
    }

    #[instrument(skip(self))]
    async fn layer_names(&self) -> Vec<String> {
        self.ee.layer_names().map(|x| x.into()).collect()
    }

    #[instrument(skip(self))]
    async fn layer_name(&self) -> Result<String, GraphError> {
        self.ee.layer_name().map(|x| x.into())
    }

    #[instrument(skip(self))]
    async fn explode(&self) -> Vec<Edge> {
        self.ee
            .explode()
            .into_iter()
            .map(|ee| ee.into())
            .collect_vec()
    }

    #[instrument(skip(self))]
    async fn explode_layers(&self) -> Vec<Edge> {
        self.ee
            .explode_layers()
            .into_iter()
            .map(|ee| ee.into())
            .collect_vec()
    }

    #[instrument(skip(self))]
    async fn history(&self) -> Vec<i64> {
        self.ee.history()
    }

    #[instrument(skip(self))]
    async fn deletions(&self) -> Vec<i64> {
        self.ee.deletions()
    }

    #[instrument(skip(self))]
    async fn is_valid(&self) -> bool {
        self.ee.is_valid()
    }

    #[instrument(skip(self))]
    async fn is_deleted(&self) -> bool {
        self.ee.is_deleted()
    }

    #[instrument(skip(self))]
    async fn is_self_loop(&self) -> bool {
        self.ee.is_self_loop()
    }

    #[instrument(skip(self))]
    async fn nbr(&self) -> Node {
        self.ee.nbr().into()
    }
}
