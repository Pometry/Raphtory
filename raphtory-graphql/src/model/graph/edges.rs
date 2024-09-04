use crate::model::graph::edge::Edge;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    db::{api::view::DynamicGraph, graph::edges::Edges},
    prelude::{LayerOps, TimeOps},
};
use tracing::instrument;

#[derive(ResolvedObject)]
pub(crate) struct GqlEdges {
    pub(crate) ee: Edges<'static, DynamicGraph>,
}

impl GqlEdges {
    fn update<E: Into<Edges<'static, DynamicGraph>>>(&self, edges: E) -> Self {
        Self::new(edges)
    }
}

impl GqlEdges {
    pub(crate) fn new<E: Into<Edges<'static, DynamicGraph>>>(edges: E) -> Self {
        Self { ee: edges.into() }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Edge> + '_> {
        let iter = self.ee.iter().map(Edge::from);
        Box::new(iter)
    }
}

#[ResolvedObjectFields]
impl GqlEdges {
    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    #[instrument(skip(self))]
    async fn layers(&self, names: Vec<String>) -> Self {
        self.update(self.ee.valid_layers(names))
    }

    #[instrument(skip(self))]
    async fn exclude_layers(&self, names: Vec<String>) -> Self {
        self.update(self.ee.exclude_valid_layers(names))
    }

    #[instrument(skip(self))]
    async fn layer(&self, name: String) -> Self {
        self.update(self.ee.valid_layers(name))
    }

    #[instrument(skip(self))]
    async fn exclude_layer(&self, name: String) -> Self {
        self.update(self.ee.exclude_valid_layers(name))
    }

    #[instrument(skip(self))]
    async fn window(&self, start: i64, end: i64) -> Self {
        self.update(self.ee.window(start, end))
    }

    #[instrument(skip(self))]
    async fn at(&self, time: i64) -> Self {
        self.update(self.ee.at(time))
    }

    #[instrument(skip(self))]
    async fn before(&self, time: i64) -> Self {
        self.update(self.ee.before(time))
    }

    #[instrument(skip(self))]
    async fn after(&self, time: i64) -> Self {
        self.update(self.ee.after(time))
    }

    #[instrument(skip(self))]
    async fn shrink_window(&self, start: i64, end: i64) -> Self {
        self.update(self.ee.shrink_window(start, end))
    }

    #[instrument(skip(self))]
    async fn shrink_start(&self, start: i64) -> Self {
        self.update(self.ee.shrink_start(start))
    }

    #[instrument(skip(self))]
    async fn shrink_end(&self, end: i64) -> Self {
        self.update(self.ee.shrink_end(end))
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    #[instrument(skip(self))]
    async fn start(&self) -> Option<i64> {
        self.ee.start()
    }

    #[instrument(skip(self))]
    async fn end(&self) -> Option<i64> {
        self.ee.end()
    }

    /////////////////
    //// List ///////
    /////////////////

    #[instrument(skip(self))]
    async fn count(&self) -> usize {
        self.iter().count()
    }

    #[instrument(skip(self))]
    async fn page(&self, limit: usize, offset: usize) -> Vec<Edge> {
        let start = offset * limit;
        self.iter().skip(start).take(limit).collect()
    }

    #[instrument(skip(self))]
    async fn list(&self) -> Vec<Edge> {
        self.iter().collect()
    }
}
