use crate::model::graph::node::Node;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    db::{api::view::DynamicGraph, graph::nodes::Nodes},
    prelude::*,
};
use tracing::instrument;

#[derive(ResolvedObject)]
pub(crate) struct GqlNodes {
    pub(crate) nn: Nodes<'static, DynamicGraph>,
}

impl GqlNodes {
    fn update<N: Into<Nodes<'static, DynamicGraph>>>(&self, nodes: N) -> Self {
        GqlNodes::new(nodes)
    }
}

impl GqlNodes {
    pub(crate) fn new<N: Into<Nodes<'static, DynamicGraph>>>(nodes: N) -> Self {
        Self { nn: nodes.into() }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Node> + '_> {
        let iter = self.nn.iter_owned().map(Node::from);
        Box::new(iter)
    }
}

#[ResolvedObjectFields]
impl GqlNodes {
    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    #[instrument(skip(self))]
    async fn layers(&self, names: Vec<String>) -> Self {
        self.update(self.nn.valid_layers(names))
    }

    #[instrument(skip(self))]
    async fn exclude_layers(&self, names: Vec<String>) -> Self {
        self.update(self.nn.exclude_valid_layers(names))
    }

    #[instrument(skip(self))]
    async fn layer(&self, name: String) -> Self {
        self.update(self.nn.valid_layers(name))
    }

    #[instrument(skip(self))]
    async fn exclude_layer(&self, name: String) -> Self {
        self.update(self.nn.exclude_valid_layers(name))
    }

    #[instrument(skip(self))]
    async fn window(&self, start: i64, end: i64) -> Self {
        self.update(self.nn.window(start, end))
    }

    #[instrument(skip(self))]
    async fn at(&self, time: i64) -> Self {
        self.update(self.nn.at(time))
    }

    #[instrument(skip(self))]
    async fn before(&self, time: i64) -> Self {
        self.update(self.nn.before(time))
    }

    #[instrument(skip(self))]
    async fn after(&self, time: i64) -> Self {
        self.update(self.nn.after(time))
    }

    #[instrument(skip(self))]
    async fn shrink_window(&self, start: i64, end: i64) -> Self {
        self.update(self.nn.shrink_window(start, end))
    }

    #[instrument(skip(self))]
    async fn shrink_start(&self, start: i64) -> Self {
        self.update(self.nn.shrink_start(start))
    }

    #[instrument(skip(self))]
    async fn shrink_end(&self, end: i64) -> Self {
        self.update(self.nn.shrink_end(end))
    }

    #[instrument(skip(self))]
    async fn type_filter(&self, node_types: Vec<String>) -> Self {
        self.update(self.nn.type_filter(&node_types))
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    #[instrument(skip(self))]
    async fn start(&self) -> Option<i64> {
        self.nn.start()
    }

    #[instrument(skip(self))]
    async fn end(&self) -> Option<i64> {
        self.nn.end()
    }

    /////////////////
    //// List ///////
    /////////////////

    #[instrument(skip(self))]
    async fn count(&self) -> usize {
        self.iter().count()
    }

    #[instrument(skip(self))]
    async fn page(&self, limit: usize, offset: usize) -> Vec<Node> {
        let start = offset * limit;
        self.iter().skip(start).take(limit).collect()
    }

    #[instrument(skip(self))]
    async fn list(&self) -> Vec<Node> {
        self.iter().collect()
    }

    #[instrument(skip(self))]
    async fn ids(&self) -> Vec<String> {
        self.nn.name().collect()
    }
}
