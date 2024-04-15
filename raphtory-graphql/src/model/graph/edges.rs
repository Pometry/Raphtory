use crate::model::{filters::edge_filter::EdgeFilter, graph::edge::Edge};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    db::{api::view::DynamicGraph, graph::edges::Edges},
    prelude::{LayerOps, TimeOps},
};

#[derive(ResolvedObject)]
pub(crate) struct GqlEdges {
    pub(crate) ee: Edges<'static, DynamicGraph>,
    pub(crate) filter: Option<EdgeFilter>,
}

impl GqlEdges {
    fn update<E: Into<Edges<'static, DynamicGraph>>>(&self, edges: E) -> Self {
        Self::new(edges, self.filter.clone())
    }
}

impl GqlEdges {
    pub(crate) fn new<E: Into<Edges<'static, DynamicGraph>>>(
        edges: E,
        filter: Option<EdgeFilter>,
    ) -> Self {
        Self {
            ee: edges.into(),
            filter,
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Edge> + '_> {
        let iter = self.ee.iter().map(Edge::from);
        match self.filter.as_ref() {
            Some(filter) => Box::new(iter.filter(|e| filter.matches(e))),
            None => Box::new(iter),
        }
    }
}

#[ResolvedObjectFields]
impl GqlEdges {
    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    async fn layers(&self, names: Vec<String>) -> Self {
        self.update(self.ee.valid_layers(names))
    }

    async fn exclude_layers(&self, names: Vec<String>) -> Self {
        self.update(self.ee.exclude_valid_layers(names))
    }

    async fn layer(&self, name: String) -> Self {
        self.update(self.ee.valid_layers(name))
    }

    async fn exclude_layer(&self, name: String) -> Self {
        self.update(self.ee.exclude_valid_layers(name))
    }

    async fn window(&self, start: i64, end: i64) -> Self {
        self.update(self.ee.window(start, end))
    }

    async fn at(&self, time: i64) -> Self {
        self.update(self.ee.at(time))
    }

    async fn before(&self, time: i64) -> Self {
        self.update(self.ee.before(time))
    }

    async fn after(&self, time: i64) -> Self {
        self.update(self.ee.after(time))
    }

    async fn shrink_window(&self, start: i64, end: i64) -> Self {
        self.update(self.ee.shrink_window(start, end))
    }

    async fn shrink_start(&self, start: i64) -> Self {
        self.update(self.ee.shrink_start(start))
    }

    async fn shrink_end(&self, end: i64) -> Self {
        self.update(self.ee.shrink_end(end))
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    async fn start(&self) -> Option<i64> {
        self.ee.start()
    }

    async fn end(&self) -> Option<i64> {
        self.ee.end()
    }

    /////////////////
    //// List ///////
    /////////////////

    async fn count(&self) -> usize {
        self.iter().count()
    }

    async fn page(&self, limit: usize, offset: usize) -> Vec<Edge> {
        let start = offset * limit;
        self.iter().skip(start).take(limit).collect()
    }

    async fn list(&self) -> Vec<Edge> {
        self.iter().collect()
    }
}
