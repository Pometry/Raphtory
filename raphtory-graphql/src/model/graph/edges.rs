use crate::model::{
    graph::edge::Edge,
    sorting::{EdgeSortBy, SortByTime},
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    db::{
        api::view::{internal::OneHopFilter, DynamicGraph},
        graph::edges::Edges,
    },
    prelude::{EdgeViewOps, LayerOps, NodeViewOps, TimeOps},
};
use raphtory_api::iter::IntoDynBoxed;
use std::{cmp::Ordering, sync::Arc};

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
        let iter = self.ee.iter().map(Edge::from_ref);
        Box::new(iter)
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
    async fn latest(&self) -> Self {
        self.update(self.ee.latest())
    }

    async fn snapshot_at(&self, time: i64) -> Self {
        self.update(self.ee.snapshot_at(time))
    }
    async fn snapshot_latest(&self) -> Self {
        self.update(self.ee.snapshot_latest())
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

    async fn explode(&self) -> Self {
        self.update(self.ee.explode())
    }

    async fn explode_layers(&self) -> Self {
        self.update(self.ee.explode_layers())
    }

    async fn sorted(&self, sort_bys: Vec<EdgeSortBy>) -> Self {
        let sorted: Arc<[_]> = self
            .ee
            .iter()
            .sorted_by(|first_edge, second_edge| {
                sort_bys
                    .clone()
                    .into_iter()
                    .fold(Ordering::Equal, |current_ordering, sort_by| {
                        current_ordering.then_with(|| {
                            let ordering = if sort_by.src == Some(true) {
                                first_edge.src().id().partial_cmp(&second_edge.src().id())
                            } else if sort_by.dst == Some(true) {
                                first_edge.dst().id().partial_cmp(&second_edge.dst().id())
                            } else if let Some(sort_by_time) = sort_by.time {
                                let (first_time, second_time) = match sort_by_time {
                                    SortByTime::Latest => {
                                        (first_edge.latest_time(), second_edge.latest_time())
                                    }
                                    SortByTime::Earliest => {
                                        (first_edge.earliest_time(), second_edge.earliest_time())
                                    }
                                };
                                first_time.partial_cmp(&second_time)
                            } else if let Some(sort_by_property) = sort_by.property {
                                let first_prop_maybe =
                                    first_edge.properties().get(&*sort_by_property);
                                let second_prop_maybe =
                                    second_edge.properties().get(&*sort_by_property);
                                first_prop_maybe.partial_cmp(&second_prop_maybe)
                            } else {
                                None
                            };
                            if let Some(ordering) = ordering {
                                if sort_by.reverse == Some(true) {
                                    ordering.reverse()
                                } else {
                                    ordering
                                }
                            } else {
                                Ordering::Equal
                            }
                        })
                    })
            })
            .map(|edge_view| edge_view.edge)
            .collect();
        self.update(Edges::new(
            self.ee.current_filter().clone(),
            self.ee.base_graph().clone(),
            Arc::new(move || {
                let sorted = sorted.clone();
                (0..sorted.len()).map(move |i| sorted[i]).into_dyn_boxed()
            }),
        ))
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
