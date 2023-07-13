use crate::model::graph::{node::Node, property::Property};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::db::{
    api::view::{
        internal::{DynamicGraph, IntoDynamic},
        EdgeViewOps, GraphViewOps,
    },
    graph::edge::EdgeView,
};

#[derive(ResolvedObject)]
pub(crate) struct Edge {
    pub(crate) ee: EdgeView<DynamicGraph>,
}

impl<G: GraphViewOps + IntoDynamic> From<EdgeView<G>> for Edge {
    fn from(value: EdgeView<G>) -> Self {
        Self {
            ee: EdgeView {
                graph: value.graph.clone().into_dynamic(),
                edge: value.edge,
            },
        }
    }
}

#[ResolvedObjectFields]
impl Edge {
    async fn earliest_time(&self) -> Option<i64> {
        self.ee.earliest_time()
    }

    async fn latest_time(&self) -> Option<i64> {
        self.ee.latest_time()
    }

    async fn src(&self) -> Node {
        self.ee.src().into()
    }

    async fn dst(&self) -> Node {
        self.ee.dst().into()
    }

    async fn property(&self, name: String) -> Option<Property> {
        self.ee
            .properties()
            .get(&name)
            .map(|prop| Property::new(name, prop))
    }

    async fn layer(&self) -> String {
        self.ee.layer_name()
    }

    async fn history(&self) -> Vec<i64> {
        self.ee.history()
    }
}
