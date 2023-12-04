use crate::model::graph::node::Node;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    db::{
        api::view::{DynamicGraph, EdgeViewOps, IntoDynamic, StaticGraphViewOps},
        graph::edge::EdgeView,
    },
    prelude::{LayerOps, TimeOps},
};

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
    async fn earliest_time(&self) -> Option<i64> {
        self.ee.earliest_time()
    }

    async fn latest_time(&self) -> Option<i64> {
        self.ee.latest_time()
    }

    async fn time(&self) -> Option<i64> {
        self.ee.time()
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

    async fn property(&self, name: &str) -> Option<String> {
        self.ee.properties().get(name).map(|prop| prop.to_string())
    }

    async fn layer(&self, layer_name: &str) -> Option<Edge> {
        self.ee.layer(layer_name).map(|ee| ee.into())
    }

    async fn layer_names(&self) -> Vec<String> {
        self.ee.layer_names().map(|x|x.into()).collect()
    }
    async fn layer_name(&self) -> Option<String> {
        self.ee.layer_name().map(|x|x.into())
    }

    async fn explode(&self) -> Vec<Edge> {
        self.ee
            .explode()
            .into_iter()
            .map(|ee| ee.into())
            .collect_vec()
    }

    async fn explode_layers(&self) -> Vec<Edge> {
        self.ee
            .explode_layers()
            .into_iter()
            .map(|ee| ee.into())
            .collect_vec()
    }

    async fn history(&self) -> Vec<i64> {
        self.ee.history()
    }
}
