use crate::data::Data;
use crate::model::algorithm::Algorithms;
use crate::model::graph::edge::Edge;
use crate::model::graph::graph::DynamicGraph;
use crate::model::graph::property::Property;
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::core::Prop;
use raphtory::db::edge::EdgeView;
use raphtory::db::vertex::VertexView;
use raphtory::db::view_api::internal::WrappedGraph;
use raphtory::db::view_api::EdgeListOps;
use raphtory::db::view_api::EdgeViewOps;
use raphtory::db::view_api::{GraphViewOps, TimeOps, VertexViewOps};
use std::sync::Arc;

#[derive(ResolvedObject)]
pub(crate) struct Node {
    pub(crate) vv: VertexView<DynamicGraph>,
}

impl From<VertexView<DynamicGraph>> for Node {
    fn from(vv: VertexView<DynamicGraph>) -> Self {
        Self { vv }
    }
}

#[ResolvedObjectFields]
impl Node {
    async fn id(&self) -> u64 {
        self.vv.id()
    }

    pub async fn name(&self) -> String {
        self.vv.name()
    }

    pub async fn node_type(&self) -> String {
        self.vv
            .property("type".to_string(), true)
            .unwrap_or(Prop::Str("NONE".to_string()))
            .to_string()
    }

    async fn property_names<'a>(&self, _ctx: &Context<'a>) -> Vec<String> {
        self.vv.property_names(true)
    }

    async fn properties(&self) -> Option<Vec<Property>> {
        Some(
            self.vv
                .properties(true)
                .into_iter()
                .map(|(k, v)| Property::new(k, v))
                .collect_vec(),
        )
    }

    async fn property(&self, name: String) -> Option<Property> {
        let prop = self.vv.property(name.clone(), true)?;
        Some(Property::new(name, prop))
    }

    async fn in_neighbours<'a>(&self, _ctx: &Context<'a>) -> Vec<Node> {
        self.vv.in_neighbours().iter().map(|vv| vv.into()).collect()
    }

    async fn out_neighbours(&self) -> Vec<Node> {
        self.vv
            .out_neighbours()
            .iter()
            .map(|vv| vv.into())
            .collect()
    }

    async fn neighbours<'a>(&self, _ctx: &Context<'a>) -> Vec<Node> {
        self.vv.neighbours().iter().map(|vv| vv.into()).collect()
    }

    async fn degree(&self) -> usize {
        self.vv.degree()
    }

    async fn out_degree(&self) -> usize {
        self.vv.out_degree()
    }

    async fn in_degree(&self) -> usize {
        self.vv.in_degree()
    }

    async fn out_edges(&self) -> Vec<Edge> {
        self.vv.out_edges().map(|ee| ee.clone().into()).collect()
    }

    async fn in_edges(&self) -> Vec<Edge> {
        self.vv.in_edges().map(|ee| ee.into()).collect()
    }

    async fn exploded_edges(&self) -> Vec<Edge> {
        self.vv.out_edges().explode().map(|ee| ee.into()).collect()
    }

    async fn start_date(&self) -> Option<i64> {
        self.vv.earliest_time()
    }

    async fn end_date(&self) -> Option<i64> {
        self.vv.latest_time()
    }
}
