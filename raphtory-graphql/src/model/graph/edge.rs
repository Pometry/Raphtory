use crate::data::Data;
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::core::Prop;
use raphtory::db::edge::EdgeView;
use raphtory::db::vertex::VertexView;
use raphtory::db::view_api::internal::{GraphViewInternalOps, WrappedGraph};
use raphtory::db::view_api::EdgeListOps;
use raphtory::db::view_api::EdgeViewOps;
use raphtory::db::view_api::{GraphViewOps, TimeOps, VertexViewOps};
use std::sync::Arc;
use crate::model::algorithm::Algorithms;
use crate::model::graph::graph::DynamicGraph;
use crate::model::graph::node::Node;
use crate::model::graph::property::Property;


#[derive(ResolvedObject)]
pub(crate) struct Edge {
    ee: EdgeView<DynamicGraph>,
}

impl From<EdgeView<DynamicGraph>> for Edge {
    fn from(ee: EdgeView<DynamicGraph>) -> Self {
        Self { ee }
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
        let prop = self.ee.property(name.clone(), true)?;
        Some(Property::new(name, prop))
    }

    async fn layer(&self) -> String {
        self.ee.layer_name()
    }

    async fn history(&self) -> Vec<i64> {
        self.ee.history()
    }
}
