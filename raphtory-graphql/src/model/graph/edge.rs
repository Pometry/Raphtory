use crate::data::Data;
use crate::model::algorithm::Algorithms;
use crate::model::graph::node::Node;
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
use crate::model::wrappers::dynamic::{DynamicGraph, IntoDynamic};


#[derive(ResolvedObject)]
pub(crate) struct Edge {
    ee: EdgeView<DynamicGraph>,
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
        let prop = self.ee.property(&name, true)?;
        Some(Property::new(name, prop))
    }

    async fn layer(&self) -> String {
        self.ee.layer_name()
    }

    async fn history(&self) -> Vec<i64> {
        self.ee.history()
    }
}
