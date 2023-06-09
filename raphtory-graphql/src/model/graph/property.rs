use std::sync::Arc;

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
use crate::model::algorithm::Algorithms;

#[derive(ResolvedObject)]
pub(crate) struct Property {
    key: String,
    value: Prop,
}

impl Property {
    pub(crate) fn new(key: String, value: Prop) -> Self {
        Self { key, value }
    }
}

#[ResolvedObjectFields]
impl Property {
    async fn key(&self, _ctx: &Context<'_>) -> String {
        self.key.to_string()
    }

    async fn value(&self, _ctx: &Context<'_>) -> String {
        self.value.to_string()
    }
}
