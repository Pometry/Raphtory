use async_graphql::{dynamic::{FieldValue, ResolverContext, TypeRef}, FieldResult};
use futures_util::future::BoxFuture;
use std::path::Path;
use std::sync::Arc;
use chrono::Utc;
use raphtory_core::core::Prop;
use raphtory_core::core::utils::errors::GraphError;
use raphtory_core::prelude::{CacheOps, GraphViewOps, ImportOps, NodeViewOps, PropertyAdditionOps};
use raphtory_graphql::data::Data;
use itertools::Itertools;
use raphtory_graphql::model::plugins::query::Query;
use raphtory_graphql::model::plugins::query_plugins::QueryPlugins;

pub(crate) struct HelloWorld;

impl<'a> Query<'a, QueryPlugins> for HelloWorld {
    type OutputType = String;

    fn output_type() -> TypeRef {
        TypeRef::named_nn(TypeRef::STRING)
    }

    fn args<'b>() -> Vec<(&'b str, TypeRef)> {
        vec![
            ("name", TypeRef::named_nn(TypeRef::STRING)),
        ]
    }

    fn apply_query<'b>(
        entry_point: &QueryPlugins,
        ctx: ResolverContext,
    ) -> BoxFuture<'b, FieldResult<Option<FieldValue<'b>>>> {
        let name = ctx
            .args
            .try_get("name").unwrap()
            .string().unwrap()
            .to_owned();

        Box::pin(async move {
            Ok(Some(FieldValue::value("Hello, ".to_owned() + name.as_str())))
        })
    }
}
