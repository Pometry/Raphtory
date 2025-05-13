use crate::model::{
    algorithms::document::Document,
    plugins::{operation::Operation, query_plugin::QueryPlugin},
};
use async_graphql::{
    dynamic::{FieldValue, ResolverContext, TypeRef},
    FieldResult,
};
use dynamic_graphql::internal::TypeName;
use futures_util::future::BoxFuture;

pub(crate) struct GlobalSearch;

impl<'a> Operation<'a, QueryPlugin> for GlobalSearch {
    type OutputType = Document;

    fn output_type() -> TypeRef {
        TypeRef::named_nn_list_nn(String::get_type_name())
    }

    fn args<'b>() -> Vec<(&'b str, TypeRef)> {
        vec![
            ("query", TypeRef::named_nn(TypeRef::STRING)),
            ("limit", TypeRef::named_nn(TypeRef::INT)),
        ]
    }

    fn apply<'b>(
        entry_point: &QueryPlugin,
        ctx: ResolverContext,
    ) -> BoxFuture<'b, FieldResult<Option<FieldValue<'b>>>> {
        Box::pin(async move {
            let docs = vec![FieldValue::owned_any("hello".to_owned())];
            Ok(Some(FieldValue::list(docs)))
        })
    }
}
