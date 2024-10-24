use async_graphql::{
    dynamic::{FieldValue, ResolverContext, TypeRef},
    FieldResult,
};
use futures_util::future::BoxFuture;
use raphtory_graphql::model::plugins::{mutation_plugin::MutationPlugin, operation::Operation};

pub(crate) struct HelloMutation;

impl<'a> Operation<'a, MutationPlugin> for HelloMutation {
    type OutputType = String;

    fn output_type() -> TypeRef {
        TypeRef::named_nn(TypeRef::STRING)
    }

    fn args<'b>() -> Vec<(&'b str, TypeRef)> {
        vec![("name", TypeRef::named_nn(TypeRef::STRING))]
    }

    fn apply<'b>(
        _entry_point: &MutationPlugin,
        ctx: ResolverContext,
    ) -> BoxFuture<'b, FieldResult<Option<FieldValue<'b>>>> {
        let name = ctx
            .args
            .try_get("name")
            .unwrap()
            .string()
            .unwrap()
            .to_owned();

        Box::pin(async move {
            Ok(Some(FieldValue::value(
                "Hello, ".to_owned() + name.as_str(),
            )))
        })
    }
}
