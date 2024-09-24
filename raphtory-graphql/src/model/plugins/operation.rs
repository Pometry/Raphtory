use async_graphql::{
    dynamic::{Field, FieldFuture, FieldValue, InputValue, Object, ResolverContext, TypeRef},
    FieldResult,
};
use dynamic_graphql::internal::{Register, Registry};
use futures_util::future::BoxFuture;
use crate::model::plugins::mutation_plugin::MutationPlugin;

pub trait Operation<'a, A: Send + Sync + 'static> {
    type OutputType: Register + 'static;

    fn output_type() -> TypeRef;

    fn args<'b>() -> Vec<(&'b str, TypeRef)>;

    fn apply<'b>(
        entry_point: &A,
        ctx: ResolverContext,
    ) -> BoxFuture<'b, FieldResult<Option<FieldValue<'b>>>>;

    fn register_operation(
        name: &str,
        registry: Registry,
        parent: Object,
    ) -> (Registry, Object) {
        let registry = registry.register::<Self::OutputType>();
        let mut field = Field::new(name, Self::output_type(), |ctx| {
            FieldFuture::new(async move {
                let entry_point: &A = ctx.parent_value.downcast_ref().unwrap();
                Self::apply(&entry_point, ctx).await
            })
        });

        for (name, type_ref) in Self::args() {
            field = field.argument(InputValue::new(name, type_ref));
        }
        let parent = parent.field(field);
        (registry, parent)
    }
}

pub(crate) struct NoOpMutation;

impl<'a> Operation<'a, MutationPlugin> for NoOpMutation {
    type OutputType = String;

    fn output_type() -> TypeRef {
        TypeRef::named_nn(TypeRef::STRING)
    }

    fn args<'b>() -> Vec<(&'b str, TypeRef)> {
        vec![]
    }

    fn apply<'b>(
        entry_point: &MutationPlugin,
        ctx: ResolverContext
    ) -> BoxFuture<'b, FieldResult<Option<FieldValue<'b>>>> {
        Box::pin(async move {
            Ok(Some(FieldValue::value("no-op".to_owned())))
        })
    }
}
