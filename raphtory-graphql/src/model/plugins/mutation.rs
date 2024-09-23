use crate::model::plugins::mutation_entry_point::MutationEntryPoint;
use async_graphql::{
    dynamic::{Field, FieldFuture, FieldValue, InputValue, Object, ResolverContext, TypeRef},
    FieldResult,
};
use dynamic_graphql::internal::{Register, Registry};
use futures_util::future::BoxFuture;
use crate::model::plugins::mutation_plugins::MutationPlugins;

pub trait Mutation<'a, A: MutationEntryPoint<'a> + 'static> {
    type OutputType: Register + 'static;

    fn output_type() -> TypeRef;

    fn args<'b>() -> Vec<(&'b str, TypeRef)>;

    fn apply_mutation<'b>(
        entry_point: &A,
        ctx: ResolverContext,
    ) -> BoxFuture<'b, FieldResult<Option<FieldValue<'b>>>>;

    fn register_mutation(name: &str, registry: Registry, parent: Object) -> (Registry, Object) {
        let registry = registry.register::<Self::OutputType>();
        let mut field = Field::new(name, Self::output_type(), |ctx| {
            FieldFuture::new(async move {
                let algos: &A = ctx.parent_value.downcast_ref().unwrap();
                Self::apply_mutation(&algos, ctx).await
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

impl<'a> Mutation<'a, MutationPlugins> for NoOpMutation {
    type OutputType = String;

    fn output_type() -> TypeRef {
        TypeRef::named_nn(TypeRef::STRING)
    }

    fn args<'b>() -> Vec<(&'b str, TypeRef)> {
        vec![]
    }

    fn apply_mutation<'b>(
        entry_point: &MutationPlugins,
        ctx: ResolverContext
    ) -> BoxFuture<'b, FieldResult<Option<FieldValue<'b>>>> {
        Box::pin(async move {
            Ok(Some(FieldValue::value("no-op".to_owned())))
        })
    }
}
