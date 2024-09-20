use crate::model::plugins::query_entry_point::QueryEntryPoint;
use async_graphql::{
    dynamic::{Field, FieldFuture, FieldValue, InputValue, Object, ResolverContext, TypeRef},
    FieldResult,
};
use dynamic_graphql::internal::{Register, Registry};
use futures_util::future::BoxFuture;

pub trait Query<'a, A: QueryEntryPoint<'a> + 'static> {
    type OutputType: Register + 'static;

    fn output_type() -> TypeRef;

    fn args<'b>() -> Vec<(&'b str, TypeRef)>;

    fn apply_query<'b>(
        entry_point: &A,
        ctx: ResolverContext,
    ) -> BoxFuture<'b, FieldResult<Option<FieldValue<'b>>>>;

    fn register_query(name: &str, registry: Registry, parent: Object) -> (Registry, Object) {
        let registry = registry.register::<Self::OutputType>();
        let mut field = Field::new(name, Self::output_type(), |ctx| {
            FieldFuture::new(async move {
                let algos: &A = ctx.parent_value.downcast_ref().unwrap();
                Self::apply_query(&algos, ctx).await
            })
        });
        for (name, type_ref) in Self::args() {
            field = field.argument(InputValue::new(name, type_ref));
        }
        let parent = parent.field(field);
        (registry, parent)
    }
}
