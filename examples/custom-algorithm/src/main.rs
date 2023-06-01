use async_graphql::dynamic::{FieldValue, ResolverContext, TypeRef};
use async_graphql::FieldResult;
use dynamic_graphql::internal::TypeName;
use dynamic_graphql::SimpleObject;
use raphtory::db::view_api::GraphViewOps;
use raphtory_graphql::{Algorithm, RaphtoryServer};

#[derive(SimpleObject)]
struct DummyAlgorithm {
    number_of_nodes: usize,
    message: String,
}

impl Algorithm for DummyAlgorithm {
    fn args<'a>() -> Vec<(&'a str, TypeRef)> {
        vec![
            ("mandatoryArg", TypeRef::named_nn(TypeRef::INT)),
            ("optionalArg", TypeRef::named(TypeRef::INT)),
        ]
    }

    fn output_type() -> TypeRef {
        TypeRef::named_nn(Self::get_type_name())
    }

    fn apply_algo<'a, G: GraphViewOps>(
        graph: &G,
        ctx: ResolverContext,
    ) -> FieldResult<Option<FieldValue<'a>>> {
        let mandatory_arg = ctx.args.try_get("mandatoryArg")?.u64()?;
        let optional_arg = ctx.args.get("optionalArg").map(|v| v.u64()).transpose()?;
        let num_vertices = graph.num_vertices();
        let output = Self {
            number_of_nodes: num_vertices,
            message: format!("mandatory arg: '{mandatory_arg}', optional arg: '{optional_arg:?}'"),
        };
        Ok(Some(FieldValue::owned_any(output)))
    }
}

#[tokio::main]
async fn main() {
    RaphtoryServer::new("/tmp/graphs")
        .register_algorithm::<DummyAlgorithm>("dummyalgo")
        .run()
        .await
        .unwrap()
}
