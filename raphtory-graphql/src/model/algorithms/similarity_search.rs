use crate::{
    data::Data,
    model::{
        algorithms::document::GqlDocument,
        plugins::{operation::Operation, vector_algorithm_plugin::VectorAlgorithmPlugin},
    },
};
use async_graphql::{
    dynamic::{FieldValue, ResolverContext, TypeRef},
    FieldResult,
};
use dynamic_graphql::internal::TypeName;
use futures_util::future::BoxFuture;
use tracing::info;

pub(crate) struct SimilaritySearch;

impl<'a> Operation<'a, VectorAlgorithmPlugin> for SimilaritySearch {
    type OutputType = GqlDocument;

    fn output_type() -> TypeRef {
        TypeRef::named_nn_list_nn(GqlDocument::get_type_name())
    }

    fn args<'b>() -> Vec<(&'b str, TypeRef)> {
        vec![
            ("query", TypeRef::named_nn(TypeRef::STRING)),
            ("limit", TypeRef::named_nn(TypeRef::INT)),
        ]
    }

    fn apply<'b>(
        entry_point: &VectorAlgorithmPlugin,
        ctx: ResolverContext,
    ) -> BoxFuture<'b, FieldResult<Option<FieldValue<'b>>>> {
        let data = ctx.data_unchecked::<Data>().clone();
        let query = ctx
            .args
            .try_get("query")
            .unwrap()
            .string()
            .unwrap()
            .to_owned();
        let limit = ctx.args.try_get("limit").unwrap().u64().unwrap() as usize;
        let graph = entry_point.graph.clone();

        Box::pin(async move {
            info!("running similarity search for {query}");
            let embedding = data.embed_query(query).await;

            let documents = graph
                .documents_by_similarity(&embedding, limit, None)
                .get_documents();

            let gql_documents = documents
                .into_iter()
                .map(|doc| FieldValue::owned_any(GqlDocument::from(doc)));
            Ok(Some(FieldValue::list(gql_documents)))
        })
    }
}
