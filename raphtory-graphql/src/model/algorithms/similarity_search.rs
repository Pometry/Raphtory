use crate::model::algorithms::{
    algorithm::Algorithm, document::GqlDocument, vector_algorithms::VectorAlgorithms,
};
use async_graphql::{
    dynamic::{FieldValue, ResolverContext, TypeRef},
    FieldResult,
};
use dynamic_graphql::internal::TypeName;
use futures_util::future::BoxFuture;
use raphtory::vectors::embeddings::openai_embedding;

pub(crate) struct SimilaritySearch;

impl<'a> Algorithm<'a, VectorAlgorithms> for SimilaritySearch {
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
    fn apply_algo<'b>(
        entry_point: &VectorAlgorithms,
        ctx: ResolverContext,
    ) -> BoxFuture<'b, FieldResult<Option<FieldValue<'b>>>> {
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
            let embedding = openai_embedding(vec![query.clone()]).await.remove(0);
            println!("running similarity search for {query}");

            let documents = graph
                .empty_selection()
                .add_new_entities(&embedding, limit)
                .get_documents();

            let gql_documents = documents
                .into_iter()
                .map(|doc| FieldValue::owned_any(GqlDocument::from(doc)));
            Ok(Some(FieldValue::list(gql_documents)))
        })
    }
}
