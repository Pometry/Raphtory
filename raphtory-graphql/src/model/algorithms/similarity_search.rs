use crate::{
    data::Data,
    model::{
        algorithms::document::Document,
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
    type OutputType = Document;

    fn output_type() -> TypeRef {
        TypeRef::named_nn_list_nn(Document::get_type_name())
    }

    fn args<'b>() -> Vec<(&'b str, TypeRef)> {
        vec![
            ("query", TypeRef::named_nn(TypeRef::STRING)),
            ("limit", TypeRef::named_nn(TypeRef::INT)),
            ("start", TypeRef::named(TypeRef::INT)),
            ("end", TypeRef::named(TypeRef::INT)),
        ]
    }

    fn apply<'b>(
        entry_point: &VectorAlgorithmPlugin,
        ctx: ResolverContext,
    ) -> BoxFuture<'b, FieldResult<Option<FieldValue<'b>>>> {
        dbg!();
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
        let start = ctx
            .args
            .try_get("start")
            .map(|start| start.u64().ok().map(|value| value as i64))
            .ok()
            .flatten();

        let end = ctx
            .args
            .try_get("end")
            .map(|end| end.u64().ok().map(|value| value as i64))
            .ok()
            .flatten();
        let window = match (start, end) {
            (Some(start), Some(end)) => Some((start, end)),
            _ => None,
        };

        Box::pin(async move {
            info!("running similarity search for {query}");
            let embedding = data.embed_query(query).await?;

            let documents = graph
                .entities_by_similarity(&embedding, limit, window)
                .get_documents();

            let gql_documents = documents
                .into_iter()
                .map(|doc| FieldValue::owned_any(Document::from(doc)));
            Ok(Some(FieldValue::list(gql_documents)))
        })
    }
}
