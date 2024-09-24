use crate::model::{
    algorithms::document::GqlDocument,
    plugins::{query::Query, query_plugin::QueryPlugin},
};
use async_graphql::{
    dynamic::{FieldValue, ResolverContext, TypeRef},
    FieldResult,
};
use dynamic_graphql::internal::TypeName;
use futures_util::future::BoxFuture;
use raphtory::vectors::{embeddings::openai_embedding, vectorised_cluster::VectorisedCluster};
use std::ops::Deref;

pub(crate) struct GlobalSearch;

impl<'a> Query<'a, QueryPlugin> for GlobalSearch {
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

    fn apply_query<'b>(
        entry_point: &QueryPlugin,
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
        let vectorised_graphs = entry_point.vectorised_graphs.clone();

        Box::pin(async move {
            let embedding = openai_embedding(vec![query.clone()]).await.remove(0);
            println!("running global search for {query}");

            let graphs = vectorised_graphs.read();

            let cluster = VectorisedCluster::new(graphs.deref());
            let documents = cluster.search_graph_documents(&embedding, limit, None); // TODO: add window

            let gql_documents = documents
                .into_iter()
                .map(|doc| FieldValue::owned_any(GqlDocument::from(doc)));
            Ok(Some(FieldValue::list(gql_documents)))
        })
    }
}
