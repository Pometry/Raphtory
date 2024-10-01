use crate::{
    data::Data,
    model::{
        algorithms::document::GqlDocument,
        plugins::{operation::Operation, query_plugin::QueryPlugin},
    },
};
use async_graphql::{
    dynamic::{FieldValue, ResolverContext, TypeRef},
    FieldResult,
};
use dynamic_graphql::internal::TypeName;
use futures_util::future::BoxFuture;
use raphtory::vectors::vectorised_cluster::VectorisedCluster;
use std::ops::Deref;
use tracing::info;

pub(crate) struct GlobalSearch;

impl<'a> Operation<'a, QueryPlugin> for GlobalSearch {
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
        entry_point: &QueryPlugin,
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
        let graphs = entry_point.graphs.clone();

        Box::pin(async move {
            info!("running global search for {query}");
            let embedding = data.embed_query(query).await;

            let cluster = VectorisedCluster::new(graphs.deref());
            let documents = cluster.search_graph_documents(&embedding, limit, None); // TODO: add window

            let gql_documents = documents
                .into_iter()
                .map(|doc| FieldValue::owned_any(GqlDocument::from(doc)));
            Ok(Some(FieldValue::list(gql_documents)))
        })
    }
}
