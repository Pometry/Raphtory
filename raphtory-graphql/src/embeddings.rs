use async_graphql::Context;
use raphtory::{core::utils::errors::GraphResult, vectors::Embedding};

use crate::data::Data;

pub(crate) trait EmbedQuery {
    async fn embed_query(&self, text: String) -> GraphResult<Embedding>;
}

impl EmbedQuery for Context<'_> {
    /// this is meant to be called from a vector context, so the embedding conf is assumed to exist
    async fn embed_query(&self, text: String) -> GraphResult<Embedding> {
        let data = self.data_unchecked::<Data>();
        let cache = &data.embedding_conf.as_ref().unwrap().cache;
        cache.get_single(text).await
    }
}
