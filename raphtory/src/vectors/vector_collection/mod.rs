use std::{future::Future, path::Path, sync::Arc};

pub(crate) mod lancedb;

use crate::{errors::GraphResult, vectors::Embedding};

pub(super) type CollectionPath = Arc<dyn AsRef<Path> + Send + Sync>;

pub(super) trait VectorCollectionFactory {
    type DbType: VectorCollection;
    async fn new_collection(
        &self,
        path: CollectionPath,
        name: &str,
        dim: usize,
    ) -> GraphResult<Self::DbType>;
    async fn from_path(
        &self,
        path: CollectionPath,
        name: &str,
        dim: usize,
    ) -> GraphResult<Self::DbType>;
}

pub(super) trait VectorCollection: Sized + Clone + Send + Sync {
    async fn insert_vectors(
        &self,
        ids: Vec<u64>,
        vectors: impl Iterator<Item = Embedding>,
    ) -> crate::errors::GraphResult<()>;
    async fn get_id(&self, id: u64) -> GraphResult<Option<Embedding>>;
    fn top_k_with_distances(
        &self,
        query: &Embedding,
        k: usize,
        candidates: Option<impl IntoIterator<Item = u64> + Send>,
    ) -> impl Future<Output = GraphResult<impl Iterator<Item = (u64, f32)> + Send>> + Send;
    async fn create_or_update_index(&self) -> GraphResult<()>;
}
