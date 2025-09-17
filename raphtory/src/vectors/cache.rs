use super::embeddings::EmbeddingFunction;
use crate::{errors::GraphResult, vectors::Embedding};
use futures_util::StreamExt;
use heed::{types::SerdeBincode, Database, Env, EnvOpenOptions};
use itertools::Itertools;
use moka::future::Cache;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    hash::{DefaultHasher, Hash, Hasher},
    path::Path,
    sync::Arc,
};

const CONTENT_SAMPLE: &str = "raphtory"; // DON'T CHANGE THIS STRING BY ANY MEANS

const MAX_DISK_ITEMS: usize = 1_000_000;
const MAX_VECTOR_DIM: usize = 8960;
const MAX_TEXT_LENGTH: usize = 200_000;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct CacheEntry {
    model: usize,
    text: String,
    vector: Embedding,
}
type VectorDb = Database<SerdeBincode<u64>, SerdeBincode<CacheEntry>>;

enum VectorStore {
    Mem(RwLock<HashMap<u64, CacheEntry>>),
    Disk { env: Env, db: VectorDb },
}

impl VectorStore {
    fn in_memory() -> Self {
        Self::Mem(Default::default())
    }
    fn on_disk(path: &Path) -> GraphResult<Self> {
        let _ = std::fs::create_dir_all(path);
        let page_size = 16384;
        let max_size =
            (MAX_DISK_ITEMS * (MAX_VECTOR_DIM * 4 + MAX_TEXT_LENGTH)) / page_size * page_size;

        let env = unsafe { EnvOpenOptions::new().map_size(max_size).open(path) }?;
        let mut wtxn = env.write_txn().unwrap();
        let db: VectorDb = env.create_database(&mut wtxn, None)?;
        wtxn.commit()?;
        Ok(Self::Disk { env, db })
    }

    fn get_disk_keys(&self) -> GraphResult<Vec<u64>> {
        match self {
            VectorStore::Mem(_) => Ok(vec![]),
            VectorStore::Disk { env, db } => {
                let rtxn = env.read_txn()?;
                let iter = db.iter(&rtxn)?;
                let result: Result<Vec<u64>, heed::Error> =
                    iter.map(|result| result.map(|(id, _)| id)).collect();
                Ok(result?) // TODO: simplify this?, use into inside of the map?
            }
        }
    }

    fn get(&self, key: &u64) -> Option<CacheEntry> {
        match self {
            VectorStore::Mem(store) => store.read().get(key).cloned(),
            VectorStore::Disk { env, db } => {
                let rtxn = env.read_txn().ok()?;
                db.get(&rtxn, key).ok()?
            }
        }
    }

    fn insert(&self, key: u64, value: CacheEntry) {
        match self {
            VectorStore::Mem(store) => {
                store.write().insert(key, value);
            }
            VectorStore::Disk { env, db } => {
                if let Ok(mut wtxn) = env.write_txn() {
                    let _ = db.put(&mut wtxn, &key, &value);
                    let _ = wtxn.commit();
                }
            }
        }
    }

    fn remove(&self, key: &u64) {
        match self {
            VectorStore::Mem(store) => {
                store.write().remove(key);
            }
            VectorStore::Disk { env, db } => {
                // this is a bit dangerous, because if delete ops fail and insert ops succeed,
                // the cache might explode in size, but that is very unlikely to happen
                if let Ok(mut wtxn) = env.write_txn() {
                    let _ = db.delete(&mut wtxn, key);
                    let _ = wtxn.commit();
                }
            }
        }
    }
}

#[derive(PartialEq, Clone)]
struct EmbeddingFootprint {
    hash: u64,
    vector_sample: Embedding,
}

// #[derive(Clone)]
pub struct VectorCache {
    store: Arc<VectorStore>,
    cache: Cache<u64, ()>,
    functions: RwLock<Vec<EmbeddingFootprint>>,
}

impl VectorCache {
    // FIXME: this doesnt need to be async anymore
    pub async fn in_memory() -> GraphResult<Self> {
        Ok(Self {
            store: VectorStore::in_memory().into(),
            cache: Cache::new(10),
            functions: Default::default(),
        })
    }

    pub async fn on_disk(path: &Path) -> GraphResult<Self> {
        let store: Arc<_> = VectorStore::on_disk(path)?.into();
        let cloned = store.clone();

        let cache: Cache<u64, ()> = Cache::builder()
            .max_capacity(MAX_DISK_ITEMS as u64)
            .eviction_listener(move |key: Arc<u64>, _value: (), _cause| cloned.remove(key.as_ref()))
            .build();

        for key in store.get_disk_keys()? {
            cache.insert(key, ()).await;
        }

        Ok(Self {
            store,
            cache,
            functions: Default::default(),
        })
    }

    async fn get(&self, model: usize, text: &str) -> Option<Embedding> {
        let hash = hash(text);
        self.cache.get(&hash).await?;
        let entry = self.store.get(&hash)?;
        if entry.model == model && entry.text == text {
            Some(entry.vector)
        } else {
            None
        }
    }

    async fn insert(&self, model: usize, text: String, vector: Embedding) {
        let hash = hash(&text);
        let entry = CacheEntry {
            model,
            text,
            vector,
        };
        self.store.insert(hash, entry);
        self.cache.insert(hash, ()).await;
    }
}

trait EmbeddingCacher {
    async fn cache(
        &mut self,
        embedding: impl EmbeddingFunction + 'static,
    ) -> GraphResult<CachedEmbeddings>;
}

impl EmbeddingCacher for Arc<VectorCache> {
    async fn cache(
        &mut self,
        function: impl EmbeddingFunction + 'static,
    ) -> GraphResult<CachedEmbeddings> {
        let mut vectors = function.call(vec![CONTENT_SAMPLE.to_owned()]).await?;
        let vector_sample = vectors.remove(0);

        let footprint = EmbeddingFootprint {
            hash: function.get_hash(),
            vector_sample,
        };

        let functions = self.functions.write();
        let maybe_id = functions.iter().find_position(|f| &&footprint == f);
        let id = if let Some((id, _)) = maybe_id {
            id
        } else {
            functions.push(footprint.clone());
            functions
                .iter()
                .find_position(|f| &&footprint == f)
                .unwrap()
                .0
        };

        Ok(CachedEmbeddings {
            function: Arc::new(function),
            cache: self.clone(),
            id,
        })
    }
}

struct CachedEmbeddings {
    function: Arc<dyn EmbeddingFunction>,
    cache: Arc<VectorCache>, // TODO: review if ok using here a parking_lot::RwLock
    id: usize,
}

impl CachedEmbeddings {
    pub(super) async fn get_embeddings(
        &self,
        texts: Vec<String>,
    ) -> GraphResult<impl Iterator<Item = Embedding> + '_> {
        // TODO: review, turned this into a vec only to make compute_embeddings work
        let results: Vec<_> = futures_util::stream::iter(texts)
            .then(|text| async move {
                match self.cache.get(self.id, &text).await {
                    Some(cached) => (text, Some(cached)),
                    None => (text, None),
                }
            })
            .collect()
            .await;
        let misses: Vec<_> = results
            .iter()
            .filter_map(|(text, vector)| match vector {
                Some(_) => None,
                None => Some(text.clone()),
            })
            .collect();
        let mut fresh_vectors: VecDeque<_> = if !misses.is_empty() {
            self.function.call(misses.clone()).await?.into()
        } else {
            vec![].into()
        };
        futures_util::stream::iter(misses.into_iter().zip(fresh_vectors.iter().cloned()))
            .for_each(|(text, vector)| self.cache.insert(self.id, text, vector))
            .await;
        let embeddings = results.into_iter().map(move |(_, vector)| match vector {
            Some(vector) => vector,
            None => fresh_vectors.pop_front().unwrap(),
        });
        Ok(embeddings)
    }

    pub async fn get_single(&self, text: String) -> GraphResult<Embedding> {
        let mut embeddings = self.get_embeddings(vec![text]).await?;
        Ok(embeddings.next().unwrap())
    }
}

// const CONTENT_SAMPLE: &str = "raphtory"; // DON'T CHANGE THIS STRING BY ANY MEANS

// async fn get_vector_sample(function: &impl EmbeddingFunction) -> GraphResult<Embedding> {
//     let mut vectors = function.call(vec![CONTENT_SAMPLE.to_owned()]).await?;
//     Ok(vectors.remove(0))
// }

fn hash(text: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    text.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod cache_tests {
    use tempfile::tempdir;

    use crate::vectors::{cache::CONTENT_SAMPLE, embeddings::EmbeddingResult, Embedding};

    use super::VectorCache;

    async fn placeholder_embedding(texts: Vec<String>) -> EmbeddingResult<Vec<Embedding>> {
        dbg!(texts);
        todo!()
    }

    async fn test_abstract_cache(cache: VectorCache) {
        let vector_a: Embedding = [1.0].into();
        let vector_b: Embedding = [0.5].into();

        assert_eq!(cache.get("a").await, None);
        assert_eq!(cache.get("b").await, None);

        cache.insert("a".to_owned(), vector_a.clone()).await;
        assert_eq!(cache.get("a").await, Some(vector_a.clone()));
        assert_eq!(cache.get("b").await, None);

        cache.insert("b".to_owned(), vector_b.clone()).await;
        assert_eq!(cache.get("a").await, Some(vector_a));
        assert_eq!(cache.get("b").await, Some(vector_b));
    }

    #[tokio::test]
    async fn test_empty_request() {
        let cache = VectorCache::in_memory(placeholder_embedding).await.unwrap();
        let result: Vec<_> = cache.get_embeddings(vec![]).await.unwrap().collect();
        assert_eq!(result, vec![]);
    }

    #[tokio::test]
    async fn test_cache() {
        let cache = VectorCache::in_memory(placeholder_embedding).await.unwrap();
        test_abstract_cache(cache).await;
        let dir = tempdir().unwrap();
        test_abstract_cache(
            VectorCache::on_disk(dir.path(), placeholder_embedding)
                .await
                .unwrap(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_on_disk_cache() {
        let vector: Embedding = [1.0].into();
        let dir = tempdir().unwrap();

        {
            let cache = VectorCache::on_disk(dir.path(), placeholder_embedding)
                .await
                .unwrap();
            cache.insert("a".to_owned(), vector.clone()).await;
        } // here the heed env gets closed

        let loaded_from_disk = VectorCache::on_disk(dir.path(), placeholder_embedding)
            .await
            .unwrap();
        assert_eq!(loaded_from_disk.get("a").await, Some(vector))
    }

    #[test]
    fn test_vector_sample_remains_unchanged() {
        assert_eq(CONTENT_SAMPLE, "raphtory");
    }
}
