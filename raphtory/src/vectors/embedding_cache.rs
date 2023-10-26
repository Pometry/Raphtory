use crate::vectors::{entity_id::EntityId, Embedding};
use parking_lot::RwLock;
use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
};

type CacheKey = (EntityId, u64);
type CacheStore = HashMap<CacheKey, Vec<Embedding>>;
pub(crate) struct EmbeddingCache {
    cache: RwLock<CacheStore>,
    path: PathBuf,
}

impl EmbeddingCache {
    pub(crate) fn load_from_disk(path: &Path) -> Option<Self> {
        let path = PathBuf::from(path);
        let file = File::open(&path).ok()?;
        let mut reader = BufReader::new(file);
        let inner_cache: CacheStore =
            bincode::deserialize_from(&mut reader).unwrap_or(HashMap::new());
        let cache = RwLock::new(inner_cache);
        Some(Self { cache, path })
    }

    pub(crate) fn get_embeddings(&self, id: EntityId, hash: u64) -> Option<Vec<Embedding>> {
        let cache_key = (id, hash);
        self.cache.read().get(&cache_key).cloned()
    }

    pub(crate) fn upsert_embeddings(&self, id: EntityId, hash: u64, embeddings: Vec<Embedding>) {
        let cache_key = (id, hash);
        self.cache.write().insert(cache_key, embeddings);
    }

    // TODO: remove entries that weren't read in the last usage
    pub(crate) fn dump_to_disk(&self) {
        let file = File::create(&self.path).expect("Couldn't create file to store embedding cache");
        let mut writer = BufWriter::new(file);
        bincode::serialize_into::<_, CacheStore>(&mut writer, &self.cache.read())
            .expect("Couldn't serialize embedding cache");
    }
}
