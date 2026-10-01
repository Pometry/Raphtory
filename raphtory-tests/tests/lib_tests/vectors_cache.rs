#[cfg(test)]
mod cache_tests {
    use once_cell::sync::Lazy;
    use tempfile::tempdir;

    use raphtory::vectors::{
        cache::{CachedEmbeddingModel, CONTENT_SAMPLE},
        embeddings::ModelConfig,
        storage::OpenAIEmbeddings,
        Embedding,
    };

    use raphtory::vectors::cache::VectorCache;

    fn placeholder_config() -> OpenAIEmbeddings {
        OpenAIEmbeddings::empty("whatever")
    }

    fn other_config() -> OpenAIEmbeddings {
        OpenAIEmbeddings::empty("other")
    }

    static PLACEHOLDER_MODEL: Lazy<ModelConfig> =
        Lazy::new(|| ModelConfig::OpenAI(placeholder_config()));

    #[test]
    fn stable_hash() {
        let hash_value = raphtory::vectors::cache::hash(&PLACEHOLDER_MODEL, CONTENT_SAMPLE);
        assert_eq!(hash_value, 17143601129976616271);
    }

    #[test]
    fn test_vector_sample_remains_unchanged() {
        assert_eq!(CONTENT_SAMPLE, "raphtory");
    }

    #[tokio::test]
    async fn test_empty_request() {
        let model = CachedEmbeddingModel {
            cache: VectorCache::in_memory(),
            // this model will definetely error out if called, as the api base is invalid
            model: ModelConfig::OpenAI(OpenAIEmbeddings::new("whatever", "invalid-api-base")),
        };
        let result: Vec<_> = model.get_embeddings(vec![]).await.unwrap().collect();
        assert_eq!(result, vec![]);
    }

    async fn test_abstract_cache(cache: VectorCache) {
        let vector_a: Embedding = [1.0].into();
        let vector_a_alt: Embedding = [1.0, 0.0].into();
        let vector_b: Embedding = [0.5].into();

        // TOOD: try to do this using VectorCache::in_memory().openai()
        let model_a = ModelConfig::OpenAI(placeholder_config());
        let model_b = ModelConfig::OpenAI(other_config());

        assert_eq!(cache.get(&model_a, "a").await, None);
        assert_eq!(cache.get(&model_b, "a").await, None);
        assert_eq!(cache.get(&model_a, "b").await, None);

        cache
            .insert(model_a.clone(), "a".to_owned(), vector_a.clone())
            .await;
        assert_eq!(cache.get(&model_a, "a").await, Some(vector_a.clone()));
        assert_eq!(cache.get(&model_b, "a").await, None);
        assert_eq!(cache.get(&model_a, "b").await, None);

        cache
            .insert(model_b.clone(), "a".to_owned(), vector_a_alt.clone())
            .await;
        assert_eq!(cache.get(&model_a, "a").await, Some(vector_a.clone()));
        assert_eq!(cache.get(&model_b, "a").await, Some(vector_a_alt.clone()));
        assert_eq!(cache.get(&model_a, "b").await, None);

        cache
            .insert(model_a.clone(), "b".to_owned(), vector_b.clone())
            .await;
        assert_eq!(cache.get(&model_a, "a").await, Some(vector_a));
        assert_eq!(cache.get(&model_b, "a").await, Some(vector_a_alt));
        assert_eq!(cache.get(&model_a, "b").await, Some(vector_b));
    }

    #[tokio::test]
    async fn test_in_memory_cache() {
        let cache = VectorCache::in_memory();
        test_abstract_cache(cache).await;
    }

    #[tokio::test]
    async fn test_on_disk_cache() {
        let dir = tempdir().unwrap();
        test_abstract_cache(VectorCache::on_disk(dir.path()).await.unwrap()).await;
    }

    #[tokio::test]
    async fn test_on_disk_cache_loading() {
        let model = ModelConfig::OpenAI(placeholder_config());
        let vector: Embedding = [1.0].into();
        let dir = tempdir().unwrap();

        {
            let cache = VectorCache::on_disk(dir.path()).await.unwrap();
            cache
                .insert(model.clone(), "a".to_owned(), vector.clone())
                .await;
        } // here the heed env gets dropped, maybe we should find some key value store that doesn't need us to do this

        let loaded_from_disk = VectorCache::on_disk(dir.path()).await.unwrap();
        assert_eq!(loaded_from_disk.get(&model, "a").await, Some(vector))
    }
}
