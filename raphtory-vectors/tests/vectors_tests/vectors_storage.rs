#[cfg(test)]
mod vector_storage_tests {
    use raphtory_vectors::storage::LazyDiskVectorCache;

    /// Every clone has to resolve to the same underlying cache, otherwise the second one to
    /// resolve fails to open the heed env that the first one already holds
    #[tokio::test]
    async fn test_clones_resolve_to_the_same_cache() {
        let dir = tempfile::tempdir().unwrap();
        let cache = LazyDiskVectorCache::new(dir.path().join("vector-cache"));
        let clone = cache.clone();
        clone.resolve().await.unwrap();
        cache.resolve().await.unwrap();
    }

    // #[test]
    // fn test_vector_meta() {
    //     let meta = VectorMeta {
    //         template: DocumentTemplate::default(),
    //         sample: vec![1.0].into(),
    //         embeddings: SampledModel::OpenAI(StoredOpenAIEmbeddings {
    //             model: "text-embedding-3-small".to_owned(),
    //             config: Default::default(),
    //         }),
    //     };
    //     let serialised = serde_json::to_string_pretty(&meta).unwrap();
    //     println!("{serialised}");

    //     if let SampledModel::OpenAI(embeddings) = meta.embeddings {
    //         let embeddings: OpenAIEmbeddings = embeddings.try_into().unwrap();
    //     } else {
    //         panic!("should not be here");
    //     }

    //     // panic!("here");
    // }
}
