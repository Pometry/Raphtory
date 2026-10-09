#[cfg(test)]
mod lancedb_tests {
    use raphtory_vectors::{
        vector_collection::{
            lancedb::{LanceDb, LanceDbCollection},
            VectorCollection, VectorCollectionFactory,
        },
        Embedding,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn test_search_with_candidates() {
        let factory = LanceDb;
        let tempdir = tempfile::tempdir().unwrap();
        let path = Arc::new(tempdir);
        let collection = factory.new_collection(path, "vectors", 2).await.unwrap();
        let ids = vec![0, 1];
        let vectors: Vec<Embedding> = vec![vec![1.0, 0.0].into(), vec![0.0, 1.0].into()];
        collection
            .insert_vectors(ids, vectors.into_iter())
            .await
            .unwrap();
        let result = collection
            .top_k_with_distances(&[1.0, 0.0].into(), 1, None::<Vec<_>>)
            .await
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (0, 0.0));

        let result = collection
            .top_k_with_distances(&[1.0, 0.0].into(), 1, Some(vec![1]))
            .await
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (1, 2.0));
    }

    /// Re-embedding an entity has to replace its row. An append would leave the old vector
    /// behind under the same id, so the entity would come back twice from a search.
    #[tokio::test]
    async fn test_insert_replaces_existing_ids() {
        let factory = LanceDb;
        let tempdir = tempfile::tempdir().unwrap();
        let collection = factory
            .new_collection(Arc::new(tempdir), "vectors", 2)
            .await
            .unwrap();

        let vectors: Vec<Embedding> = vec![vec![1.0, 0.0].into(), vec![0.0, 1.0].into()];
        collection
            .insert_vectors(vec![0, 1], vectors.into_iter())
            .await
            .unwrap();
        collection
            .insert_vectors(vec![0], vec![Embedding::from(vec![0.5, 0.5])].into_iter())
            .await
            .unwrap();

        let all = collection
            .top_k_with_distances(&[1.0, 0.0].into(), 10, None::<Vec<_>>)
            .await
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(all.len(), 2, "expected one row per id, got {all:?}");
        assert_eq!(
            collection.get_id(0).await.unwrap().unwrap(),
            Embedding::from(vec![0.5, 0.5])
        );

        // an id repeated inside a single call keeps the last vector, still one row
        collection
            .insert_vectors(
                vec![2, 2],
                vec![
                    Embedding::from(vec![0.1, 0.9]),
                    Embedding::from(vec![0.9, 0.1]),
                ]
                .into_iter(),
            )
            .await
            .unwrap();
        let all = collection
            .top_k_with_distances(&[1.0, 0.0].into(), 10, None::<Vec<_>>)
            .await
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(all.len(), 3, "expected one row per id, got {all:?}");
        assert_eq!(
            collection.get_id(2).await.unwrap().unwrap(),
            Embedding::from(vec![0.9, 0.1])
        );
    }

    const EMBEDDING_DIM: usize = 32;

    #[tokio::test]
    async fn test_index_lifecycle() {
        let factory = LanceDb;
        let tempdir = tempfile::tempdir().unwrap();
        let path = Arc::new(tempdir);
        let collection = factory
            .new_collection(path, "vectors", EMBEDDING_DIM)
            .await
            .unwrap();

        assert_empty_search(&collection).await;

        collection.create_or_update_index().await.unwrap();

        assert_empty_search(&collection).await;

        collection
            .insert_vectors(vec![0, 1], vec![embedding(0), embedding(1)].into_iter())
            .await
            .unwrap();

        assert_vector_is_searchable(&collection, 0, embedding(0)).await;
        assert_vector_is_searchable(&collection, 1, embedding(1)).await;

        collection.create_or_update_index().await.unwrap();

        assert_vector_is_searchable(&collection, 0, embedding(0)).await;
        assert_vector_is_searchable(&collection, 1, embedding(1)).await;

        // VERY IMPORTANT: we create only 300 vectors out of the 4094 posible ones so that the tails of the vectors
        // are irrelevant and quantization removes that instead os messing up the head of the vectors.
        // Also we create more than 256 to trigger the index type change from flat to IvfPq
        for index in 2..300 {
            collection
                .insert_vectors(vec![index as u64], vec![embedding(index)].into_iter())
                .await
                .unwrap();
        }

        assert_vector_is_searchable(&collection, 0, embedding(0)).await;
        assert_vector_is_searchable(&collection, 1, embedding(1)).await;
        assert_vector_is_searchable(&collection, 10, embedding(10)).await;
        assert_vector_is_searchable(&collection, 100, embedding(100)).await;
        assert_vector_is_searchable(&collection, 299, embedding(299)).await;

        collection.create_or_update_index().await.unwrap();

        assert_vector_is_searchable(&collection, 0, embedding(0)).await;
        assert_vector_is_searchable(&collection, 1, embedding(1)).await;
        assert_vector_is_searchable(&collection, 10, embedding(10)).await;
        assert_vector_is_searchable(&collection, 100, embedding(100)).await;
        assert_vector_is_searchable(&collection, 299, embedding(299)).await;
    }

    // fn embedding(index: usize) -> Embedding {
    //     assert!(index < EMBEDDING_DIM);
    //     let mut vector: Vec<f32> = vec![0.0; EMBEDDING_DIM];
    //     vector[index] = 1.0;
    //     vector.into()
    // }
    fn embedding(id: usize) -> Embedding {
        use rand::{rngs::StdRng, Rng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(id as u64);
        let vector: Vec<f32> = (0..EMBEDDING_DIM).map(|_| rng.random::<f32>()).collect();
        vector.into()
    }

    async fn assert_empty_search(collection: &LanceDbCollection) {
        let result = collection
            .top_k_with_distances(&embedding(0), 1, None::<Vec<_>>)
            .await
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(result, vec![]);
    }

    async fn assert_vector_is_searchable(
        collection: &LanceDbCollection,
        id: u64,
        vector: Embedding,
    ) {
        let result = collection
            .top_k_with_distances(&vector, 1, None::<Vec<_>>)
            .await
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 1);
        let (returned_id, _) = result[0];
        assert_eq!(returned_id, id);
        let returned_vector = collection.get_id(returned_id).await.unwrap().unwrap();
        assert_eq!(returned_vector, vector);
        // this assertion is unfortunately flaky because of quantization, as long as above remains true we are fine though
        // assert!(
        //     distance < 0.000001,
        //     "distance has to be close to 0, instead is {distance}"
        // )
    }
}
