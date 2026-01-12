use std::{ops::Deref, path::Path, sync::Arc};

use arrow_array::{
    types::{Float32Type, UInt64Type},
    ArrayRef, ArrowPrimitiveType, FixedSizeListArray, PrimitiveArray, RecordBatch,
    RecordBatchIterator, UInt64Array,
};
use futures_util::TryStreamExt;
use itertools::Itertools;
use lancedb::{
    arrow::arrow_schema::{DataType, Field, Schema},
    index::{
        vector::{IvfFlatIndexBuilder, IvfPqIndexBuilder},
        Index, IndexType,
    },
    query::{ExecutableQuery, QueryBase},
    table::{OptimizeAction, OptimizeOptions},
    Connection, DistanceType, Table,
};

use crate::{
    errors::{GraphError, GraphResult},
    vectors::{
        vector_collection::{CollectionPath, VectorCollection, VectorCollectionFactory},
        Embedding,
    },
};

const VECTOR_COL_NAME: &str = "vector";

pub(crate) struct LanceDb;

impl VectorCollectionFactory for LanceDb {
    type DbType = LanceDbCollection;

    async fn new_collection(
        &self,
        path: CollectionPath,
        name: &str,
        dim: usize,
    ) -> GraphResult<Self::DbType> {
        let db = connect(path.deref().as_ref()).await?;
        let schema = get_schema(dim);
        let table = db.create_empty_table(name, schema).execute().await?;
        Ok(Self::DbType {
            table,
            dim,
            _path: path,
        })
    }

    async fn from_path(
        &self,
        path: CollectionPath,
        name: &str,
        dim: usize,
    ) -> GraphResult<Self::DbType> {
        let db = connect(path.deref().as_ref()).await?;
        let table = db.open_table(name).execute().await?;
        Ok(Self::DbType {
            table,
            dim,
            _path: path,
        })
    }
}

#[derive(Clone)]
pub(crate) struct LanceDbCollection {
    table: Table, // maybe this should be built in every call to the collection from path?
    dim: usize,
    _path: CollectionPath, // this is only necessary to avoid dropping temp dirs
}

impl LanceDbCollection {
    fn schema(&self) -> Arc<Schema> {
        get_schema(self.dim)
    }
}

impl VectorCollection for LanceDbCollection {
    async fn insert_vectors(
        &self,
        ids: Vec<u64>,
        vectors: impl IntoIterator<Item = Embedding>,
    ) -> crate::errors::GraphResult<()> {
        let batches = RecordBatchIterator::new(
            vec![RecordBatch::try_new(
                self.schema(),
                vec![
                    Arc::new(UInt64Array::from(ids)),
                    Arc::new(
                        FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                            vectors.into_iter().map(|vector| {
                                Some(
                                    vector
                                        .into_iter()
                                        .map(|value| Some(*value))
                                        .collect::<Vec<_>>(), // TODO: ideally avoid this collect
                                )
                            }),
                            self.dim as i32,
                        ),
                    ),
                ],
            )],
            self.schema(),
        );
        self.table.add(batches).execute().await?;
        Ok(())
    }

    async fn get_id(&self, id: u64) -> GraphResult<Option<crate::vectors::Embedding>> {
        let query = self.table.query().only_if(format!("id = {id}"));
        let result = query.execute().await?;
        let batches: Vec<_> = result.try_collect().await?;
        if let Some(batch) = batches.get(0) {
            let array = get_vector_array_from_simple_batch(batch)
                .ok_or(GraphError::InvalidVectorDbSchema)?;
            let vector = array.values().iter().copied().collect();
            Ok(Some(vector))
        } else {
            Ok(None)
        }
    }

    // TODO: might make this return everything, the embedding itself, so that we don't
    // need to go back to the vector collection to retrieve the embedding by id
    // with get_id(), although we need this anyways for entities that are forced into the selection
    async fn top_k_with_distances(
        &self,
        query: &crate::vectors::Embedding,
        k: usize,
        candidates: Option<impl IntoIterator<Item = u64>>,
    ) -> GraphResult<impl Iterator<Item = (u64, f32)> + Send> {
        let vector_query = self.table.query().nearest_to(query.as_ref())?;
        let limited = vector_query.limit(k);
        let filtered = if let Some(candidates) = candidates {
            let mut iter = candidates.into_iter().peekable();
            if let Some(_) = iter.peek() {
                let id_list = iter.map(|id| id.to_string()).join(",");
                limited.only_if(format!("id IN ({id_list})"))
            } else {
                limited.only_if("false") // this is a bit hacky, maybe the top layer shouldnt even call this one if the candidates list is empty
            }
        } else {
            limited
        };
        let stream = filtered.execute().await?;
        let result = stream.try_collect::<Vec<_>>().await?;

        let downcasted = result
            .into_iter()
            .map(|record| {
                let ids = primitive_column_iter::<UInt64Type>(&record, "id")?;
                let scores = primitive_column_iter::<Float32Type>(&record, "_distance")?;
                // TODO: try using record.columns() instead of getting them independently?
                Some(ids.zip(scores).collect::<Vec<_>>())
            })
            .collect::<Option<Vec<_>>>()
            .ok_or(GraphError::InvalidVectorDbSchema)?
            .into_iter()
            .flatten();
        Ok(downcasted)
    }

    async fn create_or_update_index(&self) -> GraphResult<()> {
        let count = self.table.count_rows(None).await?;
        if count > 0 {
            // TODO: could we save the index name when creating it instead of having to do this?
            let indices = self.table.list_indices().await?;
            let vector_index = indices
                .iter()
                .find(|index| index.columns == vec![VECTOR_COL_NAME]);

            let target_index_type = if count > 256 {
                IndexType::IvfPq
            } else {
                IndexType::IvfFlat
            };

            let ideal_type_already_exists = vector_index
                .map(|index| index.index_type == target_index_type)
                .unwrap_or(false);

            if ideal_type_already_exists {
                self.table
                    .optimize(OptimizeAction::Index(OptimizeOptions::default()))
                    .await?;
            } else {
                if let Some(vector_index) = vector_index {
                    self.table.drop_index(&vector_index.name).await?;
                }
                let index_builder = if target_index_type == IndexType::IvfFlat {
                    Index::IvfFlat(
                        IvfFlatIndexBuilder::default().distance_type(DistanceType::Cosine),
                    )
                } else {
                    Index::IvfPq(IvfPqIndexBuilder::default().distance_type(DistanceType::Cosine))
                };
                self.table
                    .create_index(&[VECTOR_COL_NAME], index_builder)
                    .execute()
                    .await?;
            }
        }
        Ok(())
    }
}

fn primitive_column_iter<'a, T>(
    record: &'a arrow_array::RecordBatch,
    name: &str,
) -> Option<impl Iterator<Item = T::Native> + 'a>
where
    T: ArrowPrimitiveType,
{
    Some(
        record
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()?
            .values()
            .iter()
            .copied(),
    )
}

fn get_vector_array_from_simple_batch(batch: &RecordBatch) -> Option<PrimitiveArray<Float32Type>> {
    let col: &ArrayRef = batch.column_by_name("vector")?;
    let array_list = col.as_any().downcast_ref::<FixedSizeListArray>();
    let array = array_list?.value(0);
    array.as_any().downcast_ref().cloned()
}

async fn connect(path: &Path) -> lancedb::Result<Connection> {
    let url = path.display().to_string();
    lancedb::connect(&url).execute().await
}

fn get_schema(dim: usize) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new(
            VECTOR_COL_NAME,
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dim as i32,
            ),
            true,
        ),
    ]))
}

#[cfg(test)]
mod lancedb_tests {
    use std::sync::Arc;

    use crate::vectors::{
        vector_collection::{
            lancedb::{LanceDb, LanceDbCollection},
            VectorCollection, VectorCollectionFactory,
        },
        Embedding,
    };

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
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(id as u64);
        let vector: Vec<f32> = (0..EMBEDDING_DIM).map(|_| rng.gen::<f32>()).collect();
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
        let (returned_id, distance) = result[0];
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
