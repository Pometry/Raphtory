use std::{path::Path, sync::Arc};

use futures_util::TryStreamExt;
use itertools::Itertools;
use lancedb::{
    arrow::arrow_schema::{DataType, Field, Schema},
    index::{
        vector::{IvfFlatIndexBuilder, IvfPqIndexBuilder},
        Index,
    },
    query::{ExecutableQuery, QueryBase},
    Connection, DistanceType, Table,
};
use lancedb_arrow_array::{
    types::{Float32Type, UInt64Type},
    FixedSizeListArray, PrimitiveArray, RecordBatch, RecordBatchIterator, UInt64Array,
};

use crate::{
    errors::GraphResult,
    vectors::{
        vector_collection::{VectorCollection, VectorCollectionFactory},
        Embedding,
    },
};

const VECTOR_COL_NAME: &str = "vector";

pub(crate) struct LanceDb;

impl VectorCollectionFactory for LanceDb {
    type DbType = LanceDbCollection;

    async fn new_collection(
        &self,
        path: &Path,
        name: &str,
        dim: usize,
    ) -> GraphResult<Self::DbType> {
        let db = connect(path).await;
        let schema = get_schema(dim);
        let table = db.create_empty_table(name, schema).execute().await.unwrap(); // TODO: remove unwrap
        Ok(Self::DbType { table, dim })
    }

    async fn from_path(
        &self,
        path: &std::path::Path,
        name: &str,
        dim: usize,
    ) -> GraphResult<Self::DbType> {
        let db = connect(path).await;
        let table = db.open_table(name).execute().await.unwrap(); // TODO: remove unwrap

        // FIXME: if dim is wrong, bail from here with something like the following!!!
        // let vector_field = table
        //     .schema()
        //     .await
        //     .unwrap()
        //     .field_with_name("vectors")
        //     .unwrap(); // and get the array size
        Ok(Self::DbType { table, dim })
    }
}

#[derive(Clone)]
pub(crate) struct LanceDbCollection {
    table: Table,
    dim: usize,
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
        let size = ids.len();
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
        self.table.add(batches).execute().await.unwrap(); // TODO: remove unwrap
        Ok(())
    }

    async fn get_id(&self, id: u64) -> GraphResult<Option<crate::vectors::Embedding>> {
        dbg!(id);
        let query = self.table.query().only_if(format!("id = {id}"));
        let result = query.execute().await.unwrap();
        let batches: Vec<_> = result.try_collect().await.unwrap();
        todo!()
    }

    // TODO: make this return everything, the embedding itself, so that we don't
    // need to go back to the vector collection to retrieve the embedding by id
    // with get_id()
    // I need get_id anyways for entities that are forced into the selection
    async fn top_k_with_distances(
        &self,
        query: &crate::vectors::Embedding,
        k: usize,
        candidates: Option<impl IntoIterator<Item = u64>>,
    ) -> GraphResult<impl Iterator<Item = (u64, f32)> + Send> {
        // TODO: return IntoIter?
        let vector_query = self.table.query().nearest_to(query.as_ref()).unwrap();
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
        let stream = filtered.execute().await.unwrap();
        let result = stream.try_collect::<Vec<_>>().await.unwrap();

        let downcasted = result.into_iter().flat_map(|record| {
            // TODO: merge both things
            let ids = record
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt64Type>>()
                .unwrap()
                .values()
                .iter()
                .copied();
            let scores = record
                .column_by_name("_distance")
                .unwrap()
                .as_any()
                .downcast_ref::<PrimitiveArray<Float32Type>>()
                .unwrap()
                .values()
                .iter()
                .copied();
            // TODO: try to avoid colect maybe using record.columns() instead of getting them independently
            ids.zip(scores).collect::<Vec<_>>()
        });
        Ok(downcasted)
    }

    async fn create_index(&self) {
        let count = self.table.count_rows(None).await.unwrap(); // FIXME: remove unwrap
        if count > 0 {
            // we check the count because indexing with no rows errors out
            self.table
                .create_index(
                    &[VECTOR_COL_NAME],
                    Index::IvfFlat(
                        IvfFlatIndexBuilder::default().distance_type(DistanceType::Cosine),
                    ),
                    // Index::IvfPq(IvfPqIndexBuilder::default().distance_type(DistanceType::Cosine)), // TODO: bring this back for over 256 rows, or a greater value
                )
                // .create_index(&[VECTOR_COL_NAME], Index::Auto)
                .execute()
                .await
                .unwrap() // FIXME: remove unwrap
        }
        // FIXME: what happens if the rows are added later on???
    }
}

async fn connect(path: &Path) -> Connection {
    let url = path.display().to_string();
    lancedb::connect(&url).execute().await.unwrap() // TODO: remove unwrap
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
    use crate::vectors::{
        vector_collection::{lancedb::LanceDb, VectorCollection, VectorCollectionFactory},
        Embedding,
    };

    #[tokio::test]
    async fn test_search() {
        let factory = LanceDb;
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path();
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
}
