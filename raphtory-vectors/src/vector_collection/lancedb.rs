use crate::errors::{VectorError, VectorResult};
use crate::{vector_collection::CollectionPath, vector_collection::VectorCollection, vector_collection::VectorCollectionFactory, Embedding};
use arrow_array::{
    builder::{FixedSizeListBuilder, Float32Builder},
    types::{Float32Type, UInt64Type},
    Array, ArrayRef, ArrowPrimitiveType, FixedSizeListArray, PrimitiveArray, RecordBatch,
    RecordBatchIterator, UInt64Array,
};
use futures_util::TryStreamExt;
use itertools::Itertools;
use lancedb::{
    arrow::arrow_schema::{DataType, Field, Schema},
    database::CreateTableMode,
    index::{
        vector::{IvfFlatIndexBuilder, IvfPqIndexBuilder},
        Index, IndexType,
    },
    query::{ExecutableQuery, QueryBase, Select},
    table::{OptimizeAction, OptimizeOptions},
    Connection, DistanceType, Table,
};
use roaring::RoaringTreemap;
use std::{collections::HashSet, ops::Deref, path::Path, sync::Arc};

const VECTOR_COL_NAME: &str = "vector";

#[doc(hidden)] // pub for raphtory-tests
pub struct LanceDb;

impl VectorCollectionFactory for LanceDb {
    type DbType = LanceDbCollection;

    async fn new_collection(
        &self,
        path: CollectionPath,
        name: &str,
        dim: usize,
    ) -> VectorResult<Self::DbType> {
        let db = connect(path.deref().as_ref()).await?;
        let schema = get_schema(dim);
        // Overwrite: a re-vectorise of a graph whose collections are already on disk has to
        // rebuild them from scratch, and the default Create mode errors on an existing table
        let table = db
            .create_empty_table(name, schema)
            .mode(CreateTableMode::Overwrite)
            .execute()
            .await?;
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
    ) -> VectorResult<Self::DbType> {
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
#[doc(hidden)] // pub for raphtory-tests
pub struct LanceDbCollection {
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
    ) -> VectorResult<()> {
        // lance defines a merge with several source rows matching one target row as undefined,
        // and currently duplicates the row, so only the last vector for each id is kept
        let incoming: Vec<_> = ids.into_iter().zip(vectors).collect();
        let mut seen = HashSet::with_capacity(incoming.len());
        let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), self.dim as i32);
        let mut ids = Vec::with_capacity(incoming.len()); // duplicates should be rare

        // order after deduplication doesn't matter, can build the arrays directly
        for (id, vector) in incoming.into_iter().rev() {
            if seen.insert(id) {
                ids.push(id);
                builder.values().append_slice(&vector);
                builder.append(true);
            }
        }

        let schema = self.schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from(ids)), Arc::new(builder.finish())],
        )?;
        // upsert rather than append: re-embedding an entity has to replace its vector, otherwise
        // the stale one stays behind as a second row with the same id and both get returned
        let mut merge = self.table.merge_insert(&["id"]);
        merge
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        merge
            .execute(Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema)))
            .await?;
        Ok(())
    }

    async fn existing_ids(&self) -> VectorResult<RoaringTreemap> {
        let stream = self
            .table
            .query()
            .select(Select::columns(&["id"]))
            .execute()
            .await?;
        let batches: Vec<_> = stream.try_collect().await?;
        let mut ids = RoaringTreemap::new();
        for batch in batches {
            let column = primitive_column::<UInt64Type>(&batch, "id")
                .ok_or(VectorError::InvalidVectorDbSchema)?;
            ids.extend(column.iter().flatten());
        }
        Ok(ids)
    }

    async fn get_id(&self, id: u64) -> VectorResult<Option<Embedding>> {
        let query = self.table.query().only_if(format!("id = {id}"));
        let result = query.execute().await?;
        let batches: Vec<_> = result.try_collect().await?;
        if let Some(batch) = batches.first() {
            let array = get_vector_array_from_simple_batch(batch)
                .ok_or(VectorError::InvalidVectorDbSchema)?;
            Ok(Some(array.into()))
        } else {
            Ok(None)
        }
    }

    // TODO: might make this return everything, the embedding itself, so that we don't
    // need to go back to the vector collection to retrieve the embedding by id
    // with get_id(), although we need this anyways for entities that are forced into the selection
    async fn top_k_with_distances(
        &self,
        query: &Embedding,
        k: usize,
        candidates: Option<impl IntoIterator<Item = u64>>,
    ) -> VectorResult<impl Iterator<Item = (u64, f32)> + Send> {
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
                let ids = primitive_column::<UInt64Type>(&record, "id")?;
                let scores = primitive_column::<Float32Type>(&record, "_distance")?;
                let values = (0..ids.len()).filter_map(move |i| {
                    Some((
                        ids.is_valid(i).then(|| ids.value(i))?,
                        scores.is_valid(i).then(|| scores.value(i))?,
                    ))
                });
                Some(values)
            })
            // we need to collect the entire thing to be able to return this error if the column is missing in any of the records
            .collect::<Option<Vec<_>>>()
            .ok_or(VectorError::InvalidVectorDbSchema)?
            .into_iter()
            .flatten();
        Ok(downcasted)
    }

    async fn create_or_update_index(&self) -> VectorResult<()> {
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

fn primitive_column<T>(record: &RecordBatch, name: &str) -> Option<PrimitiveArray<T>>
where
    T: ArrowPrimitiveType,
{
    record
        .column_by_name(name)?
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .cloned()
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
