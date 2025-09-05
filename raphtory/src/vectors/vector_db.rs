use std::{borrow::Cow, path::Path};

use milvus::{
    client::Client,
    collection::{Collection, SearchOption},
    data::FieldColumn,
    index::{IndexParams, IndexType, MetricType},
    proto::{common::ErrorCode, schema::DataType},
    schema::{CollectionSchemaBuilder, FieldSchema},
    value::{Value, ValueVec},
};

use crate::{errors::GraphResult, vectors::Embedding};

const VECTOR_FIELD_NAME: &'static str = "vector";

pub(super) trait VectorDbFactory {
    type DbType: VectorDb;
    async fn new_db(&self, dim: usize) -> Self::DbType;
    fn from_path(&self, path: &Path) -> Self::DbType;
}

pub struct Milvus {
    url: String,
}

impl Milvus {
    pub fn new(url: String) -> Self {
        Self { url }
    }
}

impl VectorDbFactory for Milvus {
    type DbType = MilvusDb;

    async fn new_db(&self, dim: usize) -> Self::DbType {
        let collection = format!(
            "raphtory_{}",
            uuid::Uuid::new_v4().to_string().replace("-", "_")
        );
        let client = Client::new(self.url.to_owned()).await.unwrap();
        let schema = CollectionSchemaBuilder::new(&collection, "")
            .add_field(FieldSchema::new_int64("id", ""))
            .add_field(FieldSchema::new_float_vector(
                VECTOR_FIELD_NAME,
                "",
                dim as i64,
            ))
            .set_primary_key("id")
            .unwrap()
            .build()
            .unwrap();
        client.create_collection(schema, None).await.unwrap(); // TODO: maybe set  up somew options such as number of shards?

        Self::DbType {
            url: self.url.clone(),
            collection,
            dim,
        }
    }

    fn from_path(&self, path: &Path) -> Self::DbType {
        todo!()
    }
}

pub(super) trait VectorDb: Sized {
    async fn insert_vectors(&self, embeddings: Vec<(usize, Embedding)>) -> GraphResult<()>;

    async fn get_id(&self, id: u32) -> GraphResult<Option<Embedding>>;

    async fn top_k(&self, query: &Embedding, k: usize) -> impl Iterator<Item = (i64, f32)>;

    async fn create_index(&self);

    // fn from_path(path: &Path) -> GraphResult<Self>;
}

#[derive(Clone)]
pub(super) struct MilvusDb {
    url: String,
    collection: String,
    dim: usize,
}

impl MilvusDb {
    async fn collection(&self) -> Collection {
        let client = Client::new(self.url.to_owned()).await.unwrap();
        client.get_collection(&self.collection).await.unwrap()
    }
}

impl VectorDb for MilvusDb {
    async fn insert_vectors(&self, embeddings: Vec<(usize, Embedding)>) -> GraphResult<()> {
        let ids = embeddings.iter().map(|(id, _)| *id as i64).collect();
        let values = embeddings
            .iter()
            .flat_map(|(_, vector)| vector.iter())
            .copied()
            .collect();

        let data = vec![
            FieldColumn {
                name: "id".to_owned(),
                dtype: DataType::Int64,
                value: ValueVec::Long(ids), // the id !!!!!!!!!!!!!!!!,
                dim: 1,
                max_length: 1,
            },
            FieldColumn {
                name: "vector".to_owned(),
                dtype: DataType::FloatVector,
                value: ValueVec::Float(values),
                dim: self.dim as i64,
                max_length: 1,
            },
        ];

        let result = self.collection().await.insert(data, None).await.unwrap();
        let success = result
            .status
            .is_some_and(|status| status.error_code() == ErrorCode::Success);
        assert!(success);
        Ok(())
    }

    // FIXME: simply get the embeddings out of the search query so that I don't need to come back for them by using this function
    async fn get_id(&self, id: u32) -> GraphResult<Option<Embedding>> {
        let result = self
            .collection()
            .await
            .query::<String, Vec<String>>(format!("id == {id}"), vec![])
            .await
            .unwrap();

        if let Some(vector_col) = result.into_iter().find(|col| col.name == VECTOR_FIELD_NAME) {
            if let ValueVec::Float(values) = vector_col.value {
                Ok(Some(values.into()))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    async fn create_index(&self) {
        self.collection()
            .await
            .create_index(
                "vector",
                IndexParams::new(
                    "vector".to_owned(), // TODO: make sure the namespace for the index name is not global
                    IndexType::IvfFlat,
                    MetricType::IP,
                    Default::default(),
                ),
            )
            .await
            .unwrap();
    }

    async fn top_k(&self, query: &Embedding, k: usize) -> impl Iterator<Item = (i64, f32)> {
        let collection = self.collection().await;
        collection.load(1).await.unwrap();
        let mut result = collection
            .search(
                vec![Value::FloatArray(Cow::Borrowed(query))],
                VECTOR_FIELD_NAME,
                k as i32,
                MetricType::IP, // FIXME: why can't I use cosine?
                vec!["id"],     // TODO: remove this?
                &SearchOption::new(),
            )
            .await
            .unwrap();

        let mut search_result = result.remove(0); // careful

        let ids = search_result.field.remove(0);
        if let ValueVec::Long(ids) = ids.value {
            ids.into_iter().zip(search_result.score)
        } else {
            panic!("no ids to get");
        }
    }

    // fn from_path(path: &str) -> GraphResult<Self> {
    //     Ok(Self {
    //         url: "http://localhost:19530".to_owned(),
    //         collection: path.to_owned(),
    //     })
    // }
}

// #[cfg(test)]
// mod vector_db_tests {
//     use crate::vectors::vector_db::{VectorDB, Vilmus};

//     #[tokio::test]
//     async fn test_vilmus() {
//         let db = Vilmus::from_path("test2").unwrap();
//         db.insert_vectors(vec![(4, [0.45, 0.45].into()), (5, [0.5, 0.5].into())])
//             .await
//             .unwrap();

//         let vector = db.get_id(5).await.unwrap().unwrap();
//         assert_eq!(vector, [0.5, 0.5].into())
//     }
// }
