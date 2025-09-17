use super::{
    cache::VectorCache,
    entity_db::{EdgeDb, EntityDb, NodeDb},
    template::DocumentTemplate,
    vectorised_graph::VectorisedGraph,
};
use crate::{
    db::api::view::StaticGraphViewOps,
    errors::{GraphError, GraphResult},
    vectors::{
        vector_collection::{lancedb::LanceDb, VectorCollectionFactory},
        Embedding,
    },
};
use async_openai::config::{OpenAIConfig, OPENAI_API_BASE};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    path::{Path, PathBuf},
};

#[derive(Serialize, Deserialize, Debug, Clone, Hash)]
pub struct OpenAIEmbeddings {
    pub model: String,
    pub api_base: Option<String>,
    pub api_key_env: Option<String>,
    pub org_id: Option<String>,
    pub project_id: Option<String>,
}

impl Default for OpenAIEmbeddings {
    fn default() -> Self {
        Self {
            model: "text-embedding-3-small".to_owned(), // TODO: double-check where am I really using this
            api_base: Default::default(),
            api_key_env: Default::default(),
            org_id: Default::default(),
            project_id: Default::default(),
        }
    }
}

impl OpenAIEmbeddings {
    pub(super) fn resolve_config(&self) -> OpenAIConfig {
        let api_key_env = self
            .api_key_env
            .clone()
            .unwrap_or("OPENAI_API_KEY".to_owned());
        let api_key = std::env::var(api_key_env).unwrap_or_default(); // TODO: raise error if api_key_env provided but not var defined

        let api_base = self.api_base.clone().unwrap_or(OPENAI_API_BASE.to_owned());

        OpenAIConfig::new()
            .with_api_base(api_base)
            .with_api_key(api_key)
            .with_org_id(self.org_id.clone().unwrap_or_default())
            .with_project_id(self.project_id.clone().unwrap_or_default())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Embeddings {
    OpenAI(OpenAIEmbeddings),
}

#[derive(Serialize, Deserialize, Debug)]
pub(super) struct VectorMeta {
    pub(super) template: DocumentTemplate,
    pub(super) sample: Embedding,
    pub(super) embeddings: Embeddings,
}

impl VectorMeta {
    pub(super) fn write_to_path(&self, path: &Path) -> Result<(), GraphError> {
        let file = File::create(meta_path(path))?;
        serde_json::to_writer(file, self)?;
        Ok(())
    }
}

impl<G: StaticGraphViewOps> VectorisedGraph<G> {
    pub async fn read_from_path(path: &Path, graph: G, cache: VectorCache) -> GraphResult<Self> {
        let meta_string = std::fs::read_to_string(meta_path(path))?;
        let meta: VectorMeta = serde_json::from_str(&meta_string)?;

        let factory = LanceDb;
        let db_path = db_path(path);
        let dim = meta.sample.len();
        // TODO: put table names in common place? maybe some trait function for EntityDb that returns it
        let node_db = NodeDb(factory.from_path(&db_path, "nodes", dim).await?);
        let edge_db = EdgeDb(factory.from_path(&db_path, "edges", dim).await?);

        Ok(VectorisedGraph {
            template: meta.template,
            source_graph: graph,
            cache,
            node_db,
            edge_db,
        })
    }
}

fn meta_path(path: &Path) -> PathBuf {
    path.join("meta")
}

pub(super) fn db_path(path: &Path) -> PathBuf {
    path.join("db")
}

#[cfg(test)]
mod vector_storage_tests {
    use async_openai::config::OpenAIConfig;

    use crate::vectors::{
        embeddings::OpenAIEmbeddings,
        storage::{Embeddings, StoredOpenAIEmbeddings, VectorMeta},
        template::DocumentTemplate,
    };

    #[test]
    fn test_vector_meta() {
        let meta = VectorMeta {
            template: DocumentTemplate::default(),
            sample: vec![1.0].into(),
            embeddings: Embeddings::OpenAI(StoredOpenAIEmbeddings {
                model: "text-embedding-3-small".to_owned(),
                config: Default::default(),
            }),
        };
        let serialised = serde_json::to_string_pretty(&meta).unwrap();
        println!("{serialised}");

        if let Embeddings::OpenAI(embeddings) = meta.embeddings {
            let embeddings: OpenAIEmbeddings = embeddings.try_into().unwrap();
        } else {
            panic!("should not be here");
        }

        // panic!("here");
    }
}
