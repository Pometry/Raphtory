use super::{
    cache::VectorCache,
    entity_db::{EdgeDb, NodeDb},
    template::DocumentTemplate,
    vectorised_graph::VectorisedGraph,
};
use crate::{
    db::api::view::StaticGraphViewOps,
    errors::{GraphError, GraphResult},
    vectors::{
        embeddings::ModelConfig,
        vector_collection::{lancedb::LanceDb, VectorCollectionFactory},
    },
};
use async_openai::config::{OpenAIConfig, OPENAI_API_BASE};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::OnceCell;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct OpenAIEmbeddings {
    pub model: String,
    pub api_base: Option<String>,
    pub api_key_env: Option<String>,
    pub org_id: Option<String>,
    pub project_id: Option<String>,
    pub dim: Option<usize>,
}

impl OpenAIEmbeddings {
    pub fn empty(name: impl AsRef<str>) -> Self {
        Self {
            model: name.as_ref().to_owned(),
            api_base: None,
            api_key_env: None,
            org_id: None,
            project_id: None,
            dim: None,
        }
    }

    pub fn new(model: impl AsRef<str>, api_base: impl AsRef<str>) -> Self {
        Self {
            model: model.as_ref().to_owned(),
            api_base: Some(api_base.as_ref().to_owned()),
            api_key_env: None,
            org_id: None,
            project_id: None,
            dim: None,
        }
    }

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

#[derive(Serialize, Deserialize, Debug)]
pub(super) struct VectorMeta {
    pub(super) template: DocumentTemplate,
    pub(super) model: ModelConfig,
    /// Which generation of the collections this meta points at. A full rebuild writes a new
    /// generation and only then updates this file, so the switch is a single atomic write and a
    /// rebuild that never finishes leaves the previous generation in use. Absent in metas written
    /// before generations existed, which are generation 0.
    #[serde(default)]
    pub(super) generation: u64,
}

/// Collection names for a generation. Generation 0 keeps the original names so stores written
/// before generations existed are read without migration.
pub(super) fn collection_names(generation: u64) -> (String, String) {
    if generation == 0 {
        ("nodes".to_owned(), "edges".to_owned())
    } else {
        (format!("nodes_{generation}"), format!("edges_{generation}"))
    }
}

impl VectorMeta {
    pub(super) fn write_to_path(&self, path: &Path) -> Result<(), GraphError> {
        let file = File::create(meta_path(path))?;
        serde_json::to_writer(file, self)?;
        Ok(())
    }

    pub(super) async fn read_from_path(path: &Path) -> GraphResult<Self> {
        let meta_string = std::fs::read_to_string(path)?;
        let meta: VectorMeta = serde_json::from_str(&meta_string)?;
        Ok(meta)
    }
}

#[derive(Clone)]
pub struct LazyDiskVectorCache {
    path: PathBuf,
    // shared by every clone: the on-disk cache is a heed env, and heed refuses to open the same
    // path twice in one process, so each clone resolving its own cell would fail
    cache: Arc<OnceCell<VectorCache>>,
}

impl LazyDiskVectorCache {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            cache: Default::default(),
        }
    }

    pub async fn resolve(&self) -> GraphResult<&VectorCache> {
        self.cache
            .get_or_try_init(async || VectorCache::on_disk(&self.path.clone()).await)
            .await
    }
}

// This is currently being used only by the GraphQL server,if that changes, accepting LazyDiskVectorCache might be too inflexible
impl<G: StaticGraphViewOps> VectorisedGraph<G> {
    pub async fn read_from_path(
        path: &Path,
        graph: G,
        cache: &LazyDiskVectorCache,
    ) -> GraphResult<Self> {
        let meta = VectorMeta::read_from_path(&meta_path(path)).await?;

        let factory = LanceDb;
        let db_path = Arc::new(db_path(path));
        // TODO: put table names in common place? maybe some trait function for EntityDb that returns it

        let resolved = cache.resolve().await?;
        let model = resolved.validate_and_set_dim(meta.model).await?;
        let dim = model.dim().ok_or_else(|| GraphError::UnresolvedModel)?;

        let (node_table, edge_table) = collection_names(meta.generation);
        let node_db = NodeDb(factory.from_path(db_path.clone(), &node_table, dim).await?);
        let edge_db = EdgeDb(factory.from_path(db_path, &edge_table, dim).await?);

        Ok(VectorisedGraph {
            template: meta.template,
            source_graph: graph,
            model,
            node_db,
            edge_db,
        })
    }
}

pub(super) fn meta_path(path: &Path) -> PathBuf {
    path.join("meta")
}

pub(super) fn db_path(path: &Path) -> PathBuf {
    path.join("db")
}

#[cfg(test)]
mod vector_storage_tests {
    use super::LazyDiskVectorCache;

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
