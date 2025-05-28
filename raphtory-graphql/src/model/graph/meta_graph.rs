use crate::{model::graph::property::GqlProperty, paths::ExistingGraphFolder};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, SimpleObject};
use raphtory::{errors::GraphError, serialise::metadata::GraphMetadata};
use tokio::task::spawn_blocking;

#[derive(ResolvedObject, Clone)]
pub(crate) struct MetaGraph {
    folder: ExistingGraphFolder,
}

impl MetaGraph {
    pub fn new(path: ExistingGraphFolder) -> Self {
        Self { folder: path }
    }
}

#[ResolvedObjectFields]
impl MetaGraph {
    async fn name(&self) -> Option<String> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.folder.get_graph_name().ok())
            .await
            .unwrap()
    }
    async fn path(&self) -> String {
        self.folder.get_original_path_str().to_owned()
    }
    async fn created(&self) -> Result<i64, GraphError> {
        self.folder.created_async().await
    }
    async fn last_opened(&self) -> Result<i64, GraphError> {
        self.folder.last_opened_async().await
    }
    async fn last_updated(&self) -> Result<i64, GraphError> {
        self.folder.last_updated_async().await
    }
    async fn metadata(&self) -> Result<GqlGraphMetadata, GraphError> {
        let self_clone = self.clone();
        spawn_blocking(move || {
            let metadata = self_clone.folder.read_metadata()?;
            Ok(GqlGraphMetadata::from(metadata))
        })
        .await
        .unwrap()
    }
}

#[derive(Clone, SimpleObject)]
#[graphql(name = "GraphMetadata")]
pub(crate) struct GqlGraphMetadata {
    pub(crate) node_count: usize,
    pub(crate) edge_count: usize,
    pub(crate) properties: Vec<GqlProperty>,
}

impl From<GraphMetadata> for GqlGraphMetadata {
    fn from(metadata: GraphMetadata) -> Self {
        GqlGraphMetadata {
            node_count: metadata.node_count,
            edge_count: metadata.edge_count,
            properties: metadata
                .properties
                .into_iter()
                .map(|(key, prop)| GqlProperty::new(key.to_string(), prop))
                .collect(),
        }
    }
}
