use crate::model::graph::property::GqlProp;
use crate::paths::ExistingGraphFolder;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, SimpleObject};
use raphtory::core::utils::errors::GraphError;
use raphtory::serialise::metadata::GraphMetadata;

#[derive(ResolvedObject)]
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
        self.folder.get_graph_name().ok()
    }
    async fn path(&self) -> String {
        self.folder.get_original_path_str().to_owned()
    }
    async fn created(&self) -> Result<i64, GraphError> {
        self.folder.created()
    }
    async fn last_opened(&self) -> Result<i64, GraphError> {
        self.folder.last_opened()
    }
    async fn last_updated(&self) -> Result<i64, GraphError> {
        self.folder.last_updated()
    }
    async fn metadata(&self) -> Result<GqlGraphMetadata, GraphError> {
        let metadata = self.folder.read_metadata()?;
        Ok(GqlGraphMetadata::from(metadata))
    }
}

#[derive(Clone, SimpleObject)]
pub(crate) struct GqlGraphMetadata {
    pub(crate) node_count: usize,
    pub(crate) edge_count: usize,
    pub(crate) properties: Vec<GqlProp>,
}

impl From<GraphMetadata> for GqlGraphMetadata {
    fn from(metadata: GraphMetadata) -> Self {
        GqlGraphMetadata {
            node_count: metadata.node_count,
            edge_count: metadata.edge_count,
            properties: metadata
                .properties
                .into_iter()
                .map(|(key, prop)| GqlProp::new(key.to_string(), prop))
                .collect(),
        }
    }
}
