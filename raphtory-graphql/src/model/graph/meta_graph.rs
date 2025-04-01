use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::core::utils::errors::GraphError;

use crate::paths::ExistingGraphFolder;

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
}
