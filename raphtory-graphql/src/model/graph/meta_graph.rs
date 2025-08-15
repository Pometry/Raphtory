use crate::{model::graph::property::GqlProperty, paths::ExistingGraphFolder};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{errors::GraphError, serialise::metadata::GraphMetadata};
use std::{cmp::Ordering, sync::Arc};
use tokio::sync::OnceCell;

///
#[derive(ResolvedObject, Clone)]
pub(crate) struct MetaGraph {
    folder: ExistingGraphFolder,
    meta: Arc<OnceCell<GraphMetadata>>,
}

impl PartialEq for MetaGraph {
    fn eq(&self, other: &Self) -> bool {
        self.folder == other.folder
    }
}

impl Eq for MetaGraph {}

impl PartialOrd for MetaGraph {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MetaGraph {
    fn cmp(&self, other: &Self) -> Ordering {
        self.folder.cmp(&other.folder)
    }
}

impl MetaGraph {
    pub fn new(path: ExistingGraphFolder) -> Self {
        Self {
            folder: path,
            meta: Default::default(),
        }
    }

    async fn meta(&self) -> Result<&GraphMetadata, GraphError> {
        self.meta
            .get_or_try_init(|| self.folder.read_metadata_async())
            .await
    }
}

#[ResolvedObjectFields]
/// Metagraphs are a GraphQL specific object that wraps  the normal graph functions. Metagraphs exist in memory and reduce the need to load full graphs from disk. 
impl MetaGraph {
    /// Returns the graph name.
    async fn name(&self) -> Option<String> {
        self.folder.get_graph_name().ok()
    }

    /// Returns path of graph.
    async fn path(&self) -> String {
        self.folder.get_original_path_str().to_owned()
    }

    /// Returns the timestamp for the creation of the graph.
    async fn created(&self) -> Result<i64, GraphError> {
        self.folder.created_async().await
    }

    /// Returns the graph's last opened timestamp according to system time.
    async fn last_opened(&self) -> Result<i64, GraphError> {
        self.folder.last_opened_async().await
    }

    /// Returns the graph's last updated timestamp.
    async fn last_updated(&self) -> Result<i64, GraphError> {
        self.folder.last_updated_async().await
    }

    /// Returns the number of nodes in the graph.
    async fn node_count(&self) -> Result<usize, GraphError> {
        Ok(self.meta().await?.node_count)
    }

    /// Returns the number of edges in the graph.
    async fn edge_count(&self) -> Result<usize, GraphError> {
        Ok(self.meta().await?.edge_count)
    }

    /// Returns the metadata of the graph.
    async fn metadata(&self) -> Result<Vec<GqlProperty>, GraphError> {
        Ok(self
            .meta()
            .await?
            .metadata
            .iter()
            .map(|(key, prop)| GqlProperty::new(key.to_string(), prop.clone()))
            .collect())
    }
}
