use crate::{
    model::graph::property::GqlProperty,
    paths::{ExistingGraphFolder, ValidGraphPaths},
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, Result};
use raphtory::{
    errors::GraphError,
    io::parquet_loaders::load_graph_props_from_parquet,
    serialise::{metadata::GraphMetadata, parquet::decode_graph_metadata, GraphPaths},
};
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

    async fn meta(&self) -> Result<&GraphMetadata> {
        Ok(self
            .meta
            .get_or_try_init(|| self.folder.read_metadata_async())
            .await?)
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
        self.folder.local_path_string()
    }

    /// Returns the timestamp for the creation of the graph.
    async fn created(&self) -> Result<i64> {
        Ok(self.folder.created_async().await?)
    }

    /// Returns the graph's last opened timestamp according to system time.
    async fn last_opened(&self) -> Result<i64> {
        Ok(self.folder.last_opened_async().await?)
    }

    /// Returns the graph's last updated timestamp.
    async fn last_updated(&self) -> Result<i64> {
        Ok(self.folder.last_updated_async().await?)
    }

    /// Returns the number of nodes in the graph.
    async fn node_count(&self) -> Result<usize> {
        Ok(self.meta().await?.node_count)
    }

    /// Returns the number of edges in the graph.
    ///
    /// Returns:
    ///     int:
    async fn edge_count(&self) -> Result<usize> {
        Ok(self.meta().await?.edge_count)
    }

    /// Returns the metadata of the graph.
    async fn metadata(&self) -> Result<Vec<GqlProperty>> {
        let res = decode_graph_metadata(&self.folder)?;
        Ok(res
            .into_iter()
            .filter_map(|(key, value)| value.map(|prop| GqlProperty::new(key, prop)))
            .collect())
    }
}
