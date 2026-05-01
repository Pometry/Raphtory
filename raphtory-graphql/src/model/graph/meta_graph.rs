use crate::{
    data::Data,
    model::graph::property::GqlProperty,
    paths::{ExistingGraphFolder, ValidGraphPaths},
};
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, Result};
use raphtory::{
    db::api::storage::storage::{read_constant_graph_properties, Extension, PersistenceStrategy},
    errors::GraphError,
    prelude::{GraphViewOps, PropertiesOps},
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

    pub(crate) fn local_path(&self) -> &str {
        self.folder.local_path()
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
        self.folder.local_path().into()
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
    ///
    /// Reads metadata without forcing a full graph load: from the
    /// in-memory cache if the graph is already loaded, otherwise directly
    /// from disk (parquet metadata for parquet-backed graphs, the
    /// `graph_props` segment for disk-backed graphs). This keeps
    /// `MetaGraph.metadata` cheap for namespace listings of many graphs.
    async fn metadata(&self, ctx: &Context<'_>) -> Result<Vec<GqlProperty>> {
        let data: &Data = ctx.data_unchecked();
        if let Some(graph) = data.get_cached_graph(self.folder.local_path()).await {
            return Ok(graph
                .graph
                .metadata()
                .iter()
                .filter_map(|(key, value)| value.map(|prop| GqlProperty::new(key.into(), prop)))
                .collect());
        }

        if Extension::disk_storage_enabled() {
            let graph_path = self
                .folder
                .graph_folder()
                .graph_path()
                .map_err(GraphError::from)?;
            let pairs = read_constant_graph_properties(&graph_path).map_err(GraphError::from)?;
            return Ok(pairs
                .into_iter()
                .map(|(key, prop)| GqlProperty::new(key.to_string(), prop))
                .collect());
        }

        Ok(decode_graph_metadata(self.folder.graph_folder())?
            .into_iter()
            .filter_map(|(key, value)| value.map(|prop| GqlProperty::new(key, prop)))
            .collect())
    }
}
