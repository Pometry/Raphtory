use crate::{
    data::Data,
    model::graph::property::GqlProperty,
    paths::{ExistingGraphFolder, ValidGraphPaths},
};
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, Result};
use raphtory::{
    db::api::storage::storage::read_constant_graph_properties,
    errors::GraphError,
    prelude::{GraphViewOps, PropertiesOps},
    serialise::{metadata::build_graph_metadata, parquet::decode_graph_metadata},
};
use raphtory_api::core::{
    entities::properties::prop::Prop,
    storage::graph_folder::{GraphMetadata, GraphPaths},
};
use std::{cmp::Ordering, sync::Arc};
use tokio::sync::OnceCell;

/// Lightweight summary of a stored graph — its name, path, counts, and
/// filesystem timestamps — served without deserializing the full graph.
/// Useful for listing what's available on the server before committing to a
/// full load.
#[derive(ResolvedObject, Clone)]
pub struct MetaGraph {
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
    pub(crate) fn new(path: ExistingGraphFolder) -> Self {
        Self {
            folder: path,
            meta: Default::default(),
        }
    }

    pub fn local_path(&self) -> &str {
        self.folder.local_path()
    }

    async fn meta(&self, data: &Data) -> Result<&GraphMetadata> {
        Ok(self
            .meta
            .get_or_try_init(|| async {
                match data.get_cached_graph(self.folder.local_path()).await {
                    None => self.folder.read_metadata_async().await,
                    Some(graph) => Ok(build_graph_metadata(graph)),
                }
            })
            .await?)
    }

    /// Whether the caller has unfiltered read; gates the summary fields below, which are read from
    /// stored metadata without the access filter applied.
    fn caller_has_full_read(&self, ctx: &Context<'_>, data: &Data) -> Result<bool> {
        match data.auth_policy.as_ref() {
            None => Ok(true),
            Some(p) => Ok(p.full_read(ctx, self.folder.local_path())?),
        }
    }

    /// Key/value metadata pairs, read the cheap way: from the in-memory cache if
    /// the graph is already loaded, otherwise directly from disk (parquet metadata
    /// for parquet-backed graphs, the `graph_props` segment for disk-backed ones).
    /// This keeps `MetaGraph.metadata` cheap for namespace listings of many graphs,
    /// and lets those listings filter and sort by metadata without materialising
    /// the whole collection for the client.
    ///
    /// `None` when the caller lacks unfiltered read, so a filter or sort can no
    /// more observe a value than the `metadata` field can return it.
    pub(crate) async fn metadata_pairs(
        &self,
        ctx: &Context<'_>,
        data: &Data,
    ) -> Result<Option<Vec<(String, Prop)>>> {
        if !self.caller_has_full_read(ctx, data)? {
            return Ok(None);
        }

        if let Some(graph) = data.get_cached_graph(self.folder.local_path()).await {
            return Ok(Some(
                graph
                    .graph()
                    .metadata()
                    .iter()
                    .filter_map(|(key, value)| value.map(|prop| (key.to_string(), prop)))
                    .collect(),
            ));
        }

        if self.meta(data).await?.is_diskgraph {
            let graph_path = self
                .folder
                .graph_folder()
                .graph_path()
                .map_err(GraphError::from)?;
            let pairs = read_constant_graph_properties(&graph_path).map_err(GraphError::from)?;
            return Ok(Some(
                pairs
                    .into_iter()
                    .map(|(key, prop)| (key.to_string(), prop))
                    .collect(),
            ));
        }

        Ok(Some(
            decode_graph_metadata(self.folder.graph_folder())?
                .into_iter()
                .filter_map(|(key, value)| value.map(|prop| (key, prop)))
                .collect(),
        ))
    }

    /// Value of a single metadata key, or `None` when the graph doesn't carry it
    /// (or the caller may not read it).
    pub(crate) async fn metadata_value(
        &self,
        ctx: &Context<'_>,
        data: &Data,
        key: &str,
    ) -> Result<Option<Prop>> {
        Ok(self
            .metadata_pairs(ctx, data)
            .await?
            .unwrap_or_default()
            .into_iter()
            .find_map(|(k, prop)| (k == key).then_some(prop)))
    }

    pub(crate) async fn created_value(&self) -> Result<i64> {
        Ok(self.folder.created_async().await?)
    }

    pub(crate) async fn last_updated_value(&self) -> Result<i64> {
        Ok(self.folder.last_updated_async().await?)
    }

    pub(crate) async fn node_count_value(
        &self,
        ctx: &Context<'_>,
        data: &Data,
    ) -> Result<Option<usize>> {
        if !self.caller_has_full_read(ctx, data)? {
            return Ok(None);
        }
        Ok(Some(self.meta(data).await?.node_count))
    }

    pub(crate) async fn edge_count_value(
        &self,
        ctx: &Context<'_>,
        data: &Data,
    ) -> Result<Option<usize>> {
        if !self.caller_has_full_read(ctx, data)? {
            return Ok(None);
        }
        Ok(Some(self.meta(data).await?.edge_count))
    }

    pub(crate) fn name_value(&self) -> Option<String> {
        self.folder.get_graph_name().ok()
    }
}

#[ResolvedObjectFields]
/// Metagraphs are a GraphQL specific object that wraps  the normal graph functions. Metagraphs exist in memory and reduce the need to load full graphs from disk.
impl MetaGraph {
    /// Returns the graph name.
    pub async fn name(&self) -> Option<String> {
        self.folder.get_graph_name().ok()
    }

    /// Returns path of graph.
    pub async fn path(&self) -> String {
        self.folder.local_path().into()
    }

    /// Returns the timestamp for the creation of the graph.
    pub async fn created(&self) -> Result<i64> {
        Ok(self.folder.created_async().await?)
    }

    /// Returns the graph's last opened timestamp according to system time.
    pub async fn last_opened(&self) -> Result<i64> {
        Ok(self.folder.last_opened_async().await?)
    }

    /// Returns the graph's last updated timestamp.
    pub async fn last_updated(&self) -> Result<i64> {
        Ok(self.folder.last_updated_async().await?)
    }

    /// Returns the number of nodes in the graph, or null if the caller lacks unfiltered read.
    pub async fn node_count(&self, ctx: &Context<'_>) -> Result<Option<usize>> {
        let data: &Data = ctx.data_unchecked();
        if !self.caller_has_full_read(ctx, data)? {
            return Ok(None);
        }
        Ok(Some(self.meta(data).await?.node_count))
    }

    /// Returns the number of edges in the graph, or null if the caller lacks unfiltered read.
    ///
    /// Returns:
    ///     int:
    pub async fn edge_count(&self, ctx: &Context<'_>) -> Result<Option<usize>> {
        let data: &Data = ctx.data_unchecked();
        if !self.caller_has_full_read(ctx, data)? {
            return Ok(None);
        }
        Ok(Some(self.meta(data).await?.edge_count))
    }

    /// Returns the metadata of the graph, or null if the caller lacks unfiltered read.
    ///
    /// Reads metadata without forcing a full graph load: from the
    /// in-memory cache if the graph is already loaded, otherwise directly
    /// from disk (parquet metadata for parquet-backed graphs, the
    /// `graph_props` segment for disk-backed graphs). This keeps
    /// `MetaGraph.metadata` cheap for namespace listings of many graphs.
    pub async fn metadata(&self, ctx: &Context<'_>) -> Result<Option<Vec<GqlProperty>>> {
        let data: &Data = ctx.data_unchecked();
        Ok(self
            .metadata_pairs(ctx, data)
            .await?
            .map(|pairs| pairs.into_iter().map(GqlProperty::from).collect()))
    }
}
