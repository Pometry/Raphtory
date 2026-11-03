use crate::{
    data::{Data, ParquetPathError},
    model::parse_json_schema,
    rayon::blocking_write,
    GQLError,
};
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, Result};
use raphtory::{
    arrow_loader::df_loaders::edges::ColumnNames,
    db::{
        api::view::{internal::InternalStorageOps, MaterializedGraph},
        graph::{graph::Graph, views::deletion_graph::PersistentGraph},
    },
    errors::GraphError,
    io::parquet_loaders::{load_edges_from_parquet, load_nodes_from_parquet},
};
use raphtory_storage::stage::Stage;
use parking_lot::Mutex;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum GqlStageError {
    #[error("Stage already finished")]
    AlreadyFinished,

    #[error("Staging requires a disk-backed graph")]
    NotDiskBacked,

    #[error("Skipped because an earlier load in this stage failed")]
    SkippedAfterPriorFailure,

    #[error("Stage discarded because a load failed")]
    DiscardedAfterLoadFailure,

    #[error(transparent)]
    ParquetPath(#[from] ParquetPathError),

    #[error(transparent)]
    Graph(#[from] GraphError),

    #[error(transparent)]
    Cache(#[from] GQLError),
}

#[derive(ResolvedObject, Clone)]
#[graphql(name = "StagedGraph")]
pub struct GqlStagedGraph {
    graph_path: String,
    graph: Arc<Mutex<Option<StagedGraph>>>,
    load_failed: Arc<AtomicBool>,
    set_active_on_finish: bool,
}

#[ResolvedObjectFields]
impl GqlStagedGraph {
    pub async fn version(&self) -> Result<u32> {
        let guard = self.graph.lock();
        let graph = guard.as_ref().ok_or(GqlStageError::AlreadyFinished)?;

        Ok(graph.version().map_err(GqlStageError::Graph)?)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn load_nodes(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Path to the parquet directory.")] data_path: String,
        #[graphql(desc = "The column name for the timestamps.")] time: String,
        #[graphql(desc = "The column name for the node IDs.")] id: String,
        #[graphql(
            desc = "A value to use as the node type for all nodes. Cannot be used in combination with node_type_col."
        )]
        node_type: Option<String>,
        #[graphql(
            desc = "The node type column name in a dataframe. Cannot be used in combination with node_type."
        )]
        node_type_col: Option<String>,
        #[graphql(desc = "List of node property column names.")] properties: Option<Vec<String>>,
        #[graphql(desc = "List of node metadata column names.")] metadata: Option<Vec<String>>,
        #[graphql(
            desc = "A JSON-formatted dict of {'column_name': column_type} to cast columns to. Defaults to None."
        )]
        schema: Option<String>,
        #[graphql(desc = "The column name for the secondary index.")] event_id: Option<String>,
        #[graphql(
            desc = "A value to use as the layer for all nodes. Cannot be used in combination with layer_col."
        )]
        layer: Option<String>,
        #[graphql(
            desc = "The node layer column name in a dataframe. Cannot be used in combination with layer."
        )]
        layer_col: Option<String>,
    ) -> Result<bool> {
        if self.load_failed.load(Ordering::Relaxed) {
            return Err(GqlStageError::SkippedAfterPriorFailure)?;
        }

        let load_result = async {
            let data = ctx.data_unchecked::<Data>();

            // NOTE: skipping shared metadata for now until we figure out parsing of types
            let properties_owned = properties.unwrap_or_default();
            let metadata_owned = metadata.unwrap_or_default();
            let schema = parse_json_schema(schema.as_deref())?;
            let arced_schema = schema.map(Arc::new);
            let data_path = PathBuf::from(data_path);

            data.is_parquet_path_allowed(&data_path).await?;
            let gql_stage = self.clone();

            blocking_write(move || {
                let property_strs: Vec<&str> =
                    properties_owned.iter().map(String::as_str).collect();
                let metadata_strs: Vec<&str> = metadata_owned.iter().map(String::as_str).collect();

                let guard = gql_stage.graph.lock();
                let graph = guard.as_ref().ok_or(GqlStageError::AlreadyFinished)?;

                load_nodes_from_parquet(
                    &graph.materialized_graph(),
                    &data_path,
                    &time,
                    event_id.as_deref(),
                    &id,
                    node_type.as_deref(),
                    node_type_col.as_deref(),
                    &property_strs,
                    &metadata_strs,
                    None,
                    layer.as_deref(),
                    layer_col.as_deref(),
                    None,
                    None,
                    true,
                    arced_schema,
                )
                .map_err(GqlStageError::Graph)
            })
            .await?;

            Ok::<(), GqlStageError>(())
        }
        .await;

        if load_result.is_err() {
            self.load_failed.store(true, Ordering::Relaxed);
        }

        load_result?;

        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn load_edges(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Path to the parquet directory.")] data_path: String,
        #[graphql(desc = "The column name for the update timestamps.")] time: String,
        #[graphql(desc = "The column name for the source node IDs.")] src: String,
        #[graphql(desc = "The column name for the destination node IDs.")] dst: String,
        #[graphql(desc = "List of edge property column names. Defaults to None.")]
        properties: Option<Vec<String>>,
        #[graphql(desc = "List of edge metadata column names. Defaults to None.")] metadata: Option<
            Vec<String>,
        >,
        #[graphql(
            desc = "A JSON-formatted dict of {'column_name': column_type} to cast columns to. Defaults to None."
        )]
        schema: Option<String>,
        #[graphql(desc = "The column name for the secondary index.")] event_id: Option<String>,
        #[graphql(
            desc = "A value to use as the layer for all edges. Cannot be used in combination with layer_col. Defaults to None."
        )]
        layer: Option<String>,
        #[graphql(
            desc = "The edge layer column name in a dataframe. Cannot be used in combination with layer. Defaults to None."
        )]
        layer_col: Option<String>,
    ) -> Result<bool> {
        if self.load_failed.load(Ordering::Relaxed) {
            return Err(GqlStageError::SkippedAfterPriorFailure)?;
        }

        let load_result = async {
            let data = ctx.data_unchecked::<Data>();

            // NOTE: skipping shared metadata for now until we figure out parsing of types
            let properties_owned = properties.unwrap_or_default();
            let metadata_owned = metadata.unwrap_or_default();
            let schema = parse_json_schema(schema.as_deref())?;
            let arced_schema = schema.map(Arc::new);
            let data_path = PathBuf::from(data_path);

            data.is_parquet_path_allowed(&data_path).await?;
            let gql_stage = self.clone();

            blocking_write(move || {
                let property_strs: Vec<&str> =
                    properties_owned.iter().map(String::as_str).collect();
                let metadata_strs: Vec<&str> = metadata_owned.iter().map(String::as_str).collect();

                let guard = gql_stage.graph.lock();
                let graph = guard.as_ref().ok_or(GqlStageError::AlreadyFinished)?;

                load_edges_from_parquet(
                    &graph.materialized_graph(),
                    &data_path,
                    ColumnNames::new(
                        time.as_str(),
                        event_id.as_deref(),
                        src.as_str(),
                        dst.as_str(),
                        layer_col.as_deref(),
                    ),
                    true,
                    &property_strs,
                    &metadata_strs,
                    None,
                    layer.as_deref(),
                    None,
                    arced_schema,
                )
                .map_err(GqlStageError::Graph)
            })
            .await?;

            Ok::<(), GqlStageError>(())
        }
        .await;

        if load_result.is_err() {
            self.load_failed.store(true, Ordering::Relaxed);
        }

        load_result?;

        Ok(true)
    }

    /// Promote the staged snapshot and reload the graph in the server cache.
    /// If any load in this stage failed, the stage is discarded instead.
    pub async fn finish(&self, ctx: &Context<'_>) -> Result<bool> {
        let graph = self
            .graph
            .lock()
            .take()
            .ok_or(GqlStageError::AlreadyFinished)?;

        if self.load_failed.load(Ordering::Relaxed) {
            blocking_write(move || graph.discard())
                .await
                .map_err(GqlStageError::Graph)?;

            return Err(GqlStageError::DiscardedAfterLoadFailure)?;
        }

        let graph_path = self.graph_path.clone();
        let set_active = self.set_active_on_finish;
        let data = ctx.data_unchecked::<Data>().clone();

        let materialized_graph = blocking_write(move || graph.finish(set_active))
            .await
            .map_err(GqlStageError::Graph)?;

        if set_active {
            data.replace_graph_in_cache(&graph_path, materialized_graph)
                .await
                .map_err(GqlStageError::Cache)?;
        }

        Ok(true)
    }
}

impl GqlStagedGraph {
    pub(crate) async fn start(
        ctx: &Context<'_>,
        graph_path: String,
        set_active_on_finish: bool,
    ) -> Result<Self> {
        let data = ctx.data_unchecked::<Data>();
        let graph = data
            .get_graph_with_write_permission(ctx, &graph_path)
            .await?;

        if graph.graph().disk_storage_path().is_none() {
            return Err(GqlStageError::NotDiskBacked)?;
        }

        let graph = blocking_write(move || -> Result<_, GqlStageError> {
            let materialized = graph.graph().clone();
            let graph = match materialized {
                MaterializedGraph::EventGraph(g) => StagedGraph::Event(g.stage()?),
                MaterializedGraph::PersistentGraph(g) => StagedGraph::Persistent(g.stage()?),
            };

            Ok(Arc::new(Mutex::new(Some(graph))))
        })
        .await?;

        Ok(Self {
            graph,
            load_failed: Arc::new(AtomicBool::new(false)),
            graph_path,
            set_active_on_finish,
        })
    }
}

enum StagedGraph {
    Event(Stage<Graph>),
    Persistent(Stage<PersistentGraph>),
}

impl StagedGraph {
    fn version(&self) -> Result<u32, GraphError> {
        match self {
            StagedGraph::Event(stage) => stage.graph().version(),
            StagedGraph::Persistent(stage) => stage.graph().version(),
        }
    }

    fn finish(self, set_active: bool) -> Result<MaterializedGraph, GraphError> {
        match self {
            StagedGraph::Event(stage) => Ok(MaterializedGraph::EventGraph(stage.finish(set_active)?)),
            StagedGraph::Persistent(stage) => {
                Ok(MaterializedGraph::PersistentGraph(stage.finish(set_active)?))
            }
        }
    }

    fn discard(self) -> Result<(), GraphError> {
        match self {
            StagedGraph::Event(stage) => Ok(stage.discard()?),
            StagedGraph::Persistent(stage) => Ok(stage.discard()?),
        }
    }

    fn materialized_graph(&self) -> MaterializedGraph {
        match self {
            StagedGraph::Event(stage) => MaterializedGraph::EventGraph(stage.graph().clone()),
            StagedGraph::Persistent(stage) => {
                MaterializedGraph::PersistentGraph(stage.graph().clone())
            }
        }
    }
}
