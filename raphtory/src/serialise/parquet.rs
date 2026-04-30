use crate::{
    arrow_loader::df_loaders::edges::ColumnNames,
    db::{
        api::{state::ops::GraphView, storage::storage::Storage, view::MaterializedGraph},
        graph::views::deletion_graph::PersistentGraph,
    },
    errors::GraphError,
    io::parquet_loaders::{
        get_parquet_file_paths, load_edge_deletions_from_parquet, load_edge_metadata_from_parquet,
        load_edges_from_parquet, load_graph_props_from_parquet, load_node_metadata_from_parquet,
        load_nodes_from_parquet, process_parquet_file_to_df,
    },
    parquet_encoder::{
        encode_edge_cprop, encode_edge_deletions, encode_edge_tprop, encode_graph_cprop,
        encode_graph_tprop, encode_nodes_cprop, encode_nodes_tprop, RecordBatchSink, DST_COL_ID,
        DST_COL_VID, EDGE_COL_ID, LAYER_COL, LAYER_ID_COL, NODE_ID_COL, NODE_VID_COL,
        SECONDARY_INDEX_COL, SRC_COL_ID, SRC_COL_VID, TIME_COL, TYPE_COL, TYPE_ID_COL,
    },
    prelude::*,
    serialise::GraphPaths,
};
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use itertools::Itertools;
use parquet::{
    arrow::{arrow_reader::ArrowReaderMetadata, ArrowWriter},
    basic::Compression,
    file::{metadata::KeyValue, properties::WriterProperties},
};
use raphtory_api::{core::entities::properties::prop::prop_col::lift_property_col, GraphType};
use raphtory_storage::core_ops::CoreGraphOps;
use std::{
    fs::File,
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use storage::Config;
use walkdir::WalkDir;
use zip::{write::FileOptions, ZipArchive, ZipWriter};

pub trait ParquetEncoder {
    /// Encode the graph as parquet data to the zip writer
    /// (note the writer is still open for appending more data after calling this function)
    ///
    /// The graph data will be written at `prefix` inside the zip.
    fn encode_parquet_to_zip<W: Write + Seek, P: AsRef<Path>>(
        &self,
        mut zip_writer: &mut ZipWriter<W>,
        prefix: P,
    ) -> Result<(), GraphError> {
        let prefix = prefix.as_ref();
        // Encode to a tmp dir using parquet, then zip it to the writer
        let temp_dir = tempfile::tempdir()?;
        self.encode_parquet(&temp_dir)?;

        // Walk through the directory and add files and directories to the zip.
        // Files and directories are stored in the archive under the GRAPH_PATH directory.
        for entry in WalkDir::new(temp_dir.path())
            .into_iter()
            .filter_map(Result::ok)
        {
            let path = entry.path();

            let relative_path = path.strip_prefix(temp_dir.path()).map_err(|e| {
                GraphError::IOErrorMsg(format!("Failed to strip prefix from path: {}", e))
            })?;

            // Attach GRAPH_PATH as a prefix to the relative path
            let zip_entry_name = prefix.join(relative_path).to_string_lossy().into_owned();

            if path.is_file() {
                zip_writer.start_file::<_, ()>(zip_entry_name, FileOptions::<()>::default())?;

                let mut file = std::fs::File::open(path)?;
                std::io::copy(&mut file, &mut zip_writer)?;
            } else if path.is_dir() {
                // Add empty directories to the zip
                zip_writer.add_directory::<_, ()>(zip_entry_name, FileOptions::<()>::default())?;
            }
        }
        Ok(())
    }

    fn encode_parquet(&self, path: impl AsRef<Path>) -> Result<(), GraphError>;
}

pub trait ParquetDecoder: Sized {
    fn decode_parquet_from_bytes<P: AsRef<Path>>(
        bytes: &[u8],
        path_for_decoded_graph: Option<&Path>,
        prefix: P,
        config: Config,
    ) -> Result<Self, GraphError> {
        // Read directly from an in-memory cursor
        let mut reader = ZipArchive::new(std::io::Cursor::new(bytes))?;
        Self::decode_parquet_from_zip(&mut reader, path_for_decoded_graph, prefix, config)
    }

    fn decode_parquet_from_zip<R: Read + Seek, P: AsRef<Path>>(
        zip: &mut ZipArchive<R>,
        path_for_decoded_graph: Option<&Path>,
        prefix: P,
        config: Config,
    ) -> Result<Self, GraphError> {
        let prefix = prefix.as_ref();
        // Unzip to a temp dir and decode parquet from there
        let temp_dir = tempfile::tempdir()?;

        for i in 0..zip.len() {
            let mut file = zip.by_index(i)?;
            let zip_entry_name = match file.enclosed_name() {
                Some(name) => name,
                None => continue,
            };

            if let Ok(relative_path) = zip_entry_name.strip_prefix(prefix) {
                let out_path = temp_dir.path().join(relative_path);
                if file.is_dir() {
                    std::fs::create_dir_all(&out_path)?;
                } else {
                    // Create any parent directories
                    if let Some(parent) = out_path.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    let mut out_file = File::create(&out_path)?;
                    std::io::copy(&mut file, &mut out_file)?;
                }
            }
        }
        Self::decode_parquet(temp_dir.path(), path_for_decoded_graph, config)
    }

    fn decode_parquet(
        path: impl AsRef<Path>,
        path_for_decoded_graph: Option<&Path>,
        config: Config,
    ) -> Result<Self, GraphError>;
}

impl ParquetEncoder for Graph {
    fn encode_parquet(&self, path: impl AsRef<Path>) -> Result<(), GraphError> {
        let gs = self.core_graph().lock();
        encode_graph_storage_to_parquet(&gs, path, GraphType::EventGraph)
    }
}

impl ParquetEncoder for PersistentGraph {
    fn encode_parquet(&self, path: impl AsRef<Path>) -> Result<(), GraphError> {
        let gs = self.core_graph().lock();
        encode_graph_storage_to_parquet(&gs, path, GraphType::PersistentGraph)
    }
}

impl ParquetEncoder for MaterializedGraph {
    fn encode_parquet(&self, path: impl AsRef<Path>) -> Result<(), GraphError> {
        match self {
            MaterializedGraph::EventGraph(graph) => graph.encode_parquet(path),
            MaterializedGraph::PersistentGraph(persistent_graph) => {
                persistent_graph.encode_parquet(path)
            }
        }
    }
}

impl ParquetDecoder for Graph {
    fn decode_parquet(
        path: impl AsRef<Path>,
        path_for_decoded_graph: Option<&Path>,
        config: Config,
    ) -> Result<Self, GraphError> {
        let batch_size = None;
        let storage = decode_graph_storage(&path, batch_size, path_for_decoded_graph, config)?;
        Ok(Graph::from_storage(storage))
    }
}

impl ParquetDecoder for PersistentGraph {
    fn decode_parquet(
        path: impl AsRef<Path>,
        path_for_decoded_graph: Option<&Path>,
        config: Config,
    ) -> Result<Self, GraphError> {
        let batch_size = None;
        let storage = decode_graph_storage(&path, batch_size, path_for_decoded_graph, config)?;
        Ok(PersistentGraph(storage))
    }
}

impl ParquetDecoder for MaterializedGraph {
    fn decode_parquet(
        path: impl AsRef<Path>,
        path_for_decoded_graph: Option<&Path>,
        config: Config,
    ) -> Result<Self, GraphError> {
        let batch_size = None;
        let graph_type = decode_graph_type(&path)?;
        let storage = decode_graph_storage(&path, batch_size, path_for_decoded_graph, config)?;

        match graph_type {
            GraphType::EventGraph => {
                Ok(MaterializedGraph::EventGraph(Graph::from_storage(storage)))
            }
            GraphType::PersistentGraph => {
                Ok(MaterializedGraph::PersistentGraph(PersistentGraph(storage)))
            }
        }
    }
}

const EDGES_T_PATH: &str = "edges_t";
const EDGES_D_PATH: &str = "edges_d"; // deletions
const EDGES_C_PATH: &str = "edges_c";
const NODES_T_PATH: &str = "nodes_t";
const NODES_C_PATH: &str = "nodes_c";
const GRAPH_T_PATH: &str = "graph_t";
const GRAPH_C_PATH: &str = "graph_c";
const GRAPH_TYPE: &str = "graph_type";
const EVENT_GRAPH_TYPE: &str = "rap_event_graph";
const PERSISTENT_GRAPH_TYPE: &str = "rap_persistent_graph";

impl<W> RecordBatchSink for ArrowWriter<W>
where
    W: Write + Seek + Send,
{
    fn send_batch(&mut self, batch: RecordBatch) -> Result<(), GraphError> {
        ArrowWriter::write(self, &batch)?;
        ArrowWriter::flush(self)?;
        Ok(())
    }

    fn finish(self) -> Result<(), GraphError> {
        self.close()?;
        Ok(())
    }
}

fn create_arrow_writer_sink(
    root_dir: &Path,
    schema: SchemaRef,
    file_id: usize,
    filename_num_digits: usize,
    key_value_metadata: Option<Vec<KeyValue>>,
) -> Result<ArrowWriter<File>, GraphError> {
    std::fs::create_dir_all(&root_dir)?;

    let writer_properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_key_value_metadata(key_value_metadata)
        .build();

    let filename = format!("{file_id:0filename_num_digits$}.parquet");
    let node_file = File::create(root_dir.join(filename))?;
    Ok(ArrowWriter::try_new(
        node_file,
        schema.clone(),
        Some(writer_properties),
    )?)
}

fn encode_graph_storage_to_parquet<G: GraphView>(
    g: &G,
    path: impl AsRef<Path>,
    graph_type: GraphType,
) -> Result<(), GraphError> {
    let base_dir = path.as_ref();
    let edge_t_file_id = AtomicUsize::new(0);
    let edge_c_file_id = AtomicUsize::new(0);
    let edge_d_file_id = AtomicUsize::new(0);

    encode_edge_tprop(g, |schema, _chunk, num_digits| {
        create_arrow_writer_sink(
            &base_dir.join(EDGES_T_PATH),
            schema.clone(),
            edge_t_file_id.fetch_add(1, Ordering::Relaxed),
            num_digits,
            None,
        )
    })?;
    encode_edge_cprop(g, |schema, _chunk, num_digits| {
        create_arrow_writer_sink(
            &base_dir.join(EDGES_C_PATH),
            schema.clone(),
            edge_c_file_id.fetch_add(1, Ordering::Relaxed),
            num_digits,
            None,
        )
    })?;
    encode_edge_deletions(g, |schema, _chunk, num_digits| {
        create_arrow_writer_sink(
            &base_dir.join(EDGES_D_PATH),
            schema.clone(),
            edge_d_file_id.fetch_add(1, Ordering::Relaxed),
            num_digits,
            None,
        )
    })?;
    encode_nodes_tprop(g, |schema, chunk, num_digits| {
        create_arrow_writer_sink(
            &base_dir.join(NODES_T_PATH),
            schema.clone(),
            chunk,
            num_digits,
            None,
        )
    })?;
    encode_nodes_cprop(g, |schema, chunk, num_digits| {
        create_arrow_writer_sink(
            &base_dir.join(NODES_C_PATH),
            schema.clone(),
            chunk,
            num_digits,
            None,
        )
    })?;
    encode_graph_tprop(g, |schema, chunk, num_digits| {
        create_arrow_writer_sink(
            &base_dir.join(GRAPH_T_PATH),
            schema.clone(),
            chunk,
            num_digits,
            None,
        )
    })?;
    encode_graph_cprop(g, |schema, chunk, num_digits| {
        let graph_type_str = match graph_type {
            GraphType::EventGraph => EVENT_GRAPH_TYPE,
            GraphType::PersistentGraph => PERSISTENT_GRAPH_TYPE,
        };
        let key_value_metadata = vec![KeyValue::new(
            GRAPH_TYPE.to_string(),
            Some(graph_type_str.to_string()),
        )];

        create_arrow_writer_sink(
            &base_dir.join(GRAPH_C_PATH),
            schema.clone(),
            chunk,
            num_digits,
            Some(key_value_metadata),
        )
    })?;
    Ok(())
}

fn decode_graph_storage(
    path: impl AsRef<Path>,
    batch_size: Option<usize>,
    path_for_decoded_graph: Option<&Path>,
    config: Config,
) -> Result<Arc<Storage>, GraphError> {
    let graph = if let Some(storage_path) = path_for_decoded_graph {
        Arc::new(Storage::new_at_path_with_config(storage_path, config)?)
    } else {
        Arc::new(Storage::new_with_config(config)?)
    };

    let c_graph_path = path.as_ref().join(GRAPH_C_PATH);

    {
        let exclude = vec![TIME_COL];
        let (c_props, _) = collect_prop_columns(&c_graph_path, &exclude)?;
        let c_props = c_props.iter().map(|s| s.as_str()).collect::<Vec<_>>();

        load_graph_props_from_parquet(
            &graph,
            &c_graph_path,
            TIME_COL,
            None,
            &[],
            &c_props,
            batch_size,
            None,
        )?;
    }

    let t_graph_path = path.as_ref().join(GRAPH_T_PATH);

    if std::fs::exists(&t_graph_path)? {
        let exclude = vec![TIME_COL, SECONDARY_INDEX_COL];
        let (t_props, _) = collect_prop_columns(&t_graph_path, &exclude)?;
        let t_props = t_props.iter().map(|s| s.as_str()).collect::<Vec<_>>();

        load_graph_props_from_parquet(
            &graph,
            &t_graph_path,
            TIME_COL,
            Some(SECONDARY_INDEX_COL),
            &t_props,
            &[],
            batch_size,
            None,
        )?;
    }

    let c_node_path = path.as_ref().join(NODES_C_PATH);

    if std::fs::exists(&c_node_path)? {
        let exclude = vec![NODE_ID_COL, NODE_VID_COL, TYPE_COL, TYPE_ID_COL];
        let (c_prop_columns, _) = collect_prop_columns(&c_node_path, &exclude)?;
        let c_prop_columns = c_prop_columns
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>();

        load_node_metadata_from_parquet(
            &graph,
            &c_node_path,
            NODE_ID_COL,
            None,
            Some(TYPE_COL),
            Some(NODE_VID_COL),
            Some(TYPE_ID_COL),
            &c_prop_columns,
            None,
            None,
            None,
            batch_size,
            None,
        )?;
    }

    let t_node_path = path.as_ref().join(NODES_T_PATH);

    if std::fs::exists(&t_node_path)? {
        let exclude = vec![
            NODE_ID_COL,
            NODE_VID_COL,
            TYPE_COL,
            TIME_COL,
            SECONDARY_INDEX_COL,
            LAYER_COL,
        ];
        let (t_prop_columns, _) = collect_prop_columns(&t_node_path, &exclude)?;
        let t_prop_columns = t_prop_columns
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>();

        load_nodes_from_parquet(
            &graph,
            &t_node_path,
            TIME_COL,
            Some(SECONDARY_INDEX_COL),
            NODE_VID_COL,
            None,
            None,
            &t_prop_columns,
            &[],
            None,
            None,
            Some(LAYER_COL),
            batch_size,
            false,
            None,
        )?;
    }

    let t_edge_path = path.as_ref().join(EDGES_T_PATH);

    if std::fs::exists(&t_edge_path)? {
        let exclude = vec![
            TIME_COL,
            SECONDARY_INDEX_COL,
            SRC_COL_VID,
            SRC_COL_ID,
            DST_COL_VID,
            DST_COL_ID,
            LAYER_COL,
            LAYER_ID_COL,
            EDGE_COL_ID,
        ];
        let (t_prop_columns, _) = collect_prop_columns(&t_edge_path, &exclude)?;
        let t_prop_columns = t_prop_columns
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>();

        load_edges_from_parquet(
            &graph,
            &t_edge_path,
            ColumnNames::new(
                TIME_COL,
                Some(SECONDARY_INDEX_COL),
                SRC_COL_ID,
                DST_COL_ID,
                Some(LAYER_COL),
            ),
            true,
            &t_prop_columns,
            &[],
            None,
            None,
            batch_size,
            None,
        )?;
    }

    let d_edge_path = path.as_ref().join(EDGES_D_PATH);

    if std::fs::exists(&d_edge_path)? {
        load_edge_deletions_from_parquet(
            graph.core_graph(),
            &d_edge_path,
            ColumnNames::new(
                TIME_COL,
                Some(SECONDARY_INDEX_COL),
                SRC_COL_ID,
                DST_COL_ID,
                Some(LAYER_COL),
            ),
            None,
            true,
            batch_size,
            None,
        )?;
    }

    let c_edge_path = path.as_ref().join(EDGES_C_PATH);

    if std::fs::exists(&c_edge_path)? {
        let exclude = vec![
            SRC_COL_VID,
            SRC_COL_ID,
            DST_COL_VID,
            DST_COL_ID,
            LAYER_COL,
            EDGE_COL_ID,
        ];
        let (c_prop_columns, _) = collect_prop_columns(&c_edge_path, &exclude)?;
        let metadata = c_prop_columns
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>();

        load_edge_metadata_from_parquet(
            &graph,
            &c_edge_path,
            SRC_COL_ID,
            DST_COL_ID,
            &metadata,
            None,
            None,
            Some(LAYER_COL),
            batch_size,
            None,
            true,
        )?;
    }
    Ok(graph)
}

pub fn decode_graph_metadata(
    path: &impl GraphPaths,
) -> Result<Vec<(String, Option<Prop>)>, GraphError> {
    let c_graph_path = path.graph_path()?.join(GRAPH_C_PATH);
    let exclude = vec![TIME_COL];
    let (c_props, _) = collect_prop_columns(&c_graph_path, &exclude)?;
    let c_props = c_props.iter().map(|s| s.as_str()).collect::<Vec<_>>();
    let mut result: Vec<(String, Option<Prop>)> =
        c_props.iter().map(|s| (s.to_string(), None)).collect();

    for path in get_parquet_file_paths(&c_graph_path)? {
        let df_view = process_parquet_file_to_df(path.as_path(), Some(&c_props), None, None)?;
        for chunk in df_view.chunks {
            let chunk = chunk?;
            for (col, res) in chunk.chunk.into_iter().zip(&mut result) {
                if let Some(value) = lift_property_col(&col).get(0) {
                    res.1 = Some(value);
                }
            }
        }
    }
    Ok(result)
}

fn decode_graph_type(path: impl AsRef<Path>) -> Result<GraphType, GraphError> {
    let c_graph_path = path.as_ref().join(GRAPH_C_PATH);

    // Assume event graph as default
    if !std::fs::exists(&c_graph_path)? {
        return Ok(GraphType::EventGraph);
    }

    let exclude = vec![TIME_COL];
    let (_, g_type) = collect_prop_columns(&c_graph_path, &exclude)?;

    g_type.ok_or_else(|| GraphError::LoadFailure("Graph type not found".to_string()))
}

fn collect_prop_columns(
    path: &Path,
    exclude: &[&str],
) -> Result<(Vec<String>, Option<GraphType>), GraphError> {
    let prop_columns_fn =
        |path: &Path, exclude: &[&str]| -> Result<(Vec<String>, Option<GraphType>), GraphError> {
            let reader = ArrowReaderMetadata::load(&File::open(path)?, Default::default())?;
            let cols = reader
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .filter(|f_name| !exclude.iter().any(|ex| ex == f_name))
                .collect_vec();
            let graph_type = reader
                .metadata()
                .file_metadata()
                .key_value_metadata()
                .and_then(|meta| {
                    meta.iter()
                        .find(|kv| kv.key == GRAPH_TYPE)
                        .and_then(|kv| kv.value.as_ref())
                        .and_then(|v| match v.as_ref() {
                            EVENT_GRAPH_TYPE => Some(GraphType::EventGraph),
                            PERSISTENT_GRAPH_TYPE => Some(GraphType::PersistentGraph),
                            _ => None,
                        })
                });
            Ok((cols, graph_type))
        };

    let mut prop_columns = vec![];
    let mut g_type: Option<GraphType> = None;

    // Collect columns from just the first file
    if let Some(path) = ls_parquet_files(path)?.next() {
        let (columns, tpe) = prop_columns_fn(&path, exclude)?;

        if g_type.is_none() {
            g_type = tpe;
        }

        prop_columns.extend_from_slice(&columns);
    }

    Ok((prop_columns, g_type))
}

fn ls_parquet_files(dir: &Path) -> Result<impl Iterator<Item = PathBuf>, GraphError> {
    Ok(std::fs::read_dir(dir)
        .inspect_err(|err| {
            eprintln!("Error reading directory {}: {}", dir.display(), err);
        })? // print out the path if it's missing
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_file() && path.extension().is_some_and(|ext| ext == "parquet")))
}
