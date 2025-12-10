use crate::{
    db::{
        api::{storage::storage::Storage, view::MaterializedGraph},
        graph::views::deletion_graph::PersistentGraph,
    },
    errors::GraphError,
    io::parquet_loaders::{
        load_edge_deletions_from_parquet, load_edge_props_from_parquet, load_edges_from_parquet,
        load_graph_props_from_parquet, load_node_props_from_parquet, load_nodes_from_parquet,
    },
    prelude::*,
    serialise::{
        graph_folder::GRAPH_PATH,
        parquet::{
            edges::encode_edge_deletions,
            graph::{encode_graph_cprop, encode_graph_tprop},
            model::get_id_type,
            nodes::{encode_nodes_cprop, encode_nodes_tprop},
        },
    },
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_json::{reader::Decoder, ReaderBuilder};
use edges::{encode_edge_cprop, encode_edge_tprop};
use itertools::Itertools;
use model::ParquetTEdge;
use parquet::{
    arrow::{arrow_reader::ArrowReaderMetadata, ArrowWriter},
    basic::Compression,
    file::properties::WriterProperties,
};
use raphtory_api::{
    core::entities::{
        properties::{meta::PropMapper, prop::arrow_dtype_from_prop_type},
        GidType,
    },
    GraphType,
};
use raphtory_storage::{core_ops::CoreGraphOps, graph::graph::GraphStorage};
use rayon::prelude::*;
use std::{
    fs::File,
    io::{Read, Seek, Write},
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};
use walkdir::WalkDir;
use zip::{write::FileOptions, ZipArchive, ZipWriter};

mod edges;
mod model;
mod nodes;

mod graph;

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
    ) -> Result<Self, GraphError> {
        // Read directly from an in-memory cursor
        let mut reader = ZipArchive::new(std::io::Cursor::new(bytes))?;
        Self::decode_parquet_from_zip(&mut reader, path_for_decoded_graph, prefix)
    }

    fn decode_parquet_from_zip<R: Read + Seek, P: AsRef<Path>>(
        zip: &mut ZipArchive<R>,
        path_for_decoded_graph: Option<&Path>,
        prefix: P,
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
                    let mut out_file = std::fs::File::create(&out_path)?;
                    std::io::copy(&mut file, &mut out_file)?;
                }
            }
        }
        Self::decode_parquet(temp_dir.path(), path_for_decoded_graph)
    }

    fn decode_parquet(
        path: impl AsRef<Path>,
        path_for_decoded_graph: Option<&Path>,
    ) -> Result<Self, GraphError>;
}

const NODE_ID_COL: &str = "rap_node_id";
const TYPE_COL: &str = "rap_node_type";
const TIME_COL: &str = "rap_time";
const SECONDARY_INDEX_COL: &str = "rap_secondary_index";
const SRC_COL: &str = "rap_src";
const DST_COL: &str = "rap_dst";
const LAYER_COL: &str = "rap_layer";
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

impl ParquetEncoder for Graph {
    fn encode_parquet(&self, path: impl AsRef<Path>) -> Result<(), GraphError> {
        let gs = self.core_graph().clone();
        encode_graph_storage(&gs, path, GraphType::EventGraph)
    }
}

impl ParquetEncoder for PersistentGraph {
    fn encode_parquet(&self, path: impl AsRef<Path>) -> Result<(), GraphError> {
        let gs = self.core_graph().clone();
        encode_graph_storage(&gs, path, GraphType::PersistentGraph)
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

fn encode_graph_storage(
    g: &GraphStorage,
    path: impl AsRef<Path>,
    graph_type: GraphType,
) -> Result<(), GraphError> {
    encode_edge_tprop(g, path.as_ref())?;
    encode_edge_cprop(g, path.as_ref())?;
    encode_edge_deletions(g, path.as_ref())?;
    encode_nodes_tprop(g, path.as_ref())?;
    encode_nodes_cprop(g, path.as_ref())?;
    encode_graph_tprop(g, path.as_ref())?;
    encode_graph_cprop(g, graph_type, path.as_ref())?;
    Ok(())
}

pub(crate) fn run_encode(
    g: &GraphStorage,
    meta: &PropMapper,
    size: usize,
    path: impl AsRef<Path>,
    suffix: &str,
    default_fields_fn: impl Fn(&DataType) -> Vec<Field>,
    encode_fn: impl Fn(
            Range<usize>,
            &GraphStorage,
            &mut Decoder,
            &mut ArrowWriter<File>,
        ) -> Result<(), GraphError>
        + Sync,
) -> Result<(), GraphError> {
    let schema = derive_schema(meta, g.id_type(), default_fields_fn)?;
    let root_dir = path.as_ref().join(suffix);
    std::fs::create_dir_all(&root_dir)?;

    if size > 0 {
        let chunk_size = (size / rayon::current_num_threads()).max(128);
        let iter = (0..size).into_par_iter().step_by(chunk_size);

        let num_digits = iter.len().to_string().len();

        iter.enumerate().try_for_each(|(chunk, first)| {
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();
            let items = first..(first + chunk_size).min(size);

            let node_file = File::create(root_dir.join(format!("{chunk:0num_digits$}.parquet")))?;
            let mut writer = ArrowWriter::try_new(node_file, schema.clone(), Some(props))?;

            let mut decoder = ReaderBuilder::new(schema.clone()).build_decoder()?;

            encode_fn(items, g, &mut decoder, &mut writer)?;

            writer.close()?;
            Ok::<_, GraphError>(())
        })?;
    }
    Ok(())
}

pub(crate) fn run_encode_indexed<Index, II: Iterator<Item = Index>>(
    g: &GraphStorage,
    meta: &PropMapper,
    items: impl ParallelIterator<Item = (usize, II)>,
    path: impl AsRef<Path>,
    suffix: &str,
    default_fields_fn: impl Fn(&DataType) -> Vec<Field>,
    encode_fn: impl Fn(II, &GraphStorage, &mut Decoder, &mut ArrowWriter<File>) -> Result<(), GraphError>
        + Sync,
) -> Result<(), GraphError> {
    let schema = derive_schema(meta, g.id_type(), default_fields_fn)?;
    let root_dir = path.as_ref().join(suffix);
    std::fs::create_dir_all(&root_dir)?;

    let num_digits = 8;

    items.try_for_each(|(chunk, items)| {
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let node_file = File::create(root_dir.join(format!("{chunk:0num_digits$}.parquet")))?;
        let mut writer = ArrowWriter::try_new(node_file, schema.clone(), Some(props))?;

        let mut decoder = ReaderBuilder::new(schema.clone()).build_decoder()?;

        encode_fn(items, g, &mut decoder, &mut writer)?;

        writer.close()?;
        Ok::<_, GraphError>(())
    })?;

    Ok(())
}

pub(crate) fn derive_schema(
    prop_meta: &PropMapper,
    id_type: Option<GidType>,
    default_fields_fn: impl Fn(&DataType) -> Vec<Field>,
) -> Result<SchemaRef, GraphError> {
    let fields = arrow_fields(prop_meta);
    let id_type = get_id_type(id_type);

    let make_schema = |id_type: DataType, prop_columns: Vec<Field>| {
        let default_fields = default_fields_fn(&id_type);

        Schema::new(
            default_fields
                .into_iter()
                .chain(prop_columns)
                .collect::<Vec<_>>(),
        )
        .into()
    };

    let schema = if let Ok(id_type) = id_type {
        make_schema(id_type, fields)
    } else {
        make_schema(DataType::UInt64, fields)
    };

    Ok(schema)
}

fn arrow_fields(meta: &PropMapper) -> Vec<Field> {
    meta.keys()
        .iter()
        .zip(meta.ids())
        .filter_map(|(name, prop_id)| {
            meta.get_dtype(prop_id)
                .map(move |prop_type| (name, prop_type))
        })
        .map(|(name, prop_type)| {
            let d_type = arrow_dtype_from_prop_type(&prop_type);
            Field::new(name, d_type, true)
        })
        .collect()
}

fn ls_parquet_files(dir: &Path) -> Result<impl Iterator<Item = PathBuf>, GraphError> {
    Ok(std::fs::read_dir(dir)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_file() && path.extension().is_some_and(|ext| ext == "parquet")))
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

fn decode_graph_storage(
    path: impl AsRef<Path>,
    batch_size: Option<usize>,
    path_for_decoded_graph: Option<&Path>,
) -> Result<Arc<Storage>, GraphError> {
    let graph = if let Some(storage_path) = path_for_decoded_graph {
        Arc::new(Storage::new_at_path(storage_path))
    } else {
        Arc::new(Storage::default())
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
        )?;
    }

    let t_node_path = path.as_ref().join(NODES_T_PATH);

    if std::fs::exists(&t_node_path)? {
        let exclude = vec![NODE_ID_COL, TIME_COL, SECONDARY_INDEX_COL, TYPE_COL];
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
            NODE_ID_COL,
            None,
            Some(TYPE_COL),
            &t_prop_columns,
            &[],
            None,
            batch_size,
        )?;
    }

    let c_node_path = path.as_ref().join(NODES_C_PATH);

    if std::fs::exists(&c_node_path)? {
        let exclude = vec![NODE_ID_COL, TYPE_COL];
        let (c_prop_columns, _) = collect_prop_columns(&c_node_path, &exclude)?;
        let c_prop_columns = c_prop_columns
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>();

        load_node_props_from_parquet(
            &graph,
            &c_node_path,
            NODE_ID_COL,
            None,
            Some(TYPE_COL),
            &c_prop_columns,
            None,
            batch_size,
        )?;
    }

    let t_edge_path = path.as_ref().join(EDGES_T_PATH);

    if std::fs::exists(&t_edge_path)? {
        let exclude = vec![TIME_COL, SECONDARY_INDEX_COL, SRC_COL, DST_COL, LAYER_COL];
        let (t_prop_columns, _) = collect_prop_columns(&t_edge_path, &exclude)?;
        let t_prop_columns = t_prop_columns
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>();

        load_edges_from_parquet(
            &graph,
            &t_edge_path,
            TIME_COL,
            Some(SECONDARY_INDEX_COL),
            SRC_COL,
            DST_COL,
            &t_prop_columns,
            &[],
            None,
            None,
            Some(LAYER_COL),
            batch_size,
        )?;
    }

    let d_edge_path = path.as_ref().join(EDGES_D_PATH);

    if std::fs::exists(&d_edge_path)? {
        load_edge_deletions_from_parquet(
            graph.core_graph(),
            &d_edge_path,
            TIME_COL,
            Some(SECONDARY_INDEX_COL),
            SRC_COL,
            DST_COL,
            None,
            Some(LAYER_COL),
            batch_size,
        )?;
    }

    let c_edge_path = path.as_ref().join(EDGES_C_PATH);

    if std::fs::exists(&c_edge_path)? {
        let exclude = vec![SRC_COL, DST_COL, LAYER_COL];
        let (c_prop_columns, _) = collect_prop_columns(&c_edge_path, &exclude)?;
        let metadata = c_prop_columns
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>();

        load_edge_props_from_parquet(
            &graph,
            &c_edge_path,
            SRC_COL,
            DST_COL,
            &metadata,
            None,
            None,
            Some(LAYER_COL),
            batch_size,
        )?;
    }

    Ok(graph)
}

impl ParquetDecoder for Graph {
    fn decode_parquet(
        path: impl AsRef<Path>,
        path_for_decoded_graph: Option<&Path>,
    ) -> Result<Self, GraphError> {
        let batch_size = None;
        let storage = decode_graph_storage(&path, batch_size, path_for_decoded_graph)?;
        Ok(Graph::from_storage(storage))
    }
}

impl ParquetDecoder for PersistentGraph {
    fn decode_parquet(
        path: impl AsRef<Path>,
        path_for_decoded_graph: Option<&Path>,
    ) -> Result<Self, GraphError> {
        let batch_size = None;
        let storage = decode_graph_storage(&path, batch_size, path_for_decoded_graph)?;
        Ok(PersistentGraph(storage))
    }
}

impl ParquetDecoder for MaterializedGraph {
    fn decode_parquet(
        path: impl AsRef<Path>,
        path_for_decoded_graph: Option<&Path>,
    ) -> Result<Self, GraphError> {
        let batch_size = None;
        let graph_type = decode_graph_type(&path)?;
        let storage = decode_graph_storage(&path, batch_size, path_for_decoded_graph)?;

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
