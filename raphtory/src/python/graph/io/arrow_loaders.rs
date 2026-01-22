use crate::{
    db::api::view::StaticGraphViewOps,
    errors::GraphError,
    io::{
        arrow::{
            dataframe::{DFChunk, DFView},
            df_loaders::{
                edges::{load_edges_from_df, ColumnNames},
                load_edge_deletions_from_df, load_edges_props_from_df,
                nodes::{load_node_props_from_df, load_nodes_from_df},
            },
        },
        parquet_loaders::cast_columns,
    },
    prelude::{AdditionOps, PropertyAdditionOps},
    test_utils::prop,
};
use arrow::{
    array::{RecordBatch, RecordBatchReader},
    datatypes::SchemaRef,
};
use arrow_csv::{reader::Format, ReaderBuilder};
use bzip2::read::BzDecoder;
use flate2::read::GzDecoder;
use itertools::chain;
use pyo3::{
    exceptions::PyValueError,
    ffi::c_str,
    prelude::*,
    pybacked::PyBackedStr,
    types::{PyCapsule, PyDict},
};
use pyo3_arrow::PyRecordBatchReader;
use raphtory_api::core::entities::properties::prop::{Prop, PropType};
use std::{
    cmp::min,
    collections::HashMap,
    fs,
    fs::File,
    iter,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing::error;

const CHUNK_SIZE: usize = 1_000_000; // split large chunks so progress bar updates reasonably

pub(crate) fn convert_py_prop_args(properties: Option<&[PyBackedStr]>) -> Option<Vec<&str>> {
    properties.map(|p| p.iter().map(|p| p.deref()).collect())
}

pub(crate) fn convert_py_schema(
    schema: Option<Bound<PyAny>>,
) -> Result<Option<HashMap<String, PropType>>, GraphError> {
    schema.map(|s| {
        if let Ok(list) = s.extract::<Vec<(String, PropType)>>() {
            Ok(list.into_iter().collect::<HashMap<String, PropType>>())
        } else if let Ok(map) = s.extract::<HashMap<String, PropType>>() {
            Ok(map)
        } else {
            Err(GraphError::from(PyValueError::new_err("Argument 'schema' must either be a list of (column_name, column_type) tuples or a dict mapping {'column_name' : column_type}")))
        }
    }).transpose()
}

pub(crate) fn load_nodes_from_arrow_c_stream<
    'py,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    graph: &G,
    data: &Bound<'py, PyAny>,
    time: &str,
    event_id: Option<&str>,
    id: &str,
    node_type: Option<&str>,
    node_type_col: Option<&str>,
    properties: &[&str],
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    schema: Option<HashMap<String, PropType>>,
) -> Result<(), GraphError> {
    let cols_to_check = [id, time]
        .into_iter()
        .chain(properties.iter())
        .chain(metadata)
        .chain(node_type_col)
        .chain(event_id)
        .collect::<Vec<_>>();

    let df_view = process_arrow_c_stream_df(data, &cols_to_check, schema)?;
    df_view.check_cols_exist(&cols_to_check)?;
    load_nodes_from_df(
        df_view,
        time,
        id,
        event_id,
        properties,
        metadata,
        shared_metadata,
        node_type,
        node_type_col,
        graph,
        true,
    )
}

pub(crate) fn load_edges_from_arrow_c_stream<
    'py,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    graph: &G,
    data: &Bound<'py, PyAny>,
    time: &str,
    src: &str,
    dst: &str,
    properties: &[&str],
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_col: Option<&str>,
    schema: Option<HashMap<String, PropType>>,
    event_id: Option<&str>,
) -> Result<(), GraphError> {
    let mut cols_to_check = [src, dst, time]
        .into_iter()
        .chain(properties)
        .chain(metadata)
        .chain(layer_col)
        .chain(event_id)
        .collect::<Vec<_>>();

    let df_view = process_arrow_c_stream_df(data, &cols_to_check, schema)?;
    df_view.check_cols_exist(&cols_to_check)?;
    load_edges_from_df(
        df_view,
        ColumnNames::new(time, event_id, src, dst, layer_col),
        true,
        properties,
        metadata,
        shared_metadata,
        layer,
        graph,
        false,
    )
}

pub(crate) fn load_node_metadata_from_arrow_c_stream<
    'py,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    graph: &G,
    data: &Bound<'py, PyAny>,
    id: &str,
    node_type: Option<&str>,
    node_type_col: Option<&str>,
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    schema: Option<HashMap<String, PropType>>,
) -> Result<(), GraphError> {
    let mut cols_to_check = [id]
        .into_iter()
        .chain(metadata)
        .chain(node_type_col)
        .collect::<Vec<_>>();

    let df_view = process_arrow_c_stream_df(data, &cols_to_check, schema)?;
    df_view.check_cols_exist(&cols_to_check)?;
    load_node_props_from_df(
        df_view,
        id,
        node_type,
        node_type_col,
        None,
        None,
        metadata,
        shared_metadata,
        graph,
    )
}

pub(crate) fn load_edge_metadata_from_arrow_c_stream<
    'py,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    graph: &G,
    data: &Bound<'py, PyAny>,
    src: &str,
    dst: &str,
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_col: Option<&str>,
    schema: Option<HashMap<String, PropType>>,
) -> Result<(), GraphError> {
    let mut cols_to_check = [src, dst]
        .into_iter()
        .chain(layer_col)
        .chain(metadata)
        .collect::<Vec<_>>();
    let df_view = process_arrow_c_stream_df(data, &cols_to_check, schema)?;
    df_view.check_cols_exist(&cols_to_check)?;
    load_edges_props_from_df(
        df_view,
        src,
        dst,
        metadata,
        shared_metadata,
        layer,
        layer_col,
        graph,
        true,
    )
}

pub(crate) fn load_edge_deletions_from_arrow_c_stream<
    'py,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    graph: &G,
    data: &Bound<'py, PyAny>,
    time: &str,
    src: &str,
    dst: &str,
    layer: Option<&str>,
    layer_col: Option<&str>,
    schema: Option<HashMap<String, PropType>>,
) -> Result<(), GraphError> {
    let mut cols_to_check = [src, dst, time]
        .into_iter()
        .chain(layer_col)
        .collect::<Vec<_>>();
    let df_view = process_arrow_c_stream_df(data, &cols_to_check, schema)?;
    df_view.check_cols_exist(&cols_to_check)?;
    load_edge_deletions_from_df(
        df_view,
        ColumnNames::new(time, None, src, dst, layer_col),
        true,
        layer,
        graph.core_graph(),
    )
}

/// Can handle any object that provides the \_\_arrow_c_stream__() interface
pub(crate) fn process_arrow_c_stream_df<'a>(
    data: &Bound<'a, PyAny>,
    col_names: &[&str],
    schema: Option<HashMap<String, PropType>>,
) -> PyResult<DFView<impl Iterator<Item = Result<DFChunk, GraphError>> + 'a>> {
    let py = data.py();
    is_jupyter(py);

    if !data.hasattr("__arrow_c_stream__")? {
        return Err(PyErr::from(GraphError::LoadFailure(
            "Object must implement __arrow_c_stream__".to_string(),
        )));
    }

    let stream_capsule_any: Bound<'a, PyAny> = data.call_method0("__arrow_c_stream__")?;
    let stream_capsule: &Bound<'a, PyCapsule> = stream_capsule_any.cast::<PyCapsule>()?;

    if !stream_capsule.is_valid() {
        return Err(PyErr::from(GraphError::LoadFailure(
            "Stream capsule is not valid".to_string(),
        )));
    }
    let reader = PyRecordBatchReader::from_arrow_pycapsule(stream_capsule)
        .map_err(|e| {
            PyErr::from(GraphError::LoadFailure(format!(
                "Arrow stream error while creating the reader: {}",
                e
            )))
        })?
        .into_reader()
        .map_err(|e| {
            PyErr::from(GraphError::LoadFailure(format!(
                "Arrow stream error while creating the reader: {}",
                e
            )))
        })?;

    // Get column names and indices once only
    let mut names: Vec<String> = Vec::with_capacity(col_names.len());
    let mut indices: Vec<usize> = Vec::with_capacity(col_names.len());

    for (idx, field) in reader.schema().fields().iter().enumerate() {
        if col_names.contains(&field.name().as_str()) {
            names.push(field.name().clone());
            indices.push(idx);
        }
    }

    let len_from_python: Option<usize> = if data.hasattr("__len__")? {
        Some(data.call_method0("__len__")?.extract()?)
    } else {
        None
    };

    let chunks = reader
        .into_iter()
        .flat_map(move |batch_res: Result<RecordBatch, _>| {
            let batch: RecordBatch = match batch_res.map_err(|e| {
                GraphError::LoadFailure(format!(
                    "Arrow stream error while reading a batch: {}",
                    e.to_string()
                ))
            }) {
                Ok(batch) => batch,
                Err(e) => return vec![Err(e)],
            };
            let casted_batch = if let Some(schema) = &schema {
                match cast_columns(batch, schema) {
                    Ok(casted_batch) => casted_batch,
                    Err(e) => return vec![Err(e)],
                }
            } else {
                batch
            };

            split_into_chunks(&casted_batch, &indices)
        });

    Ok(DFView::new(names, chunks, len_from_python))
}

/// Splits a RecordBatch into chunks of CHUNK_SIZE owned by DFChunk objects
fn split_into_chunks(batch: &RecordBatch, indices: &[usize]) -> Vec<Result<DFChunk, GraphError>> {
    // many times, all the data will be passed as a single RecordBatch, meaning the progress bar
    // will not update properly (only updates at the end of each batch). Splitting into smaller batches
    // means the progress bar will update reasonably (every CHUNK_SIZE rows)
    let num_rows = batch.num_rows();
    if num_rows > CHUNK_SIZE {
        let num_chunks = (num_rows + CHUNK_SIZE - 1) / CHUNK_SIZE;
        let mut result = Vec::with_capacity(num_chunks);
        for i in 0..num_chunks {
            let offset = i * CHUNK_SIZE;
            let length = min(CHUNK_SIZE, num_rows - offset);
            let sliced_batch = batch.slice(offset, length);
            let chunk_arrays = indices
                .iter()
                .map(|&idx| sliced_batch.column(idx).clone())
                .collect::<Vec<_>>();
            result.push(Ok(DFChunk::new(chunk_arrays)));
        }
        result
    } else {
        let chunk_arrays = indices
            .iter()
            .map(|&idx| batch.column(idx).clone())
            .collect::<Vec<_>>();
        vec![Ok(DFChunk::new(chunk_arrays))]
    }
}

pub(crate) fn is_csv_path(path: &PathBuf) -> Result<bool, std::io::Error> {
    if path.is_dir() {
        Ok(fs::read_dir(&path)?.any(|entry| {
            entry.map_or(false, |e| {
                let p = e.path();
                let s = p.to_string_lossy();
                s.ends_with(".csv") || s.ends_with(".csv.gz") || s.ends_with(".csv.bz2")
            })
        }))
    } else {
        let path_str = path.to_string_lossy();
        Ok(path_str.ends_with(".csv")
            || path_str.ends_with(".csv.gz")
            || path_str.ends_with(".csv.bz2"))
    }
}

/// CSV options we support, passed as Python dict
pub(crate) struct CsvReadOptions {
    delimiter: Option<u8>,
    comment: Option<u8>,
    escape: Option<u8>,
    quote: Option<u8>,
    terminator: Option<u8>,
    allow_truncated_rows: Option<bool>,
    has_header: Option<bool>,
}

impl<'py> FromPyObject<'_, 'py> for CsvReadOptions {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        let dict = ob.cast::<PyDict>().map_err(|e| {
            PyValueError::new_err(format!("CSV options should be passed as a dict: {e}"))
        })?;
        let get_char = |option: &str| match dict.get_item(option)? {
            None => Ok(None),
            Some(val) => {
                if let Ok(s) = val.extract::<String>() {
                    if s.len() != 1 {
                        return Err(PyValueError::new_err(format!(
                            "CSV option '{option}' must be a single character string or int 0-255",
                        )));
                    }
                    Ok(Some(s.as_bytes()[0]))
                } else if let Ok(b) = val.extract::<u8>() {
                    Ok(Some(b))
                } else {
                    return Err(PyValueError::new_err(format!(
                        "CSV option '{option}' must be a single character string or int 0-255",
                    )));
                }
            }
        };
        let get_bool = |option: &str| {
            dict.get_item(option)?
                .map(|val| val.extract::<bool>())
                .transpose()
                .map_err(|_| PyValueError::new_err(format!("CSV option '{option}' must be a bool")))
        };

        Ok(CsvReadOptions {
            delimiter: get_char("delimiter")?,
            comment: get_char("comment")?,
            escape: get_char("escape")?,
            quote: get_char("quote")?,
            terminator: get_char("terminator")?,
            allow_truncated_rows: get_bool("allow_truncated_rows")?,
            has_header: get_bool("has_header")?,
        })
    }
}

fn collect_csv_paths(path: &PathBuf) -> Result<Vec<PathBuf>, GraphError> {
    let mut csv_paths = Vec::new();
    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let p = entry.path();
            let s = p.to_string_lossy();
            if s.ends_with(".csv") || s.ends_with(".csv.gz") || s.ends_with(".csv.bz2") {
                csv_paths.push(p);
            }
        }
    } else {
        csv_paths.push(path.clone());
    }

    if csv_paths.is_empty() {
        return Err(GraphError::LoadFailure(format!(
            "No CSV files found at path '{}'",
            path.display()
        )));
    }
    Ok(csv_paths)
}

// Load from CSV files using arrow-csv
pub(crate) fn load_nodes_from_csv_path<
    'py,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    graph: &G,
    path: &PathBuf,
    time: &str,
    id: &str,
    node_type: Option<&str>,
    node_type_col: Option<&str>,
    properties: &[&str],
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    csv_options: Option<&CsvReadOptions>,
    schema: Option<Arc<HashMap<String, PropType>>>,
    event_id: Option<&str>,
) -> Result<(), GraphError> {
    let mut cols_to_check = [id, time]
        .into_iter()
        .chain(properties)
        .chain(metadata)
        .chain(node_type_col)
        .chain(event_id)
        .collect::<Vec<_>>();

    let csv_paths = collect_csv_paths(path)?;

    let df_view = process_csv_paths_df(&csv_paths, &cols_to_check, csv_options, schema)?;
    df_view.check_cols_exist(&cols_to_check)?;
    load_nodes_from_df(
        df_view,
        time,
        event_id,
        id,
        properties,
        metadata,
        shared_metadata,
        node_type,
        node_type_col,
        graph,
        true,
    )
}

pub(crate) fn load_edges_from_csv_path<
    'py,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    graph: &G,
    path: &PathBuf,
    time: &str,
    src: &str,
    dst: &str,
    properties: &[&str],
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_col: Option<&str>,
    csv_options: Option<&CsvReadOptions>,
    schema: Option<Arc<HashMap<String, PropType>>>,
    event_id: Option<&str>,
) -> Result<(), GraphError> {
    let mut cols_to_check = [src, dst, time]
        .into_iter()
        .chain(properties)
        .chain(metadata)
        .chain(layer_col)
        .collect::<Vec<_>>();

    let csv_paths = collect_csv_paths(path)?;

    let df_view = process_csv_paths_df(&csv_paths, &cols_to_check, csv_options, schema)?;
    df_view.check_cols_exist(&cols_to_check)?;
    load_edges_from_df(
        df_view,
        ColumnNames::new(time, event_id, src, dst, layer_col),
        true,
        properties,
        metadata,
        shared_metadata,
        layer,
        graph,
        false,
    )
}

pub(crate) fn load_node_metadata_from_csv_path<
    'py,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    graph: &G,
    path: &PathBuf,
    id: &str,
    node_type: Option<&str>,
    node_type_col: Option<&str>,
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    csv_options: Option<&CsvReadOptions>,
    schema: Option<Arc<HashMap<String, PropType>>>,
) -> Result<(), GraphError> {
    let cols_to_check = [id]
        .into_iter()
        .chain(metadata)
        .chain(node_type_col)
        .collect::<Vec<_>>();

    let csv_paths = collect_csv_paths(path)?;

    let df_view = process_csv_paths_df(&csv_paths, &cols_to_check, csv_options, schema)?;
    df_view.check_cols_exist(&cols_to_check)?;
    load_node_props_from_df(
        df_view,
        id,
        node_type,
        node_type_col,
        None,
        None,
        metadata,
        shared_metadata,
        graph,
    )
}

pub(crate) fn load_edge_metadata_from_csv_path<
    'py,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    graph: &G,
    path: &PathBuf,
    src: &str,
    dst: &str,
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_col: Option<&str>,
    csv_options: Option<&CsvReadOptions>,
    schema: Option<Arc<HashMap<String, PropType>>>,
) -> Result<(), GraphError> {
    let cols_to_check = [src, dst]
        .into_iter()
        .chain(metadata)
        .chain(layer_col)
        .collect::<Vec<_>>();

    let csv_paths = collect_csv_paths(path)?;

    let df_view = process_csv_paths_df(&csv_paths, &cols_to_check, csv_options, schema)?;
    df_view.check_cols_exist(&cols_to_check)?;
    load_edges_props_from_df(
        df_view,
        src,
        dst,
        metadata,
        shared_metadata,
        layer,
        layer_col,
        graph,
        true,
    )
}

pub(crate) fn load_edge_deletions_from_csv_path<
    'py,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
>(
    graph: &G,
    path: &PathBuf,
    time: &str,
    src: &str,
    dst: &str,
    layer: Option<&str>,
    layer_col: Option<&str>,
    csv_options: Option<&CsvReadOptions>,
    schema: Option<Arc<HashMap<String, PropType>>>,
    event_id: Option<&str>,
) -> Result<(), GraphError> {
    let cols_to_check = [src, dst, time]
        .into_iter()
        .chain(layer_col)
        .collect::<Vec<_>>();

    let csv_paths = collect_csv_paths(path)?;

    let df_view = process_csv_paths_df(&csv_paths, &cols_to_check, csv_options, schema)?;
    df_view.check_cols_exist(&cols_to_check)?;
    load_edge_deletions_from_df(
        df_view,
        ColumnNames::new(time, event_id, src, dst, layer_col),
        true,
        layer,
        graph.core_graph(),
    )
}

fn get_csv_reader(filename: &str, file: File) -> Box<dyn std::io::Read> {
    // Support bz2 and gz compression
    if filename.ends_with(".csv.gz") {
        Box::new(GzDecoder::new(file))
    } else if filename.ends_with(".csv.bz2") {
        Box::new(BzDecoder::new(file))
    } else {
        // no need for a BufReader because ReaderBuilder::build internally wraps into BufReader
        Box::new(file)
    }
}

fn build_csv_reader(
    path: &Path,
    csv_options: Option<&CsvReadOptions>,
) -> Result<arrow_csv::reader::Reader<Box<dyn std::io::Read>>, GraphError> {
    let file = File::open(path)?;
    let path_str = path.to_string_lossy();

    let mut format = Format::default();

    let has_header = csv_options.and_then(|o| o.has_header).unwrap_or(true);
    format = format.with_header(has_header);

    if let Some(delim) = csv_options.and_then(|o| o.delimiter) {
        format = format.with_delimiter(delim);
    }

    if let Some(comment) = csv_options.and_then(|o| o.comment) {
        format = format.with_comment(comment);
    }

    if let Some(escape) = csv_options.and_then(|o| o.escape) {
        format = format.with_escape(escape);
    }

    if let Some(quote) = csv_options.and_then(|o| o.quote) {
        format = format.with_quote(quote);
    }

    if let Some(terminator) = csv_options.and_then(|o| o.terminator) {
        format = format.with_terminator(terminator);
    }

    if let Some(allow_truncated_rows) = csv_options.and_then(|o| o.allow_truncated_rows) {
        format = format.with_truncated_rows(allow_truncated_rows);
    } else {
        // allow truncated rows to avoid crashes if the last item is missing
        format = format.with_truncated_rows(true);
    }

    // infer schema
    let reader = get_csv_reader(path_str.as_ref(), file);
    let (schema, _) = format.infer_schema(reader, Some(100)).map_err(|e| {
        GraphError::LoadFailure(format!(
            "Arrow CSV error while inferring schema from '{}': {e}",
            path.display()
        ))
    })?;
    let schema_ref: SchemaRef = Arc::new(schema);

    // we need another reader because the first one gets consumed
    let file = File::open(path)?;
    let reader = get_csv_reader(path_str.as_ref(), file);

    let mut reader_builder = ReaderBuilder::new(schema_ref)
        .with_header(has_header)
        .with_batch_size(CHUNK_SIZE);

    if let Some(delimiter) = csv_options.and_then(|o| o.delimiter) {
        reader_builder = reader_builder.with_delimiter(delimiter);
    }

    if let Some(comment) = csv_options.and_then(|o| o.comment) {
        reader_builder = reader_builder.with_comment(comment);
    }

    if let Some(escape) = csv_options.and_then(|o| o.escape) {
        reader_builder = reader_builder.with_escape(escape);
    }

    if let Some(quote) = csv_options.and_then(|o| o.quote) {
        reader_builder = reader_builder.with_quote(quote);
    }

    if let Some(terminator) = csv_options.and_then(|o| o.terminator) {
        reader_builder = reader_builder.with_terminator(terminator);
    }

    if let Some(allow_truncated_rows) = csv_options.and_then(|o| o.allow_truncated_rows) {
        reader_builder = reader_builder.with_truncated_rows(allow_truncated_rows);
    } else {
        reader_builder = reader_builder.with_truncated_rows(true);
    }

    reader_builder.build(reader).map_err(|e| {
        GraphError::LoadFailure(format!(
            "Arrow CSV error while reading '{}': {e}",
            path.display()
        ))
    })
}

fn process_csv_paths_df<'a>(
    paths: &'a [PathBuf],
    col_names: &'a [&'a str],
    csv_options: Option<&'a CsvReadOptions>,
    schema: Option<Arc<HashMap<String, PropType>>>,
) -> Result<DFView<impl Iterator<Item = Result<DFChunk, GraphError>> + 'a>, GraphError> {
    if paths.is_empty() {
        return Err(GraphError::LoadFailure(
            "No CSV files found at the provided path".to_string(),
        ));
    }
    // BoxedLIter couldn't be used because it has Send + Sync bound
    type ChunkIter<'b> = Box<dyn Iterator<Item = Result<DFChunk, GraphError>> + 'b>;

    let names = col_names.iter().map(|&name| name.to_string()).collect();
    let chunks = paths.iter().flat_map(move |path| {
        let schema = schema.clone();
        let csv_reader = match build_csv_reader(path.as_path(), csv_options) {
            Ok(r) => r,
            Err(e) => return Box::new(iter::once(Err(e))) as ChunkIter<'a>,
        };
        let mut indices = Vec::with_capacity(col_names.len());
        for required_col in col_names {
            if let Some((idx, _)) = csv_reader
                .schema()
                .fields()
                .iter()
                .enumerate()
                .find(|(_, f)| f.name() == required_col)
            {
                indices.push(idx);
            } else {
                return Box::new(iter::once(Err(GraphError::ColumnDoesNotExist(
                    required_col.to_string(),
                )))) as ChunkIter<'a>;
            }
        }
        Box::new(
            csv_reader
                .into_iter()
                .map(move |batch_res| match batch_res {
                    Ok(batch) => {
                        let casted_batch = if let Some(schema) = schema.as_deref() {
                            cast_columns(batch, schema)?
                        } else {
                            batch
                        };
                        let arrays = indices
                            .iter()
                            .map(|&idx| casted_batch.column(idx).clone())
                            .collect::<Vec<_>>();
                        Ok(DFChunk::new(arrays))
                    }
                    Err(e) => Err(GraphError::LoadFailure(format!(
                        "Arrow CSV error while reading a batch from '{}': {e}",
                        path.display()
                    ))),
                }),
        ) as ChunkIter<'a>
    });

    // we don't know the total number of rows until we read all files
    Ok(DFView::new(names, chunks, None))
}

pub(crate) fn is_jupyter(py: Python) {
    let code = c_str!(
        r#"
try:
    shell = get_ipython().__class__.__name__
    if shell == 'ZMQInteractiveShell':
        result = True   # Jupyter notebook or qtconsole
    elif shell == 'TerminalInteractiveShell':
        result = False  # Terminal running IPython
    else:
        result = False  # Other type, assuming not a Jupyter environment
except NameError:
    result = False      # Probably standard Python interpreter
"#
    );

    if let Err(e) = py.run(code, None, None) {
        error!("Error checking if running in a jupyter notebook: {}", e);
        return;
    }

    match py.eval(c_str!("result"), None, None) {
        Ok(x) => {
            if let Ok(x) = x.extract() {
                kdam::set_notebook(x);
            }
        }
        Err(e) => {
            error!("Error checking if running in a jupyter notebook: {}", e);
        }
    };
}
