use crate::{
    db::api::view::StaticGraphViewOps,
    errors::GraphError,
    io::arrow::{
        dataframe::{DFChunk, DFView},
        df_loaders::{
            load_edge_deletions_from_df, load_edges_from_df, load_edges_props_from_df,
            load_node_props_from_df, load_nodes_from_df,
        },
    },
    prelude::{AdditionOps, PropertyAdditionOps},
    python::graph::io::pandas_loaders::is_jupyter,
    serialise::incremental::InternalCache,
};
use arrow::{
    array::{
        ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
        RecordBatch, RecordBatchReader,
    },
    datatypes::SchemaRef,
};
use pyo3::{prelude::*, types::PyCapsule};
use raphtory_api::core::entities::properties::prop::Prop;
use std::{cmp::min, collections::HashMap};

const CHUNK_SIZE: usize = 1_000_000; // split large chunks so progress bar updates reasonably

pub(crate) fn load_nodes_from_arrow_c_stream<
    'py,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + InternalCache,
>(
    graph: &G,
    data: &Bound<'py, PyAny>,
    time: &str,
    id: &str,
    node_type: Option<&str>,
    node_type_col: Option<&str>,
    properties: &[&str],
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![id, time];
    cols_to_check.extend_from_slice(properties);
    cols_to_check.extend_from_slice(metadata);
    if let Some(ref node_type_col) = node_type_col {
        cols_to_check.push(node_type_col.as_ref());
    }
    let df_view = process_arrow_c_stream_df(data, cols_to_check.clone())?;
    df_view.check_cols_exist(&cols_to_check)?;
    load_nodes_from_df(
        df_view,
        time,
        id,
        properties,
        metadata,
        shared_metadata,
        node_type,
        node_type_col,
        graph,
    )
}

pub(crate) fn load_edges_from_arrow_c_stream<
    'py,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + InternalCache,
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
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![src, dst, time];
    cols_to_check.extend_from_slice(properties);
    cols_to_check.extend_from_slice(metadata);
    if let Some(layer_col) = layer_col {
        cols_to_check.push(layer_col.as_ref());
    }
    let df_view = process_arrow_c_stream_df(data, cols_to_check.clone())?;
    df_view.check_cols_exist(&cols_to_check)?;
    load_edges_from_df(
        df_view,
        time,
        src,
        dst,
        properties,
        metadata,
        shared_metadata,
        layer,
        layer_col,
        graph,
    )
}

pub(crate) fn load_node_metadata_from_arrow_c_stream<
    'py,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + InternalCache,
>(
    graph: &G,
    data: &Bound<'py, PyAny>,
    id: &str,
    node_type: Option<&str>,
    node_type_col: Option<&str>,
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![id];
    cols_to_check.extend_from_slice(metadata);
    if let Some(ref node_type_col) = node_type_col {
        cols_to_check.push(node_type_col.as_ref());
    }
    let df_view = process_arrow_c_stream_df(data, cols_to_check.clone())?;
    df_view.check_cols_exist(&cols_to_check)?;
    load_node_props_from_df(
        df_view,
        id,
        node_type,
        node_type_col,
        metadata,
        shared_metadata,
        graph,
    )
}

pub(crate) fn load_edge_metadata_from_arrow_c_stream<
    'py,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + InternalCache,
>(
    graph: &G,
    data: &Bound<'py, PyAny>,
    src: &str,
    dst: &str,
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_col: Option<&str>,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![src, dst];
    if let Some(ref layer_col) = layer_col {
        cols_to_check.push(layer_col.as_ref());
    }
    cols_to_check.extend_from_slice(metadata);
    let df_view = process_arrow_c_stream_df(data, cols_to_check.clone())?;
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
    )
}

pub fn load_edge_deletions_from_arrow_c_stream<
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
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![src, dst, time];
    if let Some(ref layer_col) = layer_col {
        cols_to_check.push(layer_col.as_ref());
    }

    let df_view = process_arrow_c_stream_df(data, cols_to_check.clone())?;
    df_view.check_cols_exist(&cols_to_check)?;
    load_edge_deletions_from_df(
        df_view,
        time,
        src,
        dst,
        layer,
        layer_col,
        graph.core_graph(),
    )
}

/// Can handle any object that provides the \_\_arrow_c_stream__() interface
pub(crate) fn process_arrow_c_stream_df<'a>(
    data: &Bound<'a, PyAny>,
    col_names: Vec<&str>,
) -> PyResult<DFView<impl Iterator<Item = Result<DFChunk, GraphError>> + 'a>> {
    let py = data.py();
    is_jupyter(py);

    if !data.hasattr("__arrow_c_stream__")? {
        return Err(PyErr::from(GraphError::LoadFailure(
            "Object must implement __arrow_c_stream__".to_string(),
        )));
    }

    let stream_capsule_any: Bound<'a, PyAny> = data.call_method0("__arrow_c_stream__")?;
    let stream_capsule: &Bound<'a, PyCapsule> = stream_capsule_any.downcast::<PyCapsule>()?;

    // We need to use the pointer to build an ArrowArrayStreamReader
    if !stream_capsule.is_valid() {
        return Err(PyErr::from(GraphError::LoadFailure(
            "Stream capsule is not valid".to_string(),
        )));
    }
    let stream_ptr = stream_capsule.pointer() as *mut FFI_ArrowArrayStream;
    let reader: ArrowArrayStreamReader = unsafe { ArrowArrayStreamReader::from_raw(stream_ptr) }
        .map_err(|e| {
            GraphError::LoadFailure(format!(
                "Arrow stream error while creating the reader: {}",
                e.to_string()
            ))
        })?;

    // Get column names and indices once only
    let schema: SchemaRef = reader.schema();
    let mut names: Vec<String> = Vec::with_capacity(col_names.len());
    let mut indices: Vec<usize> = Vec::with_capacity(col_names.len());

    for (idx, field) in schema.fields().iter().enumerate() {
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
            let batch = match batch_res.map_err(|e| {
                GraphError::LoadFailure(format!(
                    "Arrow stream error while reading a batch: {}",
                    e.to_string()
                ))
            }) {
                Ok(batch) => batch,
                Err(e) => return vec![Err(e)],
            };
            let num_rows = batch.num_rows();

            // many times, all the data will be passed as a single RecordBatch, meaning the progress bar
            // will not update properly (only updates at the end of each batch). Splitting into smaller batches
            // means the progress bar will update reasonably (every CHUNK_SIZE rows)
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
        });

    Ok(DFView::new(names, chunks, len_from_python))
}
