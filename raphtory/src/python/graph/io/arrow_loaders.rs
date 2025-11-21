use crate::{
    db::api::view::StaticGraphViewOps,
    errors::GraphError,
    io::arrow::{
        dataframe::{DFChunk, DFView},
        df_loaders::{
            load_edges_from_df, load_edges_props_from_df, load_node_props_from_df,
            load_nodes_from_df,
        },
    },
    prelude::{AdditionOps, PropertyAdditionOps},
    python::graph::io::pandas_loaders::{array_to_rust, is_jupyter},
    serialise::incremental::InternalCache,
};
use arrow::{
    array::{
        ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
        RecordBatch, RecordBatchReader,
    },
    datatypes::SchemaRef,
};
use itertools::Either;
use pyo3::{
    prelude::*,
    types::{PyCapsule, PyDict},
};
use raphtory_api::core::entities::properties::prop::Prop;
use std::collections::HashMap;

pub(crate) fn load_nodes_from_arrow_c_stream<
    'py,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + InternalCache,
>(
    graph: &G,
    df: &Bound<'py, PyAny>,
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

    let df_view = process_arrow_c_stream_df(df, cols_to_check.clone())?;
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

pub(crate) fn load_edges_from_arrow<
    'py,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + InternalCache,
>(
    graph: &G,
    df: &Bound<'py, PyAny>,
    time: &str,
    src: &str,
    dst: &str,
    properties: &[&str],
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_col: Option<&str>,
    stream_data: bool,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![src, dst, time];
    cols_to_check.extend_from_slice(properties);
    cols_to_check.extend_from_slice(metadata);
    if let Some(layer_col) = layer_col {
        cols_to_check.push(layer_col.as_ref());
    }

    if stream_data {
        let df_view = process_arrow_c_stream_df(df, cols_to_check.clone())?;
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
    } else {
        let df_view = process_arrow_py_df(df, cols_to_check.clone())?;
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
}

pub(crate) fn load_node_props_from_arrow_c_stream<
    'py,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + InternalCache,
>(
    graph: &G,
    df: &Bound<'py, PyAny>,
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
    let df_view = process_arrow_c_stream_df(df, cols_to_check.clone())?;
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

pub(crate) fn load_edge_props_from_arrow_c_stream<
    'py,
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + InternalCache,
>(
    graph: &G,
    df: &Bound<'py, PyAny>,
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
    let df_view = process_arrow_c_stream_df(df, cols_to_check.clone())?;
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

/// Can handle any object that provides the \_\_arrow_c_stream__() interface and \_\_len__() function
pub(crate) fn process_arrow_c_stream_df<'a>(
    df: &Bound<'a, PyAny>,
    col_names: Vec<&str>,
) -> PyResult<DFView<impl Iterator<Item = Result<DFChunk, GraphError>> + 'a>> {
    let py = df.py();
    is_jupyter(py);

    // Expect an object that can use the Arrow C Stream interface
    if !df.hasattr("__arrow_c_stream__")? {
        return Err(PyErr::from(GraphError::LoadFailure(
            "arrow object must implement __arrow_c_stream__".to_string(),
        )));
    }

    let stream_capsule_any: Bound<'a, PyAny> = df.call_method0("__arrow_c_stream__")?;
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
    let len_from_python: Option<usize> = if df.hasattr("__len__")? {
        Some(df.call_method0("__len__")?.extract()?)
    } else {
        None
    };

    if let Some(num_rows) = len_from_python {
        let chunks = reader
            .into_iter()
            .map(move |batch_res: Result<RecordBatch, _>| {
                let batch = batch_res.map_err(|e| {
                    GraphError::LoadFailure(format!(
                        "Arrow stream error while reading a batch: {}",
                        e.to_string()
                    ))
                })?;
                let chunk_arrays = indices
                    .iter()
                    .map(|&idx| batch.column(idx).clone())
                    .collect::<Vec<_>>();
                Ok(DFChunk::new(chunk_arrays))
            });
        Ok(DFView::new(names, Either::Left(chunks), num_rows))
    } else {
        // if the python data source has no __len__ method, collect the iterator so we can calculate the num_rows() of each batch
        let mut num_rows = 0usize;
        let mut df_chunks = Vec::new();

        for batch_res in reader {
            let batch = batch_res.map_err(|e| {
                GraphError::LoadFailure(format!(
                    "Arrow stream error while reading a batch: {}",
                    e.to_string()
                ))
            })?;
            num_rows += batch.num_rows();
            let chunk_arrays = indices
                .iter()
                .map(|&idx| batch.column(idx).clone())
                .collect::<Vec<_>>();
            df_chunks.push(Ok(DFChunk::new(chunk_arrays)));
        }

        let chunks = Either::Right(df_chunks.into_iter());
        Ok(DFView::new(names, chunks, num_rows))
    }
}

pub(crate) fn process_arrow_py_df<'a>(
    df: &Bound<'a, PyAny>,
    col_names: Vec<&str>,
) -> PyResult<DFView<impl Iterator<Item = Result<DFChunk, GraphError>> + 'a>> {
    let py = df.py();
    is_jupyter(py);

    // We assume df is an Arrow object (e.g. pyarrow Table or RecordBatchReader)
    // that implements a to_batches(max_chunksize=...) method
    let kwargs = PyDict::new(py);
    kwargs.set_item("max_chunksize", 1_000_000)?;

    // Get a list of RecordBatch-like Python objects
    let rb = df
        .call_method("to_batches", (), Some(&kwargs))?
        .extract::<Vec<Bound<PyAny>>>()?;

    // Derive the column names from the first batch's schema, then filter
    let names: Vec<String> = if let Some(batch0) = rb.first() {
        let schema = batch0.getattr("schema")?;
        schema.getattr("names")?.extract::<Vec<String>>()?
    } else {
        vec![]
    }
    .into_iter()
    .filter(|x| col_names.contains(&x.as_str()))
    .collect();

    let names_len = names.len();

    let chunks = rb.into_iter().map(move |rb| {
        let columns = rb.getattr("columns")?.extract::<Vec<Bound<PyAny>>>()?;
        let chunk = (0..names_len)
            .map(|i| {
                // `rb.column(i)` -> pyarrow.Array
                let array = &columns[i];
                let arr = array_to_rust(array).map_err(GraphError::from)?;
                Ok::<_, GraphError>(arr)
            })
            .collect::<Result<Vec<_>, GraphError>>()?;

        Ok(DFChunk { chunk })
    });

    let num_rows: usize = df.call_method0("__len__")?.extract()?;

    Ok(DFView {
        names,
        chunks,
        num_rows,
    })
}
