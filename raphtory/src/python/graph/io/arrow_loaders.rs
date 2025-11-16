use crate::{
    db::api::view::StaticGraphViewOps,
    errors::GraphError,
    io::arrow::dataframe::{DFChunk, DFView},
    prelude::{AdditionOps, PropertyAdditionOps},
    python::graph::io::pandas_loaders::{array_to_rust, is_jupyter},
    serialise::incremental::InternalCache,
};
use pyo3::{prelude::*, types::PyDict};
use raphtory_api::core::entities::properties::prop::Prop;
use std::collections::HashMap;
use crate::io::arrow::df_loaders::load_edges_from_df;

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
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![src, dst, time];
    cols_to_check.extend_from_slice(properties);
    cols_to_check.extend_from_slice(metadata);
    if let Some(layer_col) = layer_col {
        cols_to_check.push(layer_col.as_ref());
    }

    let df_view = process_arrow_py_df(df, cols_to_check.clone())?;
    df_view.check_cols_exist(&cols_to_check)?;
    load_edges_from_df(df_view, time, src, dst, properties, metadata, shared_metadata, layer, layer_col, graph)
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
