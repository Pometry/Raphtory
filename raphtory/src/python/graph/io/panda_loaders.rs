use crate::{
    core::{entities::graph::tgraph::InternalGraph, utils::errors::GraphError, Prop},
    io::arrow::{dataframe::*, df_loaders::*},
};
use polars_arrow::{array::Array, ffi};
use pyo3::{ffi::Py_uintptr_t, prelude::*, types::IntoPyDict};
use std::collections::HashMap;

pub fn load_nodes_from_pandas(
    graph: &InternalGraph,
    df: &PyAny,
    id: &str,
    time: &str,
    node_type: Option<&str>,
    node_type_in_df: Option<bool>,
    properties: Option<Vec<&str>>,
    const_properties: Option<Vec<&str>>,
    shared_const_properties: Option<HashMap<String, Prop>>,
) -> Result<(), GraphError> {
    Python::with_gil(|py| {
        let size: usize = py
            .eval(
                "index.__len__()",
                Some([("index", df.getattr("index")?)].into_py_dict(py)),
                None,
            )?
            .extract()?;

        let mut cols_to_check = vec![id, time];
        cols_to_check.extend(properties.as_ref().unwrap_or(&Vec::new()));
        cols_to_check.extend(const_properties.as_ref().unwrap_or(&Vec::new()));
        if node_type_in_df.unwrap_or(true) {
            if let Some(ref node_type) = node_type {
                cols_to_check.push(node_type.as_ref());
            }
        }

        let df = process_pandas_py_df(df, py, cols_to_check.clone())?;
        df.check_cols_exist(&cols_to_check)?;

        load_nodes_from_df(
            &df,
            size,
            id,
            time,
            properties,
            const_properties,
            shared_const_properties,
            node_type,
            node_type_in_df.unwrap_or(true),
            graph,
        )
        .map_err(|e| GraphLoadException::new_err(format!("{:?}", e)))?;
        Ok::<(), PyErr>(())
    })
    .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
    Ok(())
}

pub fn load_edges_from_pandas(
    graph: &InternalGraph,
    df: &PyAny,
    src: &str,
    dst: &str,
    time: &str,
    properties: Option<Vec<&str>>,
    const_properties: Option<Vec<&str>>,
    shared_const_properties: Option<HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_in_df: Option<bool>,
) -> Result<(), GraphError> {
    Python::with_gil(|py| {
        let size: usize = py
            .eval(
                "index.__len__()",
                Some([("index", df.getattr("index")?)].into_py_dict(py)),
                None,
            )?
            .extract()?;

        let mut cols_to_check = vec![src, dst, time];
        cols_to_check.extend(properties.as_ref().unwrap_or(&Vec::new()));
        cols_to_check.extend(const_properties.as_ref().unwrap_or(&Vec::new()));
        if layer_in_df.unwrap_or(false) {
            if let Some(ref layer) = layer {
                cols_to_check.push(layer.as_ref());
            }
        }

        let df = process_pandas_py_df(df, py, cols_to_check.clone())?;

        df.check_cols_exist(&cols_to_check)?;
        load_edges_from_df(
            &df,
            size,
            src,
            dst,
            time,
            properties,
            const_properties,
            shared_const_properties,
            layer,
            layer_in_df.unwrap_or(true),
            graph,
        )
        .map_err(|e| GraphLoadException::new_err(format!("{:?}", e)))?;

        Ok::<(), PyErr>(())
    })
    .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
    Ok(())
}

pub fn load_node_props_from_pandas(
    graph: &InternalGraph,
    df: &PyAny,
    id: &str,
    const_properties: Option<Vec<&str>>,
    shared_const_properties: Option<HashMap<String, Prop>>,
) -> Result<(), GraphError> {
    Python::with_gil(|py| {
        let size: usize = py
            .eval(
                "index.__len__()",
                Some([("index", df.getattr("index")?)].into_py_dict(py)),
                None,
            )?
            .extract()?;
        let mut cols_to_check = vec![id];
        cols_to_check.extend(const_properties.as_ref().unwrap_or(&Vec::new()));
        let df = process_pandas_py_df(df, py, cols_to_check.clone())?;
        df.check_cols_exist(&cols_to_check)?;

        load_node_props_from_df(
            &df,
            size,
            id,
            const_properties,
            shared_const_properties,
            graph,
        )
        .map_err(|e| GraphLoadException::new_err(format!("{:?}", e)))?;

        Ok::<(), PyErr>(())
    })
    .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
    Ok(())
}

pub fn load_edge_props_from_pandas(
    graph: &InternalGraph,
    df: &PyAny,
    src: &str,
    dst: &str,
    const_properties: Option<Vec<&str>>,
    shared_const_properties: Option<HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_in_df: Option<bool>,
) -> Result<(), GraphError> {
    Python::with_gil(|py| {
        let size: usize = py
            .eval(
                "index.__len__()",
                Some([("index", df.getattr("index")?)].into_py_dict(py)),
                None,
            )?
            .extract()?;
        let mut cols_to_check = vec![src, dst];
        if layer_in_df.unwrap_or(false) {
            if let Some(ref layer) = layer {
                cols_to_check.push(layer.as_ref());
            }
        }
        cols_to_check.extend(const_properties.as_ref().unwrap_or(&Vec::new()));
        let df = process_pandas_py_df(df, py, cols_to_check.clone())?;
        df.check_cols_exist(&cols_to_check)?;
        load_edges_props_from_df(
            &df,
            size,
            src,
            dst,
            const_properties,
            shared_const_properties,
            layer,
            layer_in_df.unwrap_or(true),
            graph,
        )
        .map_err(|e| GraphLoadException::new_err(format!("{:?}", e)))?;
        df.check_cols_exist(&cols_to_check)?;
        Ok::<(), PyErr>(())
    })
    .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
    Ok(())
}

pub fn load_edges_deletions_from_pandas(
    graph: &InternalGraph,
    df: &PyAny,
    src: &str,
    dst: &str,
    time: &str,
    layer: Option<&str>,
    layer_in_df: Option<bool>,
) -> Result<(), GraphError> {
    Python::with_gil(|py| {
        let size: usize = py
            .eval(
                "index.__len__()",
                Some([("index", df.getattr("index")?)].into_py_dict(py)),
                None,
            )?
            .extract()?;

        let mut cols_to_check = vec![src, dst, time];
        if layer_in_df.unwrap_or(true) {
            if let Some(ref layer) = layer {
                cols_to_check.push(layer.as_ref());
            }
        }

        let df = process_pandas_py_df(df, py, cols_to_check.clone())?;
        df.check_cols_exist(&cols_to_check)?;

        load_edges_deletions_from_df(
            &df,
            size,
            src,
            dst,
            time,
            layer,
            layer_in_df.unwrap_or(true),
            graph,
        )
        .map_err(|e| GraphLoadException::new_err(format!("{:?}", e)))?;

        Ok::<(), PyErr>(())
    })
    .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
    Ok(())
}

pub(crate) fn process_pandas_py_df(
    df: &PyAny,
    py: Python,
    col_names: Vec<&str>,
) -> PyResult<PretendDF> {
    is_jupyter(py);
    py.import("pandas")?;
    let module = py.import("pyarrow")?;
    let pa_table = module.getattr("Table")?;

    let df_columns: Vec<String> = df.getattr("columns")?.extract()?;

    let cols_to_drop: Vec<String> = df_columns
        .into_iter()
        .filter(|x| !col_names.contains(&x.as_str()))
        .collect();

    let dropped_df = if !cols_to_drop.is_empty() {
        let drop_method = df.getattr("drop")?;
        drop_method.call((cols_to_drop,), Some(vec![("axis", 1)].into_py_dict(py)))?
    } else {
        df
    };

    let _df_columns: Vec<String> = dropped_df.getattr("columns")?.extract()?;

    let table = pa_table.call_method("from_pandas", (dropped_df,), None)?;

    let rb = table.call_method0("to_batches")?.extract::<Vec<&PyAny>>()?;
    let names: Vec<String> = if let Some(batch0) = rb.get(0) {
        let schema = batch0.getattr("schema")?;
        schema.getattr("names")?.extract::<Vec<String>>()?
    } else {
        vec![]
    }
    .into_iter()
    .filter(|x| col_names.contains(&x.as_str()))
    .collect();

    let arrays = rb
        .iter()
        .map(|rb| {
            (0..names.len())
                .map(|i| {
                    let array = rb.call_method1("column", (i,))?;
                    let arr = array_to_rust(array)?;
                    Ok::<Box<dyn Array>, PyErr>(arr)
                })
                .collect::<Result<Vec<_>, PyErr>>()
        })
        .collect::<Result<Vec<_>, PyErr>>()?;

    let df = PretendDF { names, arrays };
    Ok(df)
}

pub fn array_to_rust(obj: &PyAny) -> PyResult<ArrayRef> {
    // prepare a pointer to receive the Array struct
    let array = Box::new(ffi::ArrowArray::empty());
    let schema = Box::new(ffi::ArrowSchema::empty());

    let array_ptr = &*array as *const ffi::ArrowArray;
    let schema_ptr = &*schema as *const ffi::ArrowSchema;

    // make the conversion through PyArrow's private API
    // this changes the pointer's memory and is thus unsafe. In particular, `_export_to_c` can go out of bounds
    obj.call_method1(
        "_export_to_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    unsafe {
        let field = ffi::import_field_from_c(schema.as_ref())
            .map_err(|e| ArrowErrorException::new_err(format!("{:?}", e)))?;

        let array = ffi::import_array_from_c(*array, field.data_type)
            .map_err(|e| ArrowErrorException::new_err(format!("{:?}", e)))?;

        Ok(array)
    }
}

fn is_jupyter(py: Python) {
    let code = r#"
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
"#;

    if let Err(e) = py.run(code, None, None) {
        println!("Error checking if running in a jupyter notebook: {}", e);
        return;
    }

    match py.eval("result", None, None) {
        Ok(x) => {
            if let Ok(x) = x.extract() {
                kdam::set_notebook(x);
            }
        }
        Err(e) => {
            println!("Error checking if running in a jupyter notebook: {}", e);
        }
    };
}
