use crate::core::{entities::graph::tgraph::InternalGraph, utils::errors::GraphError, Prop};
use pyo3::{prelude::*, types::IntoPyDict};
use std::collections::HashMap;

use crate::python::graph::io::{
    dataframe::{process_pandas_py_df, GraphLoadException},
    df_loaders::{
        load_edges_from_df, load_edges_props_from_df, load_node_props_from_df, load_nodes_from_df,
    },
};

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

        let df = process_pandas_py_df(df, py, size, cols_to_check.clone())?;
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

        let df = process_pandas_py_df(df, py, size, cols_to_check.clone())?;

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
        let df = process_pandas_py_df(df, py, size, cols_to_check.clone())?;
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
        let df = process_pandas_py_df(df, py, size, cols_to_check.clone())?;
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
