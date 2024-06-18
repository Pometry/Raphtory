use crate::core::{entities::graph::tgraph::InternalGraph, utils::errors::GraphError, Prop};
use std::collections::HashMap;
use std::path::Path;
use crate::python::graph::io::{dataframe::*, df_loaders::*};

pub fn load_nodes_from_parquet(
    graph: &InternalGraph,
    parquet_file_path: &Path,
    id: &str,
    time: &str,
    node_type: Option<&str>,
    node_type_in_df: Option<bool>,
    properties: Option<Vec<&str>>,
    const_properties: Option<Vec<&str>>,
    shared_const_properties: Option<HashMap<String, Prop>>,
) -> Result<(), GraphError> {
    todo!()
}

pub fn load_edges_from_parquet(
    graph: &InternalGraph,
    parquet_file_path: &Path,
    src: &str,
    dst: &str,
    time: &str,
    properties: Option<Vec<&str>>,
    const_properties: Option<Vec<&str>>,
    shared_const_properties: Option<HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_in_df: Option<bool>,
) -> Result<(), GraphError> {
    todo!()
}

pub fn load_node_props_from_parquet(
    graph: &InternalGraph,
    parquet_file_path: &Path,
    id: &str,
    const_properties: Option<Vec<&str>>,
    shared_const_properties: Option<HashMap<String, Prop>>,
) -> Result<(), GraphError> {
    todo!()
}

pub fn load_edge_props_from_parquet(
    graph: &InternalGraph,
    parquet_file_path: &Path,
    src: &str,
    dst: &str,
    const_properties: Option<Vec<&str>>,
    shared_const_properties: Option<HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_in_df: Option<bool>,
) -> Result<(), GraphError> {
    todo!()
}

pub(crate) fn process_parquet_file_to_df(
    parquet_file_path: &Path,
    col_names: Vec<&str>,
) -> Result<PretendDF, GraphError> {
    todo!()
}
