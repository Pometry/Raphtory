use crate::{
    core::{
        utils::errors::{GraphError, InvalidPathReason::PathDoesNotExist},
        Prop,
    },
    db::api::{
        mutation::internal::{InternalAdditionOps, InternalPropertyAdditionOps},
        view::StaticGraphViewOps,
    },
    io::arrow::{dataframe::*, df_loaders::*},
    prelude::DeletionOps,
};
use itertools::Itertools;
use polars_arrow::datatypes::{ArrowDataType as DataType, ArrowSchema, Field};
use polars_parquet::{
    read,
    read::{read_metadata, FileMetaData, FileReader},
};
use std::{
    collections::HashMap,
    fs,
    fs::File,
    path::{Path, PathBuf},
};

pub fn load_nodes_from_parquet<
    G: StaticGraphViewOps + InternalPropertyAdditionOps + InternalAdditionOps,
>(
    graph: &G,
    parquet_path: &Path,
    id: &str,
    time: &str,
    node_type: Option<&str>,
    node_type_in_df: Option<bool>,
    properties: Option<&[&str]>,
    const_properties: Option<&[&str]>,
    shared_const_properties: Option<&HashMap<String, Prop>>,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![id, time];
    cols_to_check.extend(properties.unwrap_or(&Vec::new()));
    cols_to_check.extend(const_properties.unwrap_or(&Vec::new()));
    if node_type_in_df.unwrap_or(true) {
        if let Some(ref node_type) = node_type {
            cols_to_check.push(node_type.as_ref());
        }
    }

    for path in get_parquet_file_paths(parquet_path)? {
        let df_view = process_parquet_file_to_df(path.as_path(), &cols_to_check)?;
        df_view.check_cols_exist(&cols_to_check)?;
        load_nodes_from_df(
            df_view,
            id,
            time,
            properties,
            const_properties,
            shared_const_properties,
            node_type,
            node_type_in_df.unwrap_or(true),
            graph,
        )
        .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
    }

    Ok(())
}

pub fn load_edges_from_parquet<
    G: StaticGraphViewOps + InternalPropertyAdditionOps + InternalAdditionOps,
>(
    graph: &G,
    parquet_path: impl AsRef<Path>,
    src: &str,
    dst: &str,
    time: &str,
    properties: Option<&[&str]>,
    const_properties: Option<&[&str]>,
    shared_const_properties: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_in_df: Option<bool>,
) -> Result<(), GraphError> {
    let parquet_path = parquet_path.as_ref();
    let mut cols_to_check = vec![src, dst, time];
    cols_to_check.extend(properties.unwrap_or(&Vec::new()));
    cols_to_check.extend(const_properties.unwrap_or(&Vec::new()));
    if layer_in_df.unwrap_or(false) {
        if let Some(ref layer) = layer {
            cols_to_check.push(layer.as_ref());
        }
    }

    for path in get_parquet_file_paths(parquet_path)? {
        let df_view = process_parquet_file_to_df(path.as_path(), &cols_to_check)?;
        df_view.check_cols_exist(&cols_to_check)?;
        let size = cols_to_check.len();
        load_edges_from_df(
            df_view,
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
        .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
    }

    Ok(())
}

pub fn load_node_props_from_parquet<
    G: StaticGraphViewOps + InternalPropertyAdditionOps + InternalAdditionOps,
>(
    graph: &G,
    parquet_path: &Path,
    id: &str,
    const_properties: Option<&[&str]>,
    shared_const_properties: Option<&HashMap<String, Prop>>,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![id];
    cols_to_check.extend(const_properties.unwrap_or(&Vec::new()));
    let size = cols_to_check.len();

    for path in get_parquet_file_paths(parquet_path)? {
        let df_view = process_parquet_file_to_df(path.as_path(), &cols_to_check)?;
        df_view.check_cols_exist(&cols_to_check)?;

        load_node_props_from_df(
            df_view,
            size,
            id,
            const_properties,
            shared_const_properties,
            graph,
        )
        .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
    }

    Ok(())
}

pub fn load_edge_props_from_parquet<
    G: StaticGraphViewOps + InternalPropertyAdditionOps + InternalAdditionOps,
>(
    graph: &G,
    parquet_path: &Path,
    src: &str,
    dst: &str,
    const_properties: Option<&[&str]>,
    shared_const_properties: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_in_df: Option<bool>,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![src, dst];
    if layer_in_df.unwrap_or(false) {
        if let Some(ref layer) = layer {
            cols_to_check.push(layer.as_ref());
        }
    }
    cols_to_check.extend(const_properties.unwrap_or(&Vec::new()));
    let size = cols_to_check.len();

    for path in get_parquet_file_paths(parquet_path)? {
        let df_view = process_parquet_file_to_df(path.as_path(), &cols_to_check)?;
        df_view.check_cols_exist(&cols_to_check)?;
        load_edges_props_from_df(
            df_view,
            size,
            src,
            dst,
            const_properties,
            shared_const_properties,
            layer,
            layer_in_df.unwrap_or(true),
            graph.core_graph(),
        )
        .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
    }

    Ok(())
}

pub fn load_edges_deletions_from_parquet<
    G: StaticGraphViewOps + InternalPropertyAdditionOps + InternalAdditionOps + DeletionOps,
>(
    graph: &G,
    parquet_path: &Path,
    src: &str,
    dst: &str,
    time: &str,
    layer: Option<&str>,
    layer_in_df: Option<bool>,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![src, dst, time];
    if layer_in_df.unwrap_or(true) {
        if let Some(ref layer) = layer {
            cols_to_check.push(layer.as_ref());
        }
    }
    let size = cols_to_check.len();
    for path in get_parquet_file_paths(parquet_path)? {
        let df_view = process_parquet_file_to_df(path.as_path(), &cols_to_check)?;
        df_view.check_cols_exist(&cols_to_check)?;
        load_edges_deletions_from_df(
            df_view,
            size,
            src,
            dst,
            time,
            layer,
            layer_in_df.unwrap_or(true),
            graph,
        )
        .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
    }
    Ok(())
}

pub(crate) fn process_parquet_file_to_df(
    parquet_file_path: &Path,
    col_names: &[&str],
) -> Result<DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>, GraphError> {
    let (names, chunks) = read_parquet_file(parquet_file_path, col_names)?;

    let names: Vec<String> = names
        .into_iter()
        .filter(|x| col_names.contains(&x.as_str()))
        .collect();

    let chunks = chunks.into_iter().map(move |result| {
        result
            .map(|r| DFChunk {
                chunk: r.into_iter().map(|boxed| boxed.clone()).collect_vec(),
            })
            .map_err(|e| {
                GraphError::LoadFailure(format!("Failed to process Parquet file: {:?}", e))
            })
    });

    Ok(DFView { names, chunks })
}

fn read_parquet_file(
    path: impl AsRef<Path>,
    col_names: &[&str],
) -> Result<(Vec<String>, FileReader<File>), GraphError> {
    let read_schema = |metadata: &FileMetaData| -> Result<ArrowSchema, GraphError> {
        let schema = read::infer_schema(metadata)?;
        let fields = schema
            .fields
            .iter()
            .map(|f| {
                if f.data_type == DataType::Utf8View {
                    Field::new(f.name.clone(), DataType::LargeUtf8, f.is_nullable)
                } else {
                    f.clone()
                }
            })
            .filter(|f| {
                // Filtered fields to avoid loading data that is not needed
                col_names.contains(&f.name.as_str())
            })
            .collect::<Vec<_>>();

        Ok(ArrowSchema::from(fields).with_metadata(schema.metadata))
    };

    let mut file = std::fs::File::open(&path)?;
    let metadata = read_metadata(&mut file)?;
    let row_groups = metadata.clone().row_groups;
    let schema = read_schema(&metadata)?;

    // Although fields are already filtered by col_names, we need names in the order as it appears
    // in the schema to create PretendDF
    let names = schema.fields.iter().map(|f| f.name.clone()).collect_vec();

    let reader = FileReader::new(file, row_groups, schema, None, None, None);
    Ok((names, reader))
}

fn get_parquet_file_paths(parquet_path: &Path) -> Result<Vec<PathBuf>, GraphError> {
    let mut parquet_files = Vec::new();
    if parquet_path.is_file() {
        parquet_files.push(parquet_path.to_path_buf());
    } else if parquet_path.is_dir() {
        for entry in fs::read_dir(parquet_path).expect("Directory not found") {
            let entry = entry.expect("Unable to read entry");
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "parquet") {
                parquet_files.push(path);
            }
        }
    } else {
        return Err(GraphError::from(PathDoesNotExist(
            parquet_path.to_path_buf(),
        )));
    }

    Ok(parquet_files)
}

#[cfg(test)]
mod test {
    use super::*;
    use polars_arrow::array::{Array, PrimitiveArray, Utf8Array};
    use std::path::PathBuf;

    #[test]
    fn test_process_parquet_file_to_df() {
        let parquet_file_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("resources/test/test_data.parquet");

        let col_names: &[&str] = &["src", "dst", "time", "weight", "marbles"];
        let df = process_parquet_file_to_df(parquet_file_path.as_path(), col_names).unwrap();

        let expected_names: Vec<String> = vec!["src", "dst", "time", "weight", "marbles"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let expected_chunks: Vec<Vec<Box<dyn Array>>> = vec![vec![
            Box::new(PrimitiveArray::<i64>::from_values(vec![1, 2, 3, 4, 5])),
            Box::new(PrimitiveArray::<i64>::from_values(vec![2, 3, 4, 5, 6])),
            Box::new(PrimitiveArray::<i64>::from_values(vec![1, 2, 3, 4, 5])),
            Box::new(PrimitiveArray::<f64>::from_values(vec![
                1f64, 2f64, 3f64, 4f64, 5f64,
            ])),
            Box::new(Utf8Array::<i64>::from_iter_values(
                vec!["red", "blue", "green", "yellow", "purple"].into_iter(),
            )),
        ]];

        let actual_names = df.names;
        let chunks: Vec<Result<DFChunk, GraphError>> = df.chunks.collect_vec();
        let chunks: Result<Vec<DFChunk>, GraphError> = chunks.into_iter().collect();
        let chunks: Vec<DFChunk> = chunks.unwrap();
        let actual_chunks: Vec<Vec<Box<dyn Array>>> =
            chunks.into_iter().map(|c: DFChunk| c.chunk).collect_vec();

        assert_eq!(actual_names, expected_names);
        assert_eq!(actual_chunks, expected_chunks);
    }
}
