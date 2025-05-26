use crate::{
    db::api::view::StaticGraphViewOps,
    errors::InvalidPathReason::PathDoesNotExist,
    io::arrow::{dataframe::*, df_loaders::*},
    prelude::DeletionOps,
    serialise::incremental::InternalCache,
};
use itertools::Itertools;
use polars_arrow::datatypes::ArrowSchema;
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

use crate::{
    errors::GraphError,
    prelude::{AdditionOps, PropertyAdditionOps},
};
#[cfg(feature = "storage")]
use polars_arrow::{
    array::StructArray,
    datatypes::{ArrowDataType, Field},
};
#[cfg(feature = "storage")]
use pometry_storage::RAError;
use raphtory_api::core::entities::properties::prop::Prop;

pub fn load_nodes_from_parquet<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + InternalCache,
>(
    graph: &G,
    parquet_path: &Path,
    time: &str,
    id: &str,
    node_type: Option<&str>,
    node_type_col: Option<&str>,
    properties: &[&str],
    constant_properties: &[&str],
    shared_constant_properties: Option<&HashMap<String, Prop>>,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![id, time];
    cols_to_check.extend_from_slice(&properties);
    cols_to_check.extend_from_slice(&constant_properties);
    if let Some(ref node_type_col) = node_type_col {
        cols_to_check.push(node_type_col.as_ref());
    }

    for path in get_parquet_file_paths(parquet_path)? {
        let df_view = process_parquet_file_to_df(path.as_path(), Some(&cols_to_check))?;
        df_view.check_cols_exist(&cols_to_check)?;
        load_nodes_from_df(
            df_view,
            time,
            id,
            &properties,
            &constant_properties,
            shared_constant_properties,
            node_type,
            node_type_col,
            graph,
        )
        .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
    }

    Ok(())
}

pub fn load_edges_from_parquet<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + InternalCache,
>(
    graph: &G,
    parquet_path: impl AsRef<Path>,
    time: &str,
    src: &str,
    dst: &str,
    properties: &[&str],
    constant_properties: &[&str],
    shared_constant_properties: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_col: Option<&str>,
) -> Result<(), GraphError> {
    let parquet_path = parquet_path.as_ref();
    let mut cols_to_check = vec![src, dst, time];
    cols_to_check.extend_from_slice(&properties);
    cols_to_check.extend_from_slice(&constant_properties);

    if let Some(ref layer_col) = layer_col {
        cols_to_check.push(layer_col.as_ref());
    }

    for path in get_parquet_file_paths(parquet_path)? {
        let df_view = process_parquet_file_to_df(path.as_path(), Some(&cols_to_check))?;
        df_view.check_cols_exist(&cols_to_check)?;
        load_edges_from_df(
            df_view,
            time,
            src,
            dst,
            &properties,
            &constant_properties,
            shared_constant_properties,
            layer,
            layer_col,
            graph,
        )
        .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
    }

    Ok(())
}

pub fn load_node_props_from_parquet<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + InternalCache,
>(
    graph: &G,
    parquet_path: &Path,
    id: &str,
    node_type: Option<&str>,
    node_type_col: Option<&str>,
    constant_properties: &[&str],
    shared_constant_properties: Option<&HashMap<String, Prop>>,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![id];
    cols_to_check.extend_from_slice(&constant_properties);

    if let Some(ref node_type_col) = node_type_col {
        cols_to_check.push(node_type_col.as_ref());
    }

    for path in get_parquet_file_paths(parquet_path)? {
        let df_view = process_parquet_file_to_df(path.as_path(), Some(&cols_to_check))?;
        df_view.check_cols_exist(&cols_to_check)?;

        load_node_props_from_df(
            df_view,
            id,
            node_type,
            node_type_col,
            &constant_properties,
            shared_constant_properties,
            graph,
        )
        .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
    }

    Ok(())
}

pub fn load_edge_props_from_parquet<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + InternalCache,
>(
    graph: &G,
    parquet_path: &Path,
    src: &str,
    dst: &str,
    constant_properties: &[&str],
    shared_const_properties: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_col: Option<&str>,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![src, dst];
    if let Some(ref layer_col) = layer_col {
        cols_to_check.push(layer_col.as_ref());
    }

    cols_to_check.extend_from_slice(&constant_properties);

    for path in get_parquet_file_paths(parquet_path)? {
        let df_view = process_parquet_file_to_df(path.as_path(), Some(&cols_to_check))?;
        df_view.check_cols_exist(&cols_to_check)?;
        load_edges_props_from_df(
            df_view,
            src,
            dst,
            &constant_properties,
            shared_const_properties,
            layer,
            layer_col,
            graph,
        )
        .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
    }

    Ok(())
}

pub fn load_edge_deletions_from_parquet<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + DeletionOps,
>(
    graph: &G,
    parquet_path: &Path,
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

    for path in get_parquet_file_paths(parquet_path)? {
        let df_view = process_parquet_file_to_df(path.as_path(), Some(&cols_to_check))?;
        df_view.check_cols_exist(&cols_to_check)?;
        load_edge_deletions_from_df(df_view, time, src, dst, layer, layer_col, graph)
            .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
    }
    Ok(())
}

pub fn load_graph_props_from_parquet<G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps>(
    graph: &G,
    parquet_path: &Path,
    time: &str,
    properties: &[&str],
    constant_properties: &[&str],
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![time];
    cols_to_check.extend_from_slice(&properties);
    cols_to_check.extend_from_slice(&constant_properties);

    for path in get_parquet_file_paths(parquet_path)? {
        let df_view = process_parquet_file_to_df(path.as_path(), Some(&cols_to_check))?;
        df_view.check_cols_exist(&cols_to_check)?;
        load_graph_props_from_df(
            df_view,
            time,
            Some(&properties),
            Some(&constant_properties),
            graph,
        )
        .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
    }

    Ok(())
}

pub(crate) fn process_parquet_file_to_df(
    parquet_file_path: &Path,
    col_names: Option<&[&str]>,
) -> Result<DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>, GraphError> {
    let (names, chunks, num_rows) = read_parquet_file(parquet_file_path, col_names)?;

    let names: Vec<String> = names
        .into_iter()
        .filter(|x| col_names.map(|cn| cn.contains(&x.as_str())).unwrap_or(true))
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

    Ok(DFView {
        names,
        chunks,
        num_rows,
    })
}

pub fn read_parquet_file(
    path: impl AsRef<Path>,
    col_names: Option<&[&str]>,
) -> Result<(Vec<String>, FileReader<File>, usize), GraphError> {
    let read_schema = |metadata: &FileMetaData| -> Result<(ArrowSchema, usize), GraphError> {
        let schema = read::infer_schema(metadata)?;
        let fields = schema
            .fields
            .into_iter()
            .filter(|f| {
                // Filtered fields to avoid loading data that is not needed
                col_names
                    .map(|cn| cn.contains(&f.name.as_str()))
                    .unwrap_or(true)
            })
            .collect::<Vec<_>>();

        Ok((
            ArrowSchema::from(fields).with_metadata(schema.metadata),
            metadata.num_rows,
        ))
    };

    let mut file = std::fs::File::open(&path)?;
    let metadata = read_metadata(&mut file)?;
    let row_groups = metadata.clone().row_groups;
    let (schema, num_rows) = read_schema(&metadata)?;

    // Although fields are already filtered by col_names, we need names in the order as it appears
    // in the schema to create PretendDF
    let names = schema.fields.iter().map(|f| f.name.clone()).collect_vec();

    let reader = FileReader::new(file, row_groups, schema, None);
    Ok((names, reader, num_rows))
}

pub fn get_parquet_file_paths(parquet_path: &Path) -> Result<Vec<PathBuf>, GraphError> {
    let mut parquet_files = Vec::new();
    if parquet_path.is_file() {
        parquet_files.push(parquet_path.to_path_buf());
    } else if parquet_path.is_dir() {
        for entry in fs::read_dir(parquet_path)? {
            let path = entry?.path();
            if path.extension().map_or(false, |ext| ext == "parquet") {
                parquet_files.push(path);
            }
        }
    } else {
        return Err(GraphError::from(PathDoesNotExist(
            parquet_path.to_path_buf(),
        )));
    }
    parquet_files.sort();

    Ok(parquet_files)
}

#[cfg(feature = "storage")]
pub fn read_struct_arrays(
    path: &Path,
    col_names: Option<&[&str]>,
) -> Result<impl Iterator<Item = Result<StructArray, RAError>>, GraphError> {
    let readers = get_parquet_file_paths(&path)?
        .into_iter()
        .map(|path| {
            read_parquet_file(path, col_names.as_deref())
                .map(|(col_names, reader, _)| (col_names, reader))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let chunks = readers.into_iter().flat_map(|(field_names, iter)| {
        iter.map(move |cols| {
            cols.map(|col| {
                let values = col.into_arrays();
                let fields = values
                    .iter()
                    .zip(field_names.iter())
                    .map(|(arr, field_name)| Field::new(field_name, arr.data_type().clone(), true))
                    .collect::<Vec<_>>();
                StructArray::new(ArrowDataType::Struct(fields), values, None)
            })
            .map_err(RAError::Arrow)
        })
    });
    Ok(chunks)
}

#[cfg(test)]
mod test {
    use super::*;
    use polars_arrow::{
        array::{Array, PrimitiveArray, StaticArray, Utf8ViewArray},
        datatypes::ArrowDataType,
    };
    use std::path::PathBuf;

    #[test]
    fn test_process_parquet_file_to_df() {
        let parquet_file_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("resources/test/test_data.parquet");

        let col_names: &[&str] = &["src", "dst", "time", "weight", "marbles"];
        let df = process_parquet_file_to_df(parquet_file_path.as_path(), Some(col_names)).unwrap();

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
            Box::new(Utf8ViewArray::from_vec(
                vec!["red", "blue", "green", "yellow", "purple"],
                ArrowDataType::Utf8View,
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
