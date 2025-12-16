use crate::{
    db::api::view::StaticGraphViewOps,
    errors::{GraphError, InvalidPathReason::PathDoesNotExist},
    io::arrow::{dataframe::*, df_loaders::*},
    prelude::{AdditionOps, DeletionOps, PropertyAdditionOps},
    python::graph::io::arrow_loaders::cast_columns,
    serialise::incremental::InternalCache,
};
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ProjectionMask};
use raphtory_api::core::entities::properties::prop::{Prop, PropType};
use std::{
    collections::HashMap,
    fs,
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};
#[cfg(feature = "storage")]
use {arrow::array::StructArray, pometry_storage::RAError};

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
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    batch_size: Option<usize>,
    schema: Option<Arc<HashMap<String, PropType>>>,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![id, time];
    cols_to_check.extend_from_slice(properties);
    cols_to_check.extend_from_slice(metadata);
    if let Some(ref node_type_col) = node_type_col {
        cols_to_check.push(node_type_col.as_ref());
    }

    for path in get_parquet_file_paths(parquet_path)? {
        let df_view = process_parquet_file_to_df(
            path.as_path(),
            Some(&cols_to_check),
            batch_size,
            schema.clone(),
        )?;
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
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_col: Option<&str>,
    batch_size: Option<usize>,
    schema: Option<Arc<HashMap<String, PropType>>>,
) -> Result<(), GraphError> {
    let parquet_path = parquet_path.as_ref();
    let mut cols_to_check = vec![src, dst, time];
    cols_to_check.extend_from_slice(properties);
    cols_to_check.extend_from_slice(metadata);

    if let Some(ref layer_col) = layer_col {
        cols_to_check.push(layer_col.as_ref());
    }

    let all_files = get_parquet_file_paths(parquet_path)?
        .into_iter()
        .map(|file| {
            let (names, _, num_rows) =
                read_parquet_file(file, (!cols_to_check.is_empty()).then_some(&cols_to_check))?;
            Ok::<_, GraphError>((names, num_rows))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut count_rows = 0;
    let mut all_names = Vec::new();
    for (names, num_rows) in all_files {
        count_rows += num_rows;
        if all_names.is_empty() {
            all_names = names;
        } else if all_names != names {
            return Err(GraphError::LoadFailure(
                "Parquet files have different column names".to_string(),
            ));
        }
    }

    let all_df_view = get_parquet_file_paths(parquet_path)?
        .into_iter()
        .flat_map(|file| {
            let df_view = process_parquet_file_to_df(
                file.as_path(),
                Some(&cols_to_check),
                batch_size,
                schema.clone(),
            )
            .expect("Failed to process Parquet file");
            df_view.chunks
        });

    let df_view = DFView {
        names: all_names,
        chunks: all_df_view,
        num_rows: Some(count_rows),
    };

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
    .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;

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
    metadata_properties: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    batch_size: Option<usize>,
    schema: Option<Arc<HashMap<String, PropType>>>,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![id];
    cols_to_check.extend_from_slice(metadata_properties);

    if let Some(ref node_type_col) = node_type_col {
        cols_to_check.push(node_type_col.as_ref());
    }

    for path in get_parquet_file_paths(parquet_path)? {
        let df_view = process_parquet_file_to_df(
            path.as_path(),
            Some(&cols_to_check),
            batch_size,
            schema.clone(),
        )?;
        df_view.check_cols_exist(&cols_to_check)?;

        load_node_props_from_df(
            df_view,
            id,
            node_type,
            node_type_col,
            metadata_properties,
            shared_metadata,
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
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    layer_col: Option<&str>,
    batch_size: Option<usize>,
    schema: Option<Arc<HashMap<String, PropType>>>,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![src, dst];
    if let Some(ref layer_col) = layer_col {
        cols_to_check.push(layer_col.as_ref());
    }

    cols_to_check.extend_from_slice(metadata);

    for path in get_parquet_file_paths(parquet_path)? {
        let df_view = process_parquet_file_to_df(
            path.as_path(),
            Some(&cols_to_check),
            batch_size,
            schema.clone(),
        )?;
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
    batch_size: Option<usize>,
    schema: Option<Arc<HashMap<String, PropType>>>,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![src, dst, time];
    if let Some(ref layer_col) = layer_col {
        cols_to_check.push(layer_col.as_ref());
    }

    for path in get_parquet_file_paths(parquet_path)? {
        let df_view = process_parquet_file_to_df(
            path.as_path(),
            Some(&cols_to_check),
            batch_size,
            schema.clone(),
        )?;
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
    metadata: &[&str],
    batch_size: Option<usize>,
    schema: Option<Arc<HashMap<String, PropType>>>,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![time];
    cols_to_check.extend_from_slice(properties);
    cols_to_check.extend_from_slice(metadata);

    for path in get_parquet_file_paths(parquet_path)? {
        let df_view = process_parquet_file_to_df(
            path.as_path(),
            Some(&cols_to_check),
            batch_size,
            schema.clone(),
        )?;
        df_view.check_cols_exist(&cols_to_check)?;
        load_graph_props_from_df(df_view, time, Some(properties), Some(metadata), graph)
            .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
    }

    Ok(())
}

pub(crate) fn process_parquet_file_to_df(
    parquet_file_path: &Path,
    col_names: Option<&[&str]>,
    batch_size: Option<usize>,
    schema: Option<Arc<HashMap<String, PropType>>>,
) -> Result<DFView<impl Iterator<Item = Result<DFChunk, GraphError>>>, GraphError> {
    let (names, chunks, num_rows) = read_parquet_file(parquet_file_path, col_names)?;

    let names: Vec<String> = names
        .into_iter()
        .filter(|x| col_names.map(|cn| cn.contains(&x.as_str())).unwrap_or(true))
        .collect();

    let chunks = match batch_size {
        None => chunks,
        Some(batch_size) => chunks.with_batch_size(batch_size),
    };

    let chunks = chunks.build()?.into_iter().map(move |result| match result {
        Ok(r) => {
            let casted_batch = if let Some(schema) = schema.as_deref() {
                cast_columns(r, schema)?
            } else {
                r
            };
            Ok(DFChunk {
                chunk: casted_batch.columns().to_vec(),
            })
        }
        Err(e) => Err(GraphError::LoadFailure(format!(
            "Failed to process Parquet file: {e:?}"
        ))),
    });

    Ok(DFView {
        names,
        chunks,
        num_rows: Some(num_rows),
    })
}

pub fn read_parquet_file(
    path: impl AsRef<Path>,
    col_names: Option<&[&str]>,
) -> Result<(Vec<String>, ParquetRecordBatchReaderBuilder<File>, usize), GraphError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
    let metadata = builder.metadata();
    let num_rows = metadata.file_metadata().num_rows() as usize;
    let schema = builder.schema();
    let (idx, names): (Vec<_>, Vec<_>) = schema
        .fields
        .into_iter()
        .enumerate()
        .filter_map(|(idx, field)| {
            col_names
                .is_none_or(|filter| filter.contains(&field.name().as_str()))
                .then(|| (idx, field.name().clone()))
        })
        .unzip();
    let projection = ProjectionMask::roots(builder.parquet_schema(), idx);
    Ok((names, builder.with_projection(projection), num_rows))
}

pub fn get_parquet_file_paths(parquet_path: &Path) -> Result<Vec<PathBuf>, GraphError> {
    let mut parquet_files = Vec::new();
    if parquet_path.is_file() {
        parquet_files.push(parquet_path.to_path_buf());
    } else if parquet_path.is_dir() {
        for entry in fs::read_dir(parquet_path)? {
            let path = entry?.path();
            if path.extension().is_some_and(|ext| ext == "parquet") {
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
    let readers = get_parquet_file_paths(path)?
        .into_iter()
        .map(|path| {
            read_parquet_file(path, col_names)
                .and_then(|(_, reader, _)| Ok::<_, GraphError>(reader.build()?))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let chunks = readers.into_iter().flat_map(|iter| {
        iter.map(move |cols| {
            cols.map(|col| StructArray::from(col))
                .map_err(RAError::ArrowRs)
        })
    });
    Ok(chunks)
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
    use itertools::Itertools;
    use std::{path::PathBuf, sync::Arc};

    #[test]
    fn test_process_parquet_file_to_df() {
        let parquet_file_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("resources/test/test_data.parquet");

        let col_names: &[&str] = &["src", "dst", "time", "weight", "marbles"];
        let df =
            process_parquet_file_to_df(parquet_file_path.as_path(), Some(col_names), None).unwrap();

        let expected_names: Vec<String> = ["src", "dst", "time", "weight", "marbles"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let expected_chunks: Vec<Vec<ArrayRef>> = vec![vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3, 4, 5])),
            Arc::new(Int64Array::from(vec![2i64, 3, 4, 5, 6])),
            Arc::new(Int64Array::from(vec![1i64, 2, 3, 4, 5])),
            Arc::new(Float64Array::from(vec![1f64, 2f64, 3f64, 4f64, 5f64])),
            Arc::new(StringArray::from(vec![
                "red", "blue", "green", "yellow", "purple",
            ])),
        ]];

        let actual_names = df.names;
        let chunks: Vec<Result<DFChunk, GraphError>> = df.chunks.collect_vec();
        let chunks: Result<Vec<DFChunk>, GraphError> = chunks.into_iter().collect();
        let chunks: Vec<DFChunk> = chunks.unwrap();
        let actual_chunks: Vec<Vec<ArrayRef>> =
            chunks.into_iter().map(|c: DFChunk| c.chunk).collect_vec();

        assert_eq!(actual_names, expected_names);
        assert_eq!(actual_chunks, expected_chunks);
    }
}
