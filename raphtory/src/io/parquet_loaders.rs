use crate::{
    db::api::view::StaticGraphViewOps,
    errors::GraphError,
    io::arrow::{
        dataframe::*,
        df_loaders::{
            edges::{load_edges_from_df, ColumnNames},
            nodes::{load_node_props_from_df, load_nodes_from_df},
            *,
        },
    },
    prelude::{AdditionOps, DeletionOps, PropertyAdditionOps},
};
use arrow::{
    array::{Array, RecordBatch, StructArray},
    compute::cast,
    datatypes::{DataType, Field, Fields},
};
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ProjectionMask};
use raphtory_api::core::entities::properties::prop::{arrow_dtype_from_prop_type, Prop, PropType};
use std::{
    collections::HashMap,
    ffi::OsStr,
    fs,
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

pub(crate) fn is_parquet_path(path: &PathBuf) -> Result<bool, std::io::Error> {
    if path.is_dir() {
        Ok(fs::read_dir(&path)?.any(|entry| {
            entry.map_or(false, |e| {
                e.path().extension().and_then(OsStr::to_str) == Some("parquet")
            })
        }))
    } else {
        Ok(path.extension().and_then(OsStr::to_str) == Some("parquet"))
    }
}

pub fn load_nodes_from_parquet<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + std::fmt::Debug,
>(
    graph: &G,
    parquet_path: &Path,
    time: &str,
    secondary_index: Option<&str>,
    id: &str,
    node_type: Option<&str>,
    node_type_col: Option<&str>,
    properties: &[&str],
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    batch_size: Option<usize>,
    resolve_nodes: bool,
    schema: Option<Arc<HashMap<String, PropType>>>,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![id, time];

    cols_to_check.extend_from_slice(properties);
    cols_to_check.extend_from_slice(metadata);

    if let Some(ref node_type_col) = node_type_col {
        cols_to_check.push(node_type_col.as_ref());
    }

    if let Some(ref secondary_index) = secondary_index {
        cols_to_check.push(secondary_index.as_ref());
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
            secondary_index,
            id,
            properties,
            metadata,
            shared_metadata,
            node_type,
            node_type_col,
            graph,
            resolve_nodes,
        )?;
    }

    Ok(())
}

pub fn load_edges_from_parquet<G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps>(
    graph: &G,
    parquet_path: impl AsRef<Path>,
    column_names: ColumnNames,
    resolve_nodes: bool,
    properties: &[&str],
    metadata: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    layer: Option<&str>,
    batch_size: Option<usize>,
    schema: Option<Arc<HashMap<String, PropType>>>,
) -> Result<(), GraphError> {
    let ColumnNames {
        time,
        secondary_index,
        src,
        dst,
        layer_col,
        layer_id_col,
        edge_id,
    } = column_names;

    let parquet_path = parquet_path.as_ref();
    let mut cols_to_check = [src, dst, time]
        .into_iter()
        .chain(layer_id_col)
        .chain(edge_id)
        .collect::<Vec<_>>();

    cols_to_check.extend_from_slice(properties);
    cols_to_check.extend_from_slice(metadata);

    if let Some(ref layer_col) = layer_col {
        cols_to_check.push(layer_col.as_ref());
    }
    if let Some(ref secondary_index) = secondary_index {
        cols_to_check.push(secondary_index.as_ref());
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
        column_names,
        resolve_nodes,
        properties,
        metadata,
        shared_metadata,
        layer,
        graph,
        false,
    )?;

    Ok(())
}

pub fn load_node_metadata_from_parquet<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + std::fmt::Debug,
>(
    graph: &G,
    parquet_path: &Path,
    id: &str,
    node_type: Option<&str>,
    node_type_col: Option<&str>,
    node_id_col: Option<&str>,      // for inner parquet use only
    node_type_id_col: Option<&str>, // for inner parquet use only
    metadata_properties: &[&str],
    shared_metadata: Option<&HashMap<String, Prop>>,
    batch_size: Option<usize>,
    schema: Option<Arc<HashMap<String, PropType>>>,
) -> Result<(), GraphError> {
    let mut cols_to_check = std::iter::once(id)
        .chain(node_type_id_col)
        .chain(node_type_col)
        .chain(node_id_col)
        .collect::<Vec<_>>();

    cols_to_check.extend_from_slice(metadata_properties);

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
            node_id_col,
            node_type_id_col,
            metadata_properties,
            shared_metadata,
            graph,
        )?;
    }

    Ok(())
}

pub fn load_edge_metadata_from_parquet<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps,
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
    resolve_nodes: bool,
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
            resolve_nodes,
        )?;
    }

    Ok(())
}

pub fn load_edge_deletions_from_parquet<
    G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps + DeletionOps,
>(
    graph: &G,
    parquet_path: &Path,
    column_names: ColumnNames,
    layer: Option<&str>,
    resolve_nodes: bool,
    batch_size: Option<usize>,
    schema: Option<Arc<HashMap<String, PropType>>>,
) -> Result<(), GraphError> {
    let ColumnNames {
        time,
        secondary_index,
        src,
        dst,
        edge_id,
        layer_col,
        layer_id_col,
    } = column_names;
    let cols_to_check = vec![src, dst, time]
        .into_iter()
        .chain(secondary_index)
        .chain(layer_col)
        .chain(layer_id_col)
        .chain(edge_id)
        .collect::<Vec<_>>();

    for path in get_parquet_file_paths(parquet_path)? {
        let df_view = process_parquet_file_to_df(
            path.as_path(),
            Some(&cols_to_check),
            batch_size,
            schema.clone(),
        )?;
        df_view.check_cols_exist(&cols_to_check)?;
        load_edge_deletions_from_df(df_view, column_names, resolve_nodes, layer, graph)?;
    }
    Ok(())
}

pub fn load_graph_props_from_parquet<G: StaticGraphViewOps + PropertyAdditionOps + AdditionOps>(
    graph: &G,
    parquet_path: &Path,
    time: &str,
    secondary_index: Option<&str>,
    properties: &[&str],
    metadata: &[&str],
    batch_size: Option<usize>,
    schema: Option<Arc<HashMap<String, PropType>>>,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![time];

    cols_to_check.extend_from_slice(properties);
    cols_to_check.extend_from_slice(metadata);

    if let Some(ref secondary_index) = secondary_index {
        cols_to_check.push(secondary_index.as_ref());
    }

    for path in get_parquet_file_paths(parquet_path)? {
        let df_view = process_parquet_file_to_df(
            path.as_path(),
            Some(&cols_to_check),
            batch_size,
            schema.clone(),
        )?;
        df_view.check_cols_exist(&cols_to_check)?;
        load_graph_props_from_df(
            df_view,
            time,
            secondary_index,
            Some(properties),
            Some(metadata),
            graph,
        )?;
    }

    Ok(())
}

pub(crate) fn process_parquet_file_to_df(
    parquet_file_path: &Path,
    col_names: Option<&[&str]>,
    batch_size: Option<usize>,
    schema: Option<Arc<HashMap<String, PropType>>>,
) -> Result<DFView<impl Iterator<Item = Result<DFChunk, GraphError>> + Send>, GraphError> {
    let (names, chunks, num_rows) = read_parquet_file(parquet_file_path, col_names)?;

    let names: Vec<String> = names
        .into_iter()
        .filter(|x| col_names.map(|cn| cn.contains(&x.as_str())).unwrap_or(true))
        .collect();

    let chunks = match batch_size {
        None => chunks.with_batch_size(100_000),
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
        return Err(GraphError::PathDoesNotExist(parquet_path.to_path_buf()));
    }
    parquet_files.sort();

    Ok(parquet_files)
}

pub(crate) fn cast_columns(
    batch: RecordBatch,
    schema: &HashMap<String, PropType>,
) -> Result<RecordBatch, GraphError> {
    let old_schema_ref = batch.schema();
    let old_fields = old_schema_ref.fields();

    let mut target_fields: Vec<Field> = Vec::with_capacity(old_fields.len());

    for field in old_fields.iter() {
        if let Some(target_prop_type) = schema.get(field.name()) {
            let target_dtype = arrow_dtype_from_prop_type(target_prop_type);
            target_fields.push(
                Field::new(field.name(), target_dtype, field.is_nullable())
                    .with_metadata(field.metadata().clone()),
            );
        } else {
            // schema doesn't say anything about this column
            target_fields.push(field.as_ref().clone());
        }
    }
    let struct_array = StructArray::from(batch);
    let target_struct_type = DataType::Struct(Fields::from(target_fields));

    // cast whole RecordBatch at once
    let casted = cast(&struct_array, &target_struct_type).map_err(|e| {
        GraphError::LoadFailure(format!(
            "Failed to cast RecordBatch to target schema {:?}: {e}",
            target_struct_type
        ))
    })?;

    let casted_struct = casted
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            GraphError::LoadFailure(
                "Internal error: casting RecordBatch did not return StructArray".to_string(),
            )
        })?;

    Ok(RecordBatch::from(casted_struct))
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
            process_parquet_file_to_df(parquet_file_path.as_path(), Some(col_names), None, None)
                .unwrap();

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
