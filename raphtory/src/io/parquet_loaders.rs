use crate::core::{entities::graph::tgraph::InternalGraph, utils::errors::GraphError, Prop};
use itertools::Itertools;
use polars_arrow::{
    array::Array,
    datatypes::{ArrowDataType as DataType, ArrowSchema, Field},
    legacy::error,
    record_batch::RecordBatch as Chunk,
};
use polars_parquet::{
    read,
    read::{read_metadata, FileMetaData, FileReader},
};
use std::{collections::HashMap, path::Path};
use crate::io::arrow::{dataframe::*, df_loaders::*};

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
    let mut cols_to_check = vec![id, time];
    cols_to_check.extend(properties.as_ref().unwrap_or(&Vec::new()));
    cols_to_check.extend(const_properties.as_ref().unwrap_or(&Vec::new()));
    if node_type_in_df.unwrap_or(true) {
        if let Some(ref node_type) = node_type {
            cols_to_check.push(node_type.as_ref());
        }
    }

    let df = process_parquet_file_to_df(parquet_file_path, cols_to_check.clone())?;
    df.check_cols_exist(&cols_to_check)?;
    let size = df.get_inner_size();

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
    .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;

    Ok(())
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
    let mut cols_to_check = vec![src, dst, time];
    cols_to_check.extend(properties.as_ref().unwrap_or(&Vec::new()));
    cols_to_check.extend(const_properties.as_ref().unwrap_or(&Vec::new()));
    if layer_in_df.unwrap_or(false) {
        if let Some(ref layer) = layer {
            cols_to_check.push(layer.as_ref());
        }
    }

    let df = process_parquet_file_to_df(parquet_file_path, cols_to_check.clone())?;
    df.check_cols_exist(&cols_to_check)?;
    let size = cols_to_check.len();

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
    .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;

    Ok(())
}

pub fn load_node_props_from_parquet(
    graph: &InternalGraph,
    parquet_file_path: &Path,
    id: &str,
    const_properties: Option<Vec<&str>>,
    shared_const_properties: Option<HashMap<String, Prop>>,
) -> Result<(), GraphError> {
    let mut cols_to_check = vec![id];
    cols_to_check.extend(const_properties.as_ref().unwrap_or(&Vec::new()));

    let df = process_parquet_file_to_df(parquet_file_path, cols_to_check.clone())?;
    df.check_cols_exist(&cols_to_check)?;
    let size = cols_to_check.len();

    load_node_props_from_df(
        &df,
        size,
        id,
        const_properties,
        shared_const_properties,
        graph,
    )
    .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;

    Ok(())
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
    let mut cols_to_check = vec![src, dst];
    if layer_in_df.unwrap_or(false) {
        if let Some(ref layer) = layer {
            cols_to_check.push(layer.as_ref());
        }
    }
    cols_to_check.extend(const_properties.as_ref().unwrap_or(&Vec::new()));

    let df = process_parquet_file_to_df(parquet_file_path, cols_to_check.clone())?;
    df.check_cols_exist(&cols_to_check)?;
    let size = cols_to_check.len();

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
    .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;

    Ok(())
}

pub fn load_edges_deletions_from_parquet(
    graph: &InternalGraph,
    parquet_file_path: &Path,
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

    let df = process_parquet_file_to_df(parquet_file_path, cols_to_check.clone())?;
    df.check_cols_exist(&cols_to_check)?;
    let size = cols_to_check.len();

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
    .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;

    Ok(())
}

pub(crate) fn process_parquet_file_to_df(
    parquet_file_path: &Path,
    col_names: Vec<&str>,
) -> Result<PretendDF, GraphError> {
    let (names, arrays) = read_parquet_file(parquet_file_path, &col_names)?;

    let names = names
        .into_iter()
        .filter(|x| col_names.contains(&x.as_str()))
        .collect();
    let arrays = arrays
        .map_ok(|r| r.into_iter().map(|boxed| boxed.clone()).collect_vec())
        .collect::<Result<Vec<_>, _>>()?;

    Ok(PretendDF { names, arrays })
}

fn read_parquet_file(
    path: impl AsRef<Path>,
    col_names: &Vec<&str>,
) -> Result<
    (
        Vec<String>,
        impl Iterator<Item = Result<Chunk<Box<dyn Array>>, error::PolarsError>>,
    ),
    GraphError,
> {
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

#[cfg(test)]
mod test {
    use super::*;
    use polars_arrow::array::{PrimitiveArray, Utf8Array};
    use std::path::PathBuf;

    #[test]
    fn test_process_parquet_file_to_df() {
        let parquet_file_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(|p| p.join("raphtory/resources/test/test_data.parquet"))
            .unwrap();

        let col_names = vec!["src", "dst", "time", "weight", "marbles"];
        let df = process_parquet_file_to_df(parquet_file_path.as_path(), col_names).unwrap();

        let df1 = PretendDF {
            names: vec!["src", "dst", "time", "weight", "marbles"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            arrays: vec![vec![
                Box::new(PrimitiveArray::<i64>::from_values(vec![1, 2, 3, 4, 5])),
                Box::new(PrimitiveArray::<i64>::from_values(vec![2, 3, 4, 5, 6])),
                Box::new(PrimitiveArray::<i64>::from_values(vec![1, 2, 3, 4, 5])),
                Box::new(PrimitiveArray::<f64>::from_values(vec![
                    1f64, 2f64, 3f64, 4f64, 5f64,
                ])),
                Box::new(Utf8Array::<i64>::from_iter_values(
                    vec!["red", "blue", "green", "yellow", "purple"].into_iter(),
                )),
            ]],
        };

        assert_eq!(df.names, df1.names);
        assert_eq!(df.arrays, df1.arrays);
    }
}
