#[cfg(test)]
mod test {
    use raphtory::{arrow_loader::dataframe::DFChunk, errors::GraphError};
    use raphtory::io::parquet_loaders::*;
    use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
    use itertools::Itertools;
    use std::{path::PathBuf, sync::Arc};

    #[test]
    fn test_process_parquet_file_to_df() {
        let parquet_file_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../raphtory/resources/test/test_data.parquet");

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
