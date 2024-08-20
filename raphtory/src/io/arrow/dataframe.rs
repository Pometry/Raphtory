use crate::core::utils::errors::GraphError;

use polars_arrow::{
    array::{Array, PrimitiveArray, Utf8Array},
    compute::cast::{self, CastOptions},
    datatypes::{ArrowDataType as DataType, TimeUnit},
    offset::Offset,
    types::NativeType,
};

use itertools::Itertools;

pub(crate) struct DFView<I> {
    pub names: Vec<String>,
    pub(crate) chunks: I,
    pub num_rows:usize
}

impl<I, E> DFView<I>
where
    I: Iterator<Item = Result<DFChunk, E>>,
{
    pub(crate) fn new(names: Vec<String>, chunks: I,num_rows:usize) -> Self {
        Self { names, chunks,num_rows }
    }

    pub fn check_cols_exist(&self, cols: &[&str]) -> Result<(), GraphError> {
        let non_cols: Vec<&&str> = cols
            .iter()
            .filter(|c| !self.names.contains(&c.to_string()))
            .collect();
        if non_cols.len() > 0 {
            return Err(GraphError::ColumnDoesNotExist(non_cols.iter().join(", ")));
        }

        Ok(())
    }

    pub(crate) fn get_index(&self, name: &str) -> Result<usize, GraphError> {
        self.names
            .iter()
            .position(|n| n == name)
            .ok_or_else(|| GraphError::ColumnDoesNotExist(name.to_string()))
    }
}

#[derive(Clone)]
pub(crate) struct DFChunk {
    pub(crate) chunk: Vec<Box<dyn Array>>,
}

impl DFChunk {
    pub(crate) fn get_inner_size(&self) -> usize {
        self.chunk.first().map(|arr| arr.len()).unwrap_or(0)
    }

    pub(crate) fn iter_col<T: NativeType>(
        &self,
        idx: usize,
    ) -> Option<impl Iterator<Item = Option<&T>> + '_> {
        let col_arr = (&self.chunk)[idx]
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()?;
        Some(col_arr.iter())
    }

    pub fn utf8<O: Offset>(&self, idx: usize) -> Option<impl Iterator<Item = Option<&str>> + '_> {
        // test that it's actually a utf8 array
        let col_arr = (&self.chunk)[idx].as_any().downcast_ref::<Utf8Array<O>>()?;

        Some(col_arr.iter())
    }

    pub fn time_iter_col(&self, idx: usize) -> Option<impl Iterator<Item = Option<i64>> + '_> {
        let col_arr = (&self.chunk)[idx]
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()?;

        let arr = if let DataType::Timestamp(_, _) = col_arr.data_type() {
            let array = cast::cast(
                col_arr,
                &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".to_string())),
                CastOptions::default(),
            )
            .unwrap();
            array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap()
                .clone()
        } else {
            col_arr.clone()
        };

        Some(arr.into_iter())
    }
}
