use crate::{
    core::utils::errors::{GraphError, LoadError},
    io::arrow::node_col::{lift_node_col, NodeCol},
};
use itertools::Itertools;
use polars_arrow::{
    array::{Array, PrimitiveArray, StaticArray},
    compute::cast::{self, CastOptions},
    datatypes::{ArrowDataType as DataType, TimeUnit},
};
use rayon::prelude::*;

pub(crate) struct DFView<I> {
    pub names: Vec<String>,
    pub(crate) chunks: I,
    pub num_rows: usize,
}

impl<I, E> DFView<I>
where
    I: Iterator<Item = Result<DFChunk, E>>,
{
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

pub struct TimeCol(PrimitiveArray<i64>);

impl TimeCol {
    fn new(arr: &dyn Array) -> Result<Self, LoadError> {
        let arr = arr
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()
            .ok_or_else(|| LoadError::InvalidTimestamp(arr.data_type().clone()))?;
        let arr = if let DataType::Timestamp(_, _) = arr.data_type() {
            let array = cast::cast(
                arr,
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
            arr.clone()
        };
        Ok(Self(arr))
    }

    pub fn par_iter(&self) -> impl IndexedParallelIterator<Item = Option<i64>> + '_ {
        (0..self.0.len()).into_par_iter().map(|i| self.0.get(i))
    }

    pub fn get(&self, i: usize) -> Option<i64> {
        self.0.get(i)
    }
}

#[derive(Clone)]
pub(crate) struct DFChunk {
    pub(crate) chunk: Vec<Box<dyn Array>>,
}

impl DFChunk {
    pub fn len(&self) -> usize {
        self.chunk.first().map(|c| c.len()).unwrap_or(0)
    }

    pub fn node_col(&self, index: usize) -> Result<NodeCol, LoadError> {
        lift_node_col(index, self)
    }

    pub fn time_col(&self, index: usize) -> Result<TimeCol, LoadError> {
        TimeCol::new(self.chunk[index].as_ref())
    }
}
