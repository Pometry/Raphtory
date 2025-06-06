use crate::{
    errors::{GraphError, LoadError},
    io::arrow::node_col::{lift_node_col, NodeCol},
};
use itertools::Itertools;
use polars_arrow::{
    array::{Array, PrimitiveArray, StaticArray},
    compute::cast::{self, CastOptionsImpl},
    datatypes::{ArrowDataType as DataType, TimeUnit},
};
use rayon::prelude::*;
use std::fmt::{Debug, Formatter};

pub(crate) struct DFView<I> {
    pub names: Vec<String>,
    pub(crate) chunks: I,
    pub num_rows: usize,
}

impl<I> Debug for DFView<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DFView")
            .field("names", &self.names)
            .field("num_rows", &self.num_rows)
            .finish()
    }
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
        if !non_cols.is_empty() {
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
        if arr.null_count() > 0 {
            return Err(LoadError::MissingTimeError);
        }
        let arr = if let DataType::Timestamp(_, _) = arr.data_type() {
            let array = cast::cast(
                arr,
                &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".to_string())),
                CastOptionsImpl::default(),
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
        (0..self.0.len()).into_par_iter().map(|i| self.get(i))
    }

    pub fn iter(&self) -> impl Iterator<Item = i64> + '_ {
        self.0.values_iter().copied()
    }

    pub fn get(&self, i: usize) -> Option<i64> {
        self.0.get(i)
    }
}

#[derive(Clone, Debug)]
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
