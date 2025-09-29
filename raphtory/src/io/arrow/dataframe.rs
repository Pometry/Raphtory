use crate::{
    errors::{GraphError, LoadError},
    io::arrow::node_col::{lift_node_col, NodeCol},
};
use arrow_array::{
    cast::AsArray,
    types::{Int64Type, TimestampMillisecondType},
    Array, ArrayRef, ArrowPrimitiveType, Int64Array, PrimitiveArray,
};
use arrow_cast::cast;
use arrow_schema::{DataType, TimeUnit};
use itertools::Itertools;
use rayon::prelude::*;
use std::{
    fmt::{Debug, Formatter},
    ops::Deref,
};

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

    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }
}

pub struct TimeCol(PrimitiveArray<Int64Type>);

impl TimeCol {
    fn new(arr: &dyn Array) -> Result<Self, LoadError> {
        if arr.null_count() > 0 {
            return Err(LoadError::MissingTimeError);
        }
        match arr.data_type() {
            DataType::Int64 => Ok(Self(arr.as_primitive::<Int64Type>().clone())),
            DataType::Timestamp(_, _) => {
                let arr = cast(
                    arr,
                    &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                )?
                .as_primitive::<TimestampMillisecondType>()
                .clone();
                Ok(Self(arr.reinterpret_cast()))
            }
            _ => Err(LoadError::InvalidTimestamp(arr.data_type().clone())),
        }
    }

    pub fn par_iter(&self) -> impl IndexedParallelIterator<Item = i64> + '_ {
        (0..self.0.len()).into_par_iter().map(|i| self.0.value(i))
    }

    pub fn iter(&self) -> impl Iterator<Item = i64> + '_ {
        self.0.values().iter().copied()
    }

    pub fn get(&self, i: usize) -> Option<i64> {
        (i < self.0.len()).then(|| self.0.value(i))
    }
}

impl Deref for TimeCol {
    type Target = [i64];

    fn deref(&self) -> &Self::Target {
        self.0.values()
    }
}

#[derive(Clone, Debug)]
pub struct DFChunk {
    pub(crate) chunk: Vec<ArrayRef>,
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
