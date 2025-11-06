use crate::{
    errors::{GraphError, LoadError},
    io::arrow::node_col::{lift_node_col, NodeCol},
};
use arrow::{
    array::{cast::AsArray, Array, ArrayRef, PrimitiveArray},
    compute::cast,
    datatypes::{DataType, Date64Type, Int64Type, TimeUnit, TimestampMillisecondType, UInt64Type},
};
use either::Either;
use itertools::Itertools;
use rayon::prelude::*;
use std::{
    fmt::{Debug, Formatter},
    ops::{Deref, Range},
};

pub struct DFView<I> {
    pub names: Vec<String>,
    pub chunks: I,
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

    pub fn new(names: Vec<String>, chunks: I, num_rows: usize) -> Self {
        Self {
            names,
            chunks,
            num_rows,
        }
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
            DataType::Date32 => {
                let arr = cast(arr, &DataType::Date64)?
                    .as_primitive::<Date64Type>()
                    .clone();
                Ok(Self(arr.reinterpret_cast()))
            }
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

    pub fn values(&self) -> &[i64] {
        self.0.values()
    }
}

impl Deref for TimeCol {
    type Target = [i64];

    fn deref(&self) -> &Self::Target {
        self.0.values()
    }
}

pub enum SecondaryIndexCol {
    DataFrame(PrimitiveArray<UInt64Type>),
    Range(Range<usize>),
}

impl SecondaryIndexCol {
    /// Load a secondary index column from a dataframe.
    pub fn new_from_df(arr: &dyn Array) -> Result<Self, LoadError> {
        if arr.null_count() > 0 {
            return Err(LoadError::MissingSecondaryIndexError);
        }

        Ok(SecondaryIndexCol::DataFrame(
            arr.as_primitive::<UInt64Type>().clone(),
        ))
    }

    /// Generate a secondary index column with values from `start` to `end` (not inclusive).
    pub fn new_from_range(start: usize, end: usize) -> Self {
        let start = start;
        let end = end;
        SecondaryIndexCol::Range(start..end)
    }

    pub fn par_iter(&self) -> impl IndexedParallelIterator<Item = usize> + '_ {
        match self {
            SecondaryIndexCol::DataFrame(arr) => {
                rayon::iter::Either::Left(arr.values().par_iter().copied().map(|v| v as usize))
            }
            SecondaryIndexCol::Range(range) => {
                rayon::iter::Either::Right(range.clone().into_par_iter())
            }
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        match self {
            SecondaryIndexCol::DataFrame(arr) => {
                Either::Left(arr.values().iter().copied().map(|v| v as usize))
            }
            SecondaryIndexCol::Range(range) => Either::Right(range.clone()),
        }
    }

    pub fn max(&self) -> usize {
        self.iter().max().unwrap_or(0)
    }
}

#[derive(Clone, Debug)]
pub struct DFChunk {
    pub chunk: Vec<ArrayRef>,
}

impl DFChunk {
    pub fn new(chunk: Vec<ArrayRef>) -> Self {
        Self { chunk }
    }

    pub fn len(&self) -> usize {
        self.chunk.first().map(|c| c.len()).unwrap_or(0)
    }

    pub fn node_col(&self, index: usize) -> Result<NodeCol, LoadError> {
        lift_node_col(index, self)
    }

    pub fn time_col(&self, index: usize) -> Result<TimeCol, LoadError> {
        TimeCol::new(self.chunk[index].as_ref())
    }

    pub fn secondary_index_col(&self, index: usize) -> Result<SecondaryIndexCol, LoadError> {
        SecondaryIndexCol::new_from_df(self.chunk[index].as_ref())
    }
}
