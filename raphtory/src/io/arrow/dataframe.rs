use crate::{
    api::core::utils::time::TryIntoTime,
    errors::{into_load_err, GraphError, LoadError},
    io::arrow::node_col::{lift_node_col, NodeCol},
};
use arrow::{
    array::{cast::AsArray, Array, ArrayRef, PrimitiveArray},
    compute::cast,
    datatypes::{DataType, Date64Type, Int64Type, TimeUnit, TimestampMillisecondType, UInt64Type},
};
use either::Either;
use itertools::Itertools;
use raphtory_api::core::storage::timeindex::AsTime;
use rayon::prelude::*;
use std::{
    fmt::{Debug, Formatter},
    ops::{Deref, Range},
};

pub struct DFView<I> {
    pub names: Vec<String>,
    pub chunks: I,
    pub num_rows: Option<usize>,
}

impl<I> Debug for DFView<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DFView")
            .field("names", &self.names)
            .field(
                "num_rows",
                &self
                    .num_rows
                    .map(|x| x.to_string())
                    .unwrap_or("Unknown".to_string()),
            )
            .finish()
    }
}

impl<I> DFView<I> {
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
        self.get_index_opt(name)
            .ok_or_else(|| GraphError::ColumnDoesNotExist(name.to_string()))
    }

    pub(crate) fn get_index_opt(&self, name: &str) -> Option<usize> {
        self.names.iter().position(|n| n == name)
    }

    /// Returns Some(_) only if we know the total number of rows.
    pub fn is_empty(&self) -> bool {
        self.num_rows.is_some_and(|num_rows| num_rows > 0)
    }

    pub fn new(names: Vec<String>, chunks: I, num_rows: Option<usize>) -> Self {
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
            DataType::Utf8 => {
                let strings = arr.as_string::<i32>();
                // filters out None values in the array
                let timestamps = strings
                    .iter()
                    .flatten()
                    .map(|v| v.try_into_time().map(|t| t.t()).map_err(into_load_err))
                    .collect::<Result<Vec<i64>, LoadError>>()?;
                let arr = PrimitiveArray::<Int64Type>::from(timestamps);
                Ok(Self(arr))
            }
            DataType::LargeUtf8 => {
                let strings = arr.as_string::<i64>();
                // filters out None values in the array
                let timestamps = strings
                    .iter()
                    .flatten()
                    .map(|v| v.try_into_time().map(|t| t.t()).map_err(into_load_err))
                    .collect::<Result<Vec<i64>, LoadError>>()?;
                let arr = PrimitiveArray::<Int64Type>::from(timestamps);
                Ok(Self(arr))
            }
            DataType::Utf8View => {
                let strings = arr.as_string_view();
                // filters out None values in the array
                let timestamps = strings
                    .iter()
                    .flatten()
                    .map(|v| v.try_into_time().map(|t| t.t()).map_err(into_load_err))
                    .collect::<Result<Vec<i64>, LoadError>>()?;
                let arr = PrimitiveArray::<Int64Type>::from(timestamps);
                Ok(Self(arr))
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

    pub fn len(&self) -> usize {
        match self {
            SecondaryIndexCol::DataFrame(arr) => arr.len(),
            SecondaryIndexCol::Range(range) => range.len(),
        }
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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
