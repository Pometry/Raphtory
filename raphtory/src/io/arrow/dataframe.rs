use crate::{
    errors::{into_load_err, GraphError, LoadError},
    io::arrow::node_col::{lift_node_col, NodeCol},
};
use arrow::{
    array::{cast::AsArray, Array, ArrayRef, PrimitiveArray},
    compute::cast,
    datatypes::{DataType, Date64Type, Int64Type, TimeUnit, TimestampMillisecondType},
};
use itertools::Itertools;
use raphtory_core::utils::time::TryIntoTime;
use rayon::prelude::*;
use std::fmt::{Debug, Formatter};

pub struct DFView<I> {
    pub names: Vec<String>,
    pub chunks: I,
}

impl<I> Debug for DFView<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DFView")
            .field("names", &self.names)
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

    pub fn new(names: Vec<String>, chunks: I) -> Self {
        Self { names, chunks }
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
                    .map(|v| v.try_into_time().map_err(into_load_err))
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
                    .map(|v| v.try_into_time().map_err(into_load_err))
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
                    .map(|v| v.try_into_time().map_err(into_load_err))
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
}
