use std::{fmt::Debug, ops::Range};

use polars_arrow::{
    array::StructArray,
    datatypes::ArrowDataType as DataType,
    types::{NativeType, Offset},
};
use raphtory_api::core::storage::timeindex::AsTime;

use crate::{
    chunked_array::{
        bool_col::ChunkedBoolCol, chunked_array::ChunkedArray, col::ChunkedPrimitiveCol,
        utf8_col::StringCol, ChunkedArraySlice,
    },
    prelude::{ArrayOps, BaseArrayOps, Chunked},
    timestamps::TimeStamps,
};

#[derive(Copy, Clone, Debug)]
pub struct EmptyTProp();

#[derive(Debug, Copy, Clone)]
pub struct TPropColumn<'a, A, T> {
    props: ChunkedArraySlice<'a, A>,
    timestamps: TimeStamps<'a, T>,
}

impl<'a, A, T> TPropColumn<'a, A, T> {
    pub fn new(props: ChunkedArraySlice<'a, A>, timestamps: TimeStamps<'a, T>) -> Self {
        Self { props, timestamps }
    }

    pub fn into_inner(self) -> (ChunkedArraySlice<'a, A>, TimeStamps<'a, T>) {
        (self.props, self.timestamps)
    }
}

impl<'a, A: ArrayOps<'a> + Chunked<Chunk = <A as ArrayOps<'a>>::Chunk>, T> TPropColumn<'a, A, T>
where
    A::IntoValue: Send,
{
    pub fn into_iter(self) -> impl Iterator<Item = (A::IntoValue, i64)> + Send + 'a {
        let (props, timestamps) = self.into_inner();
        props.into_iter().zip(timestamps.timestamps())
    }

    pub fn get(&'a self, idx: usize) -> A::Value {
        self.props.get(idx)
    }
}

impl<'a, A: ArrayOps<'a> + Chunked<Chunk = <A as ArrayOps<'a>>::Chunk>, T: AsTime>
    TPropColumn<'a, A, T>
where
    A::IntoValue: Send,
{
    pub fn iter_window_inner(
        self,
        r: Range<T>,
    ) -> impl DoubleEndedIterator<Item = (T, A::IntoValue)> + Send + 'a {
        let (props, timestamps) = self.into_inner();
        let start = timestamps.position(&r.start);
        let end = timestamps.position(&r.end);
        timestamps
            .sliced(start..end)
            .into_iter()
            .zip(props.sliced(start..end))
    }
}
#[derive(Copy, Clone, Debug)]
pub enum DiskTProp<'a, T> {
    Empty(EmptyTProp),
    Bool(TPropColumn<'a, ChunkedBoolCol<'a>, T>),
    Str64(TPropColumn<'a, StringCol<'a, i64>, T>),
    Str32(TPropColumn<'a, StringCol<'a, i32>, T>),
    I32(TPropColumn<'a, ChunkedPrimitiveCol<'a, i32>, T>),
    I64(TPropColumn<'a, ChunkedPrimitiveCol<'a, i64>, T>),
    U8(TPropColumn<'a, ChunkedPrimitiveCol<'a, u8>, T>),
    U16(TPropColumn<'a, ChunkedPrimitiveCol<'a, u16>, T>),
    U32(TPropColumn<'a, ChunkedPrimitiveCol<'a, u32>, T>),
    U64(TPropColumn<'a, ChunkedPrimitiveCol<'a, u64>, T>),
    F32(TPropColumn<'a, ChunkedPrimitiveCol<'a, f32>, T>),
    F64(TPropColumn<'a, ChunkedPrimitiveCol<'a, f64>, T>),
}

impl<'a, T> DiskTProp<'a, T> {
    pub fn empty() -> Self {
        DiskTProp::Empty(EmptyTProp())
    }

    pub fn new<'b>(
        dtype: &'b DataType,
        timestamps: TimeStamps<'a, T>,
        props: crate::chunked_array::ChunkedArraySlice<'a, &ChunkedArray<StructArray>>,
        id: usize,
    ) -> DiskTProp<'a, T> {
        match dtype {
            DataType::Boolean => {
                DiskTProp::Bool(new_bool_tprop_column::<T>(timestamps, props, id).unwrap())
            }
            DataType::Int64 => {
                DiskTProp::I64(new_primitive_tprop_column::<i64, T>(timestamps, props, id).unwrap())
            }
            DataType::Int32 => {
                DiskTProp::I32(new_primitive_tprop_column::<i32, T>(timestamps, props, id).unwrap())
            }
            DataType::UInt32 => {
                DiskTProp::U32(new_primitive_tprop_column::<u32, T>(timestamps, props, id).unwrap())
            }
            DataType::UInt64 => {
                DiskTProp::U64(new_primitive_tprop_column::<u64, T>(timestamps, props, id).unwrap())
            }
            DataType::Float32 => {
                DiskTProp::F32(new_primitive_tprop_column::<f32, T>(timestamps, props, id).unwrap())
            }
            DataType::Float64 => {
                DiskTProp::F64(new_primitive_tprop_column::<f64, T>(timestamps, props, id).unwrap())
            }
            DataType::Utf8 => {
                DiskTProp::Str32(new_str_tprop_column::<i32, T>(timestamps, props, id).unwrap())
            }
            DataType::LargeUtf8 => {
                DiskTProp::Str64(new_str_tprop_column::<i64, T>(timestamps, props, id).unwrap())
            }
            DataType::Date64 => {
                DiskTProp::I64(new_primitive_tprop_column::<i64, T>(timestamps, props, id).unwrap())
            }
            DataType::Timestamp(_, _) => {
                DiskTProp::I64(new_primitive_tprop_column::<i64, T>(timestamps, props, id).unwrap())
            }
            _ => unimplemented!(),
        }
    }
}

pub fn new_primitive_tprop_column<'a, T: NativeType, TI>(
    timestamps: TimeStamps<'a, TI>,
    props: ChunkedArraySlice<'a, &'a ChunkedArray<StructArray>>,
    col_idx: usize,
) -> Option<TPropColumn<'a, ChunkedPrimitiveCol<'a, T>, TI>> {
    let props = props.into_primitive_col(col_idx)?;
    Some(TPropColumn { props, timestamps })
}

pub fn new_bool_tprop_column<'a, TI>(
    timestamps: TimeStamps<'a, TI>,
    props: ChunkedArraySlice<'a, &'a ChunkedArray<StructArray>>,
    col_idx: usize,
) -> Option<TPropColumn<'a, ChunkedBoolCol<'a>, TI>> {
    let props = props.into_bool_col(col_idx)?;
    Some(TPropColumn { props, timestamps })
}

pub fn new_str_tprop_column<'a, I: Offset, TI>(
    timestamps: TimeStamps<'a, TI>,
    props: ChunkedArraySlice<'a, &'a ChunkedArray<StructArray>>,
    col_idx: usize,
) -> Option<TPropColumn<'a, StringCol<'a, I>, TI>> {
    let props = props.into_utf8_col(col_idx)?;
    Some(TPropColumn { props, timestamps })
}
