use crate::{
    arrow2::{
        array::{Array, PrimitiveArray, StaticArray, StructArray, Utf8Array, Utf8ViewArray},
        datatypes::ArrowDataType as DataType,
        types::NativeType,
    },
    chunked_array::chunked_array::ChunkedArray,
    prelude::Chunked,
};
use polars_arrow::array::BooleanArray;
use polars_utils::index::Indexable;

#[derive(Debug, Clone, Copy)]
pub struct Row<'a> {
    values: &'a StructArray,
    row: usize,
}

pub struct RowOwned {
    values: ChunkedArray<StructArray>,
    chunk: usize,
    row: usize,
}

impl<'a> Row<'a> {
    fn col<T: 'static>(self, col_idx: usize) -> Option<&'a T> {
        self.values.values()[col_idx].as_any().downcast_ref()
    }

    pub fn new(values: &'a StructArray, row: usize) -> Self {
        Self { values, row }
    }

    pub fn primitive_value<T: NativeType>(self, col_idx: usize) -> Option<&'a T> {
        let col: &PrimitiveArray<T> = self.col(col_idx)?;
        if !col.is_null(self.row) {
            unsafe { Some(col.values().get_unchecked(self.row)) }
        } else {
            None
        }
    }

    pub fn bool_value(self, col_idx: usize) -> Option<bool> {
        if let DataType::Struct(fields) = self.values.data_type() {
            match fields.get(col_idx)?.data_type() {
                DataType::Boolean => {
                    let col: &BooleanArray = self.col(col_idx)?;
                    col.get(self.row)
                }
                _ => None,
            }
        } else {
            None
        }
    }

    pub fn str_value(self, col_idx: usize) -> Option<&'a str> {
        if let DataType::Struct(fields) = self.values.data_type() {
            match fields.get(col_idx)?.data_type() {
                DataType::LargeUtf8 => {
                    let col: &Utf8Array<i64> = self.col(col_idx)?;
                    col.get(self.row)
                }
                DataType::Utf8 => {
                    let col: &Utf8Array<i32> = self.col(col_idx)?;
                    col.get(self.row)
                }
                DataType::Utf8View => {
                    let col: &Utf8ViewArray = self.col(col_idx)?;
                    col.get(self.row)
                }
                _ => None,
            }
        } else {
            None
        }
    }

    pub fn is_valid(&self, col_idx: usize) -> bool {
        self.values
            .values()
            .get(col_idx)
            .map(|col| col.is_valid(self.row))
            .unwrap_or(false)
    }
}

impl RowOwned {
    fn col<T: 'static>(&self, col_idx: usize) -> Option<&T> {
        self.values.chunk(self.chunk).values()[col_idx]
            .as_any()
            .downcast_ref()
    }

    pub fn new(values: ChunkedArray<StructArray>, chunk: usize, row: usize) -> Self {
        Self { values, chunk, row }
    }

    pub fn primitive_value<T: NativeType>(&self, col_idx: usize) -> Option<T> {
        let col: &PrimitiveArray<T> = self.col(col_idx)?;
        Indexable::get(col, self.row)
    }

    pub fn str_value(&self, col_idx: usize) -> Option<&str> {
        if let Some(col) = self.col::<Utf8Array<i64>>(col_idx) {
            col.get(self.row)
        } else if let Some(col) = self.col::<Utf8Array<i32>>(col_idx) {
            col.get(self.row)
        } else if let Some(col) = self.col::<Utf8ViewArray>(col_idx) {
            col.get(self.row)
        } else {
            None
        }
    }
}
