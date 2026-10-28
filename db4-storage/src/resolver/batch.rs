use crate::error::StorageError;
use arrow_array::cast::AsArray;
use arrow_array::{
    Array, LargeStringArray, StringArray, StringViewArray,
    types::{Int32Type, Int64Type, UInt32Type, UInt64Type},
};
use arrow_schema::DataType;
use raphtory_api::core::entities::{GidRef, VID};

/// Hands out VIDs for new nodes during a bulk load.
pub trait VidAllocator: Sync {
    /// Reserve up to `count` (> 0) consecutive VIDs, `hint` picks a slot to spread contention.
    /// Returns the first VID and how many were reserved (>= 1).
    fn reserve(&self, hint: usize, count: u32) -> (VID, u32);
}

impl<F: Fn(usize, u32) -> (VID, u32) + Sync> VidAllocator for F {
    fn reserve(&self, hint: usize, count: u32) -> (VID, u32) {
        self(hint, count)
    }
}

/// A typed view of an Arrow column of GIDs.
pub enum GidCol<'a> {
    U64(&'a [u64]),
    I64(&'a [i64]),
    U32(&'a [u32]),
    I32(&'a [i32]),
    Str(&'a StringArray),
    LargeStr(&'a LargeStringArray),
    View(&'a StringViewArray),
}

impl<'a> GidCol<'a> {
    pub fn new(arr: &'a dyn Array) -> Result<Self, StorageError> {
        Ok(match arr.data_type() {
            DataType::UInt64 => GidCol::U64(arr.as_primitive::<UInt64Type>().values()),
            DataType::Int64 => GidCol::I64(arr.as_primitive::<Int64Type>().values()),
            DataType::UInt32 => GidCol::U32(arr.as_primitive::<UInt32Type>().values()),
            DataType::Int32 => GidCol::I32(arr.as_primitive::<Int32Type>().values()),
            DataType::Utf8 => GidCol::Str(arr.as_string::<i32>()),
            DataType::LargeUtf8 => GidCol::LargeStr(arr.as_string::<i64>()),
            DataType::Utf8View => GidCol::View(arr.as_string_view()),
            dt => {
                return Err(StorageError::GenericFailure(format!(
                    "invalid node id type {dt:?}, expected an integer or string column"
                )));
            }
        })
    }

    pub fn len(&self) -> usize {
        match self {
            GidCol::U64(v) => v.len(),
            GidCol::I64(v) => v.len(),
            GidCol::U32(v) => v.len(),
            GidCol::I32(v) => v.len(),
            GidCol::Str(a) => a.len(),
            GidCol::LargeStr(a) => a.len(),
            GidCol::View(a) => a.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_str(&self) -> bool {
        matches!(self, GidCol::Str(_) | GidCol::LargeStr(_) | GidCol::View(_))
    }

    /// # Safety
    /// `i < self.len()` and the column is an integer column.
    #[inline(always)]
    pub unsafe fn int_unchecked(&self, i: usize) -> u64 {
        unsafe {
            match self {
                GidCol::U64(v) => *v.get_unchecked(i),
                GidCol::I64(v) => *v.get_unchecked(i) as u64,
                GidCol::U32(v) => *v.get_unchecked(i) as u64,
                GidCol::I32(v) => *v.get_unchecked(i) as i64 as u64,
                _ => std::hint::unreachable_unchecked(),
            }
        }
    }

    /// # Safety
    /// `i < self.len()` and the column is a string column.
    #[inline(always)]
    pub unsafe fn str_unchecked(&self, i: usize) -> &'a str {
        unsafe {
            match self {
                GidCol::Str(a) => a.value_unchecked(i),
                GidCol::LargeStr(a) => a.value_unchecked(i),
                GidCol::View(a) => a.value_unchecked(i),
                _ => std::hint::unreachable_unchecked(),
            }
        }
    }

    pub fn get(&self, i: usize) -> GidRef<'a> {
        assert!(i < self.len());
        unsafe {
            if self.is_str() {
                GidRef::Str(self.str_unchecked(i))
            } else {
                GidRef::U64(self.int_unchecked(i))
            }
        }
    }
}
