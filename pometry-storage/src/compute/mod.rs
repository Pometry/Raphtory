use crate::arrow2::{
    array::{Array, PrimitiveArray, Utf8Array},
    datatypes::ArrowDataType as DataType,
    offset::Offset,
    types::{Index, NativeType},
};
use polars_arrow::array::{MutableBinaryViewArray, Utf8ViewArray};
use rayon::prelude::*;
use std::cmp::Ordering;

pub fn take(
    arr: &dyn Array,
    indices: &PrimitiveArray<i64>,
) -> Result<Box<dyn Array>, super::RAError> {
    match arr.data_type() {
        DataType::Int8 => take_primitive::<i8>(arr, indices),
        DataType::Int16 => take_primitive::<i16>(arr, indices),
        DataType::Int32 => take_primitive::<i32>(arr, indices),
        DataType::Int64 => take_primitive::<i64>(arr, indices),
        DataType::UInt8 => take_primitive::<u8>(arr, indices),
        DataType::UInt16 => take_primitive::<u16>(arr, indices),
        DataType::UInt32 => take_primitive::<u32>(arr, indices),
        DataType::UInt64 => take_primitive::<u64>(arr, indices),
        DataType::Float32 => take_primitive::<f32>(arr, indices),
        DataType::Float64 => take_primitive::<f64>(arr, indices),
        DataType::Utf8 => take_string::<i32, _>(arr, indices),
        DataType::LargeUtf8 => take_string::<i64, _>(arr, indices),
        DataType::Utf8View => take_string_views(arr, indices),
        dt => Err(super::RAError::InvalidTypeColumn(format!(
            "take operation not supported for type {:?}",
            dt
        )))?,
    }
}

fn take_primitive<T: NativeType>(
    arr: &dyn Array,
    indices: &PrimitiveArray<i64>,
) -> Result<Box<dyn Array>, super::RAError> {
    if arr.validity().is_some() && arr.validity().filter(|val| val.unset_bits() != 0).is_some() {
        return Err(super::RAError::InvalidTypeColumn(
            "Cannot take null values".to_string(),
        ));
    }

    let arr = arr
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| super::RAError::InvalidTypeColumn("Invalid type".to_string()))?;
    let values = arr.values();
    let indices = indices.values();
    let mut taken = Vec::with_capacity(indices.len());
    indices
        .par_iter()
        .map(|idx| values[*idx as usize])
        .collect_into_vec(&mut taken);

    Ok(PrimitiveArray::<T>::from_vec(taken).boxed())
}

fn take_string<I: Offset, II: Offset + Index>(
    arr: &dyn Array,
    indices: &PrimitiveArray<II>,
) -> Result<Box<dyn Array>, super::RAError> {
    let arr = arr
        .as_any()
        .downcast_ref::<Utf8Array<I>>()
        .expect("not utf8 array");

    if arr.validity().is_some() && arr.validity().filter(|val| val.unset_bits() != 0).is_some() {
        return Err(super::RAError::InvalidTypeColumn(
            "Cannot take null values".to_string(),
        ));
    }
    let indices = indices.values();
    let mut taken = Vec::with_capacity(indices.len());
    indices
        .par_iter()
        .map(|idx| unsafe {
            let i = idx.to_usize();
            arr.value_unchecked(i)
        })
        .collect_into_vec(&mut taken);

    Ok(Utf8Array::<I>::from_iter_values(taken.into_iter()).boxed())
}

fn take_string_views(
    arr: &dyn Array,
    indices: &PrimitiveArray<i64>,
) -> Result<Box<dyn Array>, super::RAError> {
    let arr = arr
        .as_any()
        .downcast_ref::<Utf8ViewArray>()
        .expect("not utf8 array");

    if arr.validity().is_some() && arr.validity().filter(|val| val.unset_bits() != 0).is_some() {
        return Err(super::RAError::InvalidTypeColumn(
            "Cannot take null values".to_string(),
        ));
    }
    let indices = indices.values();
    let iter = indices.iter().map(|idx| unsafe {
        let i = idx.to_usize();
        arr.value_unchecked(i)
    });

    let mutable = MutableBinaryViewArray::from_values_iter(iter);
    let arr: Utf8ViewArray = mutable.into();

    Ok(arr.boxed())
}

pub fn sort(arr: &dyn Array) -> Result<Box<dyn Array>, super::RAError> {
    match arr.data_type() {
        DataType::Int8 => sort_primitive_non_null::<i8>(arr, i8::cmp),
        DataType::Int16 => sort_primitive_non_null::<i16>(arr, i16::cmp),
        DataType::Int32 => sort_primitive_non_null::<i32>(arr, i32::cmp),
        DataType::Int64 => sort_primitive_non_null::<i64>(arr, i64::cmp),
        DataType::UInt8 => sort_primitive_non_null::<u8>(arr, u8::cmp),
        DataType::UInt16 => sort_primitive_non_null::<u16>(arr, u16::cmp),
        DataType::UInt32 => sort_primitive_non_null::<u32>(arr, u32::cmp),
        DataType::UInt64 => sort_primitive_non_null::<u64>(arr, u64::cmp),
        DataType::Float32 => sort_primitive_non_null::<f32>(arr, f32::total_cmp),
        DataType::Float64 => sort_primitive_non_null::<f64>(arr, f64::total_cmp),
        DataType::Utf8 => sort_strings::<i32>(arr),
        DataType::LargeUtf8 => sort_strings::<i64>(arr),
        DataType::Utf8View => sort_strings_view(arr),
        dt => Err(super::RAError::InvalidTypeColumn(format!(
            "sort operation not supported for type {:?}",
            dt
        )))?,
    }
}

/// # Safety
/// This function guarantees that:
/// * `get` is only called for `0 <= i < limit`
/// * `cmp` is only called from the co-domain of `get`.
#[inline]
fn sort_unstable_by<I, T, G, F>(indices: &mut [I], get: G, cmp: F)
where
    I: Index,
    G: Fn(usize) -> T + Sync,
    F: Fn(&T, &T) -> Ordering + Sync,
{
    indices.par_sort_unstable_by(|lhs, rhs| {
        let lhs = get(lhs.to_usize());
        let rhs = get(rhs.to_usize());
        cmp(&lhs, &rhs)
    })
}

fn sort_strings<I: Index + NativeType + Offset>(
    arr: &dyn Array,
) -> Result<Box<dyn Array>, super::RAError> {
    let arr = arr
        .as_any()
        .downcast_ref::<Utf8Array<I>>()
        .ok_or_else(|| super::RAError::InvalidTypeColumn("Invalid type".to_string()))?;

    if arr.validity().is_some() && arr.validity().filter(|val| val.unset_bits() != 0).is_some() {
        return Err(super::RAError::InvalidTypeColumn(
            "Cannot take null values".to_string(),
        ));
    }
    let mut indices = I::range(0, arr.len()).unwrap().collect::<Vec<_>>();
    let get = |idx| unsafe { arr.value_unchecked(idx) };
    let cmp = |lhs: &&str, rhs: &&str| lhs.cmp(rhs);
    sort_unstable_by(&mut indices, get, cmp);
    indices.shrink_to_fit();
    take_string::<I, I>(arr, &PrimitiveArray::<I>::from_vec(indices))
}

fn sort_strings_view(arr: &dyn Array) -> Result<Box<dyn Array>, super::RAError> {
    let arr = arr
        .as_any()
        .downcast_ref::<Utf8ViewArray>()
        .ok_or_else(|| super::RAError::InvalidTypeColumn("Invalid type".to_string()))?;

    if arr.validity().is_some() && arr.validity().filter(|val| val.unset_bits() != 0).is_some() {
        return Err(super::RAError::InvalidTypeColumn(
            "Cannot take null values".to_string(),
        ));
    }
    let mut indices = i64::range(0, arr.len()).unwrap().collect::<Vec<_>>();
    let get = |idx| unsafe { arr.value_unchecked(idx) };
    let cmp = |lhs: &&str, rhs: &&str| lhs.cmp(rhs);
    sort_unstable_by(&mut indices, get, cmp);
    indices.shrink_to_fit();
    take_string_views(arr, &PrimitiveArray::<i64>::from_vec(indices))
}

#[inline]
fn sort_primitive_non_null<T: NativeType>(
    arr: &dyn Array,
    cmp: impl Fn(&T, &T) -> Ordering + Sync,
) -> Result<Box<dyn Array>, super::RAError> {
    let array = arr
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| super::RAError::InvalidTypeColumn("Invalid type".to_string()))?;

    if arr.validity().is_some() && arr.validity().filter(|val| val.unset_bits() != 0).is_some() {
        return Err(super::RAError::InvalidTypeColumn(
            "Cannot sort null values".to_string(),
        ));
    }

    let mut values = array.values().to_vec();
    values.par_sort_unstable_by(cmp);
    Ok(PrimitiveArray::<T>::from_vec(values).boxed())
}
