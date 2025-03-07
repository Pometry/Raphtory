use crate::{
    chunked_array::chunked_array::ChunkedArray,
    merge::EitherOrBothIndex,
    prelude::{ArrayOps, IntoUtf8Col, PrimitiveCol},
    RAError,
};
use ahash::HashMap;
use itertools::EitherOrBoth;
use polars_arrow::{
    array::{Array, PrimitiveArray, StructArray, Utf8Array},
    datatypes::{Field, PhysicalType, PrimitiveType},
    offset::Offset,
    types::NativeType,
};
use rayon::prelude::*;
use std::collections::hash_map::Entry;

pub mod merge_edge_props;
pub mod merge_node_props;

fn merge_fields(
    left: &[Field],
    right: &[Field],
) -> Result<(Vec<Field>, Vec<EitherOrBoth<usize>>), RAError> {
    let mut name_mapper: HashMap<_, EitherOrBoth<usize>> = left
        .iter()
        .enumerate()
        .map(|(id, Field { name, .. })| (name, EitherOrBoth::Left(id)))
        .collect();
    let mut fields = left.to_vec();
    for (id, field) in right.iter().enumerate() {
        match name_mapper.entry(&field.name) {
            Entry::Occupied(entry) => {
                let index = entry.into_mut();
                let other_type = fields[*index.as_ref().left().unwrap()].data_type();
                if field.data_type() != other_type {
                    return Err(RAError::EdgeMergeDTypeError {
                        name: field.name.clone(),
                        left_dtype: other_type.clone(),
                        right_dtype: field.data_type.clone(),
                    });
                }
                index.insert_right(id);
            }
            Entry::Vacant(entry) => {
                fields.push(field.clone());
                entry.insert(EitherOrBoth::Right(id));
            }
        }
    }
    let ids = fields
        .iter()
        .map(|Field { name, .. }| name_mapper.get(name).unwrap().clone())
        .collect();
    Ok((fields, ids))
}

fn merge_primitive<T: NativeType, Idx: Into<EitherOrBothIndex> + Copy>(
    left_props: &ChunkedArray<StructArray>,
    right_props: &ChunkedArray<StructArray>,
    col: EitherOrBoth<usize>,
    index: &[Idx],
) -> PrimitiveArray<T> {
    let left = col
        .as_ref()
        .left()
        .map(|&col| left_props.primitive_col::<T>(col).unwrap());
    let right = col
        .right()
        .map(|col| right_props.primitive_col::<T>(col).unwrap());
    PrimitiveArray::from_iter(index.iter().map(|&index| {
        match index.into() {
            EitherOrBothIndex::Empty => {
                unreachable!()
            }
            EitherOrBothIndex::Left(i) => left.and_then(|left| left.get(i)),
            EitherOrBothIndex::Right(i) => right.and_then(|right| right.get(i)),
            EitherOrBothIndex::Both(l, r) => right
                .and_then(|right| right.get(r))
                .or_else(|| left.and_then(|left| left.get(l))),
        }
    }))
}

fn merge_utf8<I: Offset, Idx: Into<EitherOrBothIndex> + Copy>(
    left_props: &ChunkedArray<StructArray>,
    right_props: &ChunkedArray<StructArray>,
    col: EitherOrBoth<usize>,
    index: &[Idx],
) -> Utf8Array<I> {
    let left = col
        .as_ref()
        .left()
        .map(|&col| left_props.into_utf8_col::<I>(col).unwrap());
    let right = col
        .right()
        .map(|col| right_props.into_utf8_col::<I>(col).unwrap());
    Utf8Array::from_iter(index.iter().map(|&index| {
        match index.into() {
            EitherOrBothIndex::Empty => {
                unreachable!()
            }
            EitherOrBothIndex::Left(i) => left.and_then(|left| left.into_get(i)),
            EitherOrBothIndex::Right(i) => right.and_then(|right| right.into_get(i)),
            EitherOrBothIndex::Both(l, r) => right
                .and_then(|right| right.into_get(r))
                .or_else(|| left.and_then(|left| left.into_get(l))),
        }
    }))
}

fn merge_props_chunk<Idx: Into<EitherOrBothIndex> + Copy + Send + Sync>(
    left_props: &ChunkedArray<StructArray>,
    right_props: &ChunkedArray<StructArray>,
    fields: &[Field],
    col_idx: &[EitherOrBoth<usize>],
    chunk_index: &[Idx],
) -> Vec<Box<dyn Array>> {
    let mut cols = Vec::with_capacity(fields.len());
    fields
        .par_iter()
        .zip(col_idx.par_iter().cloned())
        .map(|(field, col)| match field.data_type().to_physical_type() {
            PhysicalType::Primitive(dt) => match dt {
                PrimitiveType::Int8 => {
                    merge_primitive::<i8, Idx>(left_props, right_props, col, chunk_index).to_boxed()
                }
                PrimitiveType::Int16 => {
                    merge_primitive::<i16, Idx>(left_props, right_props, col, chunk_index)
                        .to_boxed()
                }
                PrimitiveType::Int32 => {
                    merge_primitive::<i32, Idx>(left_props, right_props, col, chunk_index)
                        .to_boxed()
                }
                PrimitiveType::Int64 => {
                    merge_primitive::<i64, Idx>(left_props, right_props, col, chunk_index)
                        .to_boxed()
                }
                PrimitiveType::UInt8 => {
                    merge_primitive::<u8, Idx>(left_props, right_props, col, chunk_index).to_boxed()
                }
                PrimitiveType::UInt16 => {
                    merge_primitive::<u16, Idx>(left_props, right_props, col, chunk_index)
                        .to_boxed()
                }
                PrimitiveType::UInt32 => {
                    merge_primitive::<u32, Idx>(left_props, right_props, col, chunk_index)
                        .to_boxed()
                }
                PrimitiveType::UInt64 => {
                    merge_primitive::<u64, Idx>(left_props, right_props, col, chunk_index)
                        .to_boxed()
                }
                PrimitiveType::Float32 => {
                    merge_primitive::<f32, Idx>(left_props, right_props, col, chunk_index)
                        .to_boxed()
                }
                PrimitiveType::Float64 => {
                    merge_primitive::<f64, Idx>(left_props, right_props, col, chunk_index)
                        .to_boxed()
                }
                _ => unimplemented!("{:?} not supported as property type", field.data_type()),
            },
            PhysicalType::Utf8 => {
                merge_utf8::<i32, Idx>(left_props, right_props, col, chunk_index).to_boxed()
            }
            PhysicalType::LargeUtf8 => {
                merge_utf8::<i64, Idx>(left_props, right_props, col, chunk_index).to_boxed()
            }
            _ => unimplemented!("{:?} not supported as property type", field.data_type()),
        })
        .collect_into_vec(&mut cols);
    cols
}
