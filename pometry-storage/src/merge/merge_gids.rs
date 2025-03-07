use crate::{
    global_order::{GIDArray, GlobalOrder},
    merge::merge_chunks::IndexedView,
    RAError,
};
use polars_arrow::{
    array::{PrimitiveArray, Utf8Array, Utf8ViewArray},
    types::Offset,
};
use rayon::prelude::*;
use std::ops::Range;
use tracing::instrument;

#[derive(Copy, Clone, Debug)]
struct MergeNonNullUtf8Array<'a, O: Offset> {
    values: &'a Utf8Array<O>,
    index: usize,
}

impl<'a, O: Offset> MergeNonNullUtf8Array<'a, O> {
    #[inline]
    fn index(&self, index: usize) -> usize {
        self.index + index
    }

    fn new(values: &'a Utf8Array<O>) -> Self {
        Self { values, index: 0 }
    }
}

impl<'a, O: Offset> IndexedView for MergeNonNullUtf8Array<'a, O> {
    type Value = &'a str;

    fn get(self, index: usize) -> Option<Self::Value> {
        let index = self.index(index);
        if index < self.values.len() {
            Some(self.values.value(index))
        } else {
            None
        }
    }

    fn range(self, range: Range<usize>) -> impl Iterator<Item = Self::Value> {
        (self.index(range.start)..self.index(range.end)).map(|i| self.values.value(i))
    }

    fn advance(&mut self, by: usize) {
        self.index = (self.index + by).max(self.values.len())
    }

    fn len(&self) -> usize {
        self.values.len() - self.index
    }
}

#[derive(Copy, Clone, Debug)]
struct MergeNonNullUtf8ViewArray<'a> {
    values: &'a Utf8ViewArray,
    index: usize,
}

impl<'a> MergeNonNullUtf8ViewArray<'a> {
    #[inline]
    fn index(&self, index: usize) -> usize {
        self.index + index
    }

    fn new(values: &'a Utf8ViewArray) -> Self {
        Self { values, index: 0 }
    }
}

impl<'a> IndexedView for MergeNonNullUtf8ViewArray<'a> {
    type Value = &'a str;

    fn get(self, index: usize) -> Option<Self::Value> {
        let index = self.index(index);
        if index < self.values.len() {
            Some(self.values.value(index))
        } else {
            None
        }
    }

    fn range(self, range: Range<usize>) -> impl Iterator<Item = Self::Value> {
        (self.index(range.start)..self.index(range.end)).map(|i| self.values.value(i))
    }

    fn advance(&mut self, by: usize) {
        self.index = (self.index + by).max(self.values.len())
    }

    fn len(&self) -> usize {
        self.values.len() - self.index
    }
}

#[instrument(level = "debug", skip(left, right))]
pub fn merge_gids(left: &GIDArray, right: &GIDArray) -> Result<GIDArray, RAError> {
    match left {
        GIDArray::I64(left_values) => match right {
            GIDArray::I64(right_values) => {
                let chunk_size = left_values.len() + right_values.len();
                let res = left_values
                    .values()
                    .merge_dedup_chunks_by(
                        right_values.values().as_slice(),
                        chunk_size,
                        |l, r| l.cmp(r),
                        |v| *v,
                    )
                    .next()
                    .unwrap_or_default();
                Ok(GIDArray::I64(PrimitiveArray::from_vec(res)))
            }
            _ => Err(RAError::NodeGIDTypeError(left.id_type(), right.id_type())),
        },
        GIDArray::U64(left_values) => match right {
            GIDArray::U64(right_values) => {
                let chunk_size = left_values.len() + right_values.len();
                let res = left_values
                    .values()
                    .merge_dedup_chunks_by(
                        right_values.values().as_slice(),
                        chunk_size,
                        |l, r| l.cmp(r),
                        |v| *v,
                    )
                    .next()
                    .unwrap_or_default();
                Ok(GIDArray::U64(PrimitiveArray::from_vec(res)))
            }
            _ => Err(RAError::NodeGIDTypeError(left.id_type(), right.id_type())),
        },
        GIDArray::Str32(left_values) => {
            let left_values = MergeNonNullUtf8Array::new(left_values);
            match right {
                GIDArray::Str32(right_values) => {
                    let chunk_size = left_values.len() + right_values.len();
                    let res = left_values
                        .merge_dedup_chunks_by::<Vec<_>>(
                            MergeNonNullUtf8Array::new(right_values),
                            chunk_size,
                            |l, r| l.cmp(r),
                            |v| v,
                        )
                        .next()
                        .unwrap_or_default();
                    Ok(GIDArray::Str32(Utf8Array::from_slice(res)))
                }
                GIDArray::Str64(right_values) => {
                    let chunk_size = left_values.len() + right_values.len();
                    let res = left_values
                        .merge_dedup_chunks_by::<Vec<_>>(
                            MergeNonNullUtf8Array::new(right_values),
                            chunk_size,
                            |l, r| l.cmp(r),
                            |v| v,
                        )
                        .next()
                        .unwrap_or_default();
                    Ok(GIDArray::Str64(Utf8Array::from_slice(res)))
                }
                GIDArray::StrView(right_values) => {
                    let chunk_size = left_values.len() + right_values.len();
                    let res = left_values
                        .merge_dedup_chunks_by::<Vec<_>>(
                            MergeNonNullUtf8ViewArray::new(right_values),
                            chunk_size,
                            |l, r| l.cmp(r),
                            |v| v,
                        )
                        .next()
                        .unwrap_or_default();
                    Ok(GIDArray::Str64(Utf8Array::from_slice(res)))
                }
                _ => Err(RAError::NodeGIDTypeError(left.id_type(), right.id_type())),
            }
        }
        GIDArray::Str64(left_values) => {
            let left_values = MergeNonNullUtf8Array::new(left_values);
            match right {
                GIDArray::Str32(right_values) => {
                    let chunk_size = left_values.len() + right_values.len();
                    let res = left_values
                        .merge_dedup_chunks_by::<Vec<_>>(
                            MergeNonNullUtf8Array::new(right_values),
                            chunk_size,
                            |l, r| l.cmp(r),
                            |v| v,
                        )
                        .next()
                        .unwrap_or_default();
                    Ok(GIDArray::Str32(Utf8Array::from_slice(res)))
                }
                GIDArray::Str64(right_values) => {
                    let chunk_size = left_values.len() + right_values.len();
                    let res = left_values
                        .merge_dedup_chunks_by::<Vec<_>>(
                            MergeNonNullUtf8Array::new(right_values),
                            chunk_size,
                            |l, r| l.cmp(r),
                            |v| v,
                        )
                        .next()
                        .unwrap_or_default();
                    Ok(GIDArray::Str64(Utf8Array::from_slice(res)))
                }
                GIDArray::StrView(right_values) => {
                    let chunk_size = left_values.len() + right_values.len();
                    let res = left_values
                        .merge_dedup_chunks_by::<Vec<_>>(
                            MergeNonNullUtf8ViewArray::new(right_values),
                            chunk_size,
                            |l, r| l.cmp(r),
                            |v| v,
                        )
                        .next()
                        .unwrap_or_default();
                    Ok(GIDArray::Str64(Utf8Array::from_slice(res)))
                }
                _ => Err(RAError::NodeGIDTypeError(left.id_type(), right.id_type())),
            }
        }
        GIDArray::StrView(left_values) => {
            let left_values = MergeNonNullUtf8ViewArray::new(left_values);
            match right {
                GIDArray::Str32(right_values) => {
                    let chunk_size = left_values.len() + right_values.len();
                    let res = left_values
                        .merge_dedup_chunks_by::<Vec<_>>(
                            MergeNonNullUtf8Array::new(right_values),
                            chunk_size,
                            |l, r| l.cmp(r),
                            |v| v,
                        )
                        .next()
                        .unwrap_or_default();
                    Ok(GIDArray::Str32(Utf8Array::from_slice(res)))
                }
                GIDArray::Str64(right_values) => {
                    let chunk_size = left_values.len() + right_values.len();
                    let res = left_values
                        .merge_dedup_chunks_by::<Vec<_>>(
                            MergeNonNullUtf8Array::new(right_values),
                            chunk_size,
                            |l, r| l.cmp(r),
                            |v| v,
                        )
                        .next()
                        .unwrap_or_default();
                    Ok(GIDArray::Str64(Utf8Array::from_slice(res)))
                }
                GIDArray::StrView(right_values) => {
                    let chunk_size = left_values.len() + right_values.len();
                    let res = left_values
                        .merge_dedup_chunks_by::<Vec<_>>(
                            MergeNonNullUtf8ViewArray::new(right_values),
                            chunk_size,
                            |l, r| l.cmp(r),
                            |v| v,
                        )
                        .next()
                        .unwrap_or_default();
                    Ok(GIDArray::Str64(Utf8Array::from_slice(res)))
                }
                _ => Err(RAError::NodeGIDTypeError(left.id_type(), right.id_type())),
            }
        }
    }
}

pub fn build_local_id_map(
    global_id_map: &(impl GlobalOrder + Send + Sync),
    old_ids: &GIDArray,
) -> Vec<usize> {
    let mut new_ids = Vec::with_capacity(old_ids.len());
    old_ids
        .par_iter()
        .map(|id| global_id_map.find(id).unwrap())
        .collect_into_vec(&mut new_ids);
    new_ids
}

#[cfg(test)]
mod test {
    use crate::{global_order::GIDArray, merge::merge_gids::merge_gids};
    use itertools::Itertools;
    use polars_arrow::array::{Array, PrimitiveArray, Utf8Array};
    use proptest::prelude::*;

    prop_compose! {
        fn two_sorted_str_vecs(max_size: usize)(mut first_vec in prop::collection::vec(any::<String>(), 1..max_size), mut second_vec in prop::collection::vec(any::<String>(), 1..max_size)) -> (Vec<String>, Vec<String>) {
            first_vec.sort();
            first_vec.dedup();
            second_vec.sort();
            second_vec.dedup();
            (first_vec, second_vec)
        }
    }

    fn test_str_inner(left_vec: Vec<String>, right_vec: Vec<String>) {
        let left = GIDArray::Str32(Utf8Array::from_slice(&left_vec));
        let right = GIDArray::Str64(Utf8Array::from_slice(&right_vec));
        let merged_values: Vec<_> = left_vec.into_iter().merge(right_vec).dedup().collect();
        let merged = merge_gids(&left, &right).unwrap();
        assert_eq!(
            merged.to_boxed(),
            <Utf8Array<i64>>::from_slice(&merged_values).to_boxed()
        );
    }

    #[test]
    fn test_str_merge() {
        proptest!(|((left, right) in two_sorted_str_vecs(100))| {
            test_str_inner(left, right);
        })
    }

    #[test]
    fn test_gid_merge() {
        let left_values = vec![0, 10, 20, 30];
        let left = GIDArray::U64(PrimitiveArray::from_vec(left_values));
        let right = GIDArray::U64(PrimitiveArray::from_vec(vec![1, 2, 30, 40]));

        let merged = merge_gids(&left, &right).unwrap();
        assert_eq!(
            merged.to_boxed(),
            PrimitiveArray::from_vec(vec![0u64, 1, 2, 10, 20, 30, 40]).to_boxed()
        )
    }
}
