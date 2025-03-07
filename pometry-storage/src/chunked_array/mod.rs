pub mod array_ops;
pub mod bool_col;
pub mod can_slice_and_len;
mod chain;
pub mod chunked_array;
pub mod chunked_iter;
pub mod chunked_offsets;
pub mod col;
pub mod list_array;
pub mod mutable_chunked_array;
pub mod row;
pub mod slice;
pub mod utf8_col;

pub use slice::ChunkedArraySlice;

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::arrow2::{
        array::{PrimitiveArray, Utf8Array},
        datatypes::{ArrowDataType as DataType, Field},
    };
    use itertools::Itertools;
    use polars_arrow::array::BooleanArray;
    use rayon::iter::ParallelIterator;

    use crate::chunked_array::array_ops::{
        ArrayOps, BaseArrayOps, IntoPrimitiveCol, IntoUtf8Col, NonNullPrimitiveCol,
    };

    use super::chunked_array::ChunkedArray;

    use proptest::{collection::size_range, prelude::*};

    fn sanity_test_chunk_array_utf8(arr: Vec<&str>, chunk_size: usize) {
        let chunks: Vec<_> = arr
            .iter()
            .chunks(chunk_size)
            .into_iter()
            .map(|chunk| {
                let chunk = chunk.copied().collect::<Vec<_>>();
                Utf8Array::<i32>::from_slice(&chunk)
            })
            .collect();

        let chunked_array = ChunkedArray::from(chunks);
        assert_eq!(chunked_array.len(), arr.len());
        assert_eq!(chunked_array.clone().into_get(0), Some(arr[0].to_string()));
        assert_eq!(chunked_array.get(0), Some(arr[0]));

        // par iter and iter
        let actual: Vec<_> = chunked_array.par_iter().flatten().collect();
        assert_eq!(actual, arr);

        let actual = chunked_array.iter().flatten().collect::<Vec<_>>();
        assert_eq!(actual, arr);

        // slice checks
        let actual = chunked_array
            .slice(0..chunked_array.len() / 2)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(actual, &arr[0..arr.len() / 2]);

        let actual = chunked_array
            .slice(chunked_array.len() / 2..)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(actual, &arr[arr.len() / 2..]);
    }

    fn sanity_test_chunk_array_bool(arr: Vec<bool>, chunk_size: usize) {
        let chunks: Vec<_> = arr
            .iter()
            .chunks(chunk_size)
            .into_iter()
            .map(|chunk| {
                let chunk = chunk.copied().collect::<Vec<_>>();
                BooleanArray::from_slice(&chunk)
            })
            .collect();

        let chunked_array = ChunkedArray::from(chunks);

        assert_eq!(chunked_array.len(), arr.len());
        assert_eq!(chunked_array.clone().into_get(0), Some(arr[0]));
        assert_eq!(chunked_array.get(0), Some(arr[0]));

        // par iter and iter
        let actual: Vec<_> = chunked_array.par_iter().flatten().collect();
        assert_eq!(actual, arr);

        let actual = chunked_array.iter().flatten().collect::<Vec<_>>();
        assert_eq!(actual, arr);

        // slice checks
        let actual = chunked_array
            .slice(0..chunked_array.len() / 2)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(actual, &arr[0..arr.len() / 2]);

        let actual = chunked_array
            .slice(chunked_array.len() / 2..)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(actual, &arr[arr.len() / 2..]);
    }

    fn sanity_test_chunk_array(arr: Vec<i32>, chunk_size: usize) {
        let chunks: Vec<_> = arr
            .iter()
            .chunks(chunk_size)
            .into_iter()
            .map(|chunk| {
                let chunk = chunk.copied().collect::<Vec<_>>();
                PrimitiveArray::<i32>::from_slice(&chunk)
            })
            .collect();

        let chunked_array = ChunkedArray::from(chunks);
        assert_eq!(chunked_array.len(), arr.len());
        assert_eq!(chunked_array.clone().into_get(0), Some(arr[0]));
        assert_eq!(chunked_array.get(0), Some(arr[0]));

        // par iter and iter
        let actual: Vec<_> = chunked_array.par_iter().flatten().collect();
        assert_eq!(actual, arr);

        let actual = chunked_array.iter().flatten().collect::<Vec<_>>();
        assert_eq!(actual, arr);

        // slice checks
        let actual = chunked_array
            .slice(0..chunked_array.len() / 2)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(actual, &arr[0..arr.len() / 2]);

        let actual = chunked_array
            .slice(chunked_array.len() / 2..)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(actual, &arr[arr.len() / 2..]);
    }

    fn sanity_test_chunk_array_struct(chunk_size: usize, col1: Vec<&str>, col2: Vec<u64>) {
        let chunks = make_chunks(&col1, &col2, chunk_size);

        let chunked_array = ChunkedArray::from(chunks);
        assert_eq!(chunked_array.len(), col1.len());
        assert_eq!(chunked_array.len(), col2.len());

        for (i, (v1, v2)) in col1.iter().zip(&col2).enumerate() {
            let row = chunked_array.get(i);
            assert_eq!(row.str_value(0), Some(*v1));
            assert_eq!(row.primitive_value::<u64>(1), Some(v2));
        }

        // par iter and iter
        let ck_col1 = chunked_array.into_utf8_col::<i32>(0).unwrap();
        let ck_col2 = chunked_array.into_primitive_col::<u64>(1).unwrap();

        let half = ck_col1
            .slice(0..ck_col1.len() / 2)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(
            ck_col1.slice(0..ck_col1.len() / 2).len(),
            col1[0..col1.len() / 2].len()
        );
        assert_eq!(half, &col1[0..col1.len() / 2]);

        let actual: Vec<_> = ck_col1.par_iter().flatten().collect();
        assert_eq!(actual, col1);

        let actual: Vec<_> = ck_col2.par_iter().flatten().collect();
        assert_eq!(actual, col2);

        let actual = ck_col1.into_iter().flatten().collect::<Vec<_>>();
        assert_eq!(actual, col1);

        let actual = ck_col2.into_iter().flatten().collect::<Vec<_>>();
        assert_eq!(actual, col2);

        // slice checks
        // col1
        let actual = &chunked_array
            .into_utf8_col::<i32>(0)
            .unwrap()
            .sliced(0..chunked_array.len() / 2)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(actual, &col1[0..col1.len() / 2]);
        // col2
        let actual = &chunked_array
            .into_primitive_col::<u64>(1)
            .unwrap()
            .sliced(0..chunked_array.len() / 2)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(actual, &col2[0..col2.len() / 2]);

        let actual = &chunked_array
            .into_utf8_col::<i32>(0)
            .unwrap()
            .par_iter_chunks_window(0..chunked_array.len() / 2)
            .flat_map_iter(|v| v.into_iter().flatten().map(|s| s.to_owned()).collect_vec())
            .collect::<Vec<_>>();
        assert_eq!(actual, &col1[0..col1.len() / 2]);
    }

    fn make_chunks(
        col1: &Vec<&str>,
        col2: &Vec<u64>,
        chunk_size: usize,
    ) -> Vec<crate::arrow2::array::StructArray> {
        let chunks: Vec<_> = col1
            .iter()
            .chunks(chunk_size)
            .into_iter()
            .zip(col2.iter().chunks(chunk_size).into_iter())
            .map(|(chunk_col1, chunk_col2)| {
                let chunk1 = chunk_col1.copied().collect::<Vec<_>>();
                let col1 = Utf8Array::<i32>::from_slice(&chunk1).boxed();
                let chunk2 = chunk_col2.copied().collect::<Vec<_>>();
                let col2 = PrimitiveArray::<u64>::from_slice(&chunk2).boxed();

                let arr = vec![col1, col2];
                crate::arrow2::array::StructArray::new(
                    DataType::Struct(vec![
                        Field::new("col1", DataType::Utf8, false),
                        Field::new("col2", DataType::UInt64, false),
                    ]),
                    arr,
                    None,
                )
            })
            .collect();
        chunks
    }

    fn sanity_test_chunk_array_binary_search(chunk_size: usize, col1: Vec<&str>, col2: Vec<u64>) {
        let chunks = make_chunks(&col1, &col2, chunk_size);

        let chunked_array = ChunkedArray::from(chunks);
        assert_eq!(chunked_array.len(), col1.len());
        assert_eq!(chunked_array.len(), col2.len());

        let pos = col2.len() / 2;
        let needle = col2[pos];
        check_bin_search_pos(&chunked_array, needle, pos);

        let pos = col2.len() - 1;
        let needle = col2[pos];
        check_bin_search_pos(&chunked_array, needle, pos);

        let pos = 0;
        let needle = col2[pos];
        check_bin_search_pos(&chunked_array, needle, pos);
    }

    fn check_bin_search_pos(
        chunked_array: &ChunkedArray<crate::arrow2::array::StructArray>,
        needle: u64,
        pos: usize,
    ) {
        let actual = chunked_array
            .into_primitive_col::<u64>(1)
            .unwrap()
            .partition_point(|i| i < Some(needle));
        assert_eq!(actual, pos);

        let actual = chunked_array
            .non_null_primitive_col::<u64>(1)
            .unwrap()
            .partition_point(|i| i < needle);
        assert_eq!(actual, pos);
    }

    proptest! {
        #[test]
        fn chunk_primitive_array(arr in any::<Arc<[i32]>>()
            .prop_filter("at least one", |v| !v.is_empty())
            .prop_flat_map(|v| any::<usize>().prop_filter("at least 1", |chunk_size| chunk_size > &1).prop_map(move |i| (v.clone(), i)))) {
            sanity_test_chunk_array(Vec::from(&arr.0[..]), arr.1);
        }

        #[test]
        fn chunk_boolean_array(arr in any::<Arc<[bool]>>()
            .prop_filter("at least one", |v| !v.is_empty())
            .prop_flat_map(|v| any::<usize>().prop_filter("at least 1", |chunk_size| chunk_size > &1).prop_map(move |i| (v.clone(), i)))) {
            sanity_test_chunk_array_bool(Vec::from(&arr.0[..]), arr.1);
        }

        #[test]
        fn chunk_utf8_array(arr in any::<Arc<[String]>>()
            .prop_filter("at least one", |v| !v.is_empty())
            .prop_flat_map(|v| any::<usize>().prop_filter("at least 1", |chunk_size| chunk_size > &1).prop_map(move |i| (v.clone(), i)))) {
           let str_vec = arr.0.iter().map(|s| s.as_str()).collect_vec();
           sanity_test_chunk_array_utf8(str_vec, arr.1);
        }

        #[test]
        fn chunk_struct_array(arr in ( 1usize..200 )
                .prop_flat_map(|c| any_with::<Vec<String>>(size_range(c..=c).lift())
                    .prop_flat_map(move |col1| any_with::<Vec<u64>>(size_range(c..=c).lift())
                    .prop_map(move |col2| (col1.clone(), col2)))), chunk_size in 1usize..20) {
            let (col1, col2) = arr;
            let str_vec = col1.iter().map(|s| s.as_str()).collect_vec();
            sanity_test_chunk_array_struct(chunk_size, str_vec, col2)
        }

        #[test]
        fn binary_search(arr in ( 1usize..200 )
                .prop_flat_map(|c| any_with::<Vec<String>>(size_range(c..=c).lift())
                    .prop_flat_map(move |col1| any_with::<Vec<u64>>(size_range(c..=c).lift())
                    .prop_map(move |col2| (col1.clone(), col2)))), chunk_size in 1usize..20) {
                        let (col1, mut col2) = arr;
                        let mut col1 = Vec::from(&col1[..]);
                        col1.sort();
                        col2.sort();
                        let str_vec = col1.iter().map(|s| s.as_str()).collect_vec();
                        sanity_test_chunk_array_binary_search(chunk_size, str_vec, col2)
                    }
    }
}
