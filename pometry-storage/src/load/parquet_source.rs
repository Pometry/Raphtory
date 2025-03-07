use std::path::PathBuf;

use crate::arrow2::{
    array::{Array, Utf8Array, Utf8ViewArray},
    datatypes::ArrowDataType as DataType,
    record_batch::RecordBatchT as Chunk,
};
use itertools::Itertools;
use polars_arrow::datatypes::ArrowSchema;
use rayon::prelude::*;

use crate::{global_order::GlobalOrder, load::read_schema, RAError, GID};

use super::parquet_reader::{read_file_metadata, read_parquet_file};

pub(crate) struct ParquetSource<F, ST>
where
    F: Fn(Chunk<Box<dyn Array>>) -> Result<ST, RAError>,
{
    files: Vec<PathBuf>,
    concurrent_files: usize,
    src_col: String,
    dst_col: String,
    mapper: F,
}

impl<F, ST> ParquetSource<F, ST>
where
    F: Fn(Chunk<Box<dyn Array>>) -> Result<ST, RAError>,
{
    pub fn new(
        files: Vec<PathBuf>,
        concurrent_files: usize,
        src_col: &str,
        dst_col: &str,
        mapper: F,
    ) -> Self {
        Self {
            files,
            concurrent_files,
            src_col: src_col.to_owned(),
            dst_col: dst_col.to_owned(),
            mapper,
        }
    }
}

impl<F, ST> ParquetSource<F, ST>
where
    F: Fn(Chunk<Box<dyn Array>>) -> Result<ST, RAError> + Sync,
    ST: Send,
{
    pub(crate) fn produce<S, CB: Fn(&mut S, PathBuf, usize, ST) -> Result<(), RAError>>(
        &self,
        s: &mut S,
        cb: CB,
    ) -> Result<(), RAError> {
        let file_groups = self
            .files
            .iter()
            .chunks(self.concurrent_files)
            .into_iter()
            .map(|c| c.cloned().collect_vec())
            .collect_vec();

        for file_group in file_groups {
            self.process_file_group(file_group, &cb, s)?;
        }
        Ok(())
    }

    fn process_file_group<S, CB: Fn(&mut S, PathBuf, usize, ST) -> Result<(), RAError>>(
        &self,
        file_group: Vec<PathBuf>,
        cb: &CB,
        s: &mut S,
    ) -> Result<(), RAError> {
        let mut chunks = file_group
            .into_par_iter()
            .flat_map(|file| {
                self.read_one_parquet_file(file.clone())
                    .unwrap_or_else(|_| panic!("error reading file {:?}", &file))
            })
            .collect::<Result<Vec<_>, _>>()?;

        chunks.sort_by_key(|(file, id, _)| (file.clone(), *id));

        for (file, id, chunk) in chunks {
            cb(s, file.clone(), id, chunk)?
        }
        Ok(())
    }

    fn read_one_parquet_file(
        &self,
        file: PathBuf,
    ) -> Result<impl ParallelIterator<Item = Result<(PathBuf, usize, ST), RAError>> + '_, RAError>
    {
        let metadata = read_file_metadata(&file)?;
        let schema = read_schema(&metadata)?;

        let src_field = schema
            .fields
            .iter()
            .find(|field| &field.name == &self.src_col)
            .ok_or_else(|| RAError::ColumnNotFound(self.dst_col.clone()))?;
        let dst_field = schema
            .fields
            .iter()
            .find(|field| &field.name == &self.dst_col)
            .ok_or_else(|| RAError::ColumnNotFound(self.dst_col.clone()))?;

        let schema = ArrowSchema::from(vec![src_field.clone(), dst_field.clone()])
            .with_metadata(schema.metadata);
        let iter = read_parquet_file(file.clone(), schema.clone())?;

        Ok(iter
            .filter_map(|rb| rb.ok())
            .enumerate()
            .par_bridge()
            .map(move |(id, chunk)| {
                let chunk = (self.mapper)(chunk);
                chunk.map(|state| (file.clone(), id, state))
            }))
    }
}

pub(crate) fn resolve_and_dedup_chunk<GO: GlobalOrder + Send + Sync>(
    chunk: &[Box<dyn Array>],
    go: &GO,
) -> Result<LoadingState, RAError> {
    let chunk = chunk;
    let src = &chunk[0];
    let dst = &chunk[1];

    // three cases u64, i64 and str

    assert_eq!(
        src.data_type(),
        dst.data_type(),
        "src and dst must have the same type"
    );

    let mut mapped_nodes = match src.data_type() {
        DataType::Int64 => {
            let src = src
                .as_any()
                .downcast_ref::<crate::arrow2::array::PrimitiveArray<i64>>()
                .unwrap();

            let dst = dst
                .as_any()
                .downcast_ref::<crate::arrow2::array::PrimitiveArray<i64>>()
                .unwrap();

            let values = [src, dst]
                .into_par_iter()
                .map(|arr| {
                    arr.values_iter()
                        .map(|id| go.find(&GID::U64(*id as u64)).map(|id| id as u64).unwrap())
                        .collect_vec()
                })
                .collect::<Vec<_>>();

            values
        }
        DataType::UInt64 => {
            let src = src
                .as_any()
                .downcast_ref::<crate::arrow2::array::PrimitiveArray<u64>>()
                .unwrap();

            let dst = dst
                .as_any()
                .downcast_ref::<crate::arrow2::array::PrimitiveArray<u64>>()
                .unwrap();

            let values = [src, dst]
                .into_par_iter()
                .map(|arr| {
                    arr.values_iter()
                        .map(|id| go.find(&GID::U64(*id)).map(|id| id as u64).unwrap())
                        .collect_vec()
                })
                .collect::<Vec<_>>();

            values
        }
        DataType::LargeUtf8 => {
            let src = src.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();

            let dst = dst.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();

            let values = make_str_mapped_nodes(go, src, dst, |arr| arr.values_iter());

            values
        }
        DataType::Utf8 => {
            let src = src.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();

            let dst = dst.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();

            let values = make_str_mapped_nodes(go, src, dst, |arr| arr.values_iter());

            values
        }
        DataType::Utf8View => {
            let src = src.as_any().downcast_ref::<Utf8ViewArray>().unwrap();

            let dst = dst.as_any().downcast_ref::<Utf8ViewArray>().unwrap();

            let values = make_str_mapped_nodes(go, src, dst, |arr| arr.values_iter());

            values
        }
        _ => panic!("Unsupported type"),
    };

    let dst = mapped_nodes.pop().unwrap();
    let src = mapped_nodes.pop().unwrap();

    let mut iter = src.iter().copied().zip(dst.iter().copied());
    let first = iter.next().ok_or_else(|| RAError::EmptyChunk)?;
    let mut last @ (last_src, last_dst) = first;
    let mut counts: Vec<usize> = vec![1];
    let mut src_counts = vec![(first.0, 1)];

    let mut deduped_src_ids = vec![last_src];
    let mut deduped_dst_ids = vec![last_dst];

    for edge @ (src, dst) in iter {
        if edge == last {
            *counts.last_mut().expect("this is not empty") += 1;
        } else {
            assert!(edge >= last, "edge list not sorted ({}, {}), ({}, {})", src, dst, last.0, last.1);
            counts.push(1);

            deduped_src_ids.push(src);
            deduped_dst_ids.push(dst);
        }
        let (last_src, last_dst) = last;
        if src == last_src {
            if dst != last_dst {
                src_counts.last_mut().expect("this should not be empty").1 += 1;
            }
        } else {
            src_counts.push((src, 1));
        }
        last = edge;
    }

    Ok(LoadingState {
        deduped_src_ids,
        deduped_dst_ids,
        edge_counts: counts,
        src_counts,
    })
}

fn make_str_mapped_nodes<
    'a,
    A: Array,
    I: Iterator<Item = &'a str>,
    GO: GlobalOrder + Send + Sync,
>(
    go: &GO,
    src: &'a A,
    dst: &'a A,
    values_iter: impl Fn(&'a A) -> I + Sync,
) -> Vec<Vec<u64>> {
    [src, dst]
        .into_par_iter()
        .map(|arr| {
            values_iter(arr)
                .map(|id| {
                    go.find(&GID::Str(id.to_owned()))
                        .map(|id| id as u64)
                        .unwrap()
                })
                .collect_vec()
        })
        .collect::<Vec<_>>()
}

#[derive(Debug, PartialEq)]
pub(crate) struct LoadingState {
    pub(crate) deduped_src_ids: Vec<u64>,
    pub(crate) deduped_dst_ids: Vec<u64>,

    pub(crate) edge_counts: Vec<usize>, // used to compute the offsets for edge temporal properties
    pub(crate) src_counts: Vec<(u64, usize)>, // used to compute the offsets for the static graph (adj lists)
}

#[cfg(test)]
mod test {
    use crate::arrow2::array::PrimitiveArray;

    use crate::{global_order::GlobalMap, GID};

    use super::*;

    #[test]
    fn dedup_1_row() {
        let chunk = vec![
            PrimitiveArray::from_vec(vec![1u64]).boxed(),
            PrimitiveArray::from_vec(vec![2u64]).boxed(),
        ];

        let go = GlobalMap::from(vec![GID::U64(1), GID::U64(2)]);
        let actual = resolve_and_dedup_chunk(&chunk, &go).unwrap();

        assert_eq!(
            actual,
            LoadingState {
                deduped_src_ids: vec![0],
                deduped_dst_ids: vec![1],
                edge_counts: vec![1],
                src_counts: vec![(0, 1)],
            }
        );
    }

    #[test]
    fn dedup_rows() {
        let chunk = vec![
            PrimitiveArray::from_vec(vec![1u64, 1, 1, 2]).boxed(),
            PrimitiveArray::from_vec(vec![2u64, 2, 3, 3]).boxed(),
        ];

        let go = GlobalMap::from(vec![GID::U64(1), GID::U64(2), GID::U64(3)]);
        let actual = resolve_and_dedup_chunk(&chunk, &go).unwrap();

        assert_eq!(
            actual,
            LoadingState {
                deduped_src_ids: vec![0, 0, 1],
                deduped_dst_ids: vec![1, 2, 2],
                edge_counts: vec![2, 1, 1],
                src_counts: vec![(0, 2), (1, 1)],
            }
        );
    }

    #[test]
    fn self_loops() {
        let chunk = vec![
            PrimitiveArray::from_vec(vec![0u64, 0, 2, 2, 2]).boxed(),
            PrimitiveArray::from_vec(vec![0u64, 1, 1, 2, 2]).boxed(),
        ];

        let go = GlobalMap::from(vec![GID::U64(0), GID::U64(1), GID::U64(2), GID::U64(3)]);
        let actual = resolve_and_dedup_chunk(&chunk, &go).unwrap();

        assert_eq!(
            actual,
            LoadingState {
                deduped_src_ids: vec![0, 0, 2, 2],
                deduped_dst_ids: vec![0, 1, 1, 2],
                edge_counts: vec![1, 1, 1, 2],
                src_counts: vec![(0, 2), (2, 2)],
            }
        );
    }
}
