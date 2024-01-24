use crate::arrow::{
    Error, GID,
};
use arrow2::{
    array::{
        Array, Utf8Array,
    },
    chunk::Chunk,
    datatypes::DataType,
    io::parquet::read::infer_schema,
};
use itertools::Itertools;
use rayon::prelude::*;
use std::path::PathBuf;

use super::{
    global_order::GlobalOrder,
    parquet_reader::{read_file_metadata, read_parquet_file},
};

pub(crate) struct ParquetSource<F, ST>
where
    F: Fn(Chunk<Box<dyn Array>>) -> Result<ST, Error>,
{
    files: Vec<PathBuf>,
    concurrent_files: usize,
    projection: Option<Vec<String>>,
    mapper: F,
}

impl<F, ST> ParquetSource<F, ST>
where
    F: Fn(Chunk<Box<dyn Array>>) -> Result<ST, Error>,
{
    pub fn new(
        files: Vec<PathBuf>,
        concurrent_files: usize,
        projection: Option<Vec<&str>>,
        mapper: F,
    ) -> Self {
        Self {
            files,
            concurrent_files,
            projection: projection.map(|p| p.into_iter().map(|s| s.to_string()).collect_vec()),
            mapper,
        }
    }
}

impl<F, ST> ParquetSource<F, ST>
where
    F: Fn(Chunk<Box<dyn Array>>) -> Result<ST, Error> + Sync,
    ST: Send
{
    pub(crate) fn produce<S, CB: Fn(&mut S, PathBuf, usize, ST) -> Result<(), Error>>(
        &self,
        s: &mut S,
        cb: CB,
    ) -> Result<(), Error> {
        let file_groups = self
            .files
            .iter()
            .chunks(self.concurrent_files)
            .into_iter()
            .map(|c| c.cloned().collect_vec())
            .collect_vec();

        file_groups.into_iter().for_each(|file_group| {
            let mut chunks = file_group
                .into_par_iter()
                .flat_map(|file| {
                    let metadata = read_file_metadata(&file)
                        .expect(format!("Failed to read metadata for file {:?}", file).as_str());
                    let schema = infer_schema(&metadata)
                        .expect(format!("Failed to infer schema for file {:?}", file).as_str());
                    let schema = self
                        .projection
                        .as_ref()
                        .map(|p| schema.clone().filter(|_, field| p.contains(&field.name)))
                        .unwrap_or(schema);
                    read_parquet_file(file.clone(), schema)
                        .expect(format!("Failed to read parquet file {:?}", file).as_str())
                        .flatten()
                        .enumerate()
                        .par_bridge()
                        .map(move |(id, chunk)| {
                            let chunk = (&self.mapper)(chunk);
                            chunk.map(|state| (file.clone(), id, state))
                        })
                })
                .collect::<Result<Vec<_>, _>>()
                .expect("Failed to collect chunks");

            chunks.sort_by_key(|(file, id, _)| (file.clone(), *id));

            chunks.into_iter().for_each(|(file, id, chunk)| {
                cb(s, file.clone(), id, chunk).expect(
                    format!("Failed to process chunk {:?} from file {:?}", id, file).as_str(),
                )
            });
        });
        Ok(())
    }
}

pub(crate) fn resolve_and_dedup_chunk<GO: GlobalOrder + Send + Sync>(
    chunk: Chunk<Box<dyn Array>>,
    go: &GO,
) -> Result<LoadingState, Error> {
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
                .downcast_ref::<arrow2::array::PrimitiveArray<i64>>()
                .unwrap();

            let dst = dst
                .as_any()
                .downcast_ref::<arrow2::array::PrimitiveArray<i64>>()
                .unwrap();

            let values = [src, dst]
                .into_par_iter()
                .map(|arr| {
                    arr.values_iter()
                        .map(|id| go.find(&GID::I64(*id)).map(|id| id as u64).unwrap())
                        .collect_vec()
                })
                .collect::<Vec<_>>();

            values
        }
        DataType::UInt64 => {
            let src = src
                .as_any()
                .downcast_ref::<arrow2::array::PrimitiveArray<u64>>()
                .unwrap();

            let dst = dst
                .as_any()
                .downcast_ref::<arrow2::array::PrimitiveArray<u64>>()
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

            let values = [src, dst]
                .into_par_iter()
                .map(|arr| {
                    arr.values_iter()
                        .map(|id| {
                            go.find(&GID::Str(id.to_owned()))
                                .map(|id| id as u64)
                                .unwrap()
                        })
                        .collect_vec()
                })
                .collect::<Vec<_>>();

            values
        }
        DataType::Utf8 => {
            let src = src.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();

            let dst = dst.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();

            let values = [src, dst]
                .into_par_iter()
                .map(|arr| {
                    arr.values_iter()
                        .map(|id| {
                            go.find(&GID::Str(id.to_owned()))
                                .map(|id| id as u64)
                                .unwrap()
                        })
                        .collect_vec()
                })
                .collect::<Vec<_>>();

            values
        }
        _ => panic!("Unsupported type"),
    };

    let dst = mapped_nodes.pop().unwrap();
    let src = mapped_nodes.pop().unwrap();

    let mut iter = src.iter().copied().zip(dst.iter().copied());
    let first = iter.next().ok_or_else(|| Error::EmptyChunk)?;
    let mut last @ (last_src, last_dst) = first.clone();
    let mut counts: Vec<usize> = vec![1];
    let mut src_counts = vec![(first.0, 1)];

    let mut deduped_src_ids = vec![last_src];
    let mut deduped_dst_ids = vec![last_dst];

    for edge @ (src, dst) in iter {
        if edge == last {
            *counts.last_mut().expect("this is not empty") += 1;
        } else {
            counts.push(1);

            deduped_src_ids.push(src);
            deduped_dst_ids.push(dst);
        }
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

#[derive(Debug, PartialEq)]
pub(crate) struct LoadingState {
    pub(crate) deduped_src_ids: Vec<u64>,
    pub(crate) deduped_dst_ids: Vec<u64>,

    pub(crate) edge_counts: Vec<usize>, // used to compute the offsets for edge temporal properties
    pub(crate) src_counts: Vec<(u64, usize)>, // used to compute the offsets for the static graph (adj lists)
}

#[cfg(test)]
mod test {
    use arrow2::{array::PrimitiveArray, chunk::Chunk};

    use crate::arrow::{global_order::GlobalMap, GID};

    use super::*;

    #[test]
    fn dedup_1_row() {
        let chunk = Chunk::new(vec![
            PrimitiveArray::from_vec(vec![1u64]).boxed(),
            PrimitiveArray::from_vec(vec![2u64]).boxed(),
        ]);

        let go = GlobalMap::from(vec![GID::U64(1), GID::U64(2)]);
        let actual = resolve_and_dedup_chunk(chunk, &go).unwrap();

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
        let chunk = Chunk::new(vec![
            PrimitiveArray::from_vec(vec![1i64, 1, 1, 2]).boxed(),
            PrimitiveArray::from_vec(vec![2i64, 2, 3, 3]).boxed(),
        ]);

        let go = GlobalMap::from(vec![GID::I64(1), GID::I64(2), GID::I64(3)]);
        let actual = resolve_and_dedup_chunk(chunk, &go).unwrap();

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

}
