use crate::arrow::{
    adj_schema,
    mmap::{mmap_batch, write_batches},
    Error, GID,
};
use arrow2::{
    array::{
        Array, ListArray, MutableListArray, MutablePrimitiveArray, MutableStructArray, Utf8Array,
    },
    chunk::Chunk,
    datatypes::{DataType, Field, Schema},
    error::Result as ArrowResult,
    io::parquet::read::infer_schema,
    offset::Offsets,
};
use itertools::Itertools;
use rayon::prelude::*;
use std::path::{Path, PathBuf};

use super::{
    global_order::GlobalOrder,
    node_chunk::NodeChunk,
    parquet_reader::{read_file_metadata, read_parquet_file},
};

pub(crate) struct NodeBuilder {
    pub(crate) adj_out_chunks: Vec<NodeChunk>, // chunks for the adjacency list, these are ListArrays with a struct {eid, vid}
    thread_handles: Vec<std::thread::JoinHandle<ArrowResult<NodeChunk>>>,

    adj_out_dst: Vec<u64>, // the dst of the adjacency list for the current chunk
    adj_out_eid: Vec<u64>, // the eid of the adjacency list for the current chunk
    adj_out_offsets: Vec<i64>, // the offsets of the adjacency list for the current chunk

    chunk_size: usize,
    num_nodes: usize,
    chunk_adj_out_offset: i64,
    pub(crate) last_edge: Option<(u64, u64)>,
    last_dst_idx: usize,
    last_src_idx: usize,
    last_chunk: Option<usize>,
    e_id: u64,
    location_path: PathBuf,
}

impl NodeBuilder {
    pub(crate) fn new<P: AsRef<Path>>(chunk_size: usize, num_nodes: usize, path: P) -> Self {
        Self {
            adj_out_chunks: vec![],
            thread_handles: vec![],
            adj_out_dst: vec![],
            adj_out_eid: vec![],
            adj_out_offsets: vec![],
            chunk_size,
            num_nodes,
            chunk_adj_out_offset: 0,
            last_edge: None,
            last_dst_idx: 0,
            last_src_idx: 0,
            last_chunk: None,
            e_id: 0,
            location_path: path.as_ref().to_path_buf(),
        }
    }

    fn push_chunk(&mut self) -> ArrowResult<()> {
        let mut adj_out_eid_prev = Vec::with_capacity(self.adj_out_eid.len());
        let mut adj_out_dst_prev = Vec::with_capacity(self.adj_out_dst.len());
        let mut adj_out_offsets_prev = Vec::with_capacity(self.adj_out_offsets.len());

        std::mem::swap(&mut adj_out_eid_prev, &mut self.adj_out_eid);
        std::mem::swap(&mut adj_out_dst_prev, &mut self.adj_out_dst);
        std::mem::swap(&mut adj_out_offsets_prev, &mut self.adj_out_offsets);

        let id = self.adj_out_chunks.len();
        self.adj_out_chunks.push(NodeChunk::empty()); // placeholder
        adj_out_offsets_prev.push(self.chunk_adj_out_offset);
        let location_path = self.location_path.clone();

        let file_path = self
            .location_path
            .join(format!("adj_out_chunk_{:08}.ipc", id));

        // TODO: add a thread pool
        let handle = std::thread::spawn(move || {
            let col =
                new_arrow_adj_list_chunk(adj_out_dst_prev, adj_out_eid_prev, adj_out_offsets_prev);

            Self::persist_and_mmap_adj_chunk(location_path, id, col)
                .expect(format!("Failed to persist and mmap adj chunk {id}").as_str());

            let mmapped_chunk = unsafe { mmap_batch(file_path.as_path(), 0) };
            mmapped_chunk.map(|chunk| NodeChunk::new(chunk))
        });

        self.thread_handles.push(handle);

        // fill chunk with empty adjacency lists
        self.chunk_adj_out_offset = 0i64;
        Ok(())
    }

    fn load_all_chunks_replace_placeholders(&mut self) -> ArrowResult<()> {
        self.adj_out_chunks
            .iter_mut()
            .zip(self.thread_handles.drain(..))
            .for_each(|(node_chunk, handle)| {
                let t_node_chunk = handle
                    .join()
                    .expect("Failed to join thread")
                    .expect("Failed to load chunk");
                *node_chunk = t_node_chunk;
            });
        Ok(())
    }

    fn persist_and_mmap_adj_chunk(
        location_path: PathBuf,
        id: usize,
        col: Box<dyn Array>,
    ) -> ArrowResult<()> {
        let dtype = col.data_type().clone();
        let schema = Schema::from(vec![Field::new("adj_out", dtype, false)]);
        let file_path = location_path.join(format!("adj_out_chunk_{:08}.ipc", id));
        let chunk = [Chunk::try_new(vec![col])?];
        write_batches(file_path.as_path(), schema.clone(), &chunk)?;
        Ok(())
    }

    pub(crate) fn push_update(&mut self, src: u64, dst: u64) -> Result<(), Error> {
        let same_edge = self
            .last_edge
            .as_ref()
            .map(|(prev_src, prev_dst)| prev_src == &src && prev_dst == &dst)
            .unwrap_or_default();

        let not_same_source = self
            .last_edge
            .as_ref()
            .filter(|(prev_src, _)| prev_src == &src)
            .is_none();

        if !same_edge {
            if not_same_source {
                // new source or first edge
                self.last_src_idx = src as usize;
                let chunk_id = self.last_src_idx / self.chunk_size;
                if let Some(last_chunk) = self.last_chunk {
                    assert!(
                        chunk_id >= last_chunk,
                        "Chunk id {} is less than last chunk {}",
                        chunk_id,
                        last_chunk
                    );
                }
                self.last_chunk = Some(chunk_id);
                // figure out what chunk are we in
                self.extend_empty(self.last_src_idx)?;
            }
            self.adj_out_eid.push(self.e_id);
            self.last_dst_idx = dst as usize;
            self.adj_out_dst.push(self.last_dst_idx as u64);

            self.e_id += 1;
            self.chunk_adj_out_offset += 1;
        }
        self.last_edge = Some((src, dst));
        Ok(())
    }

    fn extend_empty(&mut self, new_src: usize) -> Result<(), Error> {
        let old_chunk = self.adj_out_chunks.len();
        let new_chunk = new_src / self.chunk_size;
        for _ in old_chunk..new_chunk {
            self.adj_out_offsets
                .resize(self.chunk_size, self.chunk_adj_out_offset);
            self.push_chunk()?;
        }
        self.adj_out_offsets
            .resize((new_src % self.chunk_size) + 1, self.chunk_adj_out_offset);
        Ok(())
    }

    pub(crate) fn finalise_empty_chunks(&mut self) -> Result<(), Error> {
        if self.last_edge.is_some() {
            self.extend_empty(self.num_nodes - 1)?;
            if !self.adj_out_offsets.is_empty() {
                self.push_chunk()?;
            }
        }
        self.load_all_chunks_replace_placeholders()?;
        Ok(())
    }
}

fn new_arrow_adj_list_chunk(
    adj_out_dst: Vec<u64>,
    adj_out_eid: Vec<u64>,
    adj_out_offsets: Vec<i64>,
) -> Box<dyn Array> {
    let dst_col = Box::new(MutablePrimitiveArray::<u64>::from_vec(adj_out_dst));
    let eid_col = Box::new(MutablePrimitiveArray::<u64>::from_vec(adj_out_eid));

    let values = MutableStructArray::new(adj_schema(), vec![dst_col, eid_col]);

    let outbound2 = MutableListArray::new_from_mutable(
        values,
        Offsets::try_from(adj_out_offsets).unwrap(),
        None,
    );

    let outbound: ListArray<i64> = outbound2.into();
    Box::new(outbound)
}

pub(crate) struct ParquetSource<F> {
    files: Vec<PathBuf>,
    concurrent_files: usize,
    projection: Option<Vec<String>>,
    mapper: F,
}

impl<F> ParquetSource<F>
where
    F: Fn(Chunk<Box<dyn Array>>) -> Result<LoadingState, Error>,
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

impl<F> ParquetSource<F>
where
    F: Fn(Chunk<Box<dyn Array>>) -> Result<LoadingState, Error> + Sync,
{
    pub(crate) fn produce<S, CB: Fn(&mut S, PathBuf, usize, LoadingState) -> Result<(), Error>>(
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
    let mut src_counts: Vec<usize> = vec![1];

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
                *src_counts.last_mut().expect("this should not be empty") += 1;
            }
        } else {
            src_counts.push(1);
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
    pub(crate) src_counts: Vec<usize>, // used to compute the offsets for the static graph (adj lists)
}

#[cfg(test)]
mod test {
    use arrow2::{array::PrimitiveArray, chunk::Chunk};

    use crate::arrow::{global_order::GlobalMap, node_builder::LoadingState, GID};

    use super::resolve_and_dedup_chunk;

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
                src_counts: vec![1],
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
                src_counts: vec![2, 1],
            }
        );
    }

    #[test]
    fn something() {
        let chunk = Chunk::new(vec![
            PrimitiveArray::from_vec(vec![1i64, 1]).boxed(),
            PrimitiveArray::from_vec(vec![1i64, 2]).boxed(),
        ]);

        let go = GlobalMap::from(vec![GID::I64(1), GID::I64(2), GID::I64(3)]);
        let actual = resolve_and_dedup_chunk(chunk, &go).unwrap();
        println!("{:?}", actual);

        let chunk = Chunk::new(vec![
            PrimitiveArray::from_vec(vec![1i64, 2]).boxed(),
            PrimitiveArray::from_vec(vec![3i64, 0]).boxed(),
        ]);

        let actual = resolve_and_dedup_chunk(chunk, &go).unwrap();
        println!("{:?}", actual);
    }
}
