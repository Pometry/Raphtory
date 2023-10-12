use std::{
    io,
    io::BufReader,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{
    arrow::{
        adj_schema,
        edge_frame_builder::{EdgeFrameBuilder, EdgeOverflowChunk},
        list_buffer::{as_primitive_column, ListColumn},
        loader::{read_file_chunks, sort_dedup_2, sort_within_chunks},
        mmap::{mmap_all_chunks, mmap_batch, mmap_batches, write_batches},
        prepare_graph_dir,
        vertex_frame_builder::VertexFrameBuilder,
        Error,
    },
    core::{
        entities::{EID, VID},
        Direction,
    },
};
use arrow2::{
    array::{Array, ListArray, PrimitiveArray, StructArray},
    chunk::Chunk,
    datatypes::{Field, Schema},
    io::{
        ipc::write::{FileWriter, WriteOptions},
        parquet::read,
    },
    offset::OffsetsBuffer,
    types::NativeType,
};
use itertools::Itertools;
use rayon::prelude::*;
use tempfile::tempfile_in;

use super::{
    array_as_id_iter,
    edge_chunk::EdgeChunk,
    global_order::{GlobalMap, GlobalOrder},
    vertex_chunk::{RowOwned, VertexChunk},
    LoadChunk, Time, GID,
};

#[derive(Debug)]
pub struct TempColGraphFragment {
    vertex_chunk_size: usize,
    edge_chunk_size: usize,
    adj_out_chunks: Vec<VertexChunk>,
    adj_in_chunks: Vec<VertexChunk>,
    edge_chunks: Vec<EdgeChunk>,
    graph_dir: Box<Path>,
}

impl TempColGraphFragment {
    pub fn new<P: AsRef<Path>>(graph_dir: P) -> Result<Self, Error> {
        // iterate graph dir and split into two vectors of files edge_chunk_{j}.ipc and adj_out_chunk_{i}.ipc

        let iter = std::fs::read_dir(&graph_dir)?
            .flatten()
            .filter(|dir_entry| {
                dir_entry
                    .file_name()
                    .to_str()
                    .map(|file_name| {
                        !file_name.ends_with("_overflow")
                            && (file_name.starts_with("edge_chunk_")
                                || file_name.starts_with("adj_out_chunk_")
                                || file_name.starts_with("adj_in_chunk_"))
                    })
                    .unwrap_or(false)
            })
            .sorted_by(|f1, f2| f1.path().cmp(&f2.path()));

        let mut adj_out_chunks = Vec::default();
        let mut adj_in_chunks = Vec::default();
        let mut edge_chunks = Vec::default();
        for file_path in iter {
            let file_name = file_path.file_name();
            let file_name = file_name
                .to_str()
                .expect("file names are already filtered and thus valid");
            if file_name.starts_with("edge_chunk_") {
                let overflow_dir = graph_dir.as_ref().join(
                    &(file_path
                        .path()
                        .file_stem()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_owned()
                        + "_overflow"),
                );
                let overflow_chunks: Vec<_> = std::fs::read_dir(&overflow_dir)
                    .map(|iter| {
                        iter.flatten()
                            .filter(|dir_entry| {
                                dir_entry
                                    .file_name()
                                    .to_str()
                                    .map(|file_name| file_name.starts_with("edge_chunk_overflow_"))
                                    .unwrap_or(false)
                            })
                            .sorted_by(|a, b| a.path().cmp(&b.path()))
                            .map(|f| {
                                EdgeOverflowChunk::new(unsafe {
                                    mmap_all_chunks(f.path()).unwrap()
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                edge_chunks.push(EdgeChunk::new(
                    unsafe { mmap_batch(file_path.path(), 0) }?,
                    overflow_chunks,
                ));
            } else if file_name.starts_with("adj_in_chunk_") {
                adj_in_chunks.push(VertexChunk::new(unsafe {
                    mmap_batch(file_path.path(), 0)
                }?));
            } else if file_name.starts_with("adj_out_chunk_") {
                adj_out_chunks.push(VertexChunk::new(unsafe {
                    mmap_batch(file_path.path(), 0)
                }?));
            }
        }

        let vertex_chunk_size = adj_out_chunks.first().map(|v| v.len()).unwrap_or(0);
        let edge_chunk_size = edge_chunks.first().map(|e| e.len()).unwrap_or(0);

        Ok(Self {
            vertex_chunk_size,
            edge_chunk_size,
            adj_out_chunks,
            adj_in_chunks,
            edge_chunks,
            graph_dir: graph_dir.as_ref().into(),
        })
    }

    pub fn edge_property_id(&self, name: &str) -> Option<usize> {
        let edge_chunk = self.edge_chunks.first()?;
        edge_chunk.temporal_edge_property_id(name)
    }

    pub fn from_sorted_parquet_dir_edge_list<P: AsRef<Path> + Clone, P2: AsRef<Path>>(
        parquet_dir: P,
        src_col: &str,
        src_hash_col: &str,
        dst_col: &str,
        dst_hash_col: &str,
        time_col: &str,
        projection: Option<Vec<&str>>,
        vertex_chunk_size: usize,
        edge_chunk_size: usize,
        edge_max_list_size: usize,
        graph_dir: P2,
    ) -> Result<Self, Error> {
        let sorted_gids_path = parquet_dir
            .as_ref()
            .to_path_buf()
            .join("sorted_global_ids.ipc");

        prepare_graph_dir(graph_dir.as_ref())?;

        let srcs_parquet_files = std::fs::read_dir(&parquet_dir)?
            .map(|res| res.unwrap())
            .filter(|e| {
                e.path()
                    .extension()
                    .filter(|ext| ext == &"parquet")
                    .is_some()
            })
            .sorted_by(|f1, f2| f1.path().cmp(&f2.path()))
            .collect_vec();

        let triplets_parquet_files2 = std::fs::read_dir(&parquet_dir)?
            .map(|res| res.unwrap())
            .filter(|e| {
                e.path()
                    .extension()
                    .filter(|ext| ext == &"parquet")
                    .is_some()
            })
            .sorted_by(|f1, f2| f1.path().cmp(&f2.path()));

        let now = std::time::Instant::now();

        let sorted_vertices = if sorted_gids_path.exists() {
            let chunk = unsafe { mmap_batch(sorted_gids_path.as_path(), 0)? };
            chunk.into_arrays().into_iter().next().unwrap()
        } else {
            let (_, sorted_vertices) = srcs_parquet_files
                .par_iter()
                .map(|dir_entry| {
                    sort_within_chunks(
                        dir_entry.path(),
                        src_col,
                        src_hash_col,
                        dst_col,
                        dst_hash_col,
                    )
                })
                .flatten()
                .reduce_with(|(l_hash, l), (r_hash, r)| {
                    sort_dedup_2((&l_hash, &l), (&r_hash, &r)).unwrap()
                })
                .unwrap();

            let schema = Schema::from(vec![Field::new(
                "sorted_global_ids",
                sorted_vertices.data_type().clone(),
                false,
            )]);

            let chunk = [Chunk::try_new(vec![sorted_vertices.clone()])?];
            write_batches(sorted_gids_path.as_path(), schema, &chunk)?;
            sorted_vertices
        };

        let mut go: GlobalMap = GlobalMap::default();
        for (i, gid) in array_as_id_iter(&sorted_vertices)?.enumerate() {
            go.insert(gid, i);
        }

        println!(
            "DONE global order time: {:?}, len: {}",
            now.elapsed(),
            go.len()
        );

        let chunks = triplets_parquet_files2
            .flat_map(|dir_entry| {
                read_file_chunks(
                    dir_entry.path(),
                    src_col,
                    dst_col,
                    time_col,
                    projection.as_ref(),
                )
            })
            .flatten();

        let out = Self::build_tables_from_chunked(
            graph_dir,
            vertex_chunk_size,
            edge_chunk_size,
            edge_max_list_size,
            go.into(),
            chunks,
        )?;
        Ok(out)
    }

    pub fn from_sorted_parquet_edge_list<P: AsRef<Path> + Clone, P2: AsRef<Path>>(
        parquet_file: P,
        src_col: &str,
        dst_col: &str,
        time_col: &str,
        vertex_chunk_size: usize,
        edge_chunk_size: usize,
        edge_max_list_size: usize,
        graph_dir: P2,
    ) -> Result<Self, Error> {
        prepare_graph_dir(graph_dir.as_ref())?;

        let mut all_gids: GlobalMap =
            read_vertices_only(parquet_file.clone(), src_col, dst_col)?.collect();

        all_gids.maybe_sort();

        let chunks = read_file_chunks(parquet_file, src_col, dst_col, time_col, None)?;

        let out = Self::build_tables_from_chunked(
            graph_dir,
            vertex_chunk_size,
            edge_chunk_size,
            edge_max_list_size,
            Arc::new(all_gids),
            chunks,
        )?;
        Ok(out)
    }

    pub(crate) fn build_tables_from_chunked<P: AsRef<Path>, GO: GlobalOrder + Default>(
        base_dir: P,
        vertex_chunk_size: usize,
        edge_chunk_size: usize,
        edge_max_list_size: usize,
        global_order: Arc<GO>,
        chunks_iter: impl IntoIterator<Item = LoadChunk>,
    ) -> Result<Self, Error> {
        let mut vf_builder: VertexFrameBuilder<GO> =
            VertexFrameBuilder::new(vertex_chunk_size, global_order, &base_dir);
        let mut edge_builder =
            EdgeFrameBuilder::new(edge_chunk_size, edge_max_list_size, &base_dir);

        load_chunks(&mut vf_builder, &mut edge_builder, chunks_iter)?;

        Ok(TempColGraphFragment {
            vertex_chunk_size,
            edge_chunk_size,
            adj_out_chunks: vf_builder.adj_out_chunks,
            adj_in_chunks: Vec::default(),
            edge_chunks: edge_builder.edge_chunks,
            graph_dir: base_dir.as_ref().into(),
        })
    }

    pub fn num_vertices(&self) -> usize {
        match self.adj_out_chunks.last() {
            Some(v) => (self.adj_out_chunks.len() - 1) * self.vertex_chunk_size + v.len(), // all but the last chunk are always full
            None => 0, // we have an empty graph
        }
    }

    pub fn edges(
        &self,
        vertex_id: VID,
        dir: Direction,
    ) -> Box<dyn Iterator<Item = (EID, VID)> + Send> {
        match dir {
            Direction::IN | Direction::OUT => {
                let adj_array = self.adj_list(vertex_id.into(), dir);
                if let Some((v, e)) = adj_array {
                    let iter = v
                        .into_iter()
                        .zip(e.into_iter())
                        .map(|(vid, eid)| (EID(eid as usize), VID(vid as usize)));

                    Box::new(iter)
                } else {
                    return Box::new(std::iter::empty());
                }
            }
            Direction::BOTH => {
                let out = self.edges(vertex_id, Direction::OUT);
                let inb = self.edges(vertex_id, Direction::IN);
                Box::new(out.merge_by(inb, |(v1, _), (v2, _)| v1 < v2))
            }
        }
    }

    pub fn edge(&self, e_id: EID) -> Edge<'_> {
        let chunk_idx = e_id.0 / self.edge_chunk_size;
        let idx = e_id.0 % self.edge_chunk_size;
        let chunk = &self.edge_chunks[chunk_idx];
        Edge::new(chunk, idx)
    }

    pub fn all_edges(&self) -> impl Iterator<Item = (EID, VID, VID)> + '_ {
        self.edge_chunks
            .iter()
            .flat_map(|chunk| {
                chunk
                    .source()
                    .into_iter()
                    .flatten()
                    .zip(chunk.destination().into_iter().flatten())
            })
            .enumerate()
            .map(|(eid, (src, dst))| (EID(eid), VID(*src as usize), VID(*dst as usize)))
    }

    pub fn exploded_edges(&self) -> impl Iterator<Item = (EID, VID, VID, Time)> + '_ {
        let edge_chunk_size = self.edge_chunk_size;
        self.edge_chunks
            .iter()
            .enumerate()
            .flat_map(move |(chunk_id, chunk)| {
                chunk
                    .source()
                    .into_iter()
                    .flatten()
                    .zip(chunk.destination().into_iter().flatten())
                    .enumerate()
                    .flat_map(move |(eid, (src, dst))| {
                        chunk.timestamps(eid).flatten().map(move |time| {
                            (
                                EID(chunk_id * edge_chunk_size + eid),
                                VID(*src as usize),
                                VID(*dst as usize),
                                *time,
                            )
                        })
                    })
            })
    }

    fn adj_list(&self, vertex_id: usize, dir: Direction) -> Option<(RowOwned<u64>, RowOwned<u64>)> {
        let chunks = match dir {
            Direction::OUT => self.outbound(),
            Direction::IN => self.inbound(),
            Direction::BOTH => return None,
        };

        let chunk_size = self.vertex_chunk_size; // we assume all the chunks are the same size

        let chunk_idx = vertex_id / chunk_size;
        let idx = vertex_id % chunk_size;

        let neighbours = chunks[chunk_idx].neighbours_own(VID(idx))?;
        let edges = chunks[chunk_idx].edges_own(VID(idx))?;

        Some((neighbours, edges))
    }

    pub(crate) fn outbound(&self) -> &Vec<VertexChunk> {
        &self.adj_out_chunks
    }

    pub(crate) fn inbound(&self) -> &Vec<VertexChunk> {
        &self.adj_in_chunks
    }

    pub fn build_inbound_adj_index(&mut self) -> Result<(), Error> {
        let num_chunks = self.outbound().len();
        let tmp_schema = Schema::from(vec![Field::new(
            "adj_in",
            <ListArray<i64>>::default_datatype(adj_schema()),
            false,
        )]);
        let options = WriteOptions { compression: None };
        let tmp_files = (0..num_chunks)
            .map(|_| tempfile_in(&self.graph_dir))
            .collect::<Result<Vec<_>, io::Error>>()?;
        let mut writers: Vec<_> = tmp_files
            .iter()
            .map(|chunk_file| FileWriter::new(chunk_file, tmp_schema.clone(), None, options))
            .collect();
        if let Some(error) = writers
            .par_iter_mut()
            .find_map_any(|writer| writer.start().err())
        {
            return Err(error.into());
        }

        for (outbound_chunk_id, outbound) in self.outbound().iter().enumerate() {
            let adj_column: ListColumn<u64> = outbound.neighbours_col().unwrap();
            let edge_ids_column: ListColumn<u64> = outbound.edge_col().unwrap();

            let outbound_chunksize = outbound.len();
            let mut progress = vec![0usize; outbound_chunksize]; // keeps track of how far we got for each list
            for inbound_chunk_id in 0..num_chunks {
                // build partial inbound chunk sequentially
                let chunk_max_id = self.vertex_chunk_size * (inbound_chunk_id + 1); // id in this chunk less than this
                let inbound_chunksize = self.outbound()[inbound_chunk_id].len();
                // let offsets = vec![0i64; inbound_chunksize + 1];
                let mut counts = Vec::with_capacity(inbound_chunksize);
                counts.resize_with(inbound_chunksize, || AtomicUsize::new(0));
                let new_progress: Vec<_> = progress
                    .par_iter()
                    .enumerate()
                    .map(|(row, &start)| {
                        let vertex_ids = &adj_column[row][start..];
                        let mut row_size = 0usize;
                        for &id in vertex_ids {
                            let id = id as usize;
                            if id < chunk_max_id {
                                if let Some(counts) = counts.get(id % self.vertex_chunk_size) {
                                    counts.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    let inner_id = id % self.vertex_chunk_size;
                                    panic!("counts index out of bounds for inbound chunk {inbound_chunk_id} with size {inbound_chunksize}: id {id}, index in chunk: {inner_id}")
                                }
                                row_size += 1;
                            } else {
                                break;
                            }
                        }
                        row_size
                    })
                    .collect();
                let mut offsets = Vec::with_capacity(inbound_chunksize + 1);
                offsets.push(0i64);
                let mut cum_sum = 0;
                for v in counts {
                    cum_sum += v.load(Ordering::Relaxed);
                    offsets.push(cum_sum as i64);
                }

                // assemble the value vectors
                let mut inbound_vids = vec![0u64; cum_sum];
                let mut inbound_eids = vec![0u64; cum_sum];
                let mut extra_offsets = vec![0usize; inbound_chunksize];

                for (row, &start) in progress.iter().enumerate() {
                    let row_vertex_ids = &adj_column[row][start..start + new_progress[row]];
                    let row_edge_ids = &edge_ids_column[row][start..start + new_progress[row]];
                    // in principle we can do all these updates in parallel as they end up pointing at unique rows of the vector
                    // this would require some careful unsafe code though
                    for (&vid, &eid) in row_vertex_ids.iter().zip(row_edge_ids) {
                        let vid = vid as usize % self.vertex_chunk_size; //local index
                        let insertion_point = offsets[vid] as usize + extra_offsets[vid];
                        extra_offsets[vid] += 1;
                        inbound_vids[insertion_point] =
                            (outbound_chunk_id * self.vertex_chunk_size + row) as u64;
                        inbound_eids[insertion_point] = eid;
                    }
                }

                // assemble the array and write to file
                let dtype = <ListArray<i64>>::default_datatype(adj_schema());
                let offsets = OffsetsBuffer::try_from(offsets)?;
                let vid_array = PrimitiveArray::from_vec(inbound_vids).boxed();
                let eid_array = PrimitiveArray::from_vec(inbound_eids).boxed();
                let values =
                    StructArray::new(adj_schema(), vec![vid_array, eid_array], None).boxed();

                let inbound_array = ListArray::new(dtype, offsets, values, None).boxed();
                writers[inbound_chunk_id].write(&Chunk::new(vec![inbound_array]), None)?;

                // record the progress
                progress
                    .par_iter_mut()
                    .enumerate()
                    .for_each(|(row, v)| *v += new_progress[row]);
            }
        }

        // finalise the files
        if let Some(error) = writers
            .par_iter_mut()
            .find_map_any(|writer| writer.finish().err())
        {
            return Err(error.into());
        }

        // combine the results
        for f in tmp_files {
            let chunks: Vec<_> = unsafe { mmap_batches(&f, 0..num_chunks)? };
            let chunk_size = chunks[0].len();
            let arrays: Vec<_> = chunks
                .iter()
                .flat_map(|chunk| chunk[0].as_any().downcast_ref::<ListArray<i64>>())
                .collect();
            let mut offsets = vec![0i64; chunk_size + 1];
            for array in arrays.iter() {
                let array_offsets = array.offsets().as_slice();
                offsets
                    .par_iter_mut()
                    .zip(array_offsets)
                    .for_each(|(a, b)| *a += b);
            }

            let mut adj = vec![0u64; offsets[chunk_size] as usize];
            let mut adj_lists: Vec<&mut [u64]> = Vec::default();

            let mut eids = vec![0u64; offsets[chunk_size] as usize];
            let mut eids_lists: Vec<&mut [u64]> = Vec::default();
            let mut right_adj: &mut [u64] = &mut adj;
            let mut right_eids: &mut [u64] = &mut eids;

            for v in offsets.windows(2) {
                if let &[start, end] = v {
                    let split = (end - start) as usize;
                    let (left, right) = right_adj.split_at_mut(split);
                    adj_lists.push(left);
                    right_adj = right;
                    let (left, right) = right_eids.split_at_mut(split);
                    eids_lists.push(left);
                    right_eids = right;
                }
            }

            let mut insertion_points = vec![0usize; chunk_size];
            for array in arrays.iter() {
                let new_adj = as_primitive_column::<u64>(array, 0).unwrap();
                let new_eid_col = as_primitive_column::<u64>(array, 1).unwrap();
                adj_lists
                    .par_iter_mut()
                    .zip(eids_lists.par_iter_mut())
                    .zip(insertion_points.par_iter_mut())
                    .enumerate()
                    .for_each(|(i, ((a, e), offset))| {
                        let new_adj_i = &new_adj[i];
                        let new_eid_i = &new_eid_col[i];
                        a[*offset..*offset + new_adj_i.len()].copy_from_slice(new_adj_i);
                        e[*offset..*offset + new_eid_i.len()].copy_from_slice(new_eid_i);
                        *offset += new_adj_i.len();
                    });
            }

            let values = StructArray::new(
                adj_schema(),
                vec![
                    PrimitiveArray::from_vec(adj).to_boxed(),
                    PrimitiveArray::from_vec(eids).to_boxed(),
                ],
                None,
            );
            let res = <ListArray<i64>>::new(
                <ListArray<i64>>::default_datatype(adj_schema()),
                OffsetsBuffer::try_from(offsets)?,
                values.to_boxed(),
                None,
            )
            .to_boxed();
            let dtype = res.data_type().clone();
            let schema = Schema::from(vec![Field::new("adj_in", dtype, false)]);
            let file_path = self
                .graph_dir
                .join(format!("adj_in_chunk_{:08}.ipc", self.adj_in_chunks.len()));
            let chunk = [Chunk::try_new(vec![res])?];
            write_batches(file_path.as_path(), schema, &chunk)?;
            let mmapped_chunk = unsafe { mmap_batch(file_path.as_path(), 0)? };
            self.adj_in_chunks.push(VertexChunk::new(mmapped_chunk));
        }
        Ok(())
    }
}

pub(crate) fn load_chunks<GO: GlobalOrder + Default>(
    vf_builder: &mut VertexFrameBuilder<GO>,
    edge_builder: &mut EdgeFrameBuilder,
    chunks_iter: impl IntoIterator<Item = LoadChunk>,
) -> Result<(), Error> {
    let mut last_chunk: Option<LoadChunk> = None;
    // g_id, [{v_id1, e_id1}, {v_id2, e_id2}, ...]
    for mut chunk in chunks_iter.into_iter() {
        // when new chunk comes in, we need to finalise the previous chunk aka, copy the column?
        if let Some(last_chunk) = last_chunk {
            if let Some(t_prop_cols) = last_chunk.t_prop_cols() {
                edge_builder.extend_tprops_slice(t_prop_cols)?;
            }
        }

        // get the source and destintion columns
        let src_iter = chunk.sources()?;
        let dst_iter = chunk.destinations()?;

        for (src, dst) in src_iter.zip(dst_iter) {
            let (src_id, dst_id) = vf_builder.push_update(src, dst)?;

            edge_builder.push_update_with_props(src_id, dst_id, &mut chunk)?;
        }

        last_chunk = Some(chunk);
    }

    vf_builder.finalise_empty_chunks()?;

    // finalize edge_builder
    if let Some(mut chunk) = last_chunk {
        edge_builder.finalize(&mut chunk)?;
    }
    Ok(())
}

pub struct Edge<'a> {
    edge: &'a EdgeChunk,
    idx: usize,
}

impl<'a> Edge<'a> {
    fn new(edge: &'a EdgeChunk, idx: usize) -> Self {
        Self { edge, idx }
    }

    pub fn timestamps(&self) -> impl Iterator<Item = &[Time]> {
        self.edge.timestamps(self.idx)
    }

    pub fn props<T: NativeType>(&self, prop_id: usize) -> Option<impl Iterator<Item = Option<&T>>> {
        self.edge.temporal_primitive_prop(self.idx, prop_id)
    }
}

fn read_vertices_only<P: AsRef<Path>>(
    parquet_file: P,
    src_col: &str,
    dst_col: &str,
) -> Result<impl Iterator<Item = GID>, Error> {
    println!("pre sort reading file: {:?}", parquet_file.as_ref());
    let file = std::fs::File::open(&parquet_file)?;
    let mut reader = BufReader::new(file);
    let metadata = read::read_metadata(&mut reader)?;
    let schema = read::infer_schema(&metadata)?;

    let schema = schema.filter(|_, field| field.name == src_col || field.name == dst_col);

    let reader = read::FileReader::new(reader, metadata.row_groups, schema, None, None, None);
    Ok(reader.flatten().flat_map(|chunk| {
        array_as_id_iter(&chunk[0])
            .unwrap()
            .chain(array_as_id_iter(&chunk[1]).unwrap())
    }))
}

#[cfg(test)]
mod test {
    use crate::{arrow::global_order::GlobalMap, db::api::view::internal::GraphOps, prelude::*};
    use std::{iter, path::PathBuf};

    use super::*;
    use arrow2::datatypes::DataType;
    use proptest::prelude::*;
    use tempfile::TempDir;

    fn edges_sanity_vertex_list(edges: &[(u64, u64, i64)]) -> Vec<u64> {
        edges
            .iter()
            .map(|(s, _, _)| *s)
            .chain(edges.iter().map(|(_, d, _)| *d))
            .sorted()
            .dedup()
            .collect()
    }

    fn edges_sanity_check_build_graph<P: AsRef<Path>>(
        test_dir: P,
        edges: &[(u64, u64, i64)],
        vertices: &[u64],
        input_chunk_size: u64,
        vertex_chunk_size: usize,
        edge_chunk_size: usize,
        edge_max_list_size: usize,
    ) -> TempColGraphFragment {
        let chunks = edges
            .iter()
            .map(|(src, _, _)| *src)
            .chunks(input_chunk_size as usize);
        let srcs = chunks
            .into_iter()
            .map(|chunk| PrimitiveArray::from_vec(chunk.collect()));
        let chunks = edges
            .iter()
            .map(|(_, dst, _)| *dst)
            .chunks(input_chunk_size as usize);
        let dsts = chunks
            .into_iter()
            .map(|chunk| PrimitiveArray::from_vec(chunk.collect()));
        let chunks = edges
            .iter()
            .map(|(_, _, times)| *times)
            .chunks(input_chunk_size as usize);
        let times = chunks
            .into_iter()
            .map(|chunk| PrimitiveArray::from_vec(chunk.collect()));

        let schema = Schema::from(vec![
            Field::new("srcs", DataType::UInt64, false),
            Field::new("dsts", DataType::UInt64, false),
            Field::new("time", DataType::Int64, false),
        ]);

        let triples = srcs.zip(dsts).zip(times).map(move |((a, b), c)| {
            LoadChunk::new(
                vec![a.boxed(), b.boxed(), c.boxed()],
                0,
                1,
                2,
                schema.clone(),
            )
        });

        let go: GlobalMap = vertices.iter().copied().collect();

        let mut graph = TempColGraphFragment::build_tables_from_chunked(
            test_dir.as_ref(),
            vertex_chunk_size,
            edge_chunk_size,
            edge_max_list_size,
            go.into(),
            triples,
        )
        .unwrap();
        graph.build_inbound_adj_index().unwrap();
        graph
    }

    fn check_graph_sanity(
        edges: &[(u64, u64, i64)],
        vertices: &[u64],
        graph: &TempColGraphFragment,
    ) {
        let expected_graph = Graph::new();
        for (src, dst, t) in edges {
            expected_graph
                .add_edge(*t, *src, *dst, NO_PROPS, None)
                .unwrap();
        }

        let actual_num_verts = vertices.len();
        let g_num_verts = graph.num_vertices();
        assert_eq!(actual_num_verts, g_num_verts);
        assert!(graph
            .all_edges()
            .all(|(_, VID(src), VID(dst))| src < g_num_verts && dst < g_num_verts));

        for v in 0..g_num_verts {
            let v = VID(v);
            assert!(graph
                .edges(v, Direction::OUT)
                .map(|(_, v)| v)
                .tuple_windows()
                .all(|(v1, v2)| v1 <= v2));
            assert!(graph
                .edges(v, Direction::IN)
                .map(|(_, v)| v)
                .tuple_windows()
                .all(|(v1, v2)| v1 <= v2));
        }

        let exploded_edges: Vec<_> = graph
            .exploded_edges()
            .map(|(_, VID(v1), VID(v2), t)| (vertices[v1], vertices[v2], t))
            .collect();
        assert_eq!(exploded_edges, edges);

        // check incoming edges
        for (v_id, g_id) in vertices.iter().enumerate() {
            let vertex = expected_graph.vertex(*g_id).unwrap();
            let mut expected_inbound = vertex.in_edges().id().map(|(v, _)| v).collect::<Vec<_>>();
            expected_inbound.sort();

            let actual_inbound = graph
                .edges(VID(v_id), Direction::IN)
                .map(|(_, v)| vertices[v.0])
                .collect::<Vec<_>>();

            assert_eq!(expected_inbound, actual_inbound);
        }
    }

    fn edges_sanity_check_inner(
        edges: Vec<(u64, u64, i64)>,
        input_chunk_size: u64,
        vertex_chunk_size: usize,
        edge_chunk_size: usize,
        edge_max_list_size: usize,
    ) {
        let test_dir = TempDir::new().unwrap();
        let vertices = edges_sanity_vertex_list(&edges);
        let graph = edges_sanity_check_build_graph(
            test_dir.path(),
            &edges,
            &vertices,
            input_chunk_size,
            vertex_chunk_size,
            edge_chunk_size,
            edge_max_list_size,
        );

        // check graph is sane
        check_graph_sanity(&edges, &vertices, &graph);

        // check that reloading from graph dir works
        let reloaded_graph = TempColGraphFragment::new(test_dir.path()).unwrap();
        check_graph_sanity(&edges, &vertices, &reloaded_graph)
    }

    proptest! {
        #[test]
        fn edges_sanity_check(
            edges in any::<Vec<(u8, u8, Vec<i64>)>>().prop_map(|v| {
                let mut v: Vec<(u64, u64, i64)> = v.into_iter().flat_map(|(src, dst, times)| {
                    let src = src as u64;
                    let dst = dst as u64;
                    times.into_iter().map(move |t| (src, dst, t))}).collect();
                v.sort();
                v}),
            input_chunk_size in 1..1024u64,
            vertex_chunk_size in 1..1024usize,
            edge_chunk_size in 1..1024usize,
            edge_max_list_size in 1..128usize
        ) {
            edges_sanity_check_inner(edges, input_chunk_size, vertex_chunk_size, edge_chunk_size, edge_max_list_size);
        }
    }

    #[test]
    fn edges_sanity_chunk_1() {
        edges_sanity_check_inner(vec![(876787706323152993, 0, 0)], 1, 1, 1, 1)
    }

    #[test]
    fn large_failing_edge_sanity_repeated() {
        let edges = vec![
            (0, 0, 0),
            (0, 1, 0),
            (0, 2, 0),
            (0, 3, 0),
            (0, 4, 0),
            (0, 5, 0),
            (0, 6, -3063),
            (4, 31, -9117955907655909225),
            (4, 31, -8362242225625446267),
            (4, 31, -7795837413889833982),
            (4, 31, -7154507646909069702),
            (4, 31, -5486437098930843651),
            (4, 31, -5205399317041001146),
            (4, 31, -3512414063308360462),
            (4, 31, -3198266421202704658),
            (4, 31, -2917530111042575959),
            (4, 31, -2609952318615033874),
            (4, 31, -1444541160191106115),
            (4, 31, 1625322000590485850),
            (4, 31, 1659227511670234090),
            (4, 31, 2863581642893711319),
            (4, 31, 3360098463509330797),
            (4, 31, 4091111994678428136),
            (4, 31, 4333943464298045644),
            (4, 31, 4946024641335957112),
            (4, 31, 6516482718147720050),
            (4, 31, 9066062185993781210),
            (6, 62, -8986880921125903753),
            (6, 62, -8811449915261348658),
            (6, 62, -8179590942374036686),
            (6, 62, -8172122116185421693),
            (6, 62, -8001471147966844215),
            (6, 62, -7629162714295263906),
            (6, 62, -7561178279665807914),
            (6, 62, -7152818197615303702),
            (6, 62, -6859898522770581649),
            (6, 62, -6553666077048456744),
            (6, 62, -6448086331062153246),
            (6, 62, -6414874412561452098),
            (6, 62, -6002176792562784038),
            (6, 62, -5760828710855208758),
            (6, 62, -5586814883050666245),
            (6, 62, -4742236552991235217),
            (6, 62, -4339391232722871690),
            (6, 62, -4099650068058621367),
            (6, 62, -3794048161944150337),
            (6, 62, -3244596200814592515),
            (6, 62, -3147107594587100222),
            (6, 62, -2733490029090556573),
            (6, 62, -2602757745155832615),
            (6, 62, -1945012069518017251),
            (6, 62, -1692185630737805885),
            (6, 62, -1601757491861324655),
            (6, 62, -1182861343834444119),
            (6, 62, -481839369968282992),
            (6, 62, 835213101052467816),
            (6, 62, 1163009313594329280),
            (6, 62, 1208601095061895984),
            (6, 62, 1342945016743773803),
            (6, 62, 1808913075635755986),
            (6, 62, 2054163315248882775),
            (6, 62, 2397795382087694645),
            (6, 62, 2477420993148350032),
            (6, 62, 2482296839826324065),
            (6, 62, 3156348045706635016),
            (6, 62, 3176422415044366506),
            (6, 62, 3273801533658405875),
            (6, 62, 3836829307363623380),
            (6, 62, 4451111861229832426),
            (6, 62, 4496351463220800516),
            (6, 62, 4936490789693775754),
            (6, 62, 5628710478684111152),
            (6, 62, 5797140396990329843),
            (6, 62, 6014271849207789719),
            (6, 62, 6926526976449128054),
            (6, 62, 7285641471168809041),
            (6, 62, 7336697320621921194),
            (6, 62, 7443709444314768808),
            (6, 62, 7765511608626348041),
            (6, 62, 7942534429189703068),
            (7, 137, -9164844269042975380),
            (7, 137, -8881581332893310245),
            (7, 137, -8863867494322089141),
            (7, 137, -8075843636624915461),
            (7, 137, -8054686638300956267),
            (7, 137, -7816042459503776118),
            (7, 137, -7813810721335011192),
            (7, 137, -7736434364702609086),
            (7, 137, -7666317538545711384),
            (7, 137, -7577993338190256948),
            (7, 137, -7463118809172307253),
            (7, 137, -7382329790354929220),
            (7, 137, -6638197836273357110),
            (7, 137, -6471538744166972989),
            (7, 137, -6278001637300300983),
            (7, 137, -6202600368684911319),
            (7, 137, -6122575643243211716),
            (7, 137, -6113415971824184229),
            (7, 137, -6027997743355155857),
            (7, 137, -5955856024807977951),
            (7, 137, -5855878130125752472),
            (7, 137, -4476722007968958985),
            (7, 137, -4070082291738444334),
            (7, 137, -3696708391429794437),
            (7, 137, -3559353268581261272),
            (7, 137, -3533213422860733351),
            (7, 137, -3492432563650564544),
            (7, 137, -2387076741390994754),
            (7, 137, -1434688863323576627),
            (7, 137, -1403473860416282671),
            (7, 137, -1136745635552002740),
            (7, 137, -1085454756611682606),
            (7, 137, -1018587118303920599),
            (7, 137, -901942005052279668),
            (7, 137, -842988815475828928),
            (7, 137, -780920691594102510),
            (7, 137, -708042265680119371),
            (7, 137, -603069436427145518),
            (7, 137, -326757075835628921),
            (7, 137, -213706026482176109),
            (7, 137, 5255706696904975),
            (7, 137, 13389612395061574),
            (7, 137, 55852653216081379),
            (7, 137, 166028855866305311),
            (7, 137, 246714331295328597),
            (7, 137, 265464044175971051),
            (7, 137, 601704538207403403),
            (7, 137, 1413288821154898486),
            (7, 137, 1609062165122352546),
            (7, 137, 1612183279712448964),
            (7, 137, 1697581795659815908),
            (7, 137, 2195004409426550447),
            (7, 137, 2507815344986338596),
            (7, 137, 2766893249705365963),
            (7, 137, 3018028486094500278),
            (7, 137, 3159477681695300559),
            (7, 137, 3379502689440217812),
            (7, 137, 3386736859107286541),
            (7, 137, 3498601668508314534),
            (7, 137, 3521973262709065553),
            (7, 137, 4248085039236460345),
            (7, 137, 4648657011590870350),
            (7, 137, 4662961689257233568),
            (7, 137, 5158321226270614666),
            (7, 137, 5483262174997211248),
            (7, 137, 5927098291366132128),
            (7, 137, 5952952057618485110),
            (7, 137, 5969477690648052587),
            (7, 137, 6521946794673718656),
            (7, 137, 7275782070000443051),
            (7, 137, 7337949543456941745),
            (7, 137, 7491508976652811695),
            (7, 137, 7646915356902281181),
            (7, 137, 7852246769473949287),
            (7, 137, 8210201816689302233),
            (7, 137, 8352045484393035933),
            (7, 137, 8454243220284044813),
            (7, 137, 9047319043010071975),
            (7, 137, 9060491719264406993),
            (7, 251, -7688789376647789875),
            (7, 251, -7664972385819417520),
            (7, 251, -7505095482230523611),
            (7, 251, -7085962471923657259),
            (7, 251, -6925363241992823946),
            (7, 251, -6697962100693241797),
            (7, 251, -5292472414133723993),
            (7, 251, -1146039535043890929),
            (7, 251, 1124293342826166432),
            (7, 251, 1205170155576607913),
            (7, 251, 2284754936568347170),
            (7, 251, 2511089544945575895),
            (7, 251, 3019303510407767853),
            (7, 251, 8484284960218588131),
            (7, 251, 8930305415647647372),
            (22, 54, -8536957170928868182),
            (22, 54, -7543998308755978203),
            (22, 54, -7502630486039962438),
            (22, 54, -7007256195991309275),
            (22, 54, -6957605439004507969),
            (22, 54, -6439772810197320633),
            (22, 54, -6372007348919257935),
            (22, 54, -5917548858648471729),
            (22, 54, -4948397955917008778),
            (22, 54, -4030444527978696640),
            (22, 54, -4026205521503658372),
            (22, 54, -3535233061193581792),
            (22, 54, -3103638772731652225),
            (22, 54, -2247018304964407536),
            (22, 54, -2158898759103844654),
            (22, 54, -1980595087593585086),
            (22, 54, -1922399485839382200),
            (22, 54, -185034469892659212),
            (22, 54, 903678042634984752),
            (22, 54, 998590428185493413),
            (22, 54, 1552826822485518274),
            (22, 54, 1992496073517301715),
            (22, 54, 2680368424414198224),
            (22, 54, 3142953701362226626),
            (22, 54, 3370864338517121532),
            (22, 54, 3431619731724722096),
            (22, 54, 3608780517068030502),
            (22, 54, 4438048479027752279),
            (22, 54, 5022738640061034479),
            (22, 54, 5113021073720594066),
            (22, 54, 6852061421806479870),
            (22, 54, 6927400847037519227),
            (22, 54, 6987235189151399374),
            (22, 54, 7381242508627672976),
            (22, 54, 7965256892392515062),
            (22, 54, 8308139618844854105),
            (22, 54, 8663714294053671870),
            (22, 54, 8946067593031352272),
            (23, 42, -9220542323071494221),
            (23, 42, -9105450347319720920),
            (23, 42, -9036042937817875101),
            (23, 42, -8993414886985259716),
            (23, 42, -8552044305268513360),
            (23, 42, -8326518947956983166),
            (23, 42, -8032669158080293412),
            (23, 42, -7428428104613351835),
            (23, 42, -7393492400695732195),
            (23, 42, -7230920179597768595),
            (23, 42, -7084674458735793094),
            (23, 42, -6982083620586248007),
            (23, 42, -6969605865689767167),
            (23, 42, -6864784142590009830),
            (23, 42, -6773049807823341387),
            (23, 42, -6655994158290863006),
            (23, 42, -6600182402536654102),
            (23, 42, -6219836130811104382),
            (23, 42, -6175857438596935323),
            (23, 42, -6061535434041235722),
            (23, 42, -6029206340513114508),
            (23, 42, -5550743463123101485),
            (23, 42, -5403078040777813312),
            (23, 42, -5314041928763221906),
            (23, 42, -5253301934214699335),
            (23, 42, -5197472071065877105),
            (23, 42, -5086190762994792090),
            (23, 42, -4737322650614281377),
            (23, 42, -4611645017782730977),
            (23, 42, -4594372167135036355),
            (23, 42, -4539665528208644038),
            (23, 42, -3743884667211182845),
            (23, 42, -3736083959318400018),
            (23, 42, -3380069689025618944),
            (23, 42, -3372829471997252435),
            (23, 42, -3357599457292026658),
            (23, 42, -3332368166323165154),
            (23, 42, -3332090518441373470),
            (23, 42, -3102583751263215004),
            (23, 42, -2853600993349108815),
            (23, 42, -2800340300422867365),
            (23, 42, -2598693534534142921),
            (23, 42, -2510381759521480374),
            (23, 42, -2162991832308961966),
            (23, 42, -1453116951439842318),
            (23, 42, -1397289616197119411),
            (23, 42, -972744053786363505),
            (23, 42, -821231618283530276),
            (23, 42, -300477563148906614),
            (23, 42, -158315748140764165),
            (23, 42, -108832036984865753),
            (23, 42, -10535065119486349),
            (23, 42, 49812351520209024),
            (23, 42, 210490118742631432),
            (23, 42, 962778391619181421),
            (23, 42, 1024344883131969970),
            (23, 42, 1046558263901401466),
            (23, 42, 1429915834685664394),
            (23, 42, 1470915664795645083),
            (23, 42, 1555127266536604063),
            (23, 42, 1890424961668124466),
            (23, 42, 2144651908041082212),
            (23, 42, 2645984697139460947),
            (23, 42, 3040121732737220788),
            (23, 42, 3475296715650853708),
            (23, 42, 3476235738361843534),
            (23, 42, 4026980608990021758),
            (23, 42, 4285662680542231002),
            (23, 42, 4645433152241120956),
            (23, 42, 4654892241469629969),
            (23, 42, 4697581405395125817),
            (23, 42, 4809452223452679249),
            (23, 42, 4900392353476299348),
            (23, 42, 5511593762054242424),
            (23, 42, 5581516597584651934),
            (23, 42, 5928180501171988222),
            (23, 42, 6039359479441353604),
            (23, 42, 6143762061105954357),
            (23, 42, 6201071920371031386),
            (23, 42, 6406227173244985572),
            (23, 42, 6590779777273593518),
            (23, 42, 6646652203454741401),
            (23, 42, 7075585485118605532),
            (23, 42, 7276198695663503471),
            (23, 42, 7480179308339399413),
            (23, 42, 7797366081398276108),
            (23, 42, 7836866727703454628),
            (23, 42, 8135839366997557251),
            (23, 42, 8172641048469295314),
            (23, 42, 8475079309486983314),
            (23, 42, 8552703315996216338),
            (23, 42, 8941003759860288745),
            (23, 42, 9171289476506323342),
            (23, 186, -8955385125454246069),
            (23, 186, -8064699479245310003),
            (23, 186, -7277249784039138934),
            (23, 186, -6302532377968764587),
            (23, 186, -5596573587212498891),
            (23, 186, -5236033688512367293),
            (23, 186, -3512161379712148866),
            (23, 186, -3351954326415735931),
            (23, 186, -581847278857775781),
            (23, 186, -568786161682331746),
            (23, 186, 431933370935940176),
            (23, 186, 878189175030788554),
            (23, 186, 1265286454244507770),
            (23, 186, 1930426080457448431),
            (23, 186, 4386766746989899510),
            (23, 186, 5767156921112082985),
            (23, 186, 6256021112071921240),
            (23, 186, 6421184396045754199),
            (23, 186, 6506465384096918929),
            (23, 186, 6536253320468584709),
            (23, 186, 7552518488760834149),
            (23, 186, 8045467609933845274),
            (23, 215, -7386501650872465661),
            (23, 215, -4039613161698243080),
            (23, 215, 1373857880581716294),
            (23, 215, 6949130107753691039),
            (26, 60, -5758934122882524466),
            (26, 60, -5550974501436561151),
            (26, 60, -3548910356419497318),
            (26, 60, -1636046233185053724),
            (26, 60, -1610812771443285757),
            (26, 60, -438157739233969709),
            (26, 60, -257406361131306674),
            (26, 60, 736511388772708822),
            (26, 60, 1300939747825375997),
            (26, 60, 1852517915059441775),
            (26, 60, 2875442952648407434),
            (26, 60, 3246100439183782589),
            (26, 60, 4396735578349709151),
            (26, 60, 4915145937138542898),
            (26, 60, 5858532317567500384),
            (27, 150, -9210651807299944921),
            (27, 150, -8925093139783493629),
            (27, 150, -8589103450114089776),
            (27, 150, -7094379640406718723),
            (27, 150, -7042860180491786378),
            (27, 150, -6978342656133139649),
            (27, 150, -6674982488852856181),
            (27, 150, -6642582131150405921),
            (27, 150, -6407116645843274734),
            (27, 150, -6240288134836287334),
            (27, 150, -6156980285126788851),
            (27, 150, -6133910468772104844),
            (27, 150, -5978537457345303483),
            (27, 150, -5676270972947138832),
            (27, 150, -5235019737210422587),
            (27, 150, -5223369107132742836),
            (27, 150, -5179752046053005611),
            (27, 150, -5027062939512231484),
            (27, 150, -4925191594806120654),
            (27, 150, -4350153156482456020),
            (27, 150, -4104076812863482478),
            (27, 150, -3859733030164322415),
            (27, 150, -3814615755528184609),
            (27, 150, -3696373396240400575),
            (27, 150, -3326824100806769766),
            (27, 150, -3151036246231667011),
            (27, 150, -3086981063584317426),
            (27, 150, -2957256219592568245),
            (27, 150, -2611577783584113832),
            (27, 150, -2093074272645157261),
            (27, 150, -1938367728012189572),
            (27, 150, -1867075643187613224),
            (27, 150, -1819774074131754430),
            (27, 150, -1143547583334021716),
            (27, 150, -612896663748232300),
            (27, 150, -329635020640950748),
            (27, 150, -103169566117027058),
            (27, 150, 297402701004333477),
            (27, 150, 529946178515938061),
            (27, 150, 585708897253839210),
            (27, 150, 672312994713674871),
            (27, 150, 807030647883950182),
            (27, 150, 1048861943496175684),
            (27, 150, 1410259369193267318),
            (27, 150, 1685639563373698722),
            (27, 150, 1891416284103213814),
            (27, 150, 2321352715778245830),
            (27, 150, 2531422844717277388),
            (27, 150, 2625430642625194862),
            (27, 150, 2810722751669481187),
            (27, 150, 3161453883740274019),
            (27, 150, 3550646676241818678),
            (27, 150, 4019674612413415428),
            (27, 150, 4150484287641280422),
            (27, 150, 4952848220359687108),
            (27, 150, 5115561291979423797),
            (27, 150, 5258233941528499855),
            (27, 150, 5780251551385207002),
            (27, 150, 5885041942035928113),
            (27, 150, 6313298986356737368),
            (27, 150, 6646497135161441193),
            (27, 150, 6738355086503555100),
            (27, 150, 7068664585020758896),
            (27, 150, 7438086159537947349),
            (27, 150, 7587586297282420201),
            (27, 150, 8317573374930229262),
            (27, 150, 8925673414733899700),
            (27, 150, 9197215479482851468),
            (27, 249, -8815951689090750928),
            (27, 249, -8592830711567778121),
            (27, 249, -8574549115122897577),
            (27, 249, -8043679163290165276),
            (27, 249, -7985150067984525025),
            (27, 249, -7675514593521450480),
            (27, 249, -7662439340708231509),
            (27, 249, -7494469912008523346),
            (27, 249, -7131644547874424451),
            (27, 249, -7006075605960986224),
            (27, 249, -6734180069617577334),
            (27, 249, -6655090315325679565),
            (27, 249, -5908759077626036126),
            (27, 249, -5868756561187584824),
            (27, 249, -5731602149638070742),
            (27, 249, -5548901954261524058),
            (27, 249, -5331859086586288123),
            (27, 249, -5228781407664733947),
            (27, 249, -4597329102708051291),
            (27, 249, -3504495413651840092),
            (27, 249, -3166714063470113080),
            (27, 249, -3131739663082024838),
            (27, 249, -3078873786699274721),
            (27, 249, -2921072552869399138),
            (27, 249, -1845089388560936025),
            (27, 249, -1798223692662429779),
            (27, 249, -1693298407568174061),
            (27, 249, -1563064882186741915),
            (27, 249, -1018119046373526004),
            (27, 249, -549137372853144967),
            (27, 249, -402271258437453727),
            (27, 249, -374564217026304206),
            (27, 249, 864093098080522535),
            (27, 249, 1146054171859720457),
            (27, 249, 1614659944679407397),
            (27, 249, 1663457807368142311),
            (27, 249, 2032217929907172757),
            (27, 249, 2507260408729778851),
            (27, 249, 2999477071591123135),
            (27, 249, 3038671421975435497),
            (27, 249, 4767809839486307424),
            (27, 249, 4945648494835982349),
            (27, 249, 5236129555548557660),
            (27, 249, 5356343992122639207),
            (27, 249, 5413357965829198179),
            (27, 249, 5499010349528421017),
            (27, 249, 5506558796358970110),
            (27, 249, 5795401997213543061),
            (27, 249, 6154011307965914068),
            (27, 249, 6189928096235012457),
            (27, 249, 6543546144488400486),
            (27, 249, 7065648770783084771),
            (27, 249, 7371051002334561858),
            (27, 249, 7404321016981381463),
            (27, 249, 8064948731463432698),
            (27, 249, 8133221079557895174),
            (27, 249, 8601459879519893180),
            (27, 249, 8767395025634804419),
            (27, 249, 9167454181485990807),
            (31, 151, -9091900442121268951),
            (31, 151, -8973708462657652424),
            (31, 151, -8274897061448523246),
            (31, 151, -8237977957560911718),
            (31, 151, -8089258895715192961),
            (31, 151, -6702307310261890978),
            (31, 151, -6335602420588227054),
            (31, 151, -5721715752386232739),
            (31, 151, -5468025094158522343),
            (31, 151, -5335318437153712471),
            (31, 151, -5275573934414252857),
            (31, 151, -3948895742281672335),
            (31, 151, -3372603976268616361),
            (31, 151, -3310748033763218822),
            (31, 151, -3197963297639851466),
            (31, 151, -3093263016189413925),
            (31, 151, -2728620372058122922),
            (31, 151, -2328701842402401616),
            (31, 151, -1779330724094566471),
            (31, 151, -1493438319391456915),
            (31, 151, -1145858673172437967),
            (31, 151, -819982379467856537),
            (31, 151, -323293957972328761),
            (31, 151, 279453628043291766),
            (31, 151, 489960320971254548),
            (31, 151, 832759821379883398),
            (31, 151, 845784040028093692),
            (31, 151, 1557760111498315106),
            (31, 151, 1581913452280078478),
            (31, 151, 1769192163569612363),
            (31, 151, 1814257902697690998),
            (31, 151, 2247274661004264239),
            (31, 151, 2844727123306840101),
            (31, 151, 3077208467851418302),
            (31, 151, 3223770155718891953),
            (31, 151, 3254327887546057147),
            (31, 151, 3863249038882105637),
            (31, 151, 4351965628282898579),
            (31, 151, 4519431375392252572),
            (31, 151, 4848201374092218984),
            (31, 151, 5403885756959481750),
            (31, 151, 5486115558667096766),
            (31, 151, 5524772979278660500),
            (31, 151, 5572117321380082213),
            (31, 151, 5874535287212962163),
            (31, 151, 6131029630743276920),
            (31, 151, 7024244420571940412),
            (31, 151, 7572448393723777825),
            (31, 151, 7579756524861416882),
            (31, 151, 8482393418150536424),
            (31, 151, 8760209944200444079),
            (31, 151, 8875224882862043558),
            (31, 151, 8905546342500245317),
            (31, 151, 9038827751365227280),
            (33, 53, -8587621802895133990),
            (33, 53, -8175240898911018018),
            (33, 53, -7930630463286007604),
            (33, 53, -7099826595667299887),
            (33, 53, -6934084647052343220),
            (33, 53, -6871707285630012322),
            (33, 53, -6526297504848157669),
            (33, 53, -6436337412301695344),
            (33, 53, -4610032821294104391),
            (33, 53, -4548360133157838611),
            (33, 53, -4426289079400096224),
            (33, 53, -3743377708655618900),
            (33, 53, -3273354005814307785),
            (33, 53, -3107328045997237931),
            (33, 53, -2938079580966012862),
            (33, 53, -2601798384274718164),
            (33, 53, -2463101462369431182),
            (33, 53, -2292964804833053458),
            (33, 53, -487432518177554660),
            (33, 53, 142189393765355900),
            (33, 53, 335674584701376251),
            (33, 53, 987419738985308686),
            (33, 53, 2071845382950142874),
            (33, 53, 2293014180099353708),
            (33, 53, 2564406229509333173),
            (33, 53, 3486266272423143375),
            (33, 53, 3609611048793592518),
            (33, 53, 5455952854762762865),
            (33, 53, 6206818801691191835),
            (33, 53, 6530821361039382998),
            (33, 53, 6877423098065953914),
            (33, 53, 7356229291526512168),
            (33, 53, 7427700417501822262),
            (33, 53, 8018686663933023362),
            (33, 53, 8565454496747299633),
            (36, 169, -8637450686975185246),
            (36, 169, -8563557397208815799),
            (36, 169, -8395174814642891222),
            (36, 169, -7957961829766084302),
            (36, 169, -7860982320779793743),
            (36, 169, -7629303196538252286),
            (36, 169, -7581572221516341021),
            (36, 169, -7527530296094862340),
            (36, 169, -7340711944405503743),
            (36, 169, -7310757146292090666),
            (36, 169, -7299394269486884940),
            (36, 169, -7163338622225153366),
            (36, 169, -6851274136246096993),
            (36, 169, -6799166593430527734),
            (36, 169, -6771647394311823342),
            (36, 169, -6523097614400688041),
            (36, 169, -6361961230568057329),
            (36, 169, -6313705218675899077),
            (36, 169, -6166016882409170223),
            (36, 169, -6087964259609367792),
            (36, 169, -5985282697572798222),
            (36, 169, -5887889525817956873),
            (36, 169, -5827402499086883184),
            (36, 169, -5014996014999490153),
            (36, 169, -4880196668959562346),
            (36, 169, -4834418209405729416),
            (36, 169, -4735278591870073500),
            (36, 169, -4727614297939193762),
            (36, 169, -4701510404478119571),
            (36, 169, -4615582154255498514),
            (36, 169, -4270482158029159066),
            (36, 169, -3507908308109294115),
            (36, 169, -3354312449276604288),
            (36, 169, -2990449903894149990),
            (36, 169, -2917374065779796249),
            (36, 169, -2566270083970250509),
            (36, 169, -2523482693509110410),
            (36, 169, -2436719887381147300),
            (36, 169, -2158990248315593752),
            (36, 169, -2040693971105927268),
            (36, 169, -1946145828725430807),
            (36, 169, -1858527305293425844),
            (36, 169, -1857130453742481470),
            (36, 169, -1800011445962133504),
            (36, 169, -1770654516233084157),
            (36, 169, -1677202164424601916),
            (36, 169, -1653048752321632411),
            (36, 169, -1457225885101690932),
            (36, 169, -1280113247104618687),
            (36, 169, -835208532342576983),
            (36, 169, -521788701401251288),
            (36, 169, -243593753849684848),
            (36, 169, -174289830062811070),
            (36, 169, 40536871439536170),
            (36, 169, 377461366266632139),
            (36, 169, 988903390234609417),
            (36, 169, 1097751461310284831),
            (36, 169, 1458839101498122662),
            (36, 169, 1946847904891248730),
            (36, 169, 2144373562383729621),
            (36, 169, 2297329612775488683),
            (36, 169, 2318168746968798225),
            (36, 169, 2322200552035462840),
            (36, 169, 2565469442935134594),
            (36, 169, 3357833810512258679),
            (36, 169, 3414764748903341758),
            (36, 169, 3646053069432880106),
            (36, 169, 3722353568163005183),
            (36, 169, 3836139591104451776),
            (36, 169, 3869033764684800475),
            (36, 169, 3951298215779017212),
            (36, 169, 4075798953867215599),
            (36, 169, 4188176570937128390),
            (36, 169, 4502627049233537031),
            (36, 169, 5005761089045196520),
            (36, 169, 5043477133193263861),
            (36, 169, 5368359450217653638),
            (36, 169, 5393441884034679912),
            (36, 169, 5403619323486691029),
            (36, 169, 5497411161261879262),
            (36, 169, 5715062801158315829),
            (36, 169, 6201479559164484310),
            (36, 169, 6342307656396334066),
            (36, 169, 6509011268670016378),
            (36, 169, 6697609508805476890),
            (36, 169, 6998697231212155609),
            (36, 169, 7089570988827004209),
            (36, 169, 7167919614426402496),
            (36, 169, 7350195724642206843),
            (36, 169, 7603825398355532266),
            (36, 169, 7889236533642868442),
            (36, 169, 8277168017828055680),
            (36, 169, 8294646609523017193),
            (36, 169, 8401969686431990553),
            (36, 169, 8460433536929697017),
            (36, 169, 9061463831571316450),
            (37, 179, -9062989039483702614),
            (37, 179, -9013428098070303460),
            (37, 179, -8832006242138999553),
            (37, 179, -7778391004167621160),
            (37, 179, -5290578912776418529),
            (37, 179, -5258905780886268222),
            (37, 179, -5020620207781321269),
            (37, 179, -4895996288536982558),
            (37, 179, -4602294178294000946),
            (37, 179, -4422068259808309796),
            (37, 179, -4275238968405001271),
            (37, 179, -4022448767353488182),
            (37, 179, -3529927283256266458),
            (37, 179, -3311491129249555034),
            (37, 179, -2860248150263179981),
            (37, 179, -2745900213349621692),
            (37, 179, -1941919603537622285),
            (37, 179, -1814538024129850037),
            (37, 179, -643241502881072830),
            (37, 179, -419723787262536725),
            (37, 179, -394909738801571277),
            (37, 179, 73923867682401345),
            (37, 179, 608569319478985251),
            (37, 179, 1192599143397889305),
            (37, 179, 3953978391300860770),
            (37, 179, 4250785848249719964),
            (37, 179, 4979168101140937596),
            (37, 179, 5693587122989762785),
            (37, 179, 6403206433099083685),
            (37, 179, 6463430932379010189),
            (37, 179, 6583621514762619237),
            (37, 179, 6851278088532164957),
            (37, 179, 7931072489156496492),
            (37, 179, 8690534624922689529),
            (42, 201, -8869866032877576068),
            (42, 201, -8764712077668741451),
            (42, 201, -8742175199040961078),
            (42, 201, -8597728197784166745),
            (42, 201, -8441158353931271328),
            (42, 201, -8242531608923992376),
            (42, 201, -7446891707315426551),
            (42, 201, -7442549808315602098),
            (42, 201, -7339955480587283325),
            (42, 201, -7262198207349760506),
            (42, 201, -7142839200025799813),
            (42, 201, -6976909841628387724),
            (42, 201, -6782768672957790704),
            (42, 201, -6458797979245133180),
            (42, 201, -6242267025173282185),
            (42, 201, -6179873862512490373),
            (42, 201, -5934630072288150007),
            (42, 201, -5151318335350573774),
            (42, 201, -4601413900966067345),
            (42, 201, -4364848957257611753),
            (42, 201, -4309716091793474997),
            (42, 201, -4291586556896276474),
            (42, 201, -3843930589342541405),
            (42, 201, -3819449672923098193),
            (42, 201, -3653874326218491323),
            (42, 201, -3618407462362286481),
            (42, 201, -3603413888941783797),
            (42, 201, -3498334119001212874),
            (42, 201, -3270176212443471624),
            (42, 201, -3140548856372002592),
            (42, 201, -2949102490038695970),
            (42, 201, -2184422920397745149),
            (42, 201, -2023306797487693945),
            (42, 201, -1998031910892292312),
            (42, 201, -1772591130633898968),
            (42, 201, -1610017324823592949),
            (42, 201, -1203602180083859562),
            (42, 201, -769152867762715034),
            (42, 201, -615146287680580008),
            (42, 201, -486330267968464624),
            (42, 201, -398442438426126743),
            (42, 201, -345730496225425207),
            (42, 201, -193040749524301277),
            (42, 201, -179345544633069396),
            (42, 201, 387216732267630005),
            (42, 201, 404275072864081193),
            (42, 201, 817282021554317940),
            (42, 201, 872912404393340879),
            (42, 201, 996144450559709663),
            (42, 201, 1287163478270624062),
            (42, 201, 1578168549462609749),
            (42, 201, 1745593614658129108),
            (42, 201, 1777823062002935485),
            (42, 201, 1930235610671335789),
            (42, 201, 1983916147169458814),
            (42, 201, 1992767206369686506),
            (42, 201, 2706137941218094525),
            (42, 201, 3130357143524810566),
            (42, 201, 3163568817161839870),
            (42, 201, 3245363895445680227),
            (42, 201, 3604150683176853385),
            (42, 201, 3655291757262961148),
            (42, 201, 3689838416929533478),
            (42, 201, 3881379862800869175),
            (42, 201, 3940574981288856222),
            (42, 201, 4014625159954245181),
            (42, 201, 4224764078877898862),
            (42, 201, 4268952967359324988),
            (42, 201, 4275002800924421317),
            (42, 201, 4425955826048093387),
            (42, 201, 4525629883501409049),
            (42, 201, 5540216014632345590),
            (42, 201, 5659750668054004431),
            (42, 201, 5725425785990133523),
            (42, 201, 5800591663655651767),
            (42, 201, 5830133140628404890),
            (42, 201, 6157513419925614553),
            (42, 201, 6472776685408122119),
            (42, 201, 6559884121852533695),
            (42, 201, 6774669640327350190),
            (42, 201, 6880918054767509486),
            (42, 201, 6881603359116885096),
            (42, 201, 6981890440877258424),
            (42, 201, 7294674623850981828),
            (42, 201, 7511501474376869265),
            (42, 201, 8041920239681144228),
            (42, 201, 8140576912728620923),
            (42, 201, 8278127812235103859),
            (42, 201, 8601222420192037211),
            (42, 201, 8730466541461750580),
            (42, 201, 8775335987093691531),
            (42, 201, 8812922584760463025),
            (42, 201, 8980725136462609968),
            (42, 201, 9034387154596203941),
            (42, 201, 9209658955067527233),
            (53, 164, -9214487610814475178),
            (53, 164, -9138189211919612324),
            (53, 164, -8720180267793522520),
            (53, 164, -8689397990237914886),
            (53, 164, -8643798170344811006),
            (53, 164, -8063323078196688677),
            (53, 164, -7945373543968424608),
            (53, 164, -7780532569172141643),
            (53, 164, -7644206585840967500),
            (53, 164, -6499179534971058422),
            (53, 164, -6465745168385006699),
            (53, 164, -6111566058362481116),
            (53, 164, -5784126237226940584),
            (53, 164, -5510230861832070030),
            (53, 164, -4877165201538730460),
            (53, 164, -4717004720066954122),
            (53, 164, -4081022236434961022),
            (53, 164, -3697975572249187026),
            (53, 164, -3658354272186120553),
            (53, 164, -3419434629278473117),
            (53, 164, -3166582279307856452),
            (53, 164, -2872821181510006216),
            (53, 164, -2867648937683306325),
            (53, 164, -2048892065284956207),
            (53, 164, -2023432985349453005),
            (53, 164, -1944008924363227243),
            (53, 164, -951939548283879812),
            (53, 164, -674570416931141560),
            (53, 164, -592973318452804994),
            (53, 164, -521414179260039715),
            (53, 164, -182183670115200706),
            (53, 164, 505841650924933671),
            (53, 164, 1027678315003638838),
            (53, 164, 1058712738771437998),
            (53, 164, 1192113414969375479),
            (53, 164, 1315692499007005436),
            (53, 164, 1362180149060618127),
            (53, 164, 1858196079624757338),
            (53, 164, 1879593928498633543),
            (53, 164, 2121212038153422249),
            (53, 164, 2167926146746678450),
            (53, 164, 2462582191558111381),
            (53, 164, 2665856245553043258),
            (53, 164, 2932280388284196838),
            (53, 164, 3010217663376052829),
            (53, 164, 3047155462639899042),
            (53, 164, 3106795923910515645),
            (53, 164, 3114665754315299232),
            (53, 164, 3130151256722011719),
            (53, 164, 3188730816059897167),
            (53, 164, 3590522987637514148),
            (53, 164, 3595738910588205610),
            (53, 164, 3926312481091195761),
            (53, 164, 3986771903404216402),
            (53, 164, 4276753218915556647),
            (53, 164, 4816284043696905753),
            (53, 164, 4821302960964277487),
            (53, 164, 4946553921038949615),
            (53, 164, 5031840211165212400),
            (53, 164, 5108428852717636948),
            (53, 164, 5766816395284010812),
            (53, 164, 5816335066332534440),
            (53, 164, 5873719734762999480),
            (53, 164, 6163046485297962279),
            (53, 164, 6311849976812509930),
            (53, 164, 6444899566576048235),
            (53, 164, 6623754429872730529),
            (53, 164, 6846114238445252845),
            (53, 164, 7070811523037960431),
            (53, 164, 7099732769700072262),
            (53, 164, 7736424998151120071),
            (53, 164, 8070093854771964838),
            (53, 164, 8486697610338137554),
            (53, 164, 8540088154677325971),
            (53, 164, 8993374983673517079),
            (66, 16, -8733709930135671822),
            (66, 16, -8692351969224240219),
            (66, 16, -8428816103593186297),
            (66, 16, -8108533711937154460),
            (66, 16, -6716980956605388595),
            (66, 16, -6594543427283885018),
            (66, 16, -6063180035212598770),
            (66, 16, -5964356244500796978),
            (66, 16, -5936721377648519799),
            (66, 16, -5701602829013114253),
            (66, 16, -5607982338188167154),
            (66, 16, -3346073414549021976),
            (66, 16, -3002966773350586498),
            (66, 16, -2843395059866699234),
            (66, 16, -2507166657712298083),
            (66, 16, -1481447318882726278),
            (66, 16, -896032502072363253),
            (66, 16, 673731911942932487),
            (66, 16, 689744269893623312),
            (66, 16, 1728387852746905612),
            (66, 16, 1854815098183540375),
            (66, 16, 1961290868334524634),
            (66, 16, 2201797332483537351),
            (66, 16, 2371843877067100905),
            (66, 16, 2536710776081861756),
            (66, 16, 2922277093963718172),
            (66, 16, 3240043666555490197),
            (66, 16, 3245472775334513987),
            (66, 16, 4008265884767032698),
            (66, 16, 4428558354153752384),
            (66, 16, 4650544226834936064),
            (66, 16, 5722556517339814898),
            (66, 16, 6192979026708902177),
            (66, 16, 6306339106092398988),
            (66, 16, 6582389790180958083),
            (66, 16, 7355291467502563481),
            (66, 16, 8795499644306593380),
            (66, 16, 9138932574711486487),
            (66, 16, 9153894479094699111),
            (68, 251, -8824101972770146222),
            (68, 251, -8558976467412265187),
            (68, 251, -8355351949973757220),
            (68, 251, -8169587854020505233),
            (68, 251, -8127657045011443443),
            (68, 251, -8109650837130212373),
            (68, 251, -7824879589025232798),
            (68, 251, -7640670740384141375),
            (68, 251, -7518611044788428504),
            (68, 251, -7482259983530489305),
            (68, 251, -7362285222412779469),
            (68, 251, -6935456276281270485),
            (68, 251, -6809233048748512800),
            (68, 251, -6683457800315354597),
            (68, 251, -6632729690096198904),
            (68, 251, -6408112781731291952),
            (68, 251, -6312028466589971117),
            (68, 251, -6309549062099864322),
            (68, 251, -6099714394176721145),
            (68, 251, -5762961778205363450),
            (68, 251, -5507922947410054747),
            (68, 251, -5372874350183926031),
            (68, 251, -5148456168102689788),
            (68, 251, -4205881517837466719),
            (68, 251, -3924586108740774853),
            (68, 251, -3529536240190818476),
            (68, 251, -3529209664339220907),
            (68, 251, -3101167540090755869),
            (68, 251, -2922369855100972516),
            (68, 251, -2818830253165264381),
            (68, 251, -2696808656532600549),
            (68, 251, -2559992480103888201),
            (68, 251, -2482320049846317194),
            (68, 251, -2467982874089924728),
            (68, 251, -2249858273063306294),
            (68, 251, -2245823256550291346),
            (68, 251, -2214862985121572103),
            (68, 251, -2202715119024282081),
            (68, 251, -2141873255270679039),
            (68, 251, -1924422146448339197),
            (68, 251, -1885683967070132235),
            (68, 251, -1600261164320110671),
            (68, 251, -1374179754477422334),
            (68, 251, -1362138809189366610),
            (68, 251, -1311572886170290415),
            (68, 251, -1203817483953210026),
            (68, 251, -1033813775296611418),
            (68, 251, -1005756669070645901),
            (68, 251, -946000955246867128),
            (68, 251, -857611546393016313),
            (68, 251, -619300214672607907),
            (68, 251, -583247247958182499),
            (68, 251, -348995132225160918),
            (68, 251, -93725183185732806),
            (68, 251, -81072011921829962),
            (68, 251, 334186307942264798),
            (68, 251, 795279625343196468),
            (68, 251, 826588752782165704),
            (68, 251, 1080644059828935178),
            (68, 251, 1391377000411014466),
            (68, 251, 1677569431073682701),
            (68, 251, 2390121212446477153),
            (68, 251, 2890185848457834538),
            (68, 251, 2961085822578598512),
            (68, 251, 3091822239271530006),
            (68, 251, 3282350404532013875),
            (68, 251, 3803544747586349714),
            (68, 251, 3813883420587940953),
            (68, 251, 4275508544551047825),
            (68, 251, 4400212950062843523),
            (68, 251, 4414660127938357570),
            (68, 251, 5534832399591262032),
            (68, 251, 5828639002718162524),
            (68, 251, 5889112254297032458),
            (68, 251, 5991408103600385261),
            (68, 251, 6031172865500724996),
            (68, 251, 6140049003988215172),
            (68, 251, 6448040962293058122),
            (68, 251, 6459102785644233501),
            (68, 251, 6651058348492785148),
            (68, 251, 6863635186441122500),
            (68, 251, 7041753777935254548),
            (68, 251, 7277438340084559385),
            (68, 251, 7285629890476647291),
            (68, 251, 7930339690615197886),
            (68, 251, 8016734310931671038),
            (68, 251, 8280855863134858354),
            (68, 251, 8640228936093228194),
            (68, 251, 8757839317728192028),
            (68, 251, 9057018208628357474),
            (74, 148, -9049770853231714982),
            (74, 148, -8985628575163197221),
            (74, 148, -8948453613289101561),
            (74, 148, -8657937750690347957),
            (74, 148, -8184867554592297970),
            (74, 148, -7955298887138450713),
            (74, 148, -7550766744884620471),
            (74, 148, -7288123182704215183),
            (74, 148, -7272640037307112961),
            (74, 148, -6602195985611153766),
            (74, 148, -6209810344079610006),
            (74, 148, -5978485727538859409),
            (74, 148, -5943090710193494316),
            (74, 148, -5749676404961611010),
            (74, 148, -5748189676685835905),
            (74, 148, -5730788060708426794),
            (74, 148, -5660046516511983697),
            (74, 148, -5505245801736191712),
            (74, 148, -5405317411349332896),
            (74, 148, -5348313335137558241),
            (74, 148, -5228212959226055596),
            (74, 148, -4936874651711393746),
            (74, 148, -4919398891464854800),
            (74, 148, -4912678332251619428),
            (74, 148, -4811029306033223073),
            (74, 148, -4658597655424996260),
            (74, 148, -4651342819790558295),
            (74, 148, -4372748253553498879),
            (74, 148, -4370907471904910205),
            (74, 148, -4190690972952718427),
            (74, 148, -4011264813975531330),
            (74, 148, -3886685608064816698),
            (74, 148, -3548041909134044071),
            (74, 148, -3454739863268000537),
            (74, 148, -3327853723624317405),
            (74, 148, -3239754838945518530),
            (74, 148, -3194598164542780015),
            (74, 148, -3071869186426675189),
            (74, 148, -3018552838022281546),
            (74, 148, -2747797325814193420),
            (74, 148, -2659056160536753948),
            (74, 148, -2345068742110212571),
            (74, 148, -2211732174052789667),
            (74, 148, -1453868325110989224),
            (74, 148, -822229089865992499),
            (74, 148, -642067920708144010),
            (74, 148, -369984248949214220),
            (74, 148, -142472117469704467),
            (74, 148, -122272955868394359),
            (74, 148, -87893377783669976),
            (74, 148, -5334660334000897),
            (74, 148, 132265462432794868),
            (74, 148, 369283708630522274),
            (74, 148, 698600128024763125),
            (74, 148, 864560265722143116),
            (74, 148, 970831740083605765),
            (74, 148, 1151427083443927138),
            (74, 148, 1309787489505583406),
            (74, 148, 1314375784063768585),
            (74, 148, 1764149016803905716),
            (74, 148, 2098472520472694329),
            (74, 148, 2510226050373083108),
            (74, 148, 2763653299817214773),
            (74, 148, 3125806821930154027),
            (74, 148, 3145635049954423366),
            (74, 148, 3174470995515987601),
            (74, 148, 3361504059475334443),
            (74, 148, 3907198775878833378),
            (74, 148, 3960872224118641888),
            (74, 148, 4230265967792792273),
            (74, 148, 4235213110315412091),
            (74, 148, 4518390273341016907),
            (74, 148, 4688356861143965768),
            (74, 148, 4971660400223134801),
            (74, 148, 5213023232127212444),
            (74, 148, 5478342949785176845),
            (74, 148, 5696372670936018287),
            (74, 148, 5750653164662322778),
            (74, 148, 6139736759966630102),
            (74, 148, 6142735153049509354),
            (74, 148, 6289580228524051041),
            (74, 148, 6394590817526915323),
            (74, 148, 6753539596849511472),
            (74, 148, 6777827186693493895),
            (74, 148, 6952092300827431015),
            (74, 148, 7710961771229442201),
            (74, 148, 8046779927669477256),
            (74, 148, 8535243782263178045),
            (74, 148, 8644119908514905661),
            (74, 148, 8758520340673815607),
            (74, 148, 8851455023436632477),
            (74, 148, 8890018823602180707),
            (74, 148, 9035970692520285244),
            (74, 148, 9078447837171846834),
            (74, 148, 9176855360627063775),
            (80, 236, -8920472442799127707),
            (80, 236, -8049761933549392460),
            (80, 236, -7686026640707962186),
            (80, 236, -7061346171032537183),
            (80, 236, -5004059049696399313),
            (80, 236, -4388309326646961925),
            (80, 236, -3131396339546454908),
            (80, 236, -2321245271648831986),
            (80, 236, -1464343073463411202),
            (80, 236, 69677841124128694),
            (80, 236, 1559827754963492636),
            (80, 236, 2876545303787539079),
            (80, 236, 4590796226799883779),
            (80, 236, 4807814875999091081),
            (80, 236, 5758593581063889912),
            (86, 12, -9110223138724268188),
            (86, 12, -8539816555231473553),
            (86, 12, -8506194924903563652),
            (86, 12, -8092189298787614154),
            (86, 12, -7604403068444473902),
            (86, 12, -7362947595397276188),
            (86, 12, -7354832455390444607),
            (86, 12, -6839242112289831782),
            (86, 12, -6776833694106760600),
            (86, 12, -5781591068701635436),
            (86, 12, -5595037028020802393),
            (86, 12, -5586723728157734960),
            (86, 12, -5489250494333675538),
            (86, 12, -4850598344747815263),
            (86, 12, -3359194176620251887),
            (86, 12, -2948486830261252444),
            (86, 12, -2807065864703737468),
            (86, 12, -2353513418626947067),
            (86, 12, -2272218810311036472),
            (86, 12, -1829606340866352349),
            (86, 12, -1243828378788419859),
            (86, 12, -1137400853598023430),
            (86, 12, -627324488479615290),
            (86, 12, -450041219019397399),
            (86, 12, 504001055531282384),
            (86, 12, 678164520726231795),
            (86, 12, 1109983113815944460),
            (86, 12, 1919049834982975744),
            (86, 12, 2043464387063111976),
            (86, 12, 3070636267918329912),
            (86, 12, 3087702714993023396),
            (86, 12, 4394627973441028987),
            (86, 12, 4837095342603232664),
            (86, 12, 8274404049265404736),
            (86, 12, 8403778868065229965),
            (86, 12, 8617743825648338411),
            (86, 12, 8680328372743211153),
            (86, 12, 8990102400852113606),
            (86, 212, -8823676481304829396),
            (86, 212, -8771874855608059477),
            (86, 212, -8738007034793335089),
            (86, 212, -8540329021701888681),
            (86, 212, -8439653053184527311),
            (86, 212, -8401509384977812026),
            (86, 212, -8282694027431890013),
            (86, 212, -7943371596177633149),
            (86, 212, -7842804500832971033),
            (86, 212, -7441148212050957278),
            (86, 212, -7434086002382944063),
            (86, 212, -6871304836114836476),
            (86, 212, -5933099852065053825),
            (86, 212, -5865657394368299090),
            (86, 212, -5421814074687355959),
            (86, 212, -4665389080077946852),
            (86, 212, -4570180692918245525),
            (86, 212, -3651919963071686276),
            (86, 212, -3503724442274927242),
            (86, 212, -2068178361657706234),
            (86, 212, -1901112950911651571),
            (86, 212, -1646262991331555006),
            (86, 212, -1424879296339173665),
            (86, 212, -1038308940574113420),
            (86, 212, -1004663619994582187),
            (86, 212, -907177293592514812),
            (86, 212, -404485270987687467),
            (86, 212, 268956298191799689),
            (86, 212, 482533939986158360),
            (86, 212, 543984206433895861),
            (86, 212, 1478539373730405673),
            (86, 212, 2410375335964321177),
            (86, 212, 3554350778238442863),
            (86, 212, 3800831673796491713),
            (86, 212, 4381149146504553170),
            (86, 212, 4919627843944579053),
            (86, 212, 5011732246441318077),
            (86, 212, 5223297543677806992),
            (86, 212, 5448812559668351856),
            (86, 212, 5482349662558632880),
            (86, 212, 5816826532915836963),
            (86, 212, 6298095338896273533),
            (86, 212, 6926230652885432693),
            (86, 212, 7744291782868205020),
            (86, 212, 8144703176794014693),
            (86, 212, 8819985518764702051),
            (86, 212, 9003259045276738828),
            (87, 251, -5180928203826493786),
            (87, 251, 179547898194611109),
            (87, 251, 1653706416438262122),
            (88, 5, -8828906740731270542),
            (88, 5, -7252741187868162317),
            (88, 5, -5620818832138502768),
            (88, 5, -5161617423497216179),
            (88, 5, -5109168273940102123),
            (88, 5, -3404076404747917841),
            (88, 5, -2569322074784405615),
            (88, 5, -2190024672446125807),
            (88, 5, -1883538759989834143),
            (88, 5, -1745747366550683586),
            (88, 5, -818573548953743393),
            (88, 5, 2091156538704721438),
            (88, 5, 3717597089323008753),
            (94, 158, -8966008600854693212),
            (94, 158, -8834706104799981271),
            (94, 158, -8772554380292209832),
            (94, 158, -8757432215146996279),
            (94, 158, -8631080544581573665),
            (94, 158, -8534616955379220511),
            (94, 158, -8183045590139668145),
            (94, 158, -7700095349756030382),
            (94, 158, -7690567122084182479),
            (94, 158, -7659572378328367860),
            (94, 158, -7309291985877198072),
            (94, 158, -7053899350805586728),
            (94, 158, -7004881599208963944),
            (94, 158, -6913089924070363662),
            (94, 158, -6835956027002933730),
            (94, 158, -6595945892014904863),
            (94, 158, -6547858255129346963),
            (94, 158, -6485015339584876425),
            (94, 158, -6350355545380903806),
            (94, 158, -6243005191888005066),
            (94, 158, -6233785580957502329),
            (94, 158, -6122220294750541551),
            (94, 158, -6004691494106195950),
            (94, 158, -5912897335071925896),
            (94, 158, -5806255878204561329),
            (94, 158, -5786146564439868691),
            (94, 158, -5432534480414861309),
            (94, 158, -5351383690402989359),
            (94, 158, -5338869337051386175),
            (94, 158, -5263412179656320819),
            (94, 158, -4957147307419252054),
            (94, 158, -4568957942771946341),
            (94, 158, -4494502932423090523),
            (94, 158, -4269100436671322009),
            (94, 158, -4034636073418179803),
            (94, 158, -3978229378495858413),
            (94, 158, -3173169299564211590),
            (94, 158, -3053196966460842889),
            (94, 158, -3035866877342626329),
            (94, 158, -2710073447757510905),
            (94, 158, -2507750013640729219),
            (94, 158, -2430744662111662678),
            (94, 158, -2122189703391804703),
            (94, 158, -2070511954038509492),
            (94, 158, -1792484723522090108),
            (94, 158, -1725193190910331950),
            (94, 158, -1695782923610690481),
            (94, 158, -1687177959482527754),
            (94, 158, -1504835966824723714),
            (94, 158, -1470784280832010102),
            (94, 158, -1037863631761744980),
            (94, 158, -1021302483889507261),
            (94, 158, -381877342880680375),
            (94, 158, 33999677592102008),
            (94, 158, 368353443848564271),
            (94, 158, 886699785015896628),
            (94, 158, 1131592049215537079),
            (94, 158, 1292200552001308750),
            (94, 158, 1652805570251996359),
            (94, 158, 1895654482806905427),
            (94, 158, 1901138094607933565),
            (94, 158, 2418009775065010894),
            (94, 158, 2637629180924286363),
            (94, 158, 2675212171825444457),
            (94, 158, 2840642494026255739),
            (94, 158, 2907936279392957404),
            (94, 158, 3132709554049241468),
            (94, 158, 3768532592672432110),
            (94, 158, 4083679252080526593),
            (94, 158, 4090463506033003656),
            (94, 158, 4307594179099754809),
            (94, 158, 4539585458138501667),
            (94, 158, 4741069385976302090),
            (94, 158, 4793734801371337301),
            (94, 158, 4925961002265042494),
            (94, 158, 5014383106642147126),
            (94, 158, 5155749706071728456),
            (94, 158, 5326419462549758521),
            (94, 158, 5444410566164612856),
            (94, 158, 5479294755454213187),
            (94, 158, 5590692143500920076),
            (94, 158, 5759726924815250758),
            (94, 158, 5850269576009669420),
            (94, 158, 5915796648587606638),
            (94, 158, 6047140437937888319),
            (94, 158, 6072522157343773706),
            (94, 158, 6318114197292418138),
            (94, 158, 6442863995337731278),
            (94, 158, 6504347725674501549),
            (94, 158, 6703295167348398206),
            (94, 158, 6861782689114267676),
            (94, 158, 7687936254256275242),
            (94, 158, 8287921917813128630),
            (94, 158, 9125860366349505564),
            (98, 26, -8800415900250575481),
            (98, 26, -5749964317234780706),
            (98, 26, -5749640749323687760),
            (98, 26, -5014212974152305482),
            (98, 26, -5003543163275083158),
            (98, 26, -4080033849029644514),
            (98, 26, -3977911745165420996),
            (98, 26, -3939421607237677327),
            (98, 26, -3624210573575633949),
            (98, 26, -3039440704737298417),
            (98, 26, -2983057433022863068),
            (98, 26, -2131317398914656062),
            (98, 26, -397024410401972793),
            (98, 26, 739183264533620158),
            (98, 26, 800796133631571608),
            (98, 26, 1341536286109745236),
            (98, 26, 1468049375117948525),
            (98, 26, 2713937963087251761),
            (98, 26, 2734507491998260276),
            (98, 26, 2806478552594564383),
            (98, 26, 3339744472704506588),
            (98, 26, 3545360422347586336),
            (98, 26, 4722313353341099660),
            (98, 26, 5358304440353709091),
            (98, 26, 5603417447517510194),
            (98, 26, 5709094673834345593),
            (98, 26, 5789561908793762475),
            (98, 26, 6611813756595713090),
            (98, 26, 7809869337666613469),
            (111, 18, -9211855659645998903),
            (111, 18, -8784942380024692638),
            (111, 18, -8644217986334323654),
            (111, 18, -8222148556336378255),
            (111, 18, -8129286662209407271),
            (111, 18, -7683679178619303681),
            (111, 18, -7353940071683686824),
            (111, 18, -7252471046325176287),
            (111, 18, -7019304167273273724),
            (111, 18, -6967628297703486752),
            (111, 18, -6649353487748624886),
            (111, 18, -6554193174137240063),
            (111, 18, -6101010902839003241),
            (111, 18, -5100821263249338292),
            (111, 18, -4976743222680113338),
            (111, 18, -4959495837385898897),
            (111, 18, -4860187890271063253),
            (111, 18, -4708768149375426223),
            (111, 18, -4616629388620351668),
            (111, 18, -3621388700309242476),
            (111, 18, -3589201207599725576),
            (111, 18, -3493309932911874324),
            (111, 18, -3326838506579598909),
            (111, 18, -3286960358011136702),
            (111, 18, -2887367875515590435),
            (111, 18, -2560345807160275016),
            (111, 18, -2346633532417659318),
            (111, 18, -2116222244764383576),
            (111, 18, -1883444614077934680),
            (111, 18, -1818945206495718865),
            (111, 18, -1427228936448126532),
            (111, 18, -900776063844060961),
            (111, 18, -725010067199130291),
            (111, 18, -637981234872210353),
            (111, 18, -405817029537074666),
            (111, 18, -86735099499496986),
            (111, 18, 76227438980927729),
            (111, 18, 95825735285766933),
            (111, 18, 347523426833087620),
            (111, 18, 415425223214230370),
            (111, 18, 588995799616106830),
            (111, 18, 739424678517813453),
            (111, 18, 796563524956865036),
            (111, 18, 1185032299546606956),
            (111, 18, 1338168346776608603),
            (111, 18, 1390908917411428014),
            (111, 18, 1414390502393566162),
            (111, 18, 1623227324990853612),
            (111, 18, 1747159112952812297),
            (111, 18, 1936854560869447932),
            (111, 18, 2600556864099892768),
            (111, 18, 3390071952739840099),
            (111, 18, 3658093487844405723),
            (111, 18, 3963947374687409733),
            (111, 18, 4039842398112927529),
            (111, 18, 4303872300975098898),
            (111, 18, 4638311506636661069),
            (111, 18, 5121301324842947251),
            (111, 18, 5146801355381526539),
            (111, 18, 5211131078455363187),
            (111, 18, 5671715126898518627),
            (111, 18, 5696862870217998322),
            (111, 18, 5980584963408584610),
            (111, 18, 7121387256505744113),
            (111, 18, 7241698069531945761),
            (111, 18, 7247217861867989689),
            (111, 18, 7348522804259999225),
            (111, 18, 7377082157018101202),
            (111, 18, 7591321917721712980),
            (111, 18, 7877536226278031323),
            (111, 18, 7966591011541352983),
            (111, 18, 8370371792817398698),
            (111, 18, 8495984618499609409),
            (111, 18, 8840233365142162752),
            (118, 81, -4194714474977578402),
            (118, 81, -4005749297113976964),
            (118, 81, -3182018141457765095),
            (118, 81, -1274069168007014093),
            (118, 81, 1620428544379662329),
            (118, 81, 9210160792326402951),
            (118, 182, -9010057082347439134),
            (118, 182, -8952768377849775263),
            (118, 182, -8845555497808598629),
            (118, 182, -8659384266976088025),
            (118, 182, -8601761502550196435),
            (118, 182, -8288813745469219819),
            (118, 182, -8079450122697346644),
            (118, 182, -7925521392043725767),
            (118, 182, -7675784629403150278),
            (118, 182, -7475481801172481377),
            (118, 182, -6925932612125964219),
            (118, 182, -6851907175729935802),
            (118, 182, -6811790383874324042),
            (118, 182, -6500332562936002681),
            (118, 182, -5858984423380085461),
            (118, 182, -5620149289496752646),
            (118, 182, -5275818194241248108),
            (118, 182, -5105333025396728771),
            (118, 182, -4446210323546699760),
            (118, 182, -3628299646119252289),
            (118, 182, -2964337845918343027),
            (118, 182, -2894488091926329957),
            (118, 182, -2696135567032831749),
            (118, 182, -2561797702829900485),
            (118, 182, -2420862708578594344),
            (118, 182, -2404539092534228785),
            (118, 182, -2082926833846659890),
            (118, 182, -1956988105938946820),
            (118, 182, -1525564091514229856),
            (118, 182, -881009639962723258),
            (118, 182, -773828200539187467),
            (118, 182, -736929617401019135),
            (118, 182, -542912523139897655),
            (118, 182, -507537613787771197),
            (118, 182, -475797386580535800),
            (118, 182, 7837393312168051),
            (118, 182, 199689190401760090),
            (118, 182, 567524267319800745),
            (118, 182, 725628817202625005),
            (118, 182, 798761834566854390),
            (118, 182, 1232774703473700600),
            (118, 182, 1429304862222946582),
            (118, 182, 1486318148997481614),
            (118, 182, 1923348892692684283),
            (118, 182, 2385605638955383088),
            (118, 182, 2525895792049107194),
            (118, 182, 2758872475529353482),
            (118, 182, 2814567125466891018),
            (118, 182, 3048374343134964108),
            (118, 182, 3437692991696097327),
            (118, 182, 3439880431820722634),
            (118, 182, 3484093846594006149),
            (118, 182, 3810452773122330840),
            (118, 182, 4103899075896407217),
            (118, 182, 4453509150785312490),
            (118, 182, 4784141958505125069),
            (118, 182, 4847097168293765828),
            (118, 182, 4902727916882408974),
            (118, 182, 5099007272890708486),
            (118, 182, 5140133457559487708),
            (118, 182, 5249894610245558745),
            (118, 182, 5508943652504621854),
            (118, 182, 5547363806206771763),
            (118, 182, 5818839228846814330),
            (118, 182, 6057643351401448837),
            (118, 182, 7568365215152722596),
            (118, 182, 7958414805417505185),
            (118, 182, 7980551083781872801),
            (118, 182, 8093706963054088867),
            (118, 182, 8803408573667130631),
            (118, 182, 8952429355708533923),
            (118, 182, 9174434432718027970),
            (122, 13, -8905889360866462791),
            (122, 13, -8777218065785630488),
            (122, 13, -8723407968233541529),
            (122, 13, -8422763629652592975),
            (122, 13, -7861321188465361116),
            (122, 13, -7562980247307511124),
            (122, 13, -7291681077133732640),
            (122, 13, -6939989286429748954),
            (122, 13, -6283795519916634690),
            (122, 13, -6093150862868023322),
            (122, 13, -6016528374598748785),
            (122, 13, -5575590661995387352),
            (122, 13, -4147050432510157093),
            (122, 13, -3341795102326539812),
            (122, 13, -3306866496640575711),
            (122, 13, -1518612608581692176),
            (122, 13, 138990646496621878),
            (122, 13, 596672407102215705),
            (122, 13, 703720512116443212),
            (122, 13, 779160702094547747),
            (122, 13, 834754227656774347),
            (122, 13, 1425825254946793781),
            (122, 13, 3209743762135980786),
            (122, 13, 3303848440810995631),
            (122, 13, 3436252407833061765),
            (122, 13, 3749717104070905088),
            (122, 13, 4004912413843805036),
            (122, 13, 4598711221180126702),
            (122, 13, 4680959939442587711),
            (122, 13, 5048442304549758143),
            (122, 13, 5089189570222217435),
            (122, 13, 5099933499879815995),
            (122, 13, 5123956355091852190),
            (122, 13, 5825845975683832137),
            (122, 13, 5902298591749925984),
            (122, 13, 5989199027588778124),
            (122, 13, 6444472993605287152),
            (122, 13, 6710111184498657489),
            (122, 13, 6768219959901980713),
            (122, 13, 6893677992077448540),
            (122, 13, 7201237238070372607),
            (122, 13, 7610039983987801688),
            (122, 13, 7702157238939757973),
            (122, 13, 7979530446017367453),
            (122, 13, 8166089376086684271),
            (122, 13, 8505606385381441126),
            (122, 13, 8838692372623759600),
            (122, 31, -7037189132012378529),
            (122, 31, -6955596001373185507),
            (122, 31, -6932299524334809394),
            (122, 31, -6886568470877175388),
            (122, 31, -6338150807013710778),
            (122, 31, -6280743770602689775),
            (122, 31, -5528144050534651071),
            (122, 31, -4799284861000347676),
            (122, 31, -4717904697287350402),
            (122, 31, -4685833031565075891),
            (122, 31, -4340969682604431200),
            (122, 31, -3282580921184043530),
            (122, 31, -3023387235683776249),
            (122, 31, -2668319355490747343),
            (122, 31, -2598806040814759216),
            (122, 31, -2466103541949899083),
            (122, 31, -2312361579554404692),
            (122, 31, -2211404838732372749),
            (122, 31, -2120720751948382544),
            (122, 31, -2108355855273834582),
            (122, 31, -1900759807646414903),
            (122, 31, -1763055045295431905),
            (122, 31, -1032664443670809956),
            (122, 31, -870821068757489188),
            (122, 31, 468416351781298344),
            (122, 31, 1540776768033533220),
            (122, 31, 1850215925042031537),
            (122, 31, 2202130495192898657),
            (122, 31, 2647401563658199717),
            (122, 31, 2972241223305618530),
            (122, 31, 3755599244855205944),
            (122, 31, 4113800676608579338),
            (122, 31, 4634479273631821812),
            (122, 31, 4739162537661760468),
            (122, 31, 5329772776265074824),
            (122, 31, 5919101639899299945),
            (122, 31, 6607767392251979232),
            (122, 31, 6835472141084565683),
            (122, 31, 7665158018581651633),
            (122, 31, 7845405899146821174),
            (122, 31, 8359704903092438735),
            (122, 31, 8600625844692190529),
            (126, 74, -8893822734348361973),
            (126, 74, -8856120095757040645),
            (126, 74, -8842295576446231274),
            (126, 74, -8161527363911062801),
            (126, 74, -7983441412952104602),
            (126, 74, -7738822633183553254),
            (126, 74, -7477331530198678281),
            (126, 74, -7317840270006262428),
            (126, 74, -7243928715517576989),
            (126, 74, -6839835001386146786),
            (126, 74, -6585397314046446072),
            (126, 74, -6565167997134287600),
            (126, 74, -6457771256272844000),
            (126, 74, -6178702287674322906),
            (126, 74, -6018778106411838786),
            (126, 74, -5595317201895390304),
            (126, 74, -5566196532984872528),
            (126, 74, -5175746709953098860),
            (126, 74, -4931593116494139938),
            (126, 74, -4683719540129872478),
            (126, 74, -4591404106439934022),
            (126, 74, -4255735672112149949),
            (126, 74, -3295026654090606270),
            (126, 74, -2651527828648978069),
            (126, 74, -2157713593297605969),
            (126, 74, -1951480087578445698),
            (126, 74, -1593001099590401920),
            (126, 74, -1585901540368279566),
            (126, 74, -1501911877681406173),
            (126, 74, -1423399253201402072),
            (126, 74, -1149634700813186569),
            (126, 74, -839552502379928398),
            (126, 74, -638143154356953012),
            (126, 74, -578835101456405124),
            (126, 74, 508200665359930609),
            (126, 74, 676712860465745013),
            (126, 74, 998616456469413927),
            (126, 74, 1233035429628518563),
            (126, 74, 1682401136163088206),
            (126, 74, 1754257141703171768),
            (126, 74, 1894299214031907106),
            (126, 74, 2281516986205740130),
            (126, 74, 2311472058779054489),
            (126, 74, 2781691774132020196),
            (126, 74, 2781792720931838385),
            (126, 74, 3278954397237112228),
            (126, 74, 3931641866671339267),
            (126, 74, 4322946514172816856),
            (126, 74, 4342919231217091983),
            (126, 74, 6109141682378515373),
            (126, 74, 6271451284054296229),
            (126, 74, 6612625565638737123),
            (126, 74, 6740066322821903977),
            (126, 74, 7289300557021315130),
            (126, 74, 7974475357638749771),
            (126, 74, 8353913845394660218),
            (126, 74, 8391988354098199464),
            (126, 74, 8714059661668365649),
            (126, 74, 9062069143994332236),
            (126, 230, -9146636541721350416),
            (126, 230, -8933380577932257993),
            (126, 230, -8923045056588882644),
            (126, 230, -8868122937387595511),
            (126, 230, -8709320649417292669),
            (126, 230, -7926737760206258129),
            (126, 230, -7298591354331097226),
            (126, 230, -7059038436026718751),
            (126, 230, -7033840753241129288),
            (126, 230, -6690605185267732214),
            (126, 230, -6543126731684464300),
            (126, 230, -6391453809956601738),
            (126, 230, -5666767316085375350),
            (126, 230, -5506383458887052171),
            (126, 230, -5457488878487731511),
            (126, 230, -5424262323111452274),
            (126, 230, -4842197886484615958),
            (126, 230, -4383592221122910764),
            (126, 230, -4193326464494590128),
            (126, 230, -4096352446385293950),
            (126, 230, -4067892814473166509),
            (126, 230, -3745670941512868860),
            (126, 230, -3704395562590255405),
            (126, 230, -3302187841287075340),
            (126, 230, -2964269411772107713),
            (126, 230, -2343433774187971089),
            (126, 230, -2318276976334559964),
            (126, 230, -1596875702400500870),
            (126, 230, -1515994549244139027),
            (126, 230, -1301999337157031311),
            (126, 230, -785142060342225685),
            (126, 230, -167699013393841995),
            (126, 230, 470418441501960034),
            (126, 230, 553902621208608848),
            (126, 230, 811772117140693780),
            (126, 230, 1161844219380409158),
            (126, 230, 1972535286568484462),
            (126, 230, 2382219925710841784),
            (126, 230, 3155323504450155733),
            (126, 230, 3683871842204082694),
            (126, 230, 3944747828202483380),
            (126, 230, 4068858368315667302),
            (126, 230, 4308178536515580104),
            (126, 230, 4626352992595473301),
            (126, 230, 4665879911938039248),
            (126, 230, 5726947292966560604),
            (126, 230, 5807759808752791908),
            (126, 230, 6292404433092270779),
            (126, 230, 6359674716792441897),
            (126, 230, 6578200529710570654),
            (126, 230, 6846223773418507182),
            (126, 230, 6872031860849629857),
            (126, 230, 6896232788880167235),
            (126, 230, 8305880246162404202),
            (126, 230, 8404481741508720060),
            (126, 230, 9031141022913277301),
            (132, 212, -9051837378934722959),
            (132, 212, -8547415355744841536),
            (132, 212, -8425669210942741567),
            (132, 212, -7856764681758897996),
            (132, 212, -7617102567001867059),
            (132, 212, -7520444261316334342),
            (132, 212, -7272767336116170095),
            (132, 212, -7191149397832760505),
            (132, 212, -7167996282165478031),
            (132, 212, -7098225958815061754),
            (132, 212, -6833888555148653276),
            (132, 212, -6746084943308494936),
            (132, 212, -6706994856090744917),
            (132, 212, -6696045262431104326),
            (132, 212, -6671220188057767705),
            (132, 212, -6195388473643411931),
            (132, 212, -5537577388238438314),
            (132, 212, -5309813026677015626),
            (132, 212, -4926025228333378977),
            (132, 212, -4887908903583206098),
            (132, 212, -4864940042829511993),
            (132, 212, -3769545690033254924),
            (132, 212, -3768122421333597079),
            (132, 212, -3654891948704811196),
            (132, 212, -3603010764116555258),
            (132, 212, -3094574514631884839),
            (132, 212, -2796694455407554340),
            (132, 212, -2768499713959783021),
            (132, 212, -2660934677263100148),
            (132, 212, -2650499566100397472),
            (132, 212, -2413312334658288215),
            (132, 212, -2125639278727987550),
            (132, 212, -2065593647654696556),
            (132, 212, -2054689540358703542),
            (132, 212, -1968139580485934066),
            (132, 212, -1911498203938894014),
            (132, 212, -1689283076981931630),
            (132, 212, -1685400522483491450),
            (132, 212, -1566558081994037080),
            (132, 212, -1378487017714524485),
            (132, 212, -1259349654567869987),
            (132, 212, -1217145792307686499),
            (132, 212, -1064672366520187802),
            (132, 212, -632299782428404250),
            (132, 212, -502551998829764681),
            (132, 212, -363450226256444333),
            (132, 212, -198568648360847740),
            (132, 212, -124117915016021180),
            (132, 212, -108629791918867220),
            (132, 212, 267041812180045480),
            (132, 212, 290333471364218842),
            (132, 212, 403456196746277082),
            (132, 212, 541363153509728442),
            (132, 212, 836914597928456084),
            (132, 212, 1461109237181330391),
            (132, 212, 1645556677985617325),
            (132, 212, 1812046360598534174),
            (132, 212, 1840298015908903527),
            (132, 212, 2035755929214992698),
            (132, 212, 2224361173444521506),
            (132, 212, 2441263891862972227),
            (132, 212, 2490236758729556821),
            (132, 212, 2830501935727850931),
            (132, 212, 3098617709682072119),
            (132, 212, 3208125640808048949),
            (132, 212, 3406188382378512444),
            (132, 212, 4049302978698713892),
            (132, 212, 4106547055615101017),
            (132, 212, 4120103590051810248),
            (132, 212, 4183782211886006511),
            (132, 212, 5044653427120704727),
            (132, 212, 5172325673041424700),
            (132, 212, 5175943174799216964),
            (132, 212, 5337246568689569545),
            (132, 212, 5413373054194801664),
            (132, 212, 5826408875989764523),
            (132, 212, 6174608435119224254),
            (132, 212, 6185307223691643668),
            (132, 212, 6271155859184230666),
            (132, 212, 6466194850792676212),
            (132, 212, 7591918969162463558),
            (132, 212, 7640170608791920681),
            (132, 212, 7653319923458646775),
            (132, 212, 8102146688642272267),
            (132, 212, 8108746728487065470),
            (132, 212, 8584358499371260973),
            (132, 212, 8895403996660681919),
            (132, 212, 8972372935698227838),
            (132, 212, 9092308884871257464),
            (137, 14, -9053896104783221289),
            (137, 14, -8922872307452157837),
            (137, 14, -7424426450792402685),
            (137, 14, -5538952254700499251),
            (137, 14, -4717600763054113698),
            (137, 14, -4534522435424716714),
            (137, 14, -3237553991374837517),
            (137, 14, -2618572451262506571),
            (137, 14, -2070202520947532108),
            (137, 14, -2068667457119491920),
            (137, 14, -2063133231922846740),
            (137, 14, -702885491056535316),
            (137, 14, -294553418991584835),
            (137, 14, 1046704628933365342),
            (137, 14, 1285254111802685559),
            (137, 14, 2415365890002568043),
            (137, 14, 4169509262993639292),
            (137, 14, 4531276389063696236),
            (137, 14, 5132053554604220012),
            (137, 14, 5164941942043239294),
            (137, 14, 5728027953818130803),
            (137, 14, 5767132319122626483),
            (137, 14, 5955035682359522123),
            (137, 14, 7925515419412928578),
            (137, 14, 8679728697126540435),
            (137, 14, 8782662555553336940),
            (141, 18, -8919152565408844657),
            (141, 18, -8225909987595654310),
            (141, 18, -8037485341276552814),
            (141, 18, -7964436850087221268),
            (141, 18, -7751155920110314879),
            (141, 18, -7679430501792024768),
            (141, 18, -7143997979257927264),
            (141, 18, -7039991416632345605),
            (141, 18, -6985186706497805666),
            (141, 18, -6825280424159200338),
            (141, 18, -6743498668370987048),
            (141, 18, -6558098140126502690),
            (141, 18, -6382708433348460806),
            (141, 18, -6302040064251157327),
            (141, 18, -6260443070922593091),
            (141, 18, -6079034050780224224),
            (141, 18, -5759348138855157224),
            (141, 18, -5415752720513965881),
            (141, 18, -4041479904085560102),
            (141, 18, -3355088857332391755),
            (141, 18, -2808843868325254589),
            (141, 18, -2623951994787932216),
            (141, 18, -2604236468182495451),
            (141, 18, -2253035776660594550),
            (141, 18, -2218511863124856605),
            (141, 18, -1999823530984625371),
            (141, 18, -1584335215545942529),
            (141, 18, -1324941386339718167),
            (141, 18, -1134543997512970598),
            (141, 18, -881641709651656237),
            (141, 18, -818805306462515581),
            (141, 18, -229682502821756957),
            (141, 18, -43745521668489431),
            (141, 18, 244964184560096696),
            (141, 18, 483532283927819955),
            (141, 18, 1037612332381880972),
            (141, 18, 1131404121799716346),
            (141, 18, 1133453424155569163),
            (141, 18, 1609568285359272766),
            (141, 18, 1951115730012669480),
            (141, 18, 2341130321858734826),
            (141, 18, 2534327883015915763),
            (141, 18, 2774542446020379742),
            (141, 18, 2783426797979214646),
            (141, 18, 3746721558789445905),
            (141, 18, 3775237075215212700),
            (141, 18, 3896498872729167950),
            (141, 18, 4015466080858884097),
            (141, 18, 4519524712290030091),
            (141, 18, 5443044608940318464),
            (141, 18, 5641925048973226070),
            (141, 18, 5961143583282807526),
            (141, 18, 6162089797940833667),
            (141, 18, 7689444749193863288),
            (141, 18, 7832898459378537536),
            (141, 18, 7874925957152363354),
            (141, 18, 8085374271925297176),
            (141, 18, 8512797632107665619),
            (141, 18, 8942645565332080001),
            (144, 174, -9130246974291272707),
            (144, 174, -7722562913921477405),
            (144, 174, -7689454419960992639),
            (144, 174, -7578868224142205709),
            (144, 174, -7400419589080746129),
            (144, 174, -7199406901066040704),
            (144, 174, -6991801294488219153),
            (144, 174, -6782830312581463478),
            (144, 174, -6774905688205640773),
            (144, 174, -6367580213995994459),
            (144, 174, -6190300410199270188),
            (144, 174, -5984922730624402333),
            (144, 174, -5524136189642811912),
            (144, 174, -5219549830404576831),
            (144, 174, -5052850372464593993),
            (144, 174, -4718281247129780092),
            (144, 174, -4217648577016700444),
            (144, 174, -3871040923641490697),
            (144, 174, -3707690248496399908),
            (144, 174, -3653320053208471269),
            (144, 174, -3413420132287531778),
            (144, 174, -2828017430966769925),
            (144, 174, -2671264641281056083),
            (144, 174, -2430760503755654506),
            (144, 174, -2233564378854643120),
            (144, 174, -2074068992843053490),
            (144, 174, -1464174944473579487),
            (144, 174, -1213846663485794820),
            (144, 174, -730415957195574619),
            (144, 174, -523156613082657776),
            (144, 174, -318084895653906285),
            (144, 174, -285986930489891177),
            (144, 174, 197413137023314880),
            (144, 174, 285136848515069713),
            (144, 174, 974007087754799229),
            (144, 174, 1527228348850922388),
            (144, 174, 1740534821726990947),
            (144, 174, 2252134716584108940),
            (144, 174, 2803880856722187847),
            (144, 174, 2815352828286130663),
            (144, 174, 2937305948584800209),
            (144, 174, 2987219713066284475),
            (144, 174, 3118893119238715816),
            (144, 174, 3197116216512552918),
            (144, 174, 3564028127280505290),
            (144, 174, 3810218762013778058),
            (144, 174, 4311109602455652536),
            (144, 174, 4394689030820879913),
            (144, 174, 4430406697672696369),
            (144, 174, 5059825680239634418),
            (144, 174, 5309782917555277800),
            (144, 174, 5420762784234226323),
            (144, 174, 5517772737927559269),
            (144, 174, 5901008869224685391),
            (144, 174, 6219360994700169571),
            (144, 174, 6855678182238033761),
            (144, 174, 6969147688691878085),
            (144, 174, 7049292018719628524),
            (144, 174, 7936396207656589761),
            (144, 174, 8529108941375369307),
            (144, 174, 8960556883678904027),
            (144, 174, 9091340288214142841),
            (146, 92, -9203024865294992425),
            (146, 92, -8947345676011332442),
            (146, 92, -8847628482310694550),
            (146, 92, -8741357460230359665),
            (146, 92, -7262563746936354577),
            (146, 92, -7171733403615927754),
            (146, 92, -6860163487168980167),
            (146, 92, -6686385053663837116),
            (146, 92, -5153814207335212440),
            (146, 92, -5065443191181907177),
            (146, 92, -3724744644203147540),
            (146, 92, -3513606636509196356),
            (146, 92, -2989173817831832671),
            (146, 92, -1072057242562474752),
            (146, 92, -481306167943036895),
            (146, 92, 164288284138615929),
            (146, 92, 2207475350096790520),
            (146, 92, 2355903278061794437),
            (146, 92, 6872000991481765283),
            (146, 92, 6919320933819775633),
            (146, 92, 7431480487289540896),
            (146, 92, 7714743161765771156),
            (146, 92, 7831547450228930318),
            (146, 92, 8006248412488427558),
            (146, 92, 8142141113956871946),
            (146, 92, 8303343098317895209),
            (156, 92, -8940388394977243234),
            (156, 92, -8883436420623898605),
            (156, 92, -7497930905303846450),
            (156, 92, -6841882685543482739),
            (156, 92, -6521127525214420626),
            (156, 92, -6403779239773020882),
            (156, 92, -6249788065601246318),
            (156, 92, -5524055479029911652),
            (156, 92, -5314134157181515451),
            (156, 92, -5132025363692153254),
            (156, 92, -3829280953240408618),
            (156, 92, -2715648168020515247),
            (156, 92, -2338264487389597173),
            (156, 92, -2077266771361525490),
            (156, 92, -1895167255226542111),
            (156, 92, -1535176079979270052),
            (156, 92, -1479594312080368664),
            (156, 92, -658768895648520848),
            (156, 92, -475109571593295272),
            (156, 92, -371643156836744049),
            (156, 92, -328137727445220683),
            (156, 92, -225690487473391221),
            (156, 92, 28447016855874256),
            (156, 92, 1522282168768867987),
            (156, 92, 1883206904607274823),
            (156, 92, 2235235508828836881),
            (156, 92, 2355081233367232447),
            (156, 92, 2490570725212190293),
            (156, 92, 3133345264898930003),
            (156, 92, 3296893642113714749),
            (156, 92, 4074344250744082486),
            (156, 92, 4201692682142725008),
            (156, 92, 4545139792294648617),
            (156, 92, 5635613116703457612),
            (156, 92, 6075918979598876373),
            (156, 92, 6297393242970851726),
            (156, 92, 6810812018725175648),
            (156, 92, 7052150202417591071),
            (156, 92, 7791363626091765033),
            (156, 92, 8000771837120758448),
            (156, 92, 8402058478361593342),
            (156, 92, 8774184013039130907),
            (156, 92, 9001692538763838711),
            (156, 92, 9075782827459115376),
            (156, 92, 9080294492710687134),
            (157, 119, -9195835615404553233),
            (157, 119, -8951545678312643968),
            (157, 119, -8712540508439286149),
            (157, 119, -8187550994537429630),
            (157, 119, -8117088919672042109),
            (157, 119, -8095992319311772135),
            (157, 119, -7907350602093576528),
            (157, 119, -7882075876034112222),
            (157, 119, -7674356182258567460),
            (157, 119, -7639866361972631105),
            (157, 119, -7243423838379486617),
            (157, 119, -7108085375366445505),
            (157, 119, -6729432272473068498),
            (157, 119, -6585557697526438452),
            (157, 119, -5898373235420598726),
            (157, 119, -5718104331436731832),
            (157, 119, -5284343524374029033),
            (157, 119, -5078645870001626041),
            (157, 119, -5044148596143560828),
            (157, 119, -4550367403933322525),
            (157, 119, -3731208569035075267),
            (157, 119, -3610754813846433718),
            (157, 119, -3608844782448786278),
            (157, 119, -3512101858778371148),
            (157, 119, -3446576790456067138),
            (157, 119, -3228784524353072524),
            (157, 119, -3016534486476210435),
            (157, 119, -3005841957975898056),
            (157, 119, -2966816146057044041),
            (157, 119, -2683076074988344796),
            (157, 119, -2545475368337360123),
            (157, 119, -2093510099062626651),
            (157, 119, -2086237808706049011),
            (157, 119, -1950293184556413803),
            (157, 119, -1628575035156539619),
            (157, 119, -1553814365882525481),
            (157, 119, -710397524754776440),
            (157, 119, -649789621495901201),
            (157, 119, -379810613304459165),
            (157, 119, -316470303537115150),
            (157, 119, -263405395302342453),
            (157, 119, 101194720434255835),
            (157, 119, 372850576724689999),
            (157, 119, 750701837640114156),
            (157, 119, 848841320421924035),
            (157, 119, 862950259178675726),
            (157, 119, 1199864295706127217),
            (157, 119, 1200541251709981493),
            (157, 119, 1321912027244587281),
            (157, 119, 1570470621155153688),
            (157, 119, 1628053862514616172),
            (157, 119, 1646843907727234629),
            (157, 119, 1659752012931170382),
            (157, 119, 1803714681140562156),
            (157, 119, 2228742377048003681),
            (157, 119, 2335753773208096837),
            (157, 119, 2369488200122899791),
            (157, 119, 2467830946650633061),
            (157, 119, 3115851756067958069),
            (157, 119, 3328082093404669599),
            (157, 119, 3328431248622372225),
            (157, 119, 3433069519887478529),
            (157, 119, 3516600487836723137),
            (157, 119, 3921080367641005392),
            (157, 119, 4524471351777060097),
            (157, 119, 4536181700067395961),
            (157, 119, 4756898178505227233),
            (157, 119, 4760651907478504562),
            (157, 119, 4867254703239961659),
            (157, 119, 4967736862446624042),
            (157, 119, 5030630594820912714),
            (157, 119, 5306207666388039883),
            (157, 119, 5325711946239316867),
            (157, 119, 5405870565957223534),
            (157, 119, 5872808359510285272),
            (157, 119, 5908613266106603230),
            (157, 119, 6126976535436234996),
            (157, 119, 6555097818400839873),
            (157, 119, 7106704424440641450),
            (157, 119, 7233740097569172559),
            (157, 119, 7317558299192883584),
            (157, 119, 7376894267104932372),
            (157, 119, 7459346739281591668),
            (157, 119, 7538303160636463972),
            (157, 119, 7573596712073977952),
            (157, 119, 7657711781583925462),
            (157, 119, 7995718556910320985),
            (157, 119, 8084169473135690469),
            (157, 119, 8086812636624515179),
            (157, 119, 8622857111994768499),
            (157, 119, 9136962584040087252),
            (238, 185, -9185804101515292638),
            (238, 185, -9102907904010997443),
            (238, 185, -8872443584985676101),
            (238, 185, -8851428757703155897),
            (238, 185, -8840271168771787502),
            (238, 185, -8794390852342464530),
            (238, 185, -8713818043083912037),
            (238, 185, -8690378960760107295),
            (238, 185, -8502670171279461556),
            (238, 185, -8320534864031966763),
            (238, 185, -8108472083072375959),
            (238, 185, -7977074983901117550),
            (238, 185, -7773116689323095543),
            (238, 185, -7766253966900119746),
            (238, 185, -6960772027589058740),
            (238, 185, -6797824859392026442),
            (238, 185, -6761076085115532124),
            (238, 185, -6714299863688720837),
            (238, 185, -6639374672111770219),
            (238, 185, -6284927565174374056),
            (238, 185, -6134354471834007351),
            (238, 185, -5842616153751674804),
            (238, 185, -5287474421726210771),
            (238, 185, -4831477920230039505),
            (238, 185, -4818668044186325384),
            (238, 185, -4739031017429122085),
            (238, 185, -4738256828434722095),
            (238, 185, -4273412109613819121),
            (238, 185, -3883978071606507207),
            (238, 185, -3629901768442151522),
            (238, 185, -3448160750867846055),
            (238, 185, -3214393363722897592),
            (238, 185, -2674950401016721249),
            (238, 185, -2622894893378585295),
            (238, 185, -2513958150374109386),
            (238, 185, -2117980401712947215),
            (238, 185, -1954137923939347717),
            (238, 185, -1940045787810792059),
            (238, 185, -1923247797913485511),
            (238, 185, -1907886303191492080),
            (238, 185, -1830888329009778248),
            (238, 185, -1330739382508395805),
            (238, 185, -1328992851220128457),
            (238, 185, -257531787074056132),
            (238, 185, -157819853139632985),
            (238, 185, -44210495839170458),
            (238, 185, 89117249446667887),
            (238, 185, 390419158008672652),
            (238, 185, 433450815020246966),
            (238, 185, 1053067467865416158),
            (238, 185, 1162357146027477976),
            (238, 185, 1430545367844731920),
            (238, 185, 1671643212963559059),
            (238, 185, 2061789456966309708),
            (238, 185, 2253673020179900587),
            (238, 185, 2431943959733580463),
            (238, 185, 2549048508365859357),
            (238, 185, 2563026298793058725),
            (238, 185, 2579835894393624752),
            (238, 185, 2692706413121668530),
            (238, 185, 2853694928068134997),
            (238, 185, 3316185764910004936),
            (238, 185, 3490365661017807551),
            (238, 185, 3502607538940058893),
            (238, 185, 3874475690114599868),
            (238, 185, 3908399726162356103),
            (238, 185, 3996320174000453257),
            (238, 185, 4070911161305328905),
            (238, 185, 4354884493912144525),
            (238, 185, 4782643032333645371),
            (238, 185, 4853936669392405746),
            (238, 185, 4897077466597081498),
            (238, 185, 4942624120226443435),
            (238, 185, 5056313576326095257),
            (238, 185, 5060425698693617867),
            (238, 185, 5117225195586758246),
            (238, 185, 5150918143069894848),
            (238, 185, 5441335177275396950),
            (238, 185, 5483740357424353556),
            (238, 185, 5589358381024056597),
            (238, 185, 5639689460100930551),
            (238, 185, 5677188699350979268),
            (238, 185, 6086126975539919449),
            (238, 185, 6602078158465733019),
            (238, 185, 6953591081980358661),
            (238, 185, 7265883636655395513),
            (238, 185, 8063050575618289259),
            (238, 185, 8172370528033368617),
            (238, 185, 8238197976252225695),
            (238, 185, 8339047607567908469),
            (238, 185, 8514718528975107369),
            (238, 185, 8551918265891791537),
            (238, 185, 8635543687852673798),
            (238, 185, 8813935255383862918),
            (238, 185, 9180970108916772719),
        ];
        let input_chunk_size = 411;
        let vertex_chunk_size = 855;
        let edge_chunk_size = 48;
        let edge_max_list_size = 41;

        edges_sanity_check_inner(
            edges,
            input_chunk_size,
            vertex_chunk_size,
            edge_chunk_size,
            edge_max_list_size,
        );
    }

    #[test]
    fn edge_sanity_chunk_broken_incoming() {
        let edges = vec![
            (0, 0, 0),
            (0, 0, 0),
            (0, 0, 66),
            (0, 1, 0),
            (2, 0, 0),
            (3, 4, 0),
            (4, 0, 0),
            (4, 4, 0),
            (4, 4, 0),
            (4, 4, 0),
            (4, 4, 0),
            (5, 0, 0),
            (6, 7, 7274856480798084567),
            (8, 3, -7707029126214574305),
        ];

        edges_sanity_check_inner(edges, 853, 4, 122, 98)
    }

    #[test]
    fn edge_sanity_chunk_broken_something() {
        let edges = vec![(0, 3, 0), (1, 2, 0), (3, 2, 0)];
        edges_sanity_check_inner(edges, 1, 1, 1, 1)
    }

    #[test]
    fn load_from_parquet() {
        let file_path: PathBuf = [
            env!("CARGO_MANIFEST_DIR"),
            "resources",
            "test",
            "part-00000-b406cce6-7ed0-4efb-883d-e6766f36d8cf-c000.snappy.parquet",
        ]
        .iter()
        .collect();
        let test_dir = TempDir::new().unwrap();

        let g = TempColGraphFragment::from_sorted_parquet_edge_list(
            file_path.as_path(),
            "source",
            "destination",
            "time",
            5,
            5,
            5,
            test_dir.path(),
        )
        .unwrap();

        println!("{:?}", g)
    }

    fn schema() -> Schema {
        let srcs = Field::new("srcs", DataType::UInt64, false);
        let dsts = Field::new("dsts", DataType::UInt64, false);
        let time = Field::new("time", DataType::Int64, false);
        let weight = Field::new("weight", DataType::Float64, true);
        Schema::from(vec![srcs, dsts, time, weight])
    }

    #[test]
    fn sort_merge_dedup_2_cols() {
        let rhs = PrimitiveArray::from_vec(vec![1u64, 2, 3, 4, 5, 6, 7, 8, 9, 10]).boxed();
        let hash_rhs = PrimitiveArray::from_vec(vec![4i64, 5, 5, 6, 7, 7, 8, 8, 9, 11]).boxed();

        let lhs = PrimitiveArray::from_vec(vec![0u64, 9, 10, 6, 7, 8, 3, 1, 4, 15]).boxed();
        let hash_lhs = PrimitiveArray::from_vec(vec![-3i64, 9, 11, 7, 8, 8, 5, 4, 6, 17]).boxed();

        let (actual_h, actual_v) = sort_dedup_2((&hash_rhs, &rhs), (&hash_lhs, &lhs)).unwrap();

        let expected_v =
            PrimitiveArray::from_vec(vec![0u64, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15]).boxed();
        let expected_h =
            PrimitiveArray::from_vec(vec![-3i64, 4, 5, 5, 6, 7, 7, 8, 8, 9, 11, 17]).boxed();

        assert_eq!(actual_h, expected_h);
        assert_eq!(actual_v, expected_v);
    }

    #[test]
    fn load_one_edge_from_sorted_adj_list_num_vertices_props() {
        let test_dir = TempDir::new().unwrap();
        let srcs = PrimitiveArray::from_vec(vec![1u64]).boxed();
        let dsts = PrimitiveArray::from_vec(vec![2u64]).boxed();
        let time = PrimitiveArray::from_vec(vec![9i64]).boxed();
        let weight = PrimitiveArray::from_vec(vec![3.14f64]).boxed();
        let chunk = LoadChunk::new(vec![srcs, dsts, time, weight], 0, 1, 2, schema());

        let graph = TempColGraphFragment::build_tables_from_chunked(
            test_dir.path(),
            100,
            100,
            100,
            GlobalMap::from(vec![1u64, 2u64]).into(),
            vec![chunk],
        )
        .unwrap();

        let actual = graph.edges(0.into(), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph.all_edges().collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(0), VID(1))];
        assert_eq!(actual, expected)
    }

    #[test]
    fn load_one_edge_from_sorted_adj_list_num_vertices_multiple_timestamps() {
        let test_dir = TempDir::new().unwrap();

        let srcs = PrimitiveArray::from_vec(vec![1u64, 1u64, 1u64]).boxed();
        let dsts = PrimitiveArray::from_vec(vec![2u64, 2u64, 2u64]).boxed();
        let time = PrimitiveArray::from_vec(vec![0i64, 3i64, 7i64]).boxed();
        let weight = PrimitiveArray::from_vec(vec![1.14f64, 2.14f64, 3.14f64]).boxed();
        let chunk = LoadChunk::new(vec![srcs, dsts, time, weight], 0, 1, 2, schema());

        let graph = TempColGraphFragment::build_tables_from_chunked(
            test_dir.path(),
            100,
            100,
            100,
            GlobalMap::from(vec![1u64, 2u64]).into(),
            vec![chunk],
        )
        .unwrap();

        let actual = graph.edges(0.into(), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph.all_edges().collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(0), VID(1))];
        assert_eq!(actual, expected)
    }

    #[test]
    fn load_muliple_sorted_edges() {
        let test_dir = TempDir::new().unwrap();

        let srcs = PrimitiveArray::from_vec(vec![
            1u64, 1u64, 1u64, 2u64, 2u64, 2u64, 3u64, 3u64, 3u64, 4u64, 4u64, 4u64,
        ])
        .boxed();
        let dsts = PrimitiveArray::from_vec(vec![
            2u64, 3u64, 4u64, 3u64, 4u64, 5u64, 4u64, 5u64, 6u64, 5u64, 6u64, 7u64,
        ])
        .boxed();
        let time = PrimitiveArray::from_vec(vec![
            0i64, 1i64, 2i64, 3i64, 4i64, 5i64, 6i64, 7i64, 8i64, 9i64, 10i64, 11i64,
        ])
        .boxed();
        let weight = PrimitiveArray::from_vec(vec![
            1.14f64, 2.14f64, 3.14f64, 4.14f64, 5.14f64, 6.14f64, 7.14f64, 8.14f64, 9.14f64,
            10.14f64, 11.14f64, 12.14f64,
        ])
        .boxed();
        let chunk = LoadChunk::new(vec![srcs, dsts, time, weight], 0, 1, 2, schema());

        let graph = TempColGraphFragment::build_tables_from_chunked(
            test_dir.path(),
            4,
            2,
            100,
            GlobalMap::from(1u64..=7u64).into(),
            vec![chunk],
        )
        .unwrap();

        let actual = graph.edges(0.into(), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2)), (EID(2), VID(3))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph.all_edges().collect::<Vec<_>>();
        let expected = vec![
            (EID(0), VID(0), VID(1)),
            (EID(1), VID(0), VID(2)),
            (EID(2), VID(0), VID(3)),
            (EID(3), VID(1), VID(2)),
            (EID(4), VID(1), VID(3)),
            (EID(5), VID(1), VID(4)),
            (EID(6), VID(2), VID(3)),
            (EID(7), VID(2), VID(4)),
            (EID(8), VID(2), VID(5)),
            (EID(9), VID(3), VID(4)),
            (EID(10), VID(3), VID(5)),
            (EID(11), VID(3), VID(6)),
        ];
        assert_eq!(actual, expected);

        let e0 = graph.edge(0.into());
        assert_eq!(e0.timestamps().next().unwrap(), &[0i64]);

        let e5 = graph.edge(5.into());
        assert_eq!(e5.timestamps().next().unwrap(), &[5i64]);
    }

    #[test]
    fn load_muliple_sorted_edges_multiple_ts() {
        let test_dir = TempDir::new().unwrap();

        let srcs = PrimitiveArray::from_vec(vec![1u64, 1u64, 1u64, 2u64, 2u64, 2u64]).boxed();
        let dsts = PrimitiveArray::from_vec(vec![2u64, 3u64, 3u64, 3u64, 4u64, 4u64]).boxed();
        let time = PrimitiveArray::from_vec(vec![0i64, 1i64, 2i64, 3i64, 4i64, 5i64]).boxed();
        let weight =
            PrimitiveArray::from_vec(vec![1.14f64, 2.14f64, 3.14f64, 4.14f64, 5.14f64, 6.14f64])
                .boxed();

        let chunk = LoadChunk::new(vec![srcs, dsts, time, weight], 0, 1, 2, schema());

        let mut graph = TempColGraphFragment::build_tables_from_chunked(
            test_dir.path(),
            100,
            100,
            100,
            GlobalMap::from(1u64..=4u64).into(),
            vec![chunk],
        )
        .unwrap();

        let actual = graph.edges(0.into(), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2))];
        assert_eq!(actual, expected);

        graph.build_inbound_adj_index().unwrap();
        let actual: Vec<_> = graph.edges(2.into(), Direction::IN).collect();
        let expected = vec![(EID(1), VID(0)), (EID(2), VID(1))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph.all_edges().collect::<Vec<_>>();
        let expected = vec![
            (EID(0), VID(0), VID(1)),
            (EID(1), VID(0), VID(2)),
            (EID(2), VID(1), VID(2)),
            (EID(3), VID(1), VID(3)),
        ];
        assert_eq!(actual, expected);
    }

    #[test]
    fn load_muliple_sorted_edges_multiple_ts_2_input_chunks() {
        let test_dir = TempDir::new().unwrap();

        let srcs = PrimitiveArray::from_vec(vec![1u64, 1u64]).boxed();
        let dsts = PrimitiveArray::from_vec(vec![2u64, 3u64]).boxed();
        let time = PrimitiveArray::from_vec(vec![0i64, 1i64]).boxed();
        let weight = PrimitiveArray::from_vec(vec![1.14f64, 2.14f64]).boxed();

        let chunk1 = LoadChunk::new(vec![srcs, dsts, time, weight], 0, 1, 2, schema());

        let srcs = PrimitiveArray::from_vec(vec![1u64, 2u64, 2u64, 2u64]).boxed();
        let dsts = PrimitiveArray::from_vec(vec![3u64, 3u64, 4u64, 4u64]).boxed();
        let time = PrimitiveArray::from_vec(vec![2i64, 3i64, 4i64, 5i64]).boxed();
        let weight = PrimitiveArray::from_vec(vec![3.14f64, 4.14f64, 5.14f64, 6.14f64]).boxed();

        let chunk2 = LoadChunk::new(vec![srcs, dsts, time, weight], 0, 1, 2, schema());

        let graph = TempColGraphFragment::build_tables_from_chunked(
            test_dir.path(),
            100,
            100,
            100,
            GlobalMap::from(1u64..=4u64).into(),
            vec![chunk1, chunk2],
        )
        .unwrap();

        let actual = graph.edges(0.into(), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph.exploded_edges().collect::<Vec<_>>();
        let expected = vec![
            (EID(0), VID(0), VID(1), 0),
            (EID(1), VID(0), VID(2), 1),
            (EID(1), VID(0), VID(2), 2),
            (EID(2), VID(1), VID(2), 3),
            (EID(3), VID(1), VID(3), 4),
            (EID(3), VID(1), VID(3), 5),
        ];
        assert_eq!(actual, expected);
    }

    #[test]
    fn load_muliple_sorted_edges_multiple_ts_chunks_size_1() {
        let test_dir = TempDir::new().unwrap();

        let srcs = PrimitiveArray::from_vec(vec![1u64, 1u64, 1u64, 2u64, 2u64, 2u64]).boxed();
        let dsts = PrimitiveArray::from_vec(vec![2u64, 3u64, 3u64, 3u64, 4u64, 4u64]).boxed();
        let time = PrimitiveArray::from_vec(vec![0i64, 1i64, 2i64, 3i64, 4i64, 5i64]).boxed();
        let weight =
            PrimitiveArray::from_vec(vec![1.14f64, 2.14f64, 3.14f64, 4.14f64, 5.14f64, 6.14f64])
                .boxed();

        let chunk = LoadChunk::new(vec![srcs, dsts, time, weight], 0, 1, 2, schema());

        let graph = TempColGraphFragment::build_tables_from_chunked(
            test_dir.path(),
            1,
            1,
            100,
            GlobalMap::from(1u64..=4u64).into(),
            vec![chunk],
        )
        .unwrap();

        let actual = graph.edges(0.into(), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph.all_edges().collect::<Vec<_>>();
        let expected = vec![
            (EID(0), VID(0), VID(1)),
            (EID(1), VID(0), VID(2)),
            (EID(2), VID(1), VID(2)),
            (EID(3), VID(1), VID(3)),
        ];
        assert_eq!(actual, expected);
    }

    #[test]
    fn load_multiple_edges_across_chunks() {
        let test_dir = TempDir::new().unwrap();

        let srcs = PrimitiveArray::from_vec(vec![
            1u64, 1u64, 1u64, 2u64, 2u64, 2u64, 3u64, 3u64, 3u64, 4u64, 4u64, 4u64,
        ])
        .boxed();

        let dsts = PrimitiveArray::from_vec(vec![
            2u64, 3u64, 4u64, 3u64, 4u64, 5u64, 4u64, 5u64, 6u64, 5u64, 6u64, 7u64,
        ])
        .boxed();

        let time = PrimitiveArray::from_vec(vec![
            0i64, 1i64, 2i64, 3i64, 4i64, 5i64, 6i64, 7i64, 8i64, 9i64, 10i64, 11i64,
        ])
        .boxed();

        let weight = PrimitiveArray::from_vec(vec![
            1.14f64, 2.14f64, 3.14f64, 4.14f64, 5.14f64, 6.14f64, 7.14f64, 8.14f64, 9.14f64,
            10.14f64, 11.14f64, 12.14f64,
        ])
        .boxed();

        let chunk = LoadChunk::new(vec![srcs, dsts, time, weight], 0, 1, 2, schema());

        let graph = TempColGraphFragment::build_tables_from_chunked(
            test_dir.path(),
            2,
            2,
            100,
            GlobalMap::from(1u64..=7u64).into(),
            vec![chunk],
        )
        .unwrap();

        let actual = graph.edges(0.into(), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2)), (EID(2), VID(3))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph.all_edges().collect::<Vec<_>>();
        let expected = vec![
            (EID(0), VID(0), VID(1)),
            (EID(1), VID(0), VID(2)),
            (EID(2), VID(0), VID(3)),
            (EID(3), VID(1), VID(2)),
            (EID(4), VID(1), VID(3)),
            (EID(5), VID(1), VID(4)),
            (EID(6), VID(2), VID(3)),
            (EID(7), VID(2), VID(4)),
            (EID(8), VID(2), VID(5)),
            (EID(9), VID(3), VID(4)),
            (EID(10), VID(3), VID(5)),
            (EID(11), VID(3), VID(6)),
        ];
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_number_of_vertices() {
        let test_dir = TempDir::new().unwrap();

        let srcs = PrimitiveArray::from_vec(vec![
            1u64, 1u64, 1u64, 2u64, 2u64, 2u64, 3u64, 3u64, 3u64, 4u64, 4u64, 4u64,
        ])
        .boxed();

        let dsts = PrimitiveArray::from_vec(vec![
            2u64, 3u64, 4u64, 3u64, 4u64, 5u64, 4u64, 5u64, 6u64, 5u64, 6u64, 7u64,
        ])
        .boxed();

        let time = PrimitiveArray::from_vec(vec![
            0i64, 1i64, 2i64, 3i64, 4i64, 5i64, 6i64, 7i64, 8i64, 9i64, 10i64, 11i64,
        ])
        .boxed();

        let weight = PrimitiveArray::from_vec(vec![
            1.14f64, 2.14f64, 3.14f64, 4.14f64, 5.14f64, 6.14f64, 7.14f64, 8.14f64, 9.14f64,
            10.14f64, 11.14f64, 12.14f64,
        ])
        .boxed();

        let chunk = LoadChunk::new(vec![srcs, dsts, time, weight], 0, 1, 2, schema());

        let graph = TempColGraphFragment::build_tables_from_chunked(
            test_dir.path(),
            2,
            2,
            100,
            GlobalMap::from(1u64..12u64).into(),
            vec![chunk],
        )
        .unwrap();
        assert_eq!(graph.num_vertices(), 11);
    }

    #[test]
    fn test_single_edge_overflow() {
        let test_dir = TempDir::new().unwrap();

        let srcs = PrimitiveArray::from_vec(iter::repeat(0u64).take(10).collect()).boxed();
        let dsts = PrimitiveArray::from_vec(iter::repeat(1u64).take(10).collect()).boxed();
        let times = PrimitiveArray::from_vec((0i64..10).collect()).boxed();
        let weights = PrimitiveArray::from_vec(iter::repeat(1f64).take(10).collect()).boxed();

        let chunk = LoadChunk::new([srcs, dsts, times, weights], 0, 1, 2, schema());
        let graph = TempColGraphFragment::build_tables_from_chunked(
            test_dir.path(),
            2,
            2,
            5,
            GlobalMap::from([0u64, 1u64]).into(),
            vec![chunk],
        )
        .unwrap();

        let all_exploded: Vec<_> = graph.exploded_edges().collect();
        let expected: Vec<_> = (0i64..10).map(|t| (EID(0), VID(0), VID(1), t)).collect();
        assert_eq!(all_exploded, expected);
    }

    #[test]
    fn missing_overflow_on_finalise() {
        let test_dir = TempDir::new().unwrap();

        let srcs = PrimitiveArray::from_vec(vec![0u64, 0, 0, 1]).boxed();
        let dsts = PrimitiveArray::from_vec(vec![1u64, 1, 1, 2]).boxed();
        let times = PrimitiveArray::from_vec(vec![0i64, 1, 2, 3]).boxed();
        let weights = PrimitiveArray::from_vec(vec![0f64, 1., 2., 3.]).boxed();

        let chunk = LoadChunk::new([srcs, dsts, times, weights], 0, 1, 2, schema());
        let mut graph = TempColGraphFragment::build_tables_from_chunked(
            test_dir.path(),
            2,
            2,
            1,
            GlobalMap::from([0u64, 1u64, 2u64]).into(),
            vec![chunk],
        )
        .unwrap();

        let all_exploded: Vec<_> = graph.exploded_edges().collect();
        let expected: Vec<_> = vec![
            (EID(0), VID(0), VID(1), 0),
            (EID(0), VID(0), VID(1), 1),
            (EID(0), VID(0), VID(1), 2),
            (EID(1), VID(1), VID(2), 3),
        ];
        assert_eq!(all_exploded, expected);
        graph.build_inbound_adj_index().unwrap();

        let reloaded_graph = TempColGraphFragment::new(test_dir.path()).unwrap();
        check_graph_sanity(
            &[(0, 1, 0), (0, 1, 1), (0, 1, 2), (1, 2, 3)],
            &[0, 1, 2],
            &reloaded_graph,
        );
    }
}
