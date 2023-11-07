use std::{
    io,
    io::BufReader,
    iter,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{
    arrow::{
        adj_schema,
        edge::{Edge, ExplodedEdge},
        edge_frame_builder::EdgeFrameBuilder,
        list_buffer::{as_primitive_column, ListColumn},
        loader::{read_file_chunks, sort_dedup_2, sort_within_chunks},
        mmap::{mmap_batch, mmap_batches, write_batches},
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
    types::{NativeType, Offset},
};
use arrow_array::RecordBatch;
use itertools::Itertools;
use rayon::prelude::*;
use tempfile::tempfile_in;

use super::{
    array_as_id_iter,
    edges::Edges,
    global_order::{GlobalMap, GlobalOrder},
    ipc,
    vertex_chunk::{RowOwned, VertexChunk},
    LoadChunk, Time, GID,
};

#[derive(Debug)]
pub struct TempColGraphFragment {
    vertex_chunk_size: usize,
    adj_out_chunks: Vec<VertexChunk>,
    adj_in_chunks: Vec<VertexChunk>,
    edges: Edges,
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
        for file_path in iter {
            let file_name = file_path.file_name();
            let file_name = file_name
                .to_str()
                .expect("file names are already filtered and thus valid");
            if file_name.starts_with("adj_in_chunk_") {
                adj_in_chunks.push(VertexChunk::new(
                    unsafe {
                        // TODO: this should perhap be configurable
                        // mmap_batch(file_path.path(), 0)
                        ipc::read_batch(file_path.path())
                    }?,
                    ipc::read_schema(file_path.path())?,
                ));
            } else if file_name.starts_with("adj_out_chunk_") {
                adj_out_chunks.push(VertexChunk::new(
                    unsafe {
                        // mmap_batch(file_path.path(), 0)
                        ipc::read_batch(file_path.path())
                    }?,
                    ipc::read_schema(file_path.path())?,
                ));
            }
        }

        let vertex_chunk_size = adj_out_chunks.first().map(|v| v.len()).unwrap_or(0);

        let edges = Edges::from_path(graph_dir.as_ref(), false)?;

        Ok(Self {
            vertex_chunk_size,
            adj_out_chunks,
            adj_in_chunks,
            edges,
            graph_dir: graph_dir.as_ref().into(),
        })
    }

    pub fn to_arrow(&self) -> impl Iterator<Item = RecordBatch> + '_ {
        self.adj_out_chunks
            .iter()
            .map(|chunk| (chunk.to_arrow(), chunk.to_arrow_fields()))
            .zip(
                self.adj_out_chunks
                    .iter()
                    .map(|chunk| (chunk.to_arrow(), chunk.to_arrow_fields())),
            )
            .map(|((out, out_fields), (into, into_fields))| {
                let mut batch = out.to_vec();
                batch.extend(into.to_vec());
                let schema = arrow_schema::Schema::new(
                    out_fields
                        .into_iter()
                        .map(Arc::new)
                        .chain(into_fields.into_iter().map(Arc::new))
                        .collect::<Vec<_>>(),
                );

                RecordBatch::try_new(Arc::new(schema), batch).expect("schema mismatch")
            })
    }

    pub fn edge_property_id(&self, name: &str) -> Option<usize> {
        self.edges.property_id(name)
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

    pub(crate) fn build_tables_from_chunked<P: AsRef<Path>, GO: GlobalOrder>(
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
            edge_chunks: edge_builder.src_chunks,
            graph_dir: base_dir.as_ref().into(),
            edges: (),
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

    pub fn edges_par_iter(
        &self,
        vertex_id: VID,
        dir: Direction,
    ) -> Option<impl ParallelIterator<Item = (EID, VID)> + '_> {
        let (v_slice, edge_slice) = match dir {
            Direction::OUT => {
                let out_slice = self.out_slice(vertex_id)?;
                let edge_out_slice = self.out_edges(vertex_id)?;
                (out_slice, edge_out_slice)
            }
            Direction::IN => {
                let in_slice = self.in_slice(vertex_id)?;
                let edge_in_slice = self.in_edges(vertex_id)?;
                (in_slice, edge_in_slice)
            }
            Direction::BOTH => panic!("No parallel iterators for both directions"),
        };
        Some(
            edge_slice
                .par_iter()
                .map(|e| EID(*e as usize))
                .zip(v_slice.par_iter().map(|v| VID(*v as usize))),
        )
    }

    pub fn edges_iter(
        &self,
        vertex_id: VID,
        dir: Direction,
    ) -> Option<impl Iterator<Item = (EID, VID)> + Send + '_> {
        let (v_slice, edge_slice) = match dir {
            Direction::OUT => {
                let out_slice = self.out_slice(vertex_id)?;
                let edge_out_slice = self.out_edges(vertex_id)?;
                (out_slice, edge_out_slice)
            }
            Direction::IN => {
                let in_slice = self.in_slice(vertex_id)?;
                let edge_in_slice = self.in_edges(vertex_id)?;
                (in_slice, edge_in_slice)
            }
            Direction::BOTH => panic!("No parallel iterators for both directions"),
        };
        Some(
            edge_slice
                .iter()
                .map(|e| EID(*e as usize))
                .zip(v_slice.iter().map(|v| VID(*v as usize))),
        )
    }

    pub fn edge(&self, e_id: EID) -> Edge<'_> {
        self.edges.edge(e_id)
    }

    pub fn all_edges(&self) -> impl Iterator<Item = Edge> + '_ {
        self.edges.iter()
    }

    pub fn exploded_edges(&self) -> impl Iterator<Item = ExplodedEdge> + '_ {
        self.edges.iter().flat_map(|e| e.explode())
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

        let neighbours = chunks[chunk_idx].neighbours_own(idx)?;
        let edges = chunks[chunk_idx].edges_own(idx)?;

        Some((neighbours, edges))
    }

    pub(crate) fn out_slice(&self, vertex_id: VID) -> Option<&[u64]> {
        self.v_slice(vertex_id, Direction::OUT)
    }

    pub(crate) fn in_slice(&self, vertex_id: VID) -> Option<&[u64]> {
        self.v_slice(vertex_id, Direction::IN)
    }

    fn v_slice(&self, vertex_id: VID, dir: Direction) -> Option<&[u64]> {
        let vertex_id: usize = vertex_id.into();
        let chunk_size = self.vertex_chunk_size; // we assume all the chunks are the same size

        let chunk_idx = vertex_id / chunk_size;
        let idx = vertex_id % chunk_size;

        let chunks = match dir {
            Direction::OUT => self.outbound(),
            Direction::IN => self.inbound(),
            Direction::BOTH => panic!("no slice for both directions"),
        };
        let neighbours = chunks[chunk_idx].neighbours_col()?;
        Some(neighbours.into_value(idx))
    }

    pub(crate) fn out_edges(&self, vertex_id: VID) -> Option<&[u64]> {
        self.e_slice(vertex_id, Direction::OUT)
    }

    pub(crate) fn in_edges(&self, vertex_id: VID) -> Option<&[u64]> {
        self.e_slice(vertex_id, Direction::IN)
    }

    fn e_slice(&self, vertex_id: VID, dir: Direction) -> Option<&[u64]> {
        let vertex_id: usize = vertex_id.into();
        let chunk_size = self.vertex_chunk_size; // we assume all the chunks are the same size

        let chunk_idx = vertex_id / chunk_size;
        let idx = vertex_id % chunk_size;

        let chunks = match dir {
            Direction::OUT => self.outbound(),
            Direction::IN => self.inbound(),
            Direction::BOTH => panic!("no slice for both directions"),
        };
        let neighbours = chunks[chunk_idx].edge_col()?;
        Some(neighbours.into_value(idx))
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
            write_batches(file_path.as_path(), schema.clone(), &chunk)?;
            let mmapped_chunk = unsafe { mmap_batch(file_path.as_path(), 0)? };
            self.adj_in_chunks
                .push(VertexChunk::new(mmapped_chunk, schema));
        }
        Ok(())
    }

    pub(crate) fn add_chunks<GO: GlobalOrder>(
        &self,
        vertex_chunk_size: usize,
        edge_chunk_size: usize,
        edge_max_list_size: usize,
        clone: Arc<super::global_order::LMDBOrdering>,
        load_chunks: impl IntoIterator<Item = LoadChunk>,
    ) -> Result<(), Error> {
        let mut last_chunk: Option<LoadChunk> = None;
        // let mut i =

        for chunk in load_chunks {
            // get the source and destintion columns
            let src_iter = chunk.sources()?;
            let dst_iter = chunk.destinations()?;

            // for (src, dst) in src_iter.zip(dst_iter) {
            //     // resolve the physical id of src
            //     let src_id =
            // }

            last_chunk = Some(chunk);
        }
        Ok(())
    }
}

pub(crate) fn load_chunks<GO: GlobalOrder>(
    vf_builder: &mut VertexFrameBuilder<GO>,
    edge_builder: &mut EdgeFrameBuilder,
    chunks_iter: impl IntoIterator<Item = LoadChunk>,
) -> Result<(), Error> {
    let mut last_chunk: Option<LoadChunk> = None;
    // g_id, [{v_id1, e_id1}, {v_id2, e_id2}, ...]
    for mut chunk in chunks_iter.into_iter() {
        // get the source and destintion columns
        let src_iter = chunk.sources()?;
        let dst_iter = chunk.destinations()?;

        for (src, dst) in src_iter.zip(dst_iter) {
            let (src_id, dst_id) = vf_builder.push_update(src, dst)?;

            edge_builder.push_update(src_id, dst_id)?;
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
    use crate::{arrow::global_order::GlobalMap, prelude::*};
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
            .all(|e| e.src().0 < g_num_verts && e.dst().0 < g_num_verts));

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
            .map(|e| (vertices[e.src().0], vertices[e.dst().0], e.timestamp()))
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

        let unique_edges = edges.iter().map(|(src, dst, _)| (*src, *dst)).dedup();

        for (e_id, (src, dst)) in unique_edges.enumerate() {
            let edge = graph.edge(EID(e_id));
            let VID(src_id) = edge.src();
            let VID(dst_id) = edge.dst();

            assert_eq!(vertices[src_id], src);
            assert_eq!(vertices[dst_id], dst);
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
            (0, 6, -30),
            (4, 7, -83),
            (4, 7, -77),
            (6, 8, -68),
            (6, 8, -65),
            (9, 10, 46),
            (9, 10, 46),
            (9, 10, 51),
            (9, 10, 54),
            (9, 10, 59),
            (9, 10, 59),
            (9, 10, 59),
            (9, 10, 65),
            (9, 11, -75),
        ];
        let input_chunk_size = 411;
        let vertex_chunk_size = 10;
        let edge_chunk_size = 5;
        let edge_max_list_size = 7;

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

        let actual = graph.edges(VID(0), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph
            .all_edges()
            .map(|e| (e.src(), e.dst()))
            .collect::<Vec<_>>();
        let expected = vec![(VID(0), VID(1))];
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

        let actual = graph.edges(VID(0), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph
            .all_edges()
            .map(|e| (e.src(), e.dst()))
            .collect::<Vec<_>>();
        let expected = vec![(VID(0), VID(1))];
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

        let actual = graph.edges(VID(0), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2)), (EID(2), VID(3))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph
            .all_edges()
            .map(|e| (e.src(), e.dst()))
            .collect::<Vec<_>>();
        let expected = vec![
            (VID(0), VID(1)),
            (VID(0), VID(2)),
            (VID(0), VID(3)),
            (VID(1), VID(2)),
            (VID(1), VID(3)),
            (VID(1), VID(4)),
            (VID(2), VID(3)),
            (VID(2), VID(4)),
            (VID(2), VID(5)),
            (VID(3), VID(4)),
            (VID(3), VID(5)),
            (VID(3), VID(6)),
        ];
        assert_eq!(actual, expected);

        let e0 = graph.edge(0.into());
        assert_eq!(e0.timestamps().into_iter().next().unwrap(), Some(0i64));

        let e5 = graph.edge(5.into());
        assert_eq!(e5.timestamps().into_iter().next().unwrap(), Some(5i64));
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

        let actual = graph.edges(VID(0), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2))];
        assert_eq!(actual, expected);

        graph.build_inbound_adj_index().unwrap();
        let actual: Vec<_> = graph.edges(VID(2), Direction::IN).collect();
        let expected = vec![(EID(1), VID(0)), (EID(2), VID(1))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph
            .all_edges()
            .map(|e| (e.src(), e.dst()))
            .collect::<Vec<_>>();
        let expected = vec![
            (VID(0), VID(1)),
            (VID(0), VID(2)),
            (VID(1), VID(2)),
            (VID(1), VID(3)),
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

        let actual = graph.edges(VID(0), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph
            .exploded_edges()
            .map(|e| (e.src(), e.dst(), e.timestamp()))
            .collect::<Vec<_>>();
        let expected = vec![
            (VID(0), VID(1), 0),
            (VID(0), VID(2), 1),
            (VID(0), VID(2), 2),
            (VID(1), VID(2), 3),
            (VID(1), VID(3), 4),
            (VID(1), VID(3), 5),
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

        let actual = graph.edges(VID(0), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph
            .all_edges()
            .map(|e| (e.src(), e.dst()))
            .collect::<Vec<_>>();
        let expected = vec![
            (VID(0), VID(1)),
            (VID(0), VID(2)),
            (VID(1), VID(2)),
            (VID(1), VID(3)),
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

        let actual = graph.edges(VID(0), Direction::OUT).collect::<Vec<_>>();
        let expected = vec![(EID(0), VID(1)), (EID(1), VID(2)), (EID(2), VID(3))];
        assert_eq!(actual, expected);

        // check edges
        let actual = graph
            .all_edges()
            .map(|e| (e.src(), e.dst()))
            .collect::<Vec<_>>();
        let expected = vec![
            (VID(0), VID(1)),
            (VID(0), VID(2)),
            (VID(0), VID(3)),
            (VID(1), VID(2)),
            (VID(1), VID(3)),
            (VID(1), VID(4)),
            (VID(2), VID(3)),
            (VID(2), VID(4)),
            (VID(2), VID(5)),
            (VID(3), VID(4)),
            (VID(3), VID(5)),
            (VID(3), VID(6)),
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

        let all_exploded: Vec<_> = graph
            .exploded_edges()
            .map(|e| (e.src(), e.dst(), e.timestamp()))
            .collect();
        let expected: Vec<_> = (0i64..10).map(|t| (VID(0), VID(1), t)).collect();
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

        let all_exploded: Vec<_> = graph
            .exploded_edges()
            .map(|e| (e.src(), e.dst(), e.timestamp()))
            .collect();
        let expected: Vec<_> = vec![
            (VID(0), VID(1), 0),
            (VID(0), VID(1), 1),
            (VID(0), VID(1), 2),
            (VID(1), VID(2), 3),
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
