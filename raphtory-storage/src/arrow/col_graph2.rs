use std::{
    io,
    io::BufReader,
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::arrow::{
    adj_schema,
    edge_frame_builder::EdgeFrameBuilder,
    mmap::{mmap_batch, mmap_batches, write_batches},
    vertex_frame_builder::VertexFrameBuilder,
    Error, E_COLUMN, V_COLUMN,
};
use arrow2::{
    array::{Array, ListArray, PrimitiveArray, StructArray},
    chunk::Chunk,
    compute::concatenate::concatenate,
    datatypes::{Field, Schema},
    io::{
        ipc::write::{FileWriter, WriteOptions},
        parquet::read,
    },
    offset::OffsetsBuffer,
};
use itertools::Itertools;
use raphtory::core::{
    entities::{EID, VID},
    Direction,
};
use rayon::prelude::*;
use tempfile::tempfile_in;

use super::{array_as_id_iter, LoadChunk, GID};

pub type Time = i64;

#[derive(Debug)]
pub struct TempColGraphFragment {
    chunk_size: usize,
    // sorted_gids: Vec<u64>,
    adj_out_chunks: Vec<Chunk<Box<dyn Array>>>,
    adj_in_chunks: Vec<Chunk<Box<dyn Array>>>,
    edge_chunks: Vec<Chunk<Box<dyn Array>>>,
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
                        file_name.starts_with("edge_chunk_")
                            || file_name.starts_with("adj_out_chunk_")
                            || file_name.starts_with("adj_in_chunk_")
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
                edge_chunks.push(unsafe { mmap_batch(file_path.path(), 0) }?);
            } else if file_name.starts_with("adj_in_chunk_") {
                adj_in_chunks.push(unsafe { mmap_batch(file_path.path(), 0) }?);
            } else if file_name.starts_with("adj_out_chunk_") {
                adj_out_chunks.push(unsafe { mmap_batch(file_path.path(), 0) }?);
            }
        }

        let chunk_size = adj_out_chunks[0][0].len();

        Ok(Self {
            chunk_size,
            adj_out_chunks,
            adj_in_chunks,
            edge_chunks,
            graph_dir: graph_dir.as_ref().into(),
        })
    }

    pub fn from_sorted_parquet_dir_edge_list<P: AsRef<Path> + Clone, P2: AsRef<Path>>(
        parquet_dir: P,
        src_col: &str,
        dst_col: &str,
        time_col: &str,
        chunk_size: usize,
        graph_dir: P2,
    ) -> Result<Self, Error> {
        Self::prepare_graph_dir(graph_dir.as_ref())?;

        let srcs_parquet_files = std::fs::read_dir(parquet_dir.clone())?
            .map(|res| res.unwrap())
            .filter(|e| {
                e.path()
                    .extension()
                    .filter(|ext| ext == &"parquet")
                    .is_some()
            })
            .sorted_by(|f1, f2| f1.path().cmp(&f2.path()));

        let triplets_parquet_files2 = std::fs::read_dir(parquet_dir)?
            .map(|res| res.unwrap())
            .filter(|e| {
                e.path()
                    .extension()
                    .filter(|ext| ext == &"parquet")
                    .is_some()
            })
            .sorted_by(|f1, f2| f1.path().cmp(&f2.path()));

        let srcs = srcs_parquet_files
            .flat_map(|dir_entry| read_file_sources(dir_entry.path(), src_col))
            .flatten();

        let chunks = triplets_parquet_files2
            .flat_map(|dir_entry| {
                read_file_chunks(dir_entry.path(), src_col, dst_col, time_col, None)
            })
            .flatten();

        let out = Self::build_tables_from_chunked(graph_dir, chunk_size, srcs, chunks)?;
        Ok(out)
    }

    pub fn from_sorted_parquet_edge_list<P: AsRef<Path> + Clone, P2: AsRef<Path>>(
        parquet_file: P,
        src_col: &str,
        dst_col: &str,
        time_col: &str,
        chunk_size: usize,
        graph_dir: P2,
    ) -> Result<Self, Error> {
        Self::prepare_graph_dir(graph_dir.as_ref())?;
        let srcs_iter = read_file_sources(parquet_file.clone(), src_col)?;

        let chunks = read_file_chunks(parquet_file, src_col, dst_col, time_col, None)?;

        let out = Self::build_tables_from_chunked(graph_dir, chunk_size, srcs_iter, chunks)?;
        Ok(out)
    }

    fn prepare_graph_dir<P: AsRef<Path>>(graph_dir: P) -> Result<(), Error> {
        // create graph dir if it does not exist
        // if it exists make sure it's empty
        std::fs::create_dir_all(&graph_dir)?;

        let mut dir_iter = std::fs::read_dir(&graph_dir)?;
        if dir_iter.next().is_some() {
            return Err(Error::GraphDirNotEmpty);
        }

        return Ok(());
    }

    fn build_tables_from_chunked<P: AsRef<Path>, ID: Into<GID> + PartialEq>(
        base_dir: P,
        chunk_size: usize,
        source_iter: impl IntoIterator<Item = ID>,
        chunks_iter: impl IntoIterator<Item = LoadChunk>,
    ) -> Result<Self, Error> {
        let mut vf_builder = VertexFrameBuilder::new(chunk_size, &base_dir);
        let mut edge_builder = EdgeFrameBuilder::new(chunk_size, &base_dir);

        // initialise vertex global id table to preserve order
        vf_builder.load_sources(source_iter);

        let mut last_chunk: Option<LoadChunk> = None;
        // g_id, [{v_id1, e_id1}, {v_id2, e_id2}, ...]
        for mut chunk in chunks_iter.into_iter() {
            // when new chunk comes in, we need to finalise the previous chunk aka, copy the column?
            if let Some(last_chunk) = last_chunk {
                edge_builder.extend_time_slice(&last_chunk.timestamp_arr()?);
                if let Some(t_prop_cols) = last_chunk.t_prop_cols() {
                    edge_builder.extend_tprops_slice(t_prop_cols);
                }
            }

            // get the source and destintion columns
            let src_iter = chunk.sources()?;
            let dst_iter = chunk.destinations()?;

            for (src, dst) in src_iter.zip(dst_iter) {
                let (src_id, dst_id) = vf_builder.push_update(src.into(), dst.into())?;

                edge_builder.push_update_with_props(src_id, dst_id, &mut chunk)?;
            }

            last_chunk = Some(chunk);
        }

        vf_builder.finalise_empty_chunks()?;

        // finalize edge_builder
        if let Some(mut chunk) = last_chunk {
            edge_builder.push_chunk_v2(&mut chunk)?;
        }

        Ok(TempColGraphFragment {
            chunk_size,
            // sorted_gids: vf_builder.sorted_gids,
            adj_out_chunks: vf_builder.adj_out_chunks,
            adj_in_chunks: Vec::default(),
            edge_chunks: edge_builder.edge_chunks,
            graph_dir: base_dir.as_ref().into(),
        })
    }

    pub fn num_vertices(&self) -> usize {
        match self.adj_out_chunks.last() {
            Some(v) => (self.adj_out_chunks.len() - 1) * self.chunk_size + v.len(), // all but the last chunk are always full
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
                if adj_array.is_none() {
                    return Box::new(std::iter::empty());
                }
                let adj_array = adj_array.unwrap();
                let v = adj_array.values()[0]
                    .as_any()
                    .downcast_ref::<PrimitiveArray<u64>>()
                    .unwrap()
                    .clone();
                let e = adj_array.values()[1]
                    .as_any()
                    .downcast_ref::<PrimitiveArray<u64>>()
                    .unwrap()
                    .clone();
                let iter = v
                    .into_iter()
                    .flatten()
                    .zip(e.into_iter().flatten())
                    .map(|(vid, eid)| (EID(eid as usize), VID(vid as usize)));

                Box::new(iter)
            }
            Direction::BOTH => {
                let out = self.edges(vertex_id, Direction::OUT);
                let inb = self.edges(vertex_id, Direction::IN);
                Box::new(out.merge_by(inb, |(v1, _), (v2, _)| v1 < v2))
            }
        }
    }

    pub fn all_edges(&self) -> impl Iterator<Item = (EID, VID, VID)> + '_ {
        self.edge_chunks
            .iter()
            .flat_map(|chunk| {
                let src = chunk[0]
                    .as_any()
                    .downcast_ref::<PrimitiveArray<u64>>()
                    .unwrap()
                    .clone();
                let dst = chunk[1]
                    .as_any()
                    .downcast_ref::<PrimitiveArray<u64>>()
                    .unwrap()
                    .clone();
                src.into_iter().flatten().zip(dst.into_iter().flatten())
            })
            .enumerate()
            .map(|(eid, (src, dst))| (EID(eid), VID(src as usize), VID(dst as usize)))
    }

    pub fn exploded_edges(&self) -> impl Iterator<Item = (EID, VID, VID, Time)> + '_ {
        self.edge_chunks
            .iter()
            .flat_map(|chunk| {
                let src = chunk[0]
                    .as_any()
                    .downcast_ref::<PrimitiveArray<u64>>()
                    .unwrap()
                    .clone();
                let dst = chunk[1]
                    .as_any()
                    .downcast_ref::<PrimitiveArray<u64>>()
                    .unwrap()
                    .clone();
                let time = chunk[2]
                    .as_any()
                    .downcast_ref::<ListArray<i64>>()
                    .unwrap()
                    .clone();

                // TODO: make this a function
                let time_into_iter =
                    (0..time.len()).map(move |i| unsafe { time.value_unchecked(i) });
                src.into_iter()
                    .flatten()
                    .zip(dst.into_iter().flatten())
                    .zip(time_into_iter)
            })
            .enumerate()
            .flat_map(|(eid, ((src, dst), time))| {
                time.as_any()
                    .downcast_ref::<PrimitiveArray<i64>>()
                    .unwrap()
                    .clone()
                    .into_iter()
                    .flatten()
                    .map(move |time| (EID(eid), VID(src as usize), VID(dst as usize), time))
            })
    }

    fn adj_list(&self, vertex_id: usize, dir: Direction) -> Option<StructArray> {
        let chunks = match dir {
            Direction::OUT => self.outbound(),
            Direction::IN => self.inbound(),
            Direction::BOTH => return None,
        };

        let chunk_size = self.chunk_size; // we assume all the chunks are the same size

        let chunk_idx = vertex_id / chunk_size;
        let idx = vertex_id % chunk_size;

        let arr = chunks.get(chunk_idx)?[0]
            .as_any()
            .downcast_ref::<ListArray<i64>>()?;
        let arr = (idx < arr.len()).then(|| arr.value(idx))?;
        let adj_list = arr.as_any().downcast_ref::<StructArray>()?;
        Some(adj_list.clone())
    }

    fn outbound(&self) -> &Vec<Chunk<Box<dyn Array>>> {
        &self.adj_out_chunks
    }

    fn inbound(&self) -> &Vec<Chunk<Box<dyn Array>>> {
        &self.adj_in_chunks
    }

    fn build_inbound_adj_index(&mut self) -> Result<(), Error> {
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

        for outbound in self.outbound() {
            // look at all outbound chunks sequentially
            let outbound = outbound[0]
                .as_any()
                .downcast_ref::<ListArray<i64>>()
                .unwrap();
            let outbound_chunksize = outbound.len();
            let mut progress = vec![0usize; outbound_chunksize]; // keeps track of how far we got for each list
            for inbound_chunk_id in 0..num_chunks {
                // build partial inbound chunk sequentially
                let chunk_max_id = self.chunk_size * (inbound_chunk_id + 1); // id in this chunk less than this
                let inbound_chunksize = self.outbound()[inbound_chunk_id].len();
                // let offsets = vec![0i64; inbound_chunksize + 1];
                let mut counts = Vec::with_capacity(inbound_chunksize);
                counts.resize_with(inbound_chunksize, || AtomicUsize::new(0));
                let new_progress: Vec<_> = progress
                    .par_iter()
                    .enumerate()
                    .map(|(row, &start)| {
                        let (vertex_ids, _) = row_from_start(outbound.value(row), start, None);
                        let mut row_size = 0usize;
                        for &id in vertex_ids.iter().flatten() {
                            let id = id as usize;
                            if id < chunk_max_id {
                                counts[id % self.chunk_size].fetch_add(1, Ordering::Relaxed);
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
                    let (row_vertex_ids, row_edge_ids) =
                        row_from_start(outbound.value(row), start, Some(new_progress[row]));
                    // in principle we can do all these updates in parallel as they end up pointing at unique rows of the vector
                    // this would require some careful unsafe code though
                    for (&vid, &eid) in row_vertex_ids
                        .iter()
                        .flatten()
                        .zip(row_edge_ids.iter().flatten())
                    {
                        let vid = vid as usize % self.chunk_size; //local index
                        let insertion_point = offsets[vid] as usize + extra_offsets[vid];
                        extra_offsets[vid] += 1;
                        inbound_vids[insertion_point] = row as u64;
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
            let chunks: Vec<_> = unsafe { mmap_batches(&f, 0..num_chunks)? }
                .into_iter()
                .flat_map(|chunk| chunk.into_arrays())
                .collect();
            let refs: Vec<_> = chunks.iter().map(|v| v.as_ref()).collect();
            let res = concatenate(&refs)?;
            let dtype = res.data_type().clone();
            let schema = Schema::from(vec![Field::new("adj_in", dtype, false)]);
            let file_path = self
                .graph_dir
                .join(format!("adj_in_chunk_{:08}.ipc", self.adj_in_chunks.len()));
            let chunk = [Chunk::try_new(vec![res])?];
            write_batches(file_path.as_path(), schema, &chunk)?;
            let mmapped_chunk = unsafe { mmap_batch(file_path.as_path(), 0)? };
            self.adj_in_chunks.push(mmapped_chunk);
        }
        Ok(())
    }
}

#[inline]
fn row_from_start(
    row: Box<dyn Array>,
    start: usize,
    end: Option<usize>,
) -> (PrimitiveArray<u64>, PrimitiveArray<u64>) {
    let end = end.unwrap_or_else(|| row.len() - start);
    let row = row.sliced(start, end);
    let row = row.as_any().downcast_ref::<StructArray>().unwrap();
    let vertex_ids = row.values()[0]
        .as_any()
        .downcast_ref::<PrimitiveArray<u64>>()
        .unwrap()
        .clone();
    let edge_ids = row.values()[1]
        .as_any()
        .downcast_ref::<PrimitiveArray<u64>>()
        .unwrap()
        .clone();
    (vertex_ids, edge_ids)
}

fn read_file_sources<P: AsRef<Path>>(
    parquet_file: P,
    src_col: &str,
) -> Result<impl Iterator<Item = GID>, Error> {
    let file = std::fs::File::open(&parquet_file)?;
    let mut reader = BufReader::new(file);
    let metadata = read::read_metadata(&mut reader)?;
    let schema = read::infer_schema(&metadata)?;

    let schema = schema.filter(|_, field| field.name == src_col);

    let reader = read::FileReader::new(reader, metadata.row_groups, schema, None, None, None);
    Ok(reader
        .flatten()
        .flat_map(|chunk| array_as_id_iter(&chunk[0]).unwrap()))
}

fn read_file_chunks<P: AsRef<Path>>(
    parquet_file: P,
    src_col: &str,
    dst_col: &str,
    time_col: &str,
    projection: Option<Vec<&str>>,
) -> Result<impl Iterator<Item = LoadChunk>, Error> {
    let file = std::fs::File::open(&parquet_file)?;
    let mut reader = BufReader::new(file);
    let metadata = read::read_metadata(&mut reader)?;
    let schema = read::infer_schema(&metadata)?;

    let schema = schema.filter(|_, field| {
        field.name == src_col
            || field.name == dst_col
            || field.name == time_col
            || projection.is_none()
            || projection
                .as_ref()
                .filter(|proj| proj.contains(&field.name.as_str()))
                .is_some()
    });

    let src_col_idx = schema
        .fields
        .iter()
        .enumerate()
        .find(|(_, f)| f.name == src_col)
        .map(|(i, _)| i)
        .ok_or_else(|| Error::ColumnNotFound(src_col.to_string()))?;

    let dst_col_idx = schema
        .fields
        .iter()
        .enumerate()
        .find(|(_, f)| f.name == dst_col)
        .map(|(i, _)| i)
        .ok_or_else(|| Error::ColumnNotFound(src_col.to_string()))?;

    let time_col_idx = schema
        .fields
        .iter()
        .enumerate()
        .find(|(_, f)| f.name == time_col)
        .map(|(i, _)| i)
        .ok_or_else(|| Error::ColumnNotFound(src_col.to_string()))?;

    let reader = read::FileReader::new(
        reader,
        metadata.row_groups,
        schema.clone(),
        None,
        None,
        None,
    );
    Ok(reader.flatten().map(move |chunk| {
        LoadChunk::from_chunk(
            chunk,
            src_col_idx,
            dst_col_idx,
            time_col_idx,
            schema.clone(),
        )
    }))
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow2::datatypes::DataType;
    use tempfile::TempDir;

    #[test]
    fn load_from_parquet() {
        let file = "part-00000-b406cce6-7ed0-4efb-883d-e6766f36d8cf-c000.snappy.parquet";

        let test_dir = TempDir::new().unwrap();

        let g = TempColGraphFragment::from_sorted_parquet_edge_list(
            file,
            "source",
            "destination",
            "time",
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
            vec![1u64],
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
            vec![1u64, 1u64, 1u64],
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
            100,
            vec![
                1u64, 1u64, 1u64, 2u64, 2u64, 2u64, 3u64, 3u64, 3u64, 4u64, 4u64, 4u64,
            ],
            vec![chunk],
        )
        .unwrap();

        println!("{:?}", graph);

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
            vec![1u64, 1u64, 1u64, 2u64, 2u64, 2u64],
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
            vec![1u64, 1u64, 1u64, 2u64, 2u64, 2u64],
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
            vec![1u64, 1u64, 1u64, 2u64, 2u64, 2u64],
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
            vec![
                1u64, 1u64, 1u64, 2u64, 2u64, 2u64, 3u64, 3u64, 3u64, 4u64, 4u64, 4u64,
            ],
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
            1u64..12u64,
            vec![chunk],
        )
        .unwrap();
        assert_eq!(graph.num_vertices(), 11);
    }
}
