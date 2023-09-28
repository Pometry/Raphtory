use std::{io::BufReader, path::Path};

use crate::arrow::{
    edge_frame_builder::EdgeFrameBuilder, mmap::mmap_batches,
    vertex_frame_builder::VertexFrameBuilder, Error,
};
use arrow2::{
    array::{Array, ListArray, PrimitiveArray, StructArray, Utf8Array},
    chunk::Chunk,
    datatypes::DataType,
    error::Result as ArrowResult,
    io::parquet::read,
};
use itertools::Itertools;
use raphtory::core::{
    entities::{vertices::input_vertex::InputVertex, EID, VID},
    Direction,
};

pub type Time = i64;

#[derive(Debug)]
pub struct TempColGraphFragment {
    chunk_size: usize,
    // sorted_gids: Vec<u64>,
    adj_out_chunks: Vec<Chunk<Box<dyn Array>>>,
    edge_chunks: Vec<Chunk<Box<dyn Array>>>,
}

fn array_as_id_iter(array: &Box<dyn Array>) -> Result<Box<dyn Iterator<Item = u64>>, Error> {
    match array.data_type() {
        DataType::UInt64 => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<u64>>()
                .unwrap()
                .clone();
            Ok(Box::new(array.into_iter().flatten()))
        }
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .unwrap()
                .clone();
            Ok(Box::new(
                (0..array.len()).map(move |i| unsafe { array.value_unchecked(i) }.id()),
            ))
        }
        DataType::LargeUtf8 => {
            let array = array
                .as_any()
                .downcast_ref::<Utf8Array<i64>>()
                .unwrap()
                .clone();
            Ok(Box::new(
                (0..array.len()).map(move |i| unsafe { array.value_unchecked(i) }.id()),
            ))
        }
        v => Err(Error::DType(v.clone())),
    }
}

impl TempColGraphFragment {
    pub fn new<P: AsRef<Path>>(graph_dir: P) -> Result<Self, Error> {
        // iterate graph dir and split into two vectors of files edge_chunk_{j}.ipc and adj_out_chunk_{i}.ipc

        let iter = std::fs::read_dir(graph_dir)?
            .into_iter()
            .flatten()
            .filter(|dir_entry| {
                let file_name = dir_entry.file_name();
                let file_name = file_name.to_str().unwrap();
                file_name.starts_with("edge_chunk_") || file_name.starts_with("adj_out_chunk_")
            })
            .sorted_by(|f1, f2| f1.path().cmp(&f2.path()));

        let (edges, vertices): (Vec<_>, Vec<_>) = iter.partition(|dir_entry| {
            let file_name = dir_entry.file_name();
            let file_name = file_name.to_str().unwrap();
            file_name.starts_with("edge_chunk_")
        });

        let edge_chunks = edges
            .into_iter()
            .flat_map(|file_path| unsafe { mmap_batches(file_path.path(), 0) })
            .collect_vec();

        let vertices_chunks = vertices
            .into_iter()
            .flat_map(|file_path| unsafe { mmap_batches(file_path.path(), 0) })
            .collect_vec();


        let chunk_size = &vertices_chunks[0][0].len();

        Ok(Self {
            chunk_size: *chunk_size,
            adj_out_chunks: vertices_chunks,
            edge_chunks,
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
        let srcs_parquet_files = std::fs::read_dir(parquet_dir.clone())?
            .map(|res| res.unwrap())
            .filter(|e| {
                e.path()
                    .extension()
                    .filter(|ext| ext == &"parquet")
                    .is_some()
            })
            .sorted_by(|f1, f2| f1.path().cmp(&f2.path()))
            .map(|file| {
                println!("reading file: {:?}", file.path());
                file
            });

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
            .flat_map(|dir_entry| Self::read_file_sources(dir_entry.path(), src_col))
            .flatten();

        let triplets = triplets_parquet_files2
            .flat_map(|dir_entry| {
                println!("reading file: {:?}", dir_entry.path());
                Self::read_file_triplets(dir_entry.path(), src_col, dst_col, time_col)
            })
            .flatten();

        let out = Self::build_tables(graph_dir, chunk_size, srcs, triplets)?;
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
        let srcs_iter = Self::read_file_sources(parquet_file.clone(), src_col)?;

        let triplets = Self::read_file_triplets(parquet_file, src_col, dst_col, time_col)?;

        let out = Self::build_tables(graph_dir, chunk_size, srcs_iter, triplets)?;
        Ok(out)
    }

    fn build_tables<P: AsRef<Path>>(
        base_dir: P,
        chunk_size: usize,
        source_iter: impl IntoIterator<Item = u64>,
        tuples_iter: impl IntoIterator<Item = (u64, u64, i64)>,
    ) -> ArrowResult<Self> {
        let mut vf_builder = VertexFrameBuilder::new(chunk_size, &base_dir);
        let mut edge_builder = EdgeFrameBuilder::new(chunk_size, &base_dir);

        // initialise vertex global id table to preserve order
        vf_builder.load_sources(source_iter);

        // g_id, [{v_id1, e_id1}, {v_id2, e_id2}, ...]
        for (src, dst, time) in tuples_iter {
            let (src_id, dst_id) = vf_builder.push_update(src, dst)?;
            edge_builder.push_update(time, src_id, dst_id)?;
        }
        vf_builder.finalise_empty_chunks()?;
        edge_builder.finalise()?;
        Ok(TempColGraphFragment {
            chunk_size,
            // sorted_gids: vf_builder.sorted_gids,
            adj_out_chunks: vf_builder.adj_out_chunks,
            edge_chunks: edge_builder.edge_chunks,
        })
    }

    fn read_file_sources<P: AsRef<Path>>(
        parquet_file: P,
        src_col: &str,
    ) -> Result<impl Iterator<Item = u64>, Error> {
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

    fn read_file_triplets<P: AsRef<Path>>(
        parquet_file: P,
        src_col: &str,
        dst_col: &str,
        time_col: &str,
    ) -> Result<impl Iterator<Item = (u64, u64, i64)>, Error> {
        let file = std::fs::File::open(&parquet_file)?;
        let mut reader = BufReader::new(file);
        let metadata = read::read_metadata(&mut reader)?;
        let schema = read::infer_schema(&metadata)?;

        let schema = schema.filter(|_, field| {
            field.name == src_col || field.name == dst_col || field.name == time_col
        });

        let src_col_idx = schema
            .fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name == src_col)
            .map(|(i, _)| i)
            .unwrap();
        let dst_col_idx = schema
            .fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name == dst_col)
            .map(|(i, _)| i)
            .unwrap();
        let time_col_idx = schema
            .fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name == time_col)
            .map(|(i, _)| i)
            .unwrap();

        let reader = read::FileReader::new(reader, metadata.row_groups, schema, None, None, None);
        let out = reader.flatten().flat_map(move |chunk| {
            let srcs = array_as_id_iter(&chunk[src_col_idx]).unwrap();
            let dsts = array_as_id_iter(&chunk[dst_col_idx]).unwrap();

            let arr = &chunk[time_col_idx];
            let times = arr
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap()
                .clone();
            srcs.zip(dsts)
                .zip(times.into_iter().flatten())
                .map(|((src, dst), time)| (src, dst, time))
        });
        Ok(out)
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
        todo!("inbound not done yet")
    }
}

#[cfg(test)]
mod test {
    use super::*;
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

    #[test]
    fn load_one_edge_from_sorted_adj_list_num_vertices_no_props() {
        let test_dir = TempDir::new().unwrap();

        let graph = TempColGraphFragment::build_tables(
            test_dir.path(),
            100,
            vec![1u64],
            vec![(1u64, 2u64, 9i64)],
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
    fn load_one_edge_from_sorted_adj_list_num_vertices_no_props_multiple_timestamps() {
        let test_dir = TempDir::new().unwrap();

        let graph = TempColGraphFragment::build_tables(
            test_dir.path(),
            100,
            vec![1u64, 1u64, 1u64],
            vec![(1u64, 2u64, 0i64), (1u64, 2u64, 3i64), (1u64, 2u64, 7i64)],
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
    fn load_muliple_sorted_edges_no_props() {
        let test_dir = TempDir::new().unwrap();

        let graph = TempColGraphFragment::build_tables(
            test_dir.path(),
            100,
            vec![
                1u64, 1u64, 1u64, 2u64, 2u64, 2u64, 3u64, 3u64, 3u64, 4u64, 4u64, 4u64,
            ],
            vec![
                (1u64, 2u64, 0i64),
                (1u64, 3u64, 1i64),
                (1u64, 4u64, 2i64),
                (2u64, 3u64, 3i64),
                (2u64, 4u64, 4i64),
                (2u64, 5u64, 5i64),
                (3u64, 4u64, 6i64),
                (3u64, 5u64, 7i64),
                (3u64, 6u64, 8i64),
                (4u64, 5u64, 9i64),
                (4u64, 6u64, 10i64),
                (4u64, 7u64, 11i64),
            ],
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
    fn load_muliple_sorted_edges_no_props_multiple_ts() {
        let test_dir = TempDir::new().unwrap();

        let graph = TempColGraphFragment::build_tables(
            test_dir.path(),
            100,
            vec![1u64, 1u64, 1u64, 2u64, 2u64, 2u64],
            vec![
                (1u64, 2u64, 0i64),
                (1u64, 3u64, 1i64),
                (1u64, 3u64, 2i64),
                (2u64, 3u64, 3i64),
                (2u64, 4u64, 4i64),
                (2u64, 4u64, 5i64),
            ],
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
    fn load_muliple_sorted_edges_no_props_multiple_ts_chunks_size_1() {
        let test_dir = TempDir::new().unwrap();

        let graph = TempColGraphFragment::build_tables(
            test_dir.path(),
            1,
            vec![1u64, 1u64, 1u64, 2u64, 2u64, 2u64],
            vec![
                (1u64, 2u64, 0i64),
                (1u64, 3u64, 1i64),
                (1u64, 3u64, 2i64),
                (2u64, 3u64, 3i64),
                (2u64, 4u64, 4i64),
                (2u64, 4u64, 5i64),
            ],
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

        let graph = TempColGraphFragment::build_tables(
            test_dir.path(),
            2,
            vec![
                1u64, 1u64, 1u64, 2u64, 2u64, 2u64, 3u64, 3u64, 3u64, 4u64, 4u64, 4u64,
            ],
            vec![
                (1u64, 2u64, 0i64),
                (1u64, 3u64, 1i64),
                (1u64, 4u64, 2i64),
                (2u64, 3u64, 3i64),
                (2u64, 4u64, 4i64),
                (2u64, 5u64, 5i64),
                (3u64, 4u64, 6i64),
                (3u64, 5u64, 7i64),
                (3u64, 6u64, 8i64),
                (4u64, 5u64, 9i64),
                (4u64, 6u64, 10i64),
                (4u64, 7u64, 11i64),
            ],
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

        let graph = TempColGraphFragment::build_tables(
            test_dir.path(),
            2,
            1u64..12u64,
            vec![
                (1u64, 2u64, 0i64),
                (1u64, 3u64, 1i64),
                (1u64, 4u64, 2i64),
                (2u64, 3u64, 3i64),
                (2u64, 4u64, 4i64),
                (2u64, 5u64, 5i64),
                (3u64, 4u64, 6i64),
                (3u64, 5u64, 7i64),
                (3u64, 6u64, 8i64),
                (4u64, 5u64, 9i64),
                (4u64, 6u64, 10i64),
                (4u64, 7u64, 11i64),
            ],
        )
        .unwrap();
        assert_eq!(graph.num_vertices(), 11);
    }
}
