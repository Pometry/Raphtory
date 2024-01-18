use crate::arrow::chunked_array::array_ops::BaseArrayOps;

use super::{
    chunked_array::{chunked_array::ChunkedArray, list_array::ChunkedListArray},
    edge_frame_builder::edge_props_builder::EdgePropsBuilder,
    Error, GraphChunk,
};
use arrow2::{
    array::{Array, StructArray},
    chunk::Chunk,
    datatypes::{DataType, Schema},
    io::parquet::{
        self,
        read::{infer_schema, RowGroupMetaData},
        write::FileMetaData,
    },
    offset::OffsetsBuffer,
};
use itertools::Itertools;
use once_cell::sync::OnceCell;
use rayon::prelude::*;
use std::{
    borrow::Borrow,
    cmp::min,
    num::NonZeroUsize,
    ops::Range,
    path::{Path, PathBuf},
};

pub(crate) struct ParquetReader<P, V = Vec<PathBuf>> {
    files: V,
    edge_t_prop_schema: Schema,
    src_dest_schema: Schema,
    edge_props_builder: EdgePropsBuilder<P>,
}

impl<P: AsRef<Path> + Clone + Send + Sync, V: Borrow<[PathBuf]> + Send + Sync> ParquetReader<P, V> {
    pub(crate) fn new_from_filelist(
        graph_dir: P,
        files: V,
        src_col: &str,
        dst_col: &str,
        time_col: &str,
        excluded_cols: &[&str],
    ) -> Result<ParquetReader<P, V>, Error> {
        // read the first file and map the excluded columns to their indices
        let first_file = files.borrow().first().ok_or_else(|| Error::NoEdgeLists)?;
        let parquet_meta = read_file_metadata(first_file)?;
        let schema = infer_schema(&parquet_meta)?;

        let src_dest_schema = schema
            .clone()
            .filter(|_, field| field.name == src_col || field.name == dst_col);

        let edge_t_prop_schema =
            schema.filter(|_, field| !excluded_cols.contains(&field.name.as_str()));
        let time_col = edge_t_prop_schema
            .fields
            .iter()
            .position(|f| f.name == time_col)
            .ok_or_else(|| Error::ColumnNotFound(time_col.to_owned()))?;

        let edge_props_builder = EdgePropsBuilder::new(graph_dir, time_col);

        Ok(Self {
            files,
            edge_t_prop_schema,
            src_dest_schema,
            edge_props_builder,
        })
    }
    fn row_group_iter(&self) -> impl Iterator<Item = RowGroupRef<PathBuf>> + '_ {
        let schema = self.edge_t_prop_schema.clone();
        self.files.borrow().iter().flat_map(move |file| {
            let schema = schema.clone();
            let metadata = read_file_metadata(&file)
                .expect(&format!("failed to read metadata for file {:?}", file));
            metadata
                .row_groups
                .into_iter()
                .map(move |row_group| RowGroupRef::new(file.clone(), row_group, schema.clone()))
        })
    }

    fn parquet_offset_iter(
        &self,
        chunk_size: usize,
    ) -> impl Iterator<Item = Vec<ParquetOffset<RowGroupRef<PathBuf>>>> + '_ {
        ParquetOffsetIter::new(self.row_group_iter(), chunk_size)
    }

    pub(crate) fn load_edges(
        &self,
        num_threads: NonZeroUsize,
        t_prop_chunk_size: usize,
    ) -> Result<ChunkedListArray<ChunkedArray<StructArray>>, Error> {
        let edge_values = self.load_t_edge_values(num_threads, t_prop_chunk_size)?;
        let edge_offsets = self.load_t_edge_offsets()?;
        assert_eq!(edge_offsets.get(0), Some(&0));
        assert!(edge_offsets
            .iter()
            .tuple_windows()
            .all(|(prev, next)| prev <= next));
        assert_eq!(edge_offsets.last(), &(edge_values.len() as i64));
        Ok(ChunkedListArray::new_from_parts(edge_values, edge_offsets))
    }

    fn load_t_edge_values(
        &self,
        num_threads: NonZeroUsize,
        chunk_size: usize,
    ) -> Result<ChunkedArray<StructArray>, Error> {
        let offset_iter = self.parquet_offset_iter(chunk_size).collect_vec();
        let iter = offset_iter.into_par_iter();

        self.edge_props_builder
            .load_t_edges_from_par_structs(num_threads, iter)
    }

    fn load_t_edge_offsets(&self) -> Result<OffsetsBuffer<i64>, Error> {
        let chunks = self.files.borrow().par_iter().flat_map_iter(|path| {
            read_parquet_file(path, self.src_dest_schema.clone())
                .expect("failed to read parquet file")
                .map_ok(move |chunk| {
                    GraphChunk::from_chunk(chunk, 0, 1)
                })
        });
        self.edge_props_builder
            .load_t_edge_offsets_from_par_chunks(chunks)
    }
}

pub(crate) fn read_parquet_file(
    path: impl AsRef<Path>,
    schema: Schema,
) -> Result<impl Iterator<Item = Result<Chunk<Box<dyn Array>>, arrow2::error::Error>>, Error> {
    let file = std::fs::File::open(&path)?;
    let metadata = read_file_metadata(&path)?;
    let row_groups = metadata.row_groups;

    let reader = parquet::read::FileReader::new(file, row_groups, schema, None, None, None);
    Ok(reader)
}

pub(crate) fn read_file_metadata(path: impl AsRef<Path>) -> Result<FileMetaData, Error> {
    let mut file = std::fs::File::open(path.as_ref())?;
    let meta = parquet::read::read_metadata(&mut file)?;
    Ok(meta)
}

pub trait NumRows: Clone {
    fn num_rows(&self) -> usize;
}

pub trait TrySlice {
    fn try_slice(&self, range: Range<usize>) -> Result<StructArray, Error>;
}

#[derive(Clone, Debug)]
struct RowGroupRef<P> {
    file: P,
    row_group: RowGroupMetaData,
    schema: Schema,
    chunk: OnceCell<Chunk<Box<dyn Array>>>,
}

impl<P: AsRef<Path>> RowGroupRef<P> {
    fn read(&self) -> Result<&Chunk<Box<dyn Array>>, Error> {
        self.chunk.get_or_try_init(|| {
            let file = std::fs::File::open(&self.file)?;
            let mut reader = parquet::read::FileReader::new(
                file,
                vec![self.row_group.clone()],
                self.schema.clone(),
                None,
                None,
                None,
            );
            if let Some(chunk_res) = reader.next() {
                Ok(chunk_res?)
            } else {
                Err(Error::EmptyChunk)
            }
        })
    }
}

impl<P> RowGroupRef<P> {
    fn new(file: P, row_group: RowGroupMetaData, schema: Schema) -> Self {
        Self {
            file,
            row_group,
            chunk: OnceCell::new(),
            schema,
        }
    }
}

impl<P: Clone> NumRows for RowGroupRef<P> {
    fn num_rows(&self) -> usize {
        self.row_group.num_rows()
    }
}

impl<P: AsRef<Path>> TrySlice for RowGroupRef<P> {
    fn try_slice(&self, range: Range<usize>) -> Result<StructArray, Error> {
        let chunk = self.read()?;
        let sliced_arrays: Vec<_> = chunk
            .arrays()
            .iter()
            .map(|array| array.sliced(range.start, range.end - range.start))
            .collect();
        Ok(StructArray::new(
            DataType::Struct(self.schema.fields.clone()),
            sliced_arrays,
            None,
        ))
    }
}

impl TrySlice for StructArray {
    fn try_slice(&self, range: Range<usize>) -> Result<StructArray, Error> {
        Ok(self.clone().sliced(range.start, range.end - range.start))
    }
}

impl NumRows for StructArray {
    fn num_rows(&self) -> usize {
        self.len()
    }
}

#[derive(PartialEq, Debug)]
pub struct ParquetOffset<G> {
    pub index: G,
    pub range: Range<usize>,
}

pub struct ParquetOffsetIter<G, I> {
    iter: I,
    chunk_size: usize,
    current: Option<G>,
    offset: usize,
}

impl<G, I: Iterator<Item = G>> ParquetOffsetIter<G, I> {
    pub fn new(mut iter: I, chunk_size: usize) -> Self {
        let first = iter.next();
        Self {
            iter,
            chunk_size,
            current: first,
            offset: 0,
        }
    }
}

// iterator
impl<G: NumRows, I: Iterator<Item = G>> Iterator for ParquetOffsetIter<G, I> {
    type Item = Vec<ParquetOffset<G>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut chunks: Vec<ParquetOffset<G>> = vec![];
        let mut chunk_len: usize = 0;
        while chunk_len < self.chunk_size {
            if let Some(current) = self.current.as_ref() {
                let remaining_in_current = current.num_rows() - self.offset;
                let needed = self.chunk_size - chunk_len;
                let from_current = min(needed, remaining_in_current);
                let next_parquet_chunk = ParquetOffset {
                    index: current.clone(),
                    range: self.offset..self.offset + from_current,
                };
                chunk_len += from_current;
                chunks.push(next_parquet_chunk);
                if remaining_in_current <= needed {
                    self.offset = 0;
                    self.current = self.iter.next();
                } else {
                    self.offset += from_current;
                }
            } else {
                // no more data, could be partial last chunk
                break;
            }
        }
        if chunks.is_empty() {
            None
        } else {
            Some(chunks)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::arrow::chunked_array::array_ops::{ArrayOps, BaseArrayOps, PrimitiveCol};

    #[derive(Clone, PartialEq, Debug)]
    struct MockRowGroup {
        num_rows: usize,
    }

    impl super::NumRows for MockRowGroup {
        fn num_rows(&self) -> usize {
            self.num_rows
        }
    }

    #[test]
    fn rechunk_row_group1_empty() {
        let row_groups: Vec<MockRowGroup> = vec![];
        let chunk_size = 100;
        let mut iter = ParquetOffsetIter::new(row_groups.into_iter(), chunk_size);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn rechunk_row_group1() {
        let row_groups: Vec<MockRowGroup> = vec![MockRowGroup { num_rows: 100 }];
        let chunk_size = 100;
        let mut iter = ParquetOffsetIter::new(row_groups.into_iter(), chunk_size);
        assert_eq!(
            iter.next(),
            Some(vec![ParquetOffset {
                index: MockRowGroup { num_rows: 100 },
                range: 0..100
            }])
        );
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn rechunk_row_group2_half() {
        let row_groups: Vec<MockRowGroup> =
            vec![MockRowGroup { num_rows: 50 }, MockRowGroup { num_rows: 50 }];
        let chunk_size = 100;
        let mut iter = ParquetOffsetIter::new(row_groups.into_iter(), chunk_size);
        let actual = vec![
            ParquetOffset {
                index: MockRowGroup { num_rows: 50 },
                range: 0..50,
            },
            ParquetOffset {
                index: MockRowGroup { num_rows: 50 },
                range: 0..50,
            },
        ];
        assert_eq!(iter.next(), Some(actual));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn rechunk_row_group2_uneven() {
        let row_groups: Vec<MockRowGroup> =
            vec![MockRowGroup { num_rows: 30 }, MockRowGroup { num_rows: 70 }];
        let chunk_size = 100;
        let mut iter = ParquetOffsetIter::new(row_groups.into_iter(), chunk_size);
        let actual = vec![
            ParquetOffset {
                index: MockRowGroup { num_rows: 30 },
                range: 0..30,
            },
            ParquetOffset {
                index: MockRowGroup { num_rows: 70 },
                range: 0..70,
            },
        ];
        assert_eq!(iter.next(), Some(actual));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn rechunk_row_group3_larger_than_chunk_size() {
        let row_groups: Vec<MockRowGroup> = vec![
            MockRowGroup { num_rows: 150 },
            MockRowGroup { num_rows: 130 },
            MockRowGroup { num_rows: 12 },
        ];
        let chunk_size = 100;
        let iter = ParquetOffsetIter::new(row_groups.into_iter(), chunk_size);
        let expected = vec![
            vec![ParquetOffset {
                index: MockRowGroup { num_rows: 150 },
                range: 0..100,
            }],
            vec![
                ParquetOffset {
                    index: MockRowGroup { num_rows: 150 },
                    range: 100..150,
                },
                ParquetOffset {
                    index: MockRowGroup { num_rows: 130 },
                    range: 0..50,
                },
            ],
            vec![
                ParquetOffset {
                    index: MockRowGroup { num_rows: 130 },
                    range: 50..130,
                },
                ParquetOffset {
                    index: MockRowGroup { num_rows: 12 },
                    range: 0..12,
                },
            ],
        ];
        let actual = iter.collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn load_edges_from_parquet() {
        let root = env!("CARGO_MANIFEST_DIR");
        let nft: PathBuf =
            PathBuf::from_iter([root, "resources", "test", "chunked", "chunked.parquet"]);
        let graph_dir = tempfile::tempdir().unwrap();

        let excluded_cols = vec!["src", "dst", "dst_hash", "src_hash"];

        let reader = ParquetReader::new_from_filelist(
            graph_dir.path(),
            vec![nft],
            "src",
            "dst",
            "epoch_time",
            &excluded_cols,
        )
        .unwrap();

        let list_arr = reader.load_edges(4.try_into().unwrap(), 17).unwrap();
        let list_arr_vs = list_arr.values();
        assert_eq!(list_arr.len(), 97);
        assert_eq!(list_arr_vs.len(), 100);

        let time = list_arr_vs
            .primitive_col::<i64>(0)
            .unwrap()
            .iter()
            .flatten()
            .collect_vec();

        assert_eq!(time.len(), 100);
        assert_eq!(&time[0..3], [7263521, 7257667, 7296325]);

        match list_arr_vs.data_type().unwrap() {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 9, "expected 9 fields, got {:?}", fields);
                assert_eq!(fields[0].name, "epoch_time");
                assert_eq!(fields[0].data_type(), &DataType::Int64);
            }
            _ => panic!("expected struct array"),
        }

        let slice = list_arr.value(11);
        assert_eq!(slice.len(), 3);
        let epoch_time: Vec<i64> = slice
            .primitive_col(0)
            .unwrap()
            .into_iter()
            .flatten()
            .collect();
        assert_eq!(epoch_time, [7258036, 7264284, 7318417]);

        // check the 3rd column
        let src_port = slice
            .primitive_col::<i64>(3)
            .unwrap()
            .iter()
            .flatten()
            .collect_vec();
        assert_eq!(src_port, [56987, 94271, 79502]);
    }
}
