use crate::{file_prefix::GraphPaths, graph_builder::props_builder::PropsBuilder};

use super::{read_schema, RAError};
use crate::arrow2::{
    array::{Array, StructArray},
    datatypes::{ArrowDataType as DataType, ArrowSchema as Schema},
    legacy::error,
    record_batch::RecordBatchT as Chunk,
};
use itertools::Itertools;
use once_cell::sync::OnceCell;
use polars_arrow::array::PrimitiveArray;
use polars_parquet::read::{read_metadata, FileMetaData, FileReader, RowGroupMetaData};
use raphtory_api::core::storage::dict_mapper::DictMapper;
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
    prop_schema: Schema,
    props_builder: PropsBuilder<P>,
}

impl<P: AsRef<Path> + Send + Sync, V: Borrow<[PathBuf]> + Send + Sync> ParquetReader<P, V> {
    pub(crate) fn new_from_filelist(
        graph_dir: P,
        files: V,
        time_col: Option<&str>,
        graph_paths: GraphPaths,
        filter_fn: impl Fn(&str) -> bool,
    ) -> Result<ParquetReader<P, V>, RAError> {
        // read the first file and map the excluded columns to their indices
        let first_file = files.borrow().first().ok_or_else(|| RAError::NoEdgeLists)?;
        let parquet_meta = read_file_metadata(first_file)?;
        let schema = read_schema(&parquet_meta)?;

        let edge_t_prop_schema = schema.filter(|_, field| filter_fn(field.name.as_str()));

        let time_col = if let Some(time_col) = time_col {
            Some(
                edge_t_prop_schema
                    .fields
                    .iter()
                    .position(|f| f.name == time_col)
                    .ok_or_else(|| RAError::ColumnNotFound(time_col.to_owned()))?,
            )
        } else {
            None
        };

        let edge_props_builder = PropsBuilder::new(graph_dir, graph_paths, time_col);

        Ok(Self {
            files,
            prop_schema: edge_t_prop_schema,
            props_builder: edge_props_builder,
        })
    }

    fn row_group_iter(&self) -> impl Iterator<Item = RowGroupRef<PathBuf>> + '_ {
        let schema = self.prop_schema.clone();
        self.files.borrow().iter().flat_map(move |file| {
            let schema = schema.clone();
            let metadata = read_file_metadata(file)
                .unwrap_or_else(|_| panic!("failed to read metadata for file {:?}", file));
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

    pub(crate) fn load_values(
        &self,
        num_threads: NonZeroUsize,
        chunk_size: usize,
    ) -> Result<Vec<StructArray>, RAError> {
        let offset_iter = self.parquet_offset_iter(chunk_size).collect_vec();
        let iter = offset_iter.into_par_iter();

        self.props_builder.load_props_from_par_structs(
            num_threads,
            iter,
            |builder, chunk_id, struct_arr| builder.write_props(struct_arr, chunk_id),
        )
    }

    pub(crate) fn load_dict_encoded_values(
        &self,
        num_threads: NonZeroUsize,
        chunk_size: usize,
        dict_mapper: &DictMapper,
    ) -> Result<Vec<PrimitiveArray<u64>>, RAError> {
        let offset_iter = self.parquet_offset_iter(chunk_size).collect_vec();
        let iter = offset_iter.into_par_iter();

        self.props_builder.load_props_from_par_structs(
            num_threads,
            iter,
            |builder, chunk_id, struct_arr| {
                builder.write_dict_encoded_props(struct_arr, chunk_id, dict_mapper)
            },
        )
    }
}

pub(crate) fn read_parquet_file(
    path: impl AsRef<Path>,
    schema: Schema,
) -> Result<impl Iterator<Item = Result<Chunk<Box<dyn Array>>, error::PolarsError>>, RAError> {
    let file = std::fs::File::open(&path)?;
    let metadata = read_file_metadata(&path)?;
    let row_groups = metadata.row_groups;

    let reader = FileReader::new(file, row_groups, schema, None, None, None);
    Ok(reader)
}

pub(crate) fn read_file_metadata(path: impl AsRef<Path>) -> Result<FileMetaData, RAError> {
    let mut file = std::fs::File::open(path.as_ref())?;
    let meta = read_metadata(&mut file)?;
    Ok(meta)
}

pub trait NumRows: Clone {
    fn num_rows(&self) -> usize;
}

pub trait TrySlice {
    fn try_slice(&self, range: Range<usize>) -> Result<StructArray, RAError>;
}

#[derive(Clone, Debug)]
struct RowGroupRef<P> {
    file: P,
    row_group: RowGroupMetaData,
    schema: Schema,
    chunk: OnceCell<Chunk<Box<dyn Array>>>,
}

impl<P: AsRef<Path>> RowGroupRef<P> {
    fn read(&self) -> Result<&Chunk<Box<dyn Array>>, RAError> {
        self.chunk.get_or_try_init(|| {
            let file = std::fs::File::open(&self.file)?;
            let mut reader = FileReader::new(
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
                Err(RAError::EmptyChunk)
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
    fn try_slice(&self, range: Range<usize>) -> Result<StructArray, RAError> {
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
    fn try_slice(&self, range: Range<usize>) -> Result<StructArray, RAError> {
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

    #[derive(Clone, PartialEq, Debug)]
    struct MockRowGroup {
        num_rows: usize,
    }

    impl NumRows for MockRowGroup {
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
                range: 0..100,
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
        // let root = env!("CARGO_MANIFEST_DIR");
        // let nft: PathBuf =
        //     PathBuf::from_iter([root, "resources", "test", "chunked", "chunked.parquet"]);
        // let graph_dir = tempfile::tempdir().unwrap();

        // let excluded_cols = vec!["src", "dst", "dst_hash", "src_hash"];

        // let reader = ParquetReader::new_from_filelist(
        //     graph_dir.path(),
        //     vec![nft],
        //     "src",
        //     "dst",
        //     "epoch_time",
        //     &excluded_cols,
        // )
        // .unwrap();

        // let (edge_offsets, _) = reader.load_offsets().unwrap();
        // let list_arr = reader
        //     .load_edges(4.try_into().unwrap(), 17, edge_offsets)
        //     .unwrap();
        // let list_arr_vs = list_arr.values();
        // assert_eq!(list_arr.len(), 97);
        // assert_eq!(list_arr_vs.len(), 100);

        // let time = list_arr_vs
        //     .primitive_col::<i64>(0)
        //     .unwrap()
        //     .iter()
        //     .flatten()
        //     .collect_vec();

        // assert_eq!(time.len(), 100);
        // assert_eq!(&time[0..3], [7263521, 7257667, 7296325]);

        // match list_arr_vs.data_type().unwrap() {
        //     DataType::Struct(fields) => {
        //         assert_eq!(fields.len(), 9, "expected 9 fields, got {:?}", fields);
        //         assert_eq!(fields[0].name, "epoch_time");
        //         assert_eq!(fields[0].data_type(), &DataType::Int64);
        //     }
        //     _ => panic!("expected struct array"),
        // }

        // let slice = list_arr.value(11);
        // assert_eq!(slice.len(), 3);
        // let epoch_time: Vec<i64> = slice
        //     .primitive_col(0)
        //     .unwrap()
        //     .into_iter()
        //     .flatten()
        //     .collect();
        // assert_eq!(epoch_time, [7258036, 7264284, 7318417]);

        // // check the 3rd column
        // let src_port = slice
        //     .primitive_col::<i64>(3)
        //     .unwrap()
        //     .iter()
        //     .flatten()
        //     .collect_vec();
        // assert_eq!(src_port, [56987, 94271, 79502]);
    }
}
