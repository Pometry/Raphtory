use std::{
    cmp::min,
    ops::Range,
    path::{Path, PathBuf},
};

use arrow2::{
    array::{Array, PrimitiveArray, StructArray},
    buffer::Buffer,
    chunk::Chunk,
    compute::concatenate::concatenate,
    datatypes::{DataType, Field, Schema},
    io::parquet::{
        self,
        read::{infer_schema, RowGroupMetaData},
        write::FileMetaData,
    },
    offset::OffsetsBuffer,
    types::NativeType,
};
use itertools::Itertools;
use rayon::prelude::*;

use super::{
    array_as_id_iter,
    chunked_array::{chunked_array::ChunkedArray, list_array::ChunkedListArray},
    mmap::{mmap_batch, write_batches},
    Error, GID,
};

pub(crate) struct ParquetReader<P> {
    files: Vec<PathBuf>,
    graph_dir: P,
    edge_t_prop_schema: Schema,
    src_dest_schema: Schema,
    src_col_idx: usize,
    dst_col_idx: usize,
    time_col_idx: usize,
}

impl<P: AsRef<Path> + Clone + Send + Sync> ParquetReader<P> {
    pub(crate) fn new(
        graph_dir: P,
        parquet_path: P,
        src_col: &str,
        dst_col: &str,
        time_col: &str,
        excluded_cols: Vec<String>,
    ) -> Result<Self, Error> {
        let meta = std::fs::metadata(parquet_path.as_ref());
        if let Ok(meta) = meta {
            let mut files = if meta.is_dir() {
                let iter = std::fs::read_dir(parquet_path.as_ref())?;
                let entries: Result<Vec<_>, _> =
                    iter.into_iter().map_ok(|res| res.path()).collect();
                entries?
            } else {
                vec![parquet_path.as_ref().to_path_buf()]
            };
            files.sort();

            // read the first file and map the excluded columns to their indices
            let first_file = files.first().ok_or_else(|| Error::NoEdgeLists)?;
            let parquet_meta = read_file_metadata(first_file)?;
            let schema = infer_schema(&parquet_meta)?;

            let src_dest_schema = schema
                .clone()
                .filter(|_, field| field.name == src_col || field.name == dst_col);

            let src_col = src_dest_schema
                .fields
                .iter()
                .position(|f| f.name == src_col)
                .ok_or_else(|| Error::ColumnNotFound(src_col.to_owned()))?;
            let dst_col = src_dest_schema
                .fields
                .iter()
                .position(|f| f.name == dst_col)
                .ok_or_else(|| Error::ColumnNotFound(dst_col.to_owned()))?;

            let edge_t_prop_schema = schema.filter(|_, field| !excluded_cols.contains(&field.name));
            let time_col = edge_t_prop_schema
                .fields
                .iter()
                .position(|f| f.name == time_col)
                .ok_or_else(|| Error::ColumnNotFound(time_col.to_owned()))?;

            Ok(Self {
                files,
                graph_dir,
                edge_t_prop_schema,
                src_dest_schema,
                src_col_idx: src_col,
                dst_col_idx: dst_col,
                time_col_idx: time_col,
            })
        } else {
            Err(Error::NoEdgeLists)
        }
    }

    fn row_group_iter(&self) -> impl Iterator<Item = (PathBuf, RowGroupMetaData)> + '_ {
        self.files.iter().flat_map(|file| {
            let metadata = read_file_metadata(&file)
                .expect(&format!("failed to read metadata for file {:?}", file));
            metadata
                .row_groups
                .into_iter()
                .map(move |row_group| (file.clone(), row_group))
        })
    }

    fn parquet_offset_iter(
        &self,
        chunk_size: usize,
    ) -> impl Iterator<Item = Vec<ParquetOffset<RowGroupMetaData>>> + '_ {
        ParquetOffsetIter::new(self.row_group_iter(), chunk_size)
    }

    pub(crate) fn load_edges(
        &self,
        t_prop_chunk_size: usize,
    ) -> Result<ChunkedListArray<StructArray>, Error> {
        let edge_values = self.load_t_edge_values(t_prop_chunk_size)?;
        let edge_offsets = self.load_t_edge_offsets()?;
        Ok(ChunkedListArray::new_from_parts(edge_values, edge_offsets))
    }

    fn load_t_edge_values(&self, chunk_size: usize) -> Result<ChunkedArray<StructArray>, Error> {
        let graph_dir = self.graph_dir.clone();
        let schema = self.edge_t_prop_schema.clone();

        let offset_iter = self.parquet_offset_iter(chunk_size).collect_vec();

        let iter = offset_iter.into_par_iter().enumerate();

        let arrays = iter
            .map(|(chunk_id, parquet_offsets)| {
                let struct_arr = into_struct_arrays(parquet_offsets, &schema);

                let file_path = graph_dir
                    .as_ref()
                    .join(format!("edge_chunk_{:08}.ipc", chunk_id));

                write_temporal_properties(file_path, struct_arr, self.time_col_idx)
            })
            .collect::<Result<Vec<_>, Error>>()?;

        Ok(ChunkedArray::from_vec(chunk_size, arrays))
    }

    fn load_t_edge_offsets(&self) -> Result<OffsetsBuffer<i64>, Error> {
        let bounds_and_counts = self
            .files
            .par_iter()
            .flat_map_iter(|path| {
                read_parquet_file(path, self.src_dest_schema.clone())
                    .expect("failed to read parquet file")
            })
            .map(|chunk| self.count_chunk(chunk))
            .collect::<Result<Vec<_>, Error>>()?;

        let mut bounds_and_counts_iter = bounds_and_counts.into_iter();
        let (mut old_bounds, counts) = bounds_and_counts_iter
            .next()
            .ok_or_else(|| Error::NoEdgeLists)?;

        let mut offsets: Vec<i64> = vec![0];
        offsets.extend(counts.into_iter().scan(0i64, |state, v| {
            let new = v as i64 + *state;
            *state = new;
            Some(new)
        }));

        for (bounds, counts) in bounds_and_counts_iter {
            let mut counts_iter = counts.into_iter();

            if bounds.first == old_bounds.last {
                let first_count = counts_iter.next().ok_or_else(|| Error::EmptyChunk)?;
                *offsets.last_mut().unwrap() += first_count as i64;
            }
            let last_value = *offsets.last().unwrap();
            offsets.extend(counts_iter.scan(last_value, |state, v| {
                let new = v as i64 + *state;
                *state = new;
                Some(new)
            }));
            old_bounds = bounds;
        }

        let buffer = Buffer::from(offsets);
        write_buffer(
            self.graph_dir.as_ref().join("edge_offsets.ipc"),
            buffer.clone(),
        )?;
        //safety: we made some offsets that are increasing and start at 0
        Ok(unsafe { OffsetsBuffer::new_unchecked(buffer) })
    }

    fn count_chunk(
        &self,
        chunk: Result<Chunk<Box<dyn Array>>, arrow2::error::Error>,
    ) -> Result<(EdgeBounds, Vec<usize>), Error> {
        let chunk = chunk?;
        let srcs = &chunk[self.src_col_idx];
        let dests = &chunk[self.dst_col_idx];

        let srcs = array_as_id_iter(srcs)?;
        let dests = array_as_id_iter(dests)?;

        let mut iter = srcs.zip(dests);
        let first = iter.next().ok_or_else(|| Error::EmptyChunk)?;
        let mut last = first.clone();
        let mut counts: Vec<usize> = vec![1];

        for edge in iter {
            if edge == last {
                *counts.last_mut().expect("this is not empty") += 1;
            } else {
                counts.push(1);
            }
            last = edge;
        }

        Ok((EdgeBounds { first, last }, counts))
    }
}

struct EdgeBounds {
    first: (GID, GID),
    last: (GID, GID),
}

fn write_buffer<T: NativeType>(
    file_path: impl AsRef<Path>,
    buffer: Buffer<T>,
) -> Result<(), Error> {
    let arr: PrimitiveArray<T> =
        unsafe { PrimitiveArray::from_inner_unchecked(T::PRIMITIVE.into(), buffer, None) };

    let schema = Schema::from(vec![Field::new("offsets", arr.data_type().clone(), false)]);
    let chunk = Chunk::new(vec![arr.boxed()]);
    write_batches(file_path, schema, &[chunk])?;
    Ok(())
}

fn write_temporal_properties(
    file_path: impl AsRef<Path>,
    chunk: StructArray,
    time_col_idx: usize,
) -> Result<StructArray, Error> {
    let (mut fields, mut values, _) = chunk.into_data();
    let schema = Schema::from(fields.clone());
    // make sure the time column is first
    values.swap(0, time_col_idx);
    fields.swap(0, time_col_idx);

    write_batches(file_path.as_ref(), schema, &[Chunk::new(values)])?;
    let mmapped_chunk = unsafe { mmap_batch(file_path.as_ref(), 0)? };
    let mmapped = StructArray::new(DataType::Struct(fields), mmapped_chunk.into_arrays(), None);
    Ok(mmapped)
}

fn read_parquet_file(
    path: impl AsRef<Path>,
    schema: Schema,
) -> Result<impl Iterator<Item = Result<Chunk<Box<dyn Array>>, arrow2::error::Error>>, Error> {
    let file = std::fs::File::open(&path)?;
    let metadata = read_file_metadata(&path)?;
    let row_groups = metadata.row_groups;

    let reader = parquet::read::FileReader::new(file, row_groups, schema, None, None, None);
    Ok(reader)
}

fn into_struct_arrays<'a>(
    offsets: Vec<ParquetOffset<RowGroupMetaData>>,
    schema: &Schema,
) -> StructArray {
    let group = offsets.iter().group_by(|&offset| &offset.file);

    let iter = group.into_iter().flat_map(|(file, offsets)| {
        let file =
            std::fs::File::open(&file).expect(&format!("failed to open parquet file {:?}", file));
        let mut row_groups = vec![];
        let mut ranges = vec![];
        for offset in offsets {
            row_groups.push(offset.row_group.clone());
            ranges.push(offset.range.clone());
        }

        let reader =
            parquet::read::FileReader::new(file, row_groups, schema.clone(), None, None, None);

        // we assume the rowgroup maps to a chunk 1-1
        reader.zip(ranges.into_iter()).map(move |(chunk, range)| {
            let chunk = chunk.expect("failed to read parquet chunk").into_arrays();
            let mut chunk = StructArray::new(DataType::Struct(schema.fields.clone()), chunk, None);
            chunk.slice(range.start, range.end - range.start);
            chunk
        })
    });

    let all_structs = iter.collect::<Vec<_>>();

    let mut type_coerce: Vec<&dyn Array> = Vec::with_capacity(all_structs.len());

    for arr in all_structs.iter() {
        type_coerce.push(arr);
    }

    let struct_arr = concatenate(&type_coerce).expect("failed to concatenate");
    struct_arr
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .clone()
}

fn read_file_metadata(path: impl AsRef<Path>) -> Result<FileMetaData, Error> {
    let mut file = std::fs::File::open(path.as_ref())?;
    let meta = parquet::read::read_metadata(&mut file)?;
    Ok(meta)
}

trait NumRows: Clone {
    fn num_rows(&self) -> usize;
}

impl NumRows for RowGroupMetaData {
    fn num_rows(&self) -> usize {
        self.num_rows()
    }
}

#[derive(PartialEq, Debug)]
pub(crate) struct ParquetOffset<G> {
    file: PathBuf,
    row_group: G,
    range: Range<usize>,
}

struct ParquetOffsetIter<G: NumRows, P: AsRef<Path>, I: Iterator<Item = (P, G)>> {
    row_groups: I,
    chunk_size: usize,
    current: Option<(P, G)>,
    offset: usize,
}

impl<G: NumRows, P: AsRef<Path>, I: Iterator<Item = (P, G)>> ParquetOffsetIter<G, P, I> {
    fn new(mut row_groups: I, chunk_size: usize) -> Self {
        let first = row_groups.next();
        Self {
            row_groups,
            chunk_size,
            current: first,
            offset: 0,
        }
    }
}

// iterator
impl<G: NumRows, P: AsRef<Path>, I: Iterator<Item = (P, G)>> Iterator
    for ParquetOffsetIter<G, P, I>
{
    type Item = Vec<ParquetOffset<G>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut chunks: Vec<ParquetOffset<G>> = vec![];
        let mut chunk_len: usize = 0;
        while chunk_len < self.chunk_size {
            if let Some((file, current)) = self.current.as_ref() {
                let remaining_in_current = current.num_rows() - self.offset;
                let needed = self.chunk_size - chunk_len;
                let from_current = min(needed, remaining_in_current);
                let next_parquet_chunk = ParquetOffset {
                    file: file.as_ref().to_path_buf(),
                    row_group: current.clone(),
                    range: self.offset..self.offset + from_current,
                };
                chunk_len += from_current;
                chunks.push(next_parquet_chunk);
                if remaining_in_current <= needed {
                    self.offset = 0;
                    self.current = self.row_groups.next();
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

    use arrow2::chunk;

    use super::*;

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
        let row_groups: Vec<(String, MockRowGroup)> = vec![];
        let chunk_size = 100;
        let mut iter = ParquetOffsetIter::new(row_groups.into_iter(), chunk_size);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn rechunk_row_group1() {
        let row_groups: Vec<(String, MockRowGroup)> =
            vec![("file1".to_string(), MockRowGroup { num_rows: 100 })];
        let chunk_size = 100;
        let mut iter = ParquetOffsetIter::new(row_groups.into_iter(), chunk_size);
        assert_eq!(
            iter.next(),
            Some(vec![ParquetOffset {
                file: "file1".into(),
                row_group: MockRowGroup { num_rows: 100 },
                range: 0..100
            }])
        );
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn rechunk_row_group2_half() {
        let row_groups: Vec<(String, MockRowGroup)> = vec![
            ("file1".to_string(), MockRowGroup { num_rows: 50 }),
            ("file1".to_string(), MockRowGroup { num_rows: 50 }),
        ];
        let chunk_size = 100;
        let mut iter = ParquetOffsetIter::new(row_groups.into_iter(), chunk_size);
        let actual = vec![
            ParquetOffset {
                file: "file1".into(),
                row_group: MockRowGroup { num_rows: 50 },
                range: 0..50,
            },
            ParquetOffset {
                file: "file1".into(),
                row_group: MockRowGroup { num_rows: 50 },
                range: 0..50,
            },
        ];
        assert_eq!(iter.next(), Some(actual));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn rechunk_row_group2_uneven() {
        let row_groups: Vec<(String, MockRowGroup)> = vec![
            ("file1".to_string(), MockRowGroup { num_rows: 30 }),
            ("file1".to_string(), MockRowGroup { num_rows: 70 }),
        ];
        let chunk_size = 100;
        let mut iter = ParquetOffsetIter::new(row_groups.into_iter(), chunk_size);
        let actual = vec![
            ParquetOffset {
                file: "file1".into(),
                row_group: MockRowGroup { num_rows: 30 },
                range: 0..30,
            },
            ParquetOffset {
                file: "file1".into(),
                row_group: MockRowGroup { num_rows: 70 },
                range: 0..70,
            },
        ];
        assert_eq!(iter.next(), Some(actual));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn rechunk_row_group3_larger_than_chunk_size() {
        let row_groups: Vec<(String, MockRowGroup)> = vec![
            ("file1".to_string(), MockRowGroup { num_rows: 150 }),
            ("file1".to_string(), MockRowGroup { num_rows: 130 }),
            ("file2".to_string(), MockRowGroup { num_rows: 12 }),
        ];
        let chunk_size = 100;
        let iter = ParquetOffsetIter::new(row_groups.into_iter(), chunk_size);
        let expected = vec![
            vec![ParquetOffset {
                file: "file1".into(),
                row_group: MockRowGroup { num_rows: 150 },
                range: 0..100,
            }],
            vec![
                ParquetOffset {
                    file: "file1".into(),
                    row_group: MockRowGroup { num_rows: 150 },
                    range: 100..150,
                },
                ParquetOffset {
                    file: "file1".into(),
                    row_group: MockRowGroup { num_rows: 130 },
                    range: 0..50,
                },
            ],
            vec![
                ParquetOffset {
                    file: "file1".into(),
                    row_group: MockRowGroup { num_rows: 130 },
                    range: 50..130,
                },
                ParquetOffset {
                    file: "file2".into(),
                    row_group: MockRowGroup { num_rows: 12 },
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
            PathBuf::from_iter([root, "resources", "test", "chunked.snappy.parquet"]);
        let graph_dir = tempfile::tempdir().unwrap();

        let excluded_cols = vec![
            "src".to_string(),
            "dst".to_string(),
            "dst_hash".to_string(),
            "src_hash".to_string(),
        ];

        let reader = ParquetReader::new(
            graph_dir.path(),
            nft.as_path(),
            "src",
            "dst",
            "epoch_time",
            excluded_cols,
        )
        .unwrap();

        let list_arr = reader.load_edges(17).unwrap();
        let list_arr_vs = list_arr.values();
        assert_eq!(list_arr.len(), 97);
        assert_eq!(list_arr_vs.len(), 100);

        let time = list_arr_vs
            .primitive_col::<i64>(0)
            .unwrap()
            .iter_chunks()
            .flat_map(|arr| arr.into_iter())
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
            .iter_chunks()
            .flat_map(|arr| arr.into_iter())
            .flatten()
            .collect_vec();
        assert_eq!(src_port, [56987, 94271, 79502]);
    }
}
