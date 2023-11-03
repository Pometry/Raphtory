use std::{
    cmp::min,
    io::{Read, Seek},
    ops::Range,
    path::{Path, PathBuf},
};

use arrow2::{
    array::{Array, StructArray},
    chunk::Chunk,
    datatypes::{Schema, DataType},
    io::parquet::{
        read::{infer_schema, RowGroupMetaData},
        write::FileMetaData, self,
    },
};
use itertools::Itertools;
use rayon::prelude::*;

use super::Error;

struct ParquetReader<P: AsRef<Path>> {
    files: Vec<PathBuf>,
    graph_dir: P,
}

impl<P: AsRef<Path> + Clone> ParquetReader<P> {
    fn new(graph_dir: P, parquet_path: P) -> Result<Self, Error> {
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

            Ok(Self { files, graph_dir })
        } else {
            Err(Error::NoEdgeLists)
        }
    }

    fn row_group_iter(self) -> impl Iterator<Item = (PathBuf, RowGroupMetaData)> {
        self.files.into_iter().flat_map(|file| {
            let metadata = read_file_metadata(&file)
                .expect(&format!("failed to read metadata for file {:?}", file));
            metadata
                .row_groups
                .into_iter()
                .map(move |row_group| (file.clone(), row_group))
        })
    }

    fn parquet_offset_iter(
        self,
        chunk_size: usize,
    ) -> impl Iterator<Item = Vec<ParquetOffset<RowGroupMetaData>>> {
        ParquetOffsetIter::new(self.row_group_iter(), chunk_size)
    }

    fn load_edges(self, chunk_size: usize) {
        let offset_iter = self.parquet_offset_iter(chunk_size);

        let iter = offset_iter.enumerate().par_bridge();
        iter.for_each(|(chunk_id, parquet_offsets)| {})
    }
}

fn into_struct_arrays(
    offsets: &[ParquetOffset<RowGroupMetaData>],
) -> impl Iterator<Item = Result<StructArray, Error>> {
    let iter = offsets
        .iter()
        .group_by(|&offset| &offset.file)
        .into_iter()
        .flat_map(|(file, offsets)| {
            let metadata = read_file_metadata(&file)
                .expect(&format!("failed to read metadata for file {:?}", file));
            let file = std::fs::File::open(&file)
                .expect(&format!("failed to open parquet file {:?}", file));
            let mut row_groups = vec![];
            let mut ranges = vec![];
            for offset in offsets {
                row_groups.push(offset.row_group.clone());
                ranges.push(offset.range.clone());
            }
            let schema = infer_schema(&metadata).expect("failed to infer schema");

            let reader = parquet::read::FileReader::new(
                file, row_groups, schema.clone(), None, None, None,
            );

            // we assume the rowgroup maps to a chunk 1-1
            reader.zip(ranges.into_iter()).map(move |(chunk, range)|{
                let chunk = chunk.expect("failed to read parquet chunk").into_arrays();
                let mut chunk = StructArray::new(DataType::Struct(schema.fields.clone()), chunk, None);
                chunk.slice(range.start, range.end);
                chunk
            })
        });
    std::iter::empty()
}

fn read_file_metadata(path: impl AsRef<Path>) -> Result<FileMetaData, Error> {
    let mut file = std::fs::File::open(path.as_ref())?;
    let meta = parquet::read::read_metadata(&mut file)?;
    Ok(meta)
}

fn parquet_chunks(
    file_path: impl AsRef<Path> + Send + Sync + 'static,
    num_row_groups: usize,
) -> Result<impl ParallelIterator<Item = Result<Chunk<Box<dyn Array>>, Error>>, Error> {
    let meta = read_file_metadata(file_path.as_ref())?;
    let schema = infer_schema(&meta)?;

    let iter = meta
        .row_groups
        .into_par_iter()
        .chunks(num_row_groups)
        .flat_map_iter(move |row_group| {
            let file = std::fs::File::open(file_path.as_ref()).expect(&format!(
                "failed to open parquet file {:?}",
                file_path.as_ref()
            ));
            read_parquet_row_groups(file, &row_group, &schema)
        });

    Ok(iter)
}

fn read_parquet_row_groups<F: Read + Seek + Sync + Send>(
    r: F,
    groups: &[RowGroupMetaData],
    schema: &Schema,
) -> impl Iterator<Item = Result<Chunk<Box<dyn Array>>, Error>> + Sync + Send {
    let iter = parquet::read::FileReader::new(
        r,
        groups.to_vec(),
        schema.clone(),
        None,
        None,
        None,
    );
    iter.map(|res| res.map_err(|e| e.into()))
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
}
