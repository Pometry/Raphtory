use std::{
    cmp::min,
    io::{Read, Seek},
    ops::Range,
    path::{Path, PathBuf},
};

use arrow2::{
    array::Array,
    chunk::Chunk,
    datatypes::Schema,
    io::parquet::{
        read::{infer_schema, RowGroupMetaData},
        write::FileMetaData,
    },
};
use itertools::Itertools;
use rayon::prelude::*;

use super::Error;

struct ParquetReader {}

impl ParquetReader {
    fn new(path: impl AsRef<Path> + Clone) -> Result<Self, Error> {
        let meta = std::fs::metadata(path.as_ref());
        if let Ok(meta) = meta {
            let mut files = if meta.is_dir() {
                let iter = std::fs::read_dir(path.as_ref())?;
                let entries: Result<Vec<_>, _> =
                    iter.into_iter().map_ok(|res| res.path()).collect();
                entries?
            } else {
                vec![path.as_ref().to_path_buf()]
            };
            files.sort();

            todo!()
        } else {
            Err(Error::NoEdgeLists)
        }
    }
}

fn read_file_metadata(path: impl AsRef<Path>) -> Result<FileMetaData, Error> {
    let mut file = std::fs::File::open(path.as_ref())?;
    let meta = arrow2::io::parquet::read::read_metadata(&mut file)?;
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
    let iter = arrow2::io::parquet::read::FileReader::new(
        r,
        groups.to_vec(),
        schema.clone(),
        None,
        None,
        None,
    );
    iter.map(|res| res.map_err(|e| e.into()))
}

struct ParquetOffset {
    file: PathBuf,
    row_group: RowGroupMetaData,
    range: Range<usize>,
}

struct ParquetOffsetIter<P: AsRef<Path>, I: Iterator<Item = (P, RowGroupMetaData)>> {
    row_groups: I,
    chunk_size: usize,
    current: Option<(P, RowGroupMetaData)>,
    offset: usize,
}

impl<P: AsRef<Path>, I: Iterator<Item = (P, RowGroupMetaData)>> ParquetOffsetIter<P, I> {
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
impl<P: AsRef<Path>, I: Iterator<Item = (P, RowGroupMetaData)>> Iterator
    for ParquetOffsetIter<P, I>
{
    type Item = Vec<ParquetOffset>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut chunks: Vec<ParquetOffset> = vec![];
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
mod test {}
