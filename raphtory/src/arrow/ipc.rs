use std::{fs::File, path::Path};

use arrow2::{array::Array, chunk::Chunk, datatypes::Schema, io::ipc::read};

use super::Error;

pub fn read_batch<P: AsRef<Path>>(path: P) -> Result<Chunk<Box<dyn Array>>, Error> {
    let mut file = File::open(path)?;
    let meta = read::read_file_metadata(&mut file)?;
    let mut reader = read::FileReader::new(file, meta, None, None);
    let chunk = reader.next().ok_or_else(|| Error::NoEdgeLists)??;
    Ok(chunk)
}

pub fn read_batch_with_projection<P: AsRef<Path>>(
    path: P,
    projection: Vec<usize>,
) -> Result<Chunk<Box<dyn Array>>, Error> {
    let mut file = File::open(path)?;
    let meta = read::read_file_metadata(&mut file)?;
    let mut reader = read::FileReader::new(file, meta, Some(projection), None);
    let chunk = reader.next().ok_or_else(|| Error::NoEdgeLists)??;
    Ok(chunk)
}

pub fn read_schema<P: AsRef<Path>>(path: P) -> Result<Schema, Error> {
    let mut file = File::open(path)?;
    let meta = read::read_file_metadata(&mut file)?;
    Ok(meta.schema)
}
