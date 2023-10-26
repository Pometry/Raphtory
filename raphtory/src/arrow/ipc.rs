use std::{path::Path, fs::File};

use arrow2::{chunk::Chunk, array::Array, io::ipc::read};

use super::Error;

pub fn read_batch<P: AsRef<Path>>(
    path: P,
    // chunk_id: usize,
) -> Result<Chunk<Box<dyn Array>>, Error> {
    let mut file = File::open(path)?;
    let meta = read::read_file_metadata(&mut file)?;
    let mut reader= read::FileReader::new(file, meta, None, None);
    let chunk = reader.next().ok_or_else(|| Error::NoEdgeLists)??;
    Ok(chunk)
}
