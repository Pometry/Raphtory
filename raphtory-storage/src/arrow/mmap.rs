use arrow2::{
    array::{Array, Int32Array, Utf8Array},
    chunk::Chunk,
    datatypes::{DataType, Field, Schema},
    error::Result,
    io::ipc::{read, write},
    mmap::{mmap_dictionaries_unchecked, mmap_unchecked},
};
use memmap2::Mmap;
use std::{fs::File, path::Path, sync::Arc};

pub fn write_batches<P: AsRef<Path>>(
    path: P,
    schema: Schema,
    chunks: &[Chunk<Box<dyn Array>>],
) -> Result<()> {
    let file = File::create(path)?;

    let options = write::WriteOptions { compression: None };
    let mut writer = write::FileWriter::new(file, schema, None, options);

    writer.start()?;
    for chunk in chunks {
        writer.write(chunk, None)?
    }
    writer.finish()
}

/// Only safe if file is never modified!
///
/// Returns mmapped chunks from an arrow-ipc file.
///
/// # Arguments
///
/// * `path` - file path to valid arrow-ipc file
/// * `chunk_id` - chunk id to map
pub unsafe fn mmap_batches<P: AsRef<Path>>(
    path: P,
    chunk_id: usize,
) -> Result<Chunk<Box<dyn Array>>> {
    let file = File::open(path)?;
    let mmap = Arc::new(Mmap::map(&file)?);
    // read the metadata
    let metadata = read::read_file_metadata(&mut std::io::Cursor::new(mmap.as_ref()))?;

    // mmap the dictionaries
    let dictionaries = unsafe { mmap_dictionaries_unchecked(&metadata, mmap.clone())? };
    let chunk = unsafe { mmap_unchecked(&metadata, &dictionaries, mmap.clone(), chunk_id)? };
    Ok(chunk)
}

#[cfg(test)]
mod test {
    use super::write_batches;
    use crate::arrow::mmap::mmap_batches;
    use arrow2::{
        array::{Int32Array, Utf8Array},
        chunk::Chunk,
        datatypes::{DataType, Field, Schema},
    };
    use tempfile::NamedTempFile;

    #[test]
    fn test_roundtrip() {
        let tf = NamedTempFile::new().unwrap();
        let file_path = tf.path();
        // create a batch
        let schema = Schema::from(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);

        let a = Int32Array::from_slice([1, 2, 3, 4, 5]);
        let b = Utf8Array::<i32>::from_slice(["a", "b", "c", "d", "e"]);

        let chunks = [Chunk::try_new(vec![a.boxed(), b.boxed()]).unwrap()];

        // write it
        write_batches(file_path, schema, &chunks).unwrap();
        let mapped = unsafe { mmap_batches(file_path, 0) }.unwrap();
        assert_eq!(chunks[0][0], mapped[0]);
    }
}
