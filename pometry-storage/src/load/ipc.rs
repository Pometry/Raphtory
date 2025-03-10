use std::{fs::File, path::Path};

use crate::arrow2::{
    array::{Array, PrimitiveArray},
    buffer::Buffer,
    datatypes::ArrowSchema as Schema,
    io::ipc::read,
    legacy::error::{PolarsError as Error, PolarsResult as ArrowResult},
    record_batch::RecordBatchT,
    types::NativeType,
};

pub fn read_batch<P: AsRef<Path>>(path: P) -> Result<RecordBatchT<Box<dyn Array>>, Error> {
    let path = path.as_ref();
    let mut file = File::open(path)?;
    let meta = read::read_file_metadata(&mut file)?;
    let mut reader = read::FileReader::new(file, meta, None, None);
    let chunk = reader.next().ok_or_else(|| {
        Error::ShapeMismatch(format!("file {:?} must contain extactly 1 chunk ", path).into())
    })??;
    Ok(chunk)
}

pub fn read_batch_with_projection<P: AsRef<Path>>(
    path: P,
    projection: Vec<usize>,
) -> Result<RecordBatchT<Box<dyn Array>>, Error> {
    let path = path.as_ref();
    let mut file = File::open(path)?;
    let meta = read::read_file_metadata(&mut file)?;
    let mut reader = read::FileReader::new(file, meta, Some(projection), None);
    let chunk = reader.next().ok_or_else(|| {
        Error::ShapeMismatch(format!("file {:?} must contain extactly 1 chunk ", path).into())
    })??;
    Ok(chunk)
}

pub fn read_schema<P: AsRef<Path>>(path: P) -> Result<Schema, Error> {
    let mut file = File::open(path)?;
    let meta = read::read_file_metadata(&mut file)?;
    Ok(meta.schema.as_ref().clone())
}

pub fn read_buffer<T: NativeType>(file_path: impl AsRef<Path>) -> ArrowResult<Buffer<T>> {
    let path = file_path.as_ref();
    let chunk = read_batch(path)?;
    let buffer = &chunk[0]
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or(Error::ShapeMismatch(
            format!(
                "failed to read buffer of type: {:?} from file: {:?}",
                T::PRIMITIVE,
                file_path.as_ref()
            )
            .into(),
        ))?;
    Ok(buffer.values().clone())
}
