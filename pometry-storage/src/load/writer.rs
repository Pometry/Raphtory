use crate::{
    arrow2::{
        array::Array,
        datatypes::{ArrowSchema as Schema, Field},
        record_batch::RecordBatch as Chunk,
    },
    file_prefix::GraphPaths,
    load::mmap::{mmap_batch, write_batches},
    RAError,
};
use std::{fs::File, path::Path, sync::Arc, thread::JoinHandle};

#[derive(Debug)]
pub struct ThreadedWriter<A: Array> {
    chunks: Vec<A>,
    in_progress: Option<JoinHandle<Result<A, RAError>>>,
    path_type: GraphPaths,
    graph_dir: Arc<Path>,
}

pub struct MemWriter<A: Array> {
    chunks: Vec<A>,
}

pub trait ChunkWriter {
    type Chunk: Array;

    fn write_chunk(&mut self, value: Self::Chunk) -> Result<(), RAError>;

    fn finish(self) -> Result<Vec<Self::Chunk>, RAError>;
}

impl<A: Array> ChunkWriter for MemWriter<A> {
    type Chunk = A;

    fn write_chunk(&mut self, value: Self::Chunk) -> Result<(), RAError> {
        self.chunks.push(value);
        Ok(())
    }

    fn finish(self) -> Result<Vec<Self::Chunk>, RAError> {
        Ok(self.chunks)
    }
}

impl<A: Array + Clone> ChunkWriter for ThreadedWriter<A> {
    type Chunk = A;

    fn write_chunk(&mut self, value: Self::Chunk) -> Result<(), RAError> {
        if let Some(last) = self.in_progress.take() {
            let chunk = last.join().unwrap()?;
            self.chunks.push(chunk);
        }
        let base_path = self.graph_dir.clone();
        let path_type = self.path_type;
        let id = self.chunks.len();
        let handle =
            std::thread::spawn(move || write_and_mmap_batch(path_type, base_path, value, id));
        self.in_progress = Some(handle);
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<Self::Chunk>, RAError> {
        if let Some(last) = self.in_progress {
            let chunk = last.join().unwrap()?;
            self.chunks.push(chunk);
        }
        Ok(self.chunks)
    }
}

impl<A: Array> ThreadedWriter<A> {
    pub fn new(graph_dir: impl AsRef<Path>, path_type: GraphPaths) -> Self {
        Self {
            chunks: vec![],
            in_progress: None,
            path_type,
            graph_dir: graph_dir.as_ref().into(),
        }
    }
}

pub(crate) fn write_and_mmap_batch<A: Array + Clone>(
    path_type: GraphPaths,
    base_path: impl AsRef<Path>,
    batch: A,
    id: usize,
) -> Result<A, RAError> {
    let path = path_type.to_path(&base_path, id);
    write_batch(path_type, base_path, batch, id)?;
    let value = unsafe { mmap_batch(&path, 0)? }[0]
        .as_any()
        .downcast_ref::<A>()
        .unwrap()
        .clone();
    Ok(value)
}

pub(crate) fn write_batch<A: Array + Clone>(
    path_type: GraphPaths,
    base_path: impl AsRef<Path>,
    batch: A,
    id: usize,
) -> Result<(), RAError> {
    let schema = Schema::from(vec![Field::new(
        path_type.as_ref(),
        batch.data_type().clone(),
        false,
    )]);
    let path = path_type.to_path(&base_path, id);
    write_batches(
        File::create_new(path)?,
        schema,
        &[Chunk::new(vec![batch.to_boxed()])],
    )?;
    Ok(())
}
