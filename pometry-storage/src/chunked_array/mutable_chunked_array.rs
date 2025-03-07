use super::chunked_array::NonNull;
use crate::{
    arrow2::{
        array::{Array, PrimitiveArray, StructArray},
        datatypes::{ArrowDataType as DataType, Field},
        offset::OffsetsBuffer,
        types::NativeType,
    },
    chunked_array::{chunked_array::ChunkedArray, chunked_offsets::ChunkedOffsets},
    file_prefix::GraphPaths,
    load::{
        mmap::{mmap_buffer, write_buffer},
        writer::{ChunkWriter, ThreadedWriter},
    },
    RAError,
};
use polars_arrow::compute::concatenate::concatenate;
use raphtory_api::compute::par_cum_sum;
use std::{
    fs::File,
    mem,
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub struct MutPrimitiveChunkedArray<T, W = ThreadedWriter<PrimitiveArray<T>>> {
    chunks: W,
    current_chunk: Vec<T>,
    chunk_size: usize,
}

impl<T: NativeType> MutPrimitiveChunkedArray<T> {
    pub fn new_persisted(
        chunk_size: usize,
        graph_dir: impl AsRef<Path>,
        path_type: GraphPaths,
    ) -> Self {
        Self {
            chunks: ThreadedWriter::new(graph_dir, path_type),
            current_chunk: vec![],
            chunk_size,
        }
    }
}

impl<T: NativeType, W: ChunkWriter<Chunk = PrimitiveArray<T>>> MutPrimitiveChunkedArray<T, W> {
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    pub fn push(&mut self, value: T) -> Result<(), RAError> {
        self.current_chunk.push(value);

        if self.current_chunk.len() == self.chunk_size {
            let chunk = mem::take(&mut self.current_chunk);
            self.push_chunk(chunk)?;
        }
        Ok(())
    }

    pub fn push_chunk(&mut self, chunk: Vec<T>) -> Result<(), RAError> {
        if !chunk.is_empty() {
            let array = PrimitiveArray::from_vec(chunk);
            self.chunks.write_chunk(array)?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<ChunkedArray<PrimitiveArray<T>, NonNull>, RAError> {
        let chunk = mem::take(&mut self.current_chunk);
        self.push_chunk(chunk)?;
        let chunks = self.chunks.finish()?;
        Ok(ChunkedArray::new(chunks, self.chunk_size))
    }
}

pub struct MutChunkedStructArray<W = ThreadedWriter<StructArray>> {
    chunks: W,
    columns: Vec<Field>,
    chunk_size: usize,
}

impl MutChunkedStructArray {
    pub fn new_persisted(
        chunk_size: usize,
        graph_dir: impl AsRef<Path>,
        path_type: GraphPaths,
        columns: Vec<Field>,
    ) -> Self {
        Self {
            chunk_size,
            chunks: ThreadedWriter::new(graph_dir, path_type),
            columns,
        }
    }
}

impl<W: ChunkWriter<Chunk = StructArray>> MutChunkedStructArray<W> {
    pub fn push_chunk(&mut self, chunk: Vec<Box<dyn Array>>) -> Result<(), RAError> {
        if !chunk.is_empty() {
            let array = StructArray::new(DataType::Struct(self.columns.clone()), chunk, None);
            self.chunks.write_chunk(array)
        } else {
            Ok(())
        }
    }

    pub fn finish(self) -> Result<ChunkedArray<StructArray>, RAError> {
        let chunks = self.chunks.finish()?;
        Ok(ChunkedArray::new(chunks, self.chunk_size))
    }

    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }
}

pub struct ChunkedStructArrayBuilder<W = ThreadedWriter<StructArray>> {
    builder: MutChunkedStructArray<W>,
    current_chunk: Vec<Box<dyn Array>>,
}

impl ChunkedStructArrayBuilder {
    pub fn new_persisted(
        chunk_size: usize,
        graph_dir: impl AsRef<Path>,
        path_type: GraphPaths,
        columns: Vec<Field>,
    ) -> Self {
        let num_fields = columns.len();
        let builder =
            MutChunkedStructArray::new_persisted(chunk_size, graph_dir, path_type, columns);
        Self {
            builder,
            current_chunk: Vec::with_capacity(num_fields),
        }
    }
}

impl<W: ChunkWriter<Chunk = StructArray>> ChunkedStructArrayBuilder<W> {
    fn current_chunk_len(&self) -> usize {
        self.current_chunk.first().map(|v| v.len()).unwrap_or(0)
    }

    pub fn push(&mut self, chunk: &mut [Box<dyn Array>]) -> Result<(), RAError> {
        if !chunk.is_empty() {
            while !chunk[0].is_empty() {
                let rem_in_chunk = chunk[0].len();
                let rem_in_current =
                    (self.builder.chunk_size - self.current_chunk_len()).min(rem_in_chunk);
                if self.current_chunk.is_empty() {
                    for new in chunk.iter_mut() {
                        self.current_chunk.push(new.sliced(0, rem_in_current));
                        new.slice(rem_in_current, rem_in_chunk - rem_in_current);
                    }
                } else {
                    for (old, new) in self.current_chunk.iter_mut().zip(chunk.iter_mut()) {
                        let old_previous = old.clone();
                        *old = concatenate(&[
                            old_previous.as_ref(),
                            new.sliced(0, rem_in_current).as_ref(),
                        ])?;
                        new.slice(rem_in_current, rem_in_chunk - rem_in_current);
                    }
                }
                if self.current_chunk_len() == self.builder.chunk_size {
                    let mut current_chunk = Vec::with_capacity(self.builder.columns.len());
                    mem::swap(&mut self.current_chunk, &mut current_chunk);
                    self.builder.push_chunk(current_chunk)?;
                }
            }
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<ChunkedArray<StructArray>, RAError> {
        self.builder.push_chunk(self.current_chunk)?;
        self.builder.finish()
    }
}

#[derive(Debug)]
pub struct MutChunkedOffsets {
    chunks: Vec<OffsetsBuffer<i64>>,
    current_chunk: Vec<usize>,
    chunk_size: usize,
    path: Option<(GraphPaths, PathBuf)>,
    mmap: bool,
}

impl MutChunkedOffsets {
    pub fn new(chunk_size: usize, path: Option<(GraphPaths, PathBuf)>, mmap: bool) -> Self {
        Self {
            chunks: vec![],
            current_chunk: vec![0],
            chunk_size,
            path,
            mmap,
        }
    }
    pub fn len(&self) -> usize {
        self.chunks.len() * self.chunk_size + self.current_chunk.len() - 1
    }

    pub fn push(&mut self, value: usize) -> Result<(), RAError> {
        self.current_chunk.push(value);
        // keep the last two values to preserve the chunk boundary as the last value may be updated
        if self.current_chunk.len() == self.chunk_size + 2 {
            let mut chunk = mem::take(&mut self.current_chunk);
            let last = chunk.pop().unwrap();
            let before_last = *chunk.last().unwrap();
            self.current_chunk.push(before_last);
            self.current_chunk.push(last);
            self.push_chunk(chunk)?;
        }
        Ok(())
    }

    pub fn push_chunk(&mut self, chunk: Vec<usize>) -> Result<(), RAError> {
        assert!(chunk.len() <= self.chunk_size + 1);
        let chunk: Vec<_> = chunk.into_iter().map(|v| v as i64).collect();
        if chunk.len() > 1 || self.chunks.is_empty() {
            let array = unsafe { OffsetsBuffer::new_unchecked(chunk.into()) };
            self.push_offsets_chunk(array)?;
        }
        Ok(())
    }

    pub fn push_offsets_chunk(&mut self, mut chunk: OffsetsBuffer<i64>) -> Result<(), RAError> {
        if let Some((path_type, base_dir)) = self.path.as_ref() {
            let chunk_id = self.chunks.len();
            let path = path_type.to_path(base_dir, chunk_id);
            write_buffer(File::create_new(&path)?, chunk.buffer().clone())?;
            if self.mmap {
                let buffer = unsafe { mmap_buffer(&path, 0)? };
                chunk = unsafe { OffsetsBuffer::new_unchecked(buffer) };
            }
        }
        self.chunks.push(chunk);
        Ok(())
    }

    pub fn finish(mut self) -> Result<ChunkedOffsets, RAError> {
        let chunk = mem::take(&mut self.current_chunk);
        self.push_chunk(chunk)?;
        Ok(ChunkedOffsets::new(self.chunks, self.chunk_size))
    }

    pub fn last(&self) -> usize {
        *self.current_chunk.last().unwrap()
    }

    pub fn update_last(&mut self, offset: usize) {
        *self.current_chunk.last_mut().unwrap() = offset;
    }

    pub fn push_count(&mut self, count: usize) -> Result<(), RAError> {
        let last = self.last();
        self.push(last + count)
    }

    pub fn push_empty(&mut self) -> Result<(), RAError> {
        let last = self.last();
        self.push(last)
    }
}

pub struct ChunkedOffsetsBuilder<V> {
    last_value: Option<V>,
    builder: MutChunkedOffsets,
}

impl<V: PartialEq + Copy> ChunkedOffsetsBuilder<V> {
    pub fn chunk_size(&self) -> usize {
        self.builder.chunk_size
    }
    pub fn new_persisted(
        chunk_size: usize,
        graph_dir: impl AsRef<Path>,
        path_type: GraphPaths,
    ) -> Self {
        let builder = MutChunkedOffsets::new(
            chunk_size,
            Some((path_type, graph_dir.as_ref().to_path_buf())),
            true,
        );
        Self {
            builder,
            last_value: None,
        }
    }
    fn last_offset_needs_update(&self, new_v: &V) -> bool {
        self.last_value.as_ref() == Some(new_v)
    }

    pub fn push_counts(
        &mut self,
        counts: &mut [usize],
        first_v: V,
        last_v: V,
    ) -> Result<(), RAError> {
        if let Some(first_c) = counts.first_mut() {
            *first_c += self.builder.last(); // keep cumulative total
            par_cum_sum(counts); // turn counts into offsets
            let mut counts_iter = counts.iter();
            if self.last_offset_needs_update(&first_v) {
                // The first value of the new chunk is the same as the last value of the previous chunk
                self.builder.update_last(*counts_iter.next().unwrap());
            }
            for o in counts_iter {
                self.builder.push(*o)?;
            }
            self.last_value = Some(last_v);
        }
        Ok(())
    }

    pub fn ensure_size(&mut self, len: usize) -> Result<(), RAError> {
        let extra_len = len.saturating_sub(self.builder.len());
        let last_v = self.builder.last();
        for _ in 0..extra_len {
            self.builder.push(last_v)?;
        }
        Ok(())
    }

    pub fn finish(self) -> Result<ChunkedOffsets, RAError> {
        self.builder.finish()
    }
}

#[cfg(test)]
mod test {
    use crate::{chunked_array::mutable_chunked_array::MutChunkedOffsets, prelude::ArrayOps};
    use itertools::Itertools;
    use polars_arrow::offset::OffsetsBuffer;

    #[test]
    fn test_push() {
        let mut builder = MutChunkedOffsets::new(2, None, false);
        let offsets = [3, 5, 7, 9, 11];
        for o in offsets {
            builder.push(o).unwrap();
        }
        let res = builder.finish().unwrap();
        let chunks = [vec![0, 3, 5], vec![5, 7, 9], vec![9, 11]]
            .into_iter()
            .map(|c| OffsetsBuffer::try_from(c).unwrap())
            .collect_vec();
        assert_eq!(res.iter_chunks().cloned().collect_vec(), chunks);
    }

    #[test]
    fn test_push_exact_chunk() {
        let mut builder = MutChunkedOffsets::new(2, None, false);
        let offsets = [3, 5, 7, 9];
        for o in offsets {
            builder.push(o).unwrap();
        }
        let res = builder.finish().unwrap();
        // when last chunk is exactly full, need to add a trivial extra chunk to avoid a lot of special cases elsewhere
        let chunks = [vec![0, 3, 5], vec![5, 7, 9], vec![9]]
            .into_iter()
            .map(|c| OffsetsBuffer::try_from(c).unwrap())
            .collect_vec();
        assert_eq!(res.iter_chunks().cloned().collect_vec(), chunks);
    }
}
