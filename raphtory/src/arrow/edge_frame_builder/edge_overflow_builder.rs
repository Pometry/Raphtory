use std::{
    fs::{File, OpenOptions},
    path::Path,
};

use crate::arrow::edge_frame_builder::extend_tprops_slice;
use arrow2::{
    array::{Array, MutableArray, MutableStructArray, PrimitiveArray, StructArray},
    chunk::Chunk,
    datatypes::Schema,
    io::ipc::write::{FileWriter, WriteOptions},
    types::NativeType,
};

use crate::arrow::{mmap::mmap_batches, Error, Time};

#[derive(Debug)]
pub(crate) struct EdgeOverflowChunk {
    chunks: Vec<Chunk<Box<dyn Array>>>,
}

impl EdgeOverflowChunk {
    pub fn new(chunks: Vec<Chunk<Box<dyn Array>>>) -> Self {
        Self { chunks }
    }
    pub fn timestamps(&self) -> impl Iterator<Item = &[Time]> {
        self.iter_arrays().map(|array| {
            array.values()[0]
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap()
                .values()
                .as_slice()
        })
    }

    pub fn iter_arrays(&self) -> impl Iterator<Item = &StructArray> {
        self.chunks
            .iter()
            .map(|chunk| chunk[0].as_any().downcast_ref().unwrap())
    }

    pub(crate) fn temporal_primitive_prop<T: NativeType>(
        &self,
        prop_id: usize,
    ) -> impl Iterator<Item = Option<&T>> + '_ {
        self.iter_arrays()
            .flat_map(move |array| array.values().get(prop_id))
            .flat_map(|array| {
                array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<T>>()
                    .into_iter()
                    .flat_map(|v| v.iter())
            })
    }
}

pub(crate) struct EdgeOverflowBuilder {
    t_props: Option<MutableStructArray>,
    writer: FileWriter<File>,
    max_list_size: usize,
    num_chunks: usize,
}

impl EdgeOverflowBuilder {
    pub(crate) fn new<P: AsRef<Path>>(
        path: P,
        schema: Schema,
        max_list_size: usize,
    ) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let writer = FileWriter::try_new(file, schema, None, WriteOptions::default())?;
        Ok(Self {
            t_props: None,
            writer,
            max_list_size,
            num_chunks: 0,
        })
    }

    fn len(&self) -> usize {
        match &self.t_props {
            None => 0,
            Some(v) => v.len(),
        }
    }

    fn write_values(&mut self) -> Result<(), Error> {
        let mut values = self.t_props.take().unwrap();
        println!("dtype: {:?}", values.data_type());
        self.writer
            .write(&Chunk::new(vec![values.as_box()]), None)?;
        self.num_chunks += 1;
        Ok(())
    }

    pub(crate) fn push_chunk(&mut self, chunk: &StructArray) -> Result<(), Error> {
        extend_tprops_slice(&mut self.t_props, chunk);
        if self.len() > self.max_list_size {
            // we don't really care if the lists are different size, just write the whole thing
            self.write_values()?;
        }
        Ok(())
    }

    pub(crate) fn finalize(mut self) -> Result<EdgeOverflowChunk, Error> {
        self.write_values()?;
        self.writer.finish()?;
        let file = self.writer.into_inner();
        println!("file: {file:?}");
        let chunks = unsafe { mmap_batches(&file, 0..self.num_chunks)? };
        Ok(EdgeOverflowChunk { chunks })
    }
}
