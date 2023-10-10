use std::{fs::File, path::Path};

use arrow2::{
    array::{MutableStructArray, StructArray},
    datatypes::Schema,
    io::ipc::write::{FileWriter, WriteOptions},
};

use crate::arrow::Error;

pub(crate) struct EdgeOverflowBuilder {
    t_props: Option<MutableStructArray>,
    writer: FileWriter<File>,
    max_list_size: usize,
}

impl EdgeOverflowBuilder {
    pub(crate) fn new<P: AsRef<Path>>(
        path: P,
        schema: Schema,
        max_list_size: usize,
    ) -> Result<Self, Error> {
        let writer =
            FileWriter::try_new(File::create(path)?, schema, None, WriteOptions::default())?;
        Ok(Self {
            t_props: None,
            writer,
            max_list_size,
        })
    }

    pub(crate) fn push_chunk(&mut self, chunk: &StructArray) -> Result<(), Error> {
        todo!()
    }

    pub(crate) fn finalize(&mut self) -> Result<(), Error> {
        todo!()
    }
}
