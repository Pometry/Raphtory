use crate::arrow::{
    mmap::{mmap_batch, write_batches},
    prepare_graph_dir, DST_COLUMN, EDGE_OVERFLOW_COLUMN, E_ADDITIONS_COLUMN, SRC_COLUMN,
};
use arrow2::{
    array::{Array, ListArray, MutableArray, MutableStructArray, PrimitiveArray, StructArray},
    chunk::Chunk,
    datatypes::{DataType, Field, Schema},
    error::Result as ArrowResult,
    offset::OffsetsBuffer,
};
use std::{
    cmp::min,
    mem,
    path::{Path, PathBuf},
};

use crate::arrow::{
    col_graph2::Edge, edge_chunk::EdgeChunk,
    edge_frame_builder::edge_overflow_builder::EdgeOverflowChunk, Error, LoadChunk, Time,
    TEMPORAL_PROPS_COLUMN,
};

use super::edge_overflow_builder::EdgeOverflowBuilder;

pub struct EdgeFrameBuilder {
    pub(crate) edge_chunks: Vec<EdgeChunk>, // chunks for the adjacency list, these are ListArrays with a struct {eid, vid}
    pub(crate) overflow_chunks: Vec<EdgeOverflowChunk>,

    t_props: Option<MutableStructArray>,
    edge_src_id: Vec<u64>,  // the src ids for the edge in the current chunk
    edge_dst_id: Vec<u64>,  // the dst ids for the edge in the current chunk
    edge_offsets: Vec<i64>, // the offsets of the edge list for the current chunk
    edge_overflow: Vec<Option<u64>>, // the overflow ids for the edge in the current chunk

    in_chunk_offset: usize, // where in the current chunk are we positioned?
    max_list_size: usize,
    in_chunk_overflow: usize,
    no_edge_updates: usize,

    overflow_frame: Option<EdgeOverflowBuilder>,

    chunk_size: usize,
    chunk_offset: i64,
    pub(crate) last_update: Option<(u64, u64)>,
    edge_count: usize,
    location_path: PathBuf,
}

impl EdgeFrameBuilder {
    pub(crate) fn new<P: AsRef<Path>>(chunk_size: usize, max_list_size: usize, path: P) -> Self {
        Self {
            edge_chunks: vec![],
            overflow_chunks: vec![],
            t_props: None,
            edge_src_id: vec![],
            edge_dst_id: vec![],
            edge_offsets: vec![],
            edge_overflow: vec![],

            in_chunk_offset: 0,
            max_list_size,
            in_chunk_overflow: 0,
            no_edge_updates: 0,
            overflow_frame: None,

            chunk_size,
            chunk_offset: 0,
            last_update: None,
            edge_count: 0,
            location_path: path.as_ref().to_path_buf(),
        }
    }

    fn push_chunk(&mut self) -> ArrowResult<()> {
        let mut edge_src_id_prev = Vec::with_capacity(self.edge_src_id.len());
        let mut edge_dst_id_prev = Vec::with_capacity(self.edge_dst_id.len());
        let mut edge_offsets_prev = Vec::with_capacity(self.edge_offsets.len());
        let mut edge_overflows_prev = Vec::with_capacity(self.edge_overflow.len());

        let struct_arrays: Option<StructArray> = self.t_props.take().map(|t_props| t_props.into());

        std::mem::swap(&mut edge_src_id_prev, &mut self.edge_src_id);
        std::mem::swap(&mut edge_dst_id_prev, &mut self.edge_dst_id);
        std::mem::swap(&mut edge_offsets_prev, &mut self.edge_offsets);
        std::mem::swap(&mut edge_overflows_prev, &mut self.edge_overflow);

        // fill chunk with empty adjacency lists
        edge_offsets_prev.push(self.chunk_offset);

        let col = new_arrow_edge_list_chunk(
            edge_src_id_prev,
            edge_dst_id_prev,
            edge_offsets_prev,
            edge_overflows_prev,
            struct_arrays,
        )?;

        self.persist_and_mmap_adj_chunk(col)?;
        self.chunk_offset = 0i64;
        Ok(())
    }

    fn persist_and_mmap_adj_chunk(&mut self, chunk: Chunk<Box<dyn Array>>) -> ArrowResult<()> {
        let schema = Schema::from(vec![
            Field::new(SRC_COLUMN, chunk[0].data_type().clone(), false),
            Field::new(DST_COLUMN, chunk[1].data_type().clone(), false),
            Field::new(EDGE_OVERFLOW_COLUMN, chunk[2].data_type().clone(), false),
            Field::new(TEMPORAL_PROPS_COLUMN, chunk[3].data_type().clone(), false),
        ]);
        let file_path = self
            .location_path
            .join(format!("edge_chunk_{:08}.ipc", self.edge_chunks.len()));
        write_batches(file_path.as_path(), schema, &[chunk])?;
        let mmapped_chunk = unsafe { mmap_batch(file_path.as_path(), 0)? };
        let overflows = mem::take(&mut self.overflow_chunks);
        self.in_chunk_overflow = 0;
        self.edge_chunks
            .push(EdgeChunk::new(mmapped_chunk, overflows));
        Ok(())
    }

    pub(crate) fn push_update_with_props(
        &mut self,
        src: u64,
        dst: u64,
        chunk: &mut LoadChunk,
    ) -> Result<(), Error> {
        if self
            .last_update
            .filter(|(prev_src, prev_dst)| prev_src != &src || prev_dst != &dst)
            .is_some()
            || self.last_update.is_none()
        {
            if self.no_edge_updates > self.max_list_size {
                self.extend_chunk_with_time_and_props(chunk)?;
                if let Some(builder) = self.overflow_frame.take() {
                    self.overflow_chunks.push(builder.finalize()?);
                }
            }
            if self.edge_count % self.chunk_size == 0 && self.last_update.is_some() {
                self.write_down_chunk(chunk)?;
            }
            self.edge_src_id.push(src);
            self.edge_dst_id.push(dst);
            self.edge_offsets.push(self.chunk_offset);
            self.edge_count += 1;
            self.no_edge_updates = 0;
            self.edge_overflow.push(None);
        }
        self.no_edge_updates += 1;
        self.in_chunk_offset += 1;
        self.chunk_offset += 1;
        self.last_update = Some((src, dst));
        Ok(())
    }

    pub(crate) fn new_overflow_builder_with_chunk(
        &mut self,
        chunk: &StructArray,
    ) -> Result<(), Error> {
        let file_path = self
            .location_path
            .join(format!("edge_chunk_{:08}_overflow", self.edge_chunks.len()))
            .join(format!(
                "edge_chunk_overflow_{:08}.ipc",
                self.in_chunk_overflow
            ));
        if self.in_chunk_overflow == 0 {
            prepare_graph_dir(file_path.parent().unwrap())?;
        }
        self.in_chunk_overflow += 1;
        let dt = chunk.data_type();

        let schema = Schema::from(vec![Field::new(
            TEMPORAL_PROPS_COLUMN,
            DataType::LargeList(Box::new(Field::new("value", dt.clone(), false))),
            false,
        )]);
        let mut builder = EdgeOverflowBuilder::new(file_path, schema, self.max_list_size)?;
        builder.push_chunk(chunk)?;
        self.overflow_frame.replace(builder);
        Ok(())
    }

    pub(crate) fn extend_tprops_slice(&mut self, copy_from: &StructArray) -> Result<(), Error> {
        if self.no_edge_updates > self.max_list_size {
            let overflowed_size = min(
                self.in_chunk_offset,
                self.no_edge_updates - self.max_list_size,
            );
            let overflow = copy_from
                .clone()
                .sliced(self.in_chunk_offset - overflowed_size, overflowed_size);
            // last edge has overflow
            match self.overflow_frame.as_mut() {
                Some(builder) => {
                    builder.push_chunk(&overflow)?;
                }
                None => {
                    self.new_overflow_builder_with_chunk(&overflow)?;
                    let last = self.edge_overflow.last_mut().unwrap();
                    last.replace(self.in_chunk_overflow as u64);
                }
            }
            let non_overflow = copy_from
                .clone()
                .sliced(0, self.in_chunk_offset - overflowed_size);
            super::extend_tprops_slice(&mut self.t_props, &non_overflow);
        } else {
            super::extend_tprops_slice(&mut self.t_props, copy_from);
        }
        self.in_chunk_offset = 0;
        Ok(())
    }

    fn extend_chunk_with_time_and_props(
        &mut self,
        load_chunk: &mut LoadChunk,
    ) -> Result<(), Error> {
        let t_props = load_chunk.split_t_props_at(self.in_chunk_offset);

        if let Some(t_props) = t_props {
            self.extend_tprops_slice(&t_props)?;
        }
        Ok(())
    }

    pub(crate) fn write_down_chunk(&mut self, load_chunk: &mut LoadChunk) -> Result<(), Error> {
        self.extend_chunk_with_time_and_props(load_chunk)?;
        self.push_chunk()?;
        Ok(())
    }
}

fn new_arrow_edge_list_chunk(
    edge_src_ids: Vec<u64>,
    edge_dst_ids: Vec<u64>,
    edge_offsets: Vec<i64>,
    edge_overflows: Vec<Option<u64>>,
    struct_arrays: Option<StructArray>,
) -> ArrowResult<Chunk<Box<dyn Array>>> {
    let dtype = <ListArray<i64>>::default_datatype(DataType::Int64);
    let offsets = OffsetsBuffer::try_from(edge_offsets)?;

    let t_props = struct_arrays.map(|t_props| {
        let dtype = <ListArray<i64>>::default_datatype(t_props.data_type().clone());
        let t_props = Box::new(ListArray::new(dtype, offsets, Box::new(t_props), None));
        t_props
    });
    let src_col: Box<dyn Array> = Box::new(PrimitiveArray::from_vec(edge_src_ids));
    let dst_col: Box<dyn Array> = Box::new(PrimitiveArray::from_vec(edge_dst_ids));
    let ov_col: Box<dyn Array> = PrimitiveArray::from(edge_overflows).to_boxed();

    let mut arrays = vec![src_col, dst_col, ov_col];

    if let Some(t_props) = t_props {
        arrays.push(t_props);
    }

    Ok(Chunk::new(arrays))
}
