use crate::arrow::{
    col_graph2::Time,
    mmap::{mmap_batch, write_batches},
    DST_COLUMN, E_ADDITIONS_COLUMN, SRC_COLUMN,
};
use arrow2::{
    array::{
        Array, BooleanArray, ListArray, MutableArray, MutableBooleanArray, MutablePrimitiveArray,
        MutableStructArray, MutableUtf8Array, PrimitiveArray, StructArray, Utf8Array,
    },
    chunk::Chunk,
    datatypes::{DataType, Field, Schema},
    error::Result as ArrowResult,
    offset::OffsetsBuffer,
    types::{f16, NativeType, Offset},
};
use std::path::{Path, PathBuf};

use super::{Error, LoadChunk, TEMPORAL_PROPS_COLUMN};

pub struct EdgeFrameBuilder {
    pub(crate) edge_chunks: Vec<Chunk<Box<dyn Array>>>, // chunks for the adjacency list, these are ListArrays with a struct {eid, vid}

    edge_timestamps: Vec<Time>, // the timestamps of the edge for the current chunk
    t_props: Option<MutableStructArray>,
    edge_src_id: Vec<u64>,  // the src ids for the edge in the current chunk
    edge_dst_id: Vec<u64>,  // the dst ids for the edge in the current chunk
    edge_offsets: Vec<i64>, // the offsets of the edge list for the current chunk

    in_chunk_offset: usize, // where in the current chunk are we positioned?

    chunk_size: usize,
    chunk_offset: i64,
    pub(crate) last_update: Option<(u64, u64)>,
    edge_count: usize,
    location_path: PathBuf,
}

impl EdgeFrameBuilder {
    pub(crate) fn new<P: AsRef<Path>>(chunk_size: usize, path: P) -> Self {
        Self {
            edge_chunks: vec![],
            edge_timestamps: vec![],
            t_props: None,
            edge_src_id: vec![],
            edge_dst_id: vec![],
            edge_offsets: vec![],
            in_chunk_offset: 0,
            chunk_size,
            chunk_offset: 0,
            last_update: None,
            edge_count: 0,
            location_path: path.as_ref().to_path_buf(),
        }
    }

    fn push_chunk(&mut self) -> ArrowResult<()> {
        let mut edge_timestamps_prev = Vec::with_capacity(self.edge_timestamps.len());
        let mut edge_src_id_prev = Vec::with_capacity(self.edge_timestamps.len());
        let mut edge_dst_id_prev = Vec::with_capacity(self.edge_timestamps.len());
        let mut edge_offsets_prev = Vec::with_capacity(self.edge_offsets.len());

        let struct_arrays: Option<StructArray> = self.t_props.take().map(|t_props| t_props.into());

        std::mem::swap(&mut edge_timestamps_prev, &mut self.edge_timestamps);
        std::mem::swap(&mut edge_src_id_prev, &mut self.edge_src_id);
        std::mem::swap(&mut edge_dst_id_prev, &mut self.edge_dst_id);
        std::mem::swap(&mut edge_offsets_prev, &mut self.edge_offsets);

        // fill chunk with empty adjacency lists
        edge_offsets_prev.push(self.chunk_offset);

        let col = new_arrow_edge_list_chunk(
            edge_timestamps_prev,
            edge_src_id_prev,
            edge_dst_id_prev,
            edge_offsets_prev,
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
            Field::new(E_ADDITIONS_COLUMN, chunk[2].data_type().clone(), false),
            Field::new(TEMPORAL_PROPS_COLUMN, chunk[3].data_type().clone(), false),
        ]);
        let file_path = self
            .location_path
            .join(format!("edge_chunk_{:08}.ipc", self.edge_chunks.len()));
        write_batches(file_path.as_path(), schema, &[chunk])?;
        let mmapped_chunk = unsafe { mmap_batch(file_path.as_path(), 0)? };
        self.edge_chunks.push(mmapped_chunk);
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
            if (self.edge_count + 1) % self.chunk_size == 0 && self.last_update.is_some() {
                self.push_chunk_v2(chunk)?;
            }
            self.edge_src_id.push(src);
            self.edge_dst_id.push(dst);
            self.edge_offsets.push(self.chunk_offset);
            self.edge_count += 1;
        }

        self.in_chunk_offset += 1;
        self.chunk_offset += 1;
        self.last_update = Some((src, dst));
        Ok(())
    }

    pub(crate) fn extend_time_slice(&mut self, arr: &PrimitiveArray<i64>) {
        let time_slice = arr.values();
        self.edge_timestamps.extend_from_slice(time_slice);
        self.in_chunk_offset = 0;
    }

    pub(crate) fn extend_tprops_slice(&mut self, copy_from: &StructArray) {
        if let Some(mut t_props) = self.t_props.take() {
            let values = t_props.mut_values();
            assert!(
                values.len() > 0,
                "if they exist properties mut have at least 1 column"
            );
            copy_from
                .values()
                .iter()
                .enumerate()
                .for_each(|(i, from_col)| {
                    let into_col = &mut values[i];
                    Self::dynamic_extend_array(into_col, from_col);
                });
            // put it back
            self.t_props = Some(t_props);
        } else {
            let mut_arrays = Self::as_mut_arrays(copy_from);
            let t_props = MutableStructArray::new(copy_from.data_type().clone(), mut_arrays);
            self.t_props = Some(t_props);
            self.extend_tprops_slice(copy_from);
        }
    }

    fn as_mut_arrays(copy_from: &StructArray) -> Vec<Box<dyn MutableArray>> {
        copy_from
            .values()
            .iter()
            .map(|arr| match arr.data_type() {
                DataType::Boolean => {
                    let arr: Box<dyn MutableArray> = Box::new(MutableBooleanArray::new());
                    arr
                }
                DataType::Int8 => Box::new(MutablePrimitiveArray::<i8>::new()),
                DataType::Int16 => Box::new(MutablePrimitiveArray::<i16>::new()),
                DataType::Int32 => Box::new(MutablePrimitiveArray::<i32>::new()),
                DataType::Int64 => Box::new(MutablePrimitiveArray::<i64>::new()),
                DataType::UInt8 => Box::new(MutablePrimitiveArray::<u8>::new()),
                DataType::UInt16 => Box::new(MutablePrimitiveArray::<u16>::new()),
                DataType::UInt32 => Box::new(MutablePrimitiveArray::<u32>::new()),
                DataType::UInt64 => Box::new(MutablePrimitiveArray::<u64>::new()),
                DataType::Float16 => Box::new(MutablePrimitiveArray::<f16>::new()),
                DataType::Float32 => Box::new(MutablePrimitiveArray::<f32>::new()),
                DataType::Float64 => Box::new(MutablePrimitiveArray::<f64>::new()),
                DataType::Utf8 => Box::new(MutableUtf8Array::<i32>::new()),
                DataType::LargeUtf8 => Box::new(MutableUtf8Array::<i64>::new()),
                _ => panic!("Unsupported data type"),
            })
            .collect()
    }

    fn dynamic_extend_array(
        into_col: &mut Box<dyn MutableArray>,
        from_col: &Box<dyn Array>,
    ) -> Option<()> {
        match into_col.data_type() {
            DataType::Boolean => {
                let into_col = into_col
                    .as_mut_any()
                    .downcast_mut::<MutableBooleanArray>()?;
                let from_col = from_col
                    .as_any()
                    .downcast_ref::<BooleanArray>()?
                    .into_iter();

                for val in from_col {
                    into_col.push(val);
                }
            }
            DataType::Int8 => Self::extend_primitive_array::<i8>(into_col, from_col)?,
            DataType::Int16 => Self::extend_primitive_array::<i64>(into_col, from_col)?,
            DataType::Int32 => Self::extend_primitive_array::<i32>(into_col, from_col)?,
            DataType::Int64 => Self::extend_primitive_array::<i64>(into_col, from_col)?,
            DataType::UInt8 => Self::extend_primitive_array::<u8>(into_col, from_col)?,
            DataType::UInt16 => Self::extend_primitive_array::<u16>(into_col, from_col)?,
            DataType::UInt32 => Self::extend_primitive_array::<u32>(into_col, from_col)?,
            DataType::UInt64 => Self::extend_primitive_array::<u64>(into_col, from_col)?,
            DataType::Float16 => Self::extend_primitive_array::<f16>(into_col, from_col)?,
            DataType::Float32 => Self::extend_primitive_array::<f32>(into_col, from_col)?,
            DataType::Float64 => Self::extend_primitive_array::<f64>(into_col, from_col)?,
            DataType::Utf8 => Self::extend_utf8_array::<i32>(into_col, from_col)?,
            DataType::LargeUtf8 => Self::extend_utf8_array::<i64>(into_col, from_col)?,
            _ => {}
        }
        Some(())
    }

    fn extend_primitive_array<T: NativeType + Copy>(
        into_col: &mut Box<dyn MutableArray>,
        from_col: &Box<dyn Array>,
    ) -> Option<()> {
        let into_col = into_col
            .as_mut_any()
            .downcast_mut::<MutablePrimitiveArray<T>>()?;

        let from_col = from_col.as_any().downcast_ref::<PrimitiveArray<T>>()?;

        // happy fast path
        if into_col.validity().is_none() && from_col.validity().is_none() {
            into_col.extend_from_slice(from_col.values())
        } else {
            for val in from_col.into_iter() {
                into_col.push(val.copied());
            }
        }
        Some(())
    }

    fn extend_utf8_array<I: Offset>(
        into_col: &mut Box<dyn MutableArray>,
        from_col: &Box<dyn Array>,
    ) -> Option<()> {
        let into_col = into_col
            .as_mut_any()
            .downcast_mut::<MutableUtf8Array<I>>()?;

        let from_col = from_col.as_any().downcast_ref::<Utf8Array<I>>()?;

        for val in from_col.into_iter() {
            into_col.push(val);
        }

        Some(())
    }

    pub(crate) fn push_chunk_v2(&mut self, load_chunk: &mut LoadChunk) -> ArrowResult<()> {
        let arr = load_chunk.split_timestamps_at(self.in_chunk_offset);
        let t_props = load_chunk.split_t_props_at(self.in_chunk_offset);

        self.extend_time_slice(&arr);

        if let Some(t_props) = t_props {
            self.extend_tprops_slice(&t_props);
        }
        self.push_chunk()?;
        Ok(())
    }
}

fn new_arrow_edge_list_chunk(
    edge_timestamps: Vec<Time>,
    edge_src_ids: Vec<u64>,
    edge_dst_ids: Vec<u64>,
    edge_offsets: Vec<i64>,
    struct_arrays: Option<StructArray>,
) -> ArrowResult<Chunk<Box<dyn Array>>> {
    let timestamp_values = PrimitiveArray::from_vec(edge_timestamps);
    let dtype = <ListArray<i64>>::default_datatype(DataType::Int64);
    let offsets = OffsetsBuffer::try_from(edge_offsets)?;

    let timestamps: Box<dyn Array> = Box::new(ListArray::new(
        dtype,
        offsets.clone(),
        Box::new(timestamp_values),
        None,
    ));

    let t_props = struct_arrays.map(|t_props| {
        let dtype = <ListArray<i64>>::default_datatype(t_props.data_type().clone());
        let t_props = Box::new(ListArray::new(dtype, offsets, Box::new(t_props), None));
        t_props
    });
    let src_col: Box<dyn Array> = Box::new(PrimitiveArray::from_vec(edge_src_ids));
    let dst_col: Box<dyn Array> = Box::new(PrimitiveArray::from_vec(edge_dst_ids));

    let mut arrays = vec![src_col, dst_col, timestamps];

    if let Some(t_props) = t_props {
        arrays.push(t_props);
    }

    Ok(Chunk::new(arrays))
}
