use crate::{
    arrow::{
        chunked_array::{
            chunked_array::{ChunkedArray, NonNull},
            list_array::ChunkedListArray,
            ChunkedArraySlice,
        },
        graph_impl::tprops::{new_primitive_tprop_column, new_str_tprop_column, TPropColumn},
        prelude::{ArrayOps, BaseArrayOps, IntoPrimitiveCol},
        timestamps::TimeStamps,
    },
    core::{entities::properties::tprop::TPropOps, Prop},
    prelude::TimeIndexEntry,
};
use arrow2::{
    array::{PrimitiveArray, StructArray},
    datatypes::{DataType, Field},
    types::NativeType,
};
use rayon::prelude::*;
use std::{marker::PhantomData, ops::Range};

#[derive(Debug, Clone)]
pub struct Properties<Index> {
    constant_props: ChunkedArray<StructArray>,
    temporal_props: ChunkedListArray<'static, ChunkedArray<StructArray>>,
    temporal_timestamps: ChunkedListArray<'static, ChunkedArray<PrimitiveArray<i64>, NonNull>>,
    _index: PhantomData<Index>,
}

impl<Index> Properties<Index>
where
    usize: From<Index>,
{
    pub fn temporal_dtype(&self, id: usize) -> &Field {
        if let DataType::Struct(fields) = self.temporal_props.data_type().unwrap() {
            &fields[id]
        } else {
            unreachable!()
        }
    }

    pub fn constant_dtype(&self, id: usize) -> &Field {
        if let DataType::Struct(fields) = self.constant_props.data_type().unwrap() {
            &fields[id]
        } else {
            unreachable!()
        }
    }

    pub fn has_temporal_prop(&self, index: Index, id: usize) -> bool {
        let i: usize = index.into();
        self.temporal_props
            .value(i)
            .par_iter()
            .any(|row| row.is_valid(id))
    }

    pub fn has_temporal_prop_window(&self, index: Index, id: usize, w: Range<i64>) -> bool {
        let times = self.temporal_timestamps.value(index.into());
        let start = times.insertion_point(w.start);
        let times = times.sliced(start..);
        let end = times.insertion_point(w.end);
        let times = times.sliced(..end);
        if times.is_empty() {
            return false;
        }
        let props = self.temporal_props.values().slice(times.range().clone());
        props.par_iter().any(|row| row.is_valid(id))
    }

    pub fn temporal_prop(&self, index: Index, id: usize) -> Box<dyn TPropOps + '_> {
        let dtype = self.temporal_dtype(id);
        let i: usize = index.into();
        let timestamps = TimeStamps::new(self.temporal_timestamps.value(i), None);
        let props = self.temporal_props.value(i);
        match dtype.data_type() {
            DataType::Int64 => {
                Box::new(new_primitive_tprop_column::<i64>(timestamps, props, id).unwrap())
            }
            DataType::Int32 => {
                Box::new(new_primitive_tprop_column::<i32>(timestamps, props, id).unwrap())
            }
            DataType::UInt32 => {
                Box::new(new_primitive_tprop_column::<u32>(timestamps, props, id).unwrap())
            }
            DataType::UInt64 => {
                Box::new(new_primitive_tprop_column::<u64>(timestamps, props, id).unwrap())
            }
            DataType::Float32 => {
                Box::new(new_primitive_tprop_column::<f32>(timestamps, props, id).unwrap())
            }
            DataType::Float64 => {
                Box::new(new_primitive_tprop_column::<f64>(timestamps, props, id).unwrap())
            }
            DataType::Utf8 => Box::new(new_str_tprop_column::<i32>(timestamps, props, id).unwrap()),
            DataType::LargeUtf8 => {
                Box::new(new_str_tprop_column::<i64>(timestamps, props, id).unwrap())
            }
            _ => unimplemented!(),
        }
    }

    pub fn constant_prop(&self, index: Index, id: usize) -> Prop {
        let dtype = self.constant_dtype(id);
        let i = index.into();
        match dtype.data_type() {
            DataType::Int64 => Prop::I64(self.constant_props.get(i).primitive_value(id).unwrap()),
            _ => unimplemented!(),
        }
    }
}
