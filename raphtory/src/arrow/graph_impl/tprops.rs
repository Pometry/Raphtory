use std::ops::Range;

use arrow2::{
    array::{PrimitiveArray, StructArray},
    datatypes::{DataType, Field},
    types::{NativeType, Offset},
};

use crate::{
    arrow::{
        chunked_array::{
            array_ops::{ArrayOps, BaseArrayOps},
            chunked_array::ChunkedArray,
            col::ChunkedPrimitiveCol,
            utf8_col::GenericChunkedUtf8Col,
            ChunkedArraySlice,
        },
        edge::Edge,
        prelude::{IntoPrimitiveCol, IntoUtf8Col, PrimitiveCol},
        timestamps::TimeStamps,
    },
    core::{entities::properties::tprop::TPropOps, storage::timeindex::TimeIndexOps},
    prelude::{Prop, TimeIndexEntry},
};

pub struct TPropColumn<'a, A> {
    props: ChunkedArraySlice<'a, A>,
    timestamps: TimeStamps<'a, TimeIndexEntry>,
}

pub fn new_primitive_tprop_column<'a, T: NativeType + Into<Prop>>(
    timestamps: TimeStamps<'a, TimeIndexEntry>,
    props: ChunkedArraySlice<'a, &'a ChunkedArray<StructArray>>,
    col_idx: usize,
) -> Option<TPropColumn<'a, ChunkedPrimitiveCol<'a, T>>> {
    let props = props.into_primitive_col(col_idx)?;
    Some(TPropColumn { props, timestamps })
}

pub fn new_str_tprop_column<'a, I: Offset>(
    timestamps: TimeStamps<'a, TimeIndexEntry>,
    props: ChunkedArraySlice<'a, &'a ChunkedArray<StructArray>>,
    col_idx: usize,
) -> Option<TPropColumn<'a, GenericChunkedUtf8Col<'a, I>>> {
    let props = props.into_utf8_col(col_idx)?;
    Some(TPropColumn { props, timestamps })
}

impl<T: NativeType + Into<Prop>> TPropOps for TPropColumn<'_, ChunkedPrimitiveCol<'_, T>> {
    fn last_before(&self, t: i64) -> Option<(i64, Prop)> {
        let (t, t_index) = self.timestamps.last_before(t)?;
        let v = self.props.get(t_index)?;
        Some((t.0, v.into()))
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (i64, Prop)> + '_> {
        Box::new(
            self.timestamps
                .iter_t()
                .zip(self.props.iter())
                .filter_map(|(t, v)| v.map(|v| (t, v.into()))),
        )
    }

    fn iter_window(&self, r: Range<i64>) -> Box<dyn Iterator<Item = (i64, Prop)> + '_> {
        let (start, end) = self.timestamps.partition_points(r.clone());
        Box::new(
            self.timestamps
                .slice(start..end)
                .into_iter_t()
                .zip(self.props.slice(start..end).into_iter())
                .filter_map(|(t, v)| v.map(|v| (t, v.into()))),
        )
    }

    fn iter_window_te(
        &self,
        r: Range<TimeIndexEntry>,
    ) -> Box<dyn Iterator<Item = (i64, Prop)> + '_> {
        let start = self.timestamps.position(&r.start);
        let end = self.timestamps.position(&r.end);

        Box::new(
            self.timestamps
                .slice(start..end)
                .into_iter_t()
                .zip(self.props.slice(start..end).into_iter())
                .filter_map(|(t, v)| v.map(|v| (t, v.into()))),
        )
    }

    fn at(&self, ti: &TimeIndexEntry) -> Option<Prop> {
        let t_index = self.timestamps.position(ti);
        self.props.get(t_index).map(|v| v.into())
    }
}

impl<I: Offset> TPropOps for TPropColumn<'_, GenericChunkedUtf8Col<'_, I>> {
    fn last_before(&self, t: i64) -> Option<(i64, Prop)> {
        let (t, t_index) = self.timestamps.last_before(t)?;
        let v = self.props.get(t_index)?;
        Some((t.0, v.into()))
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (i64, Prop)> + '_> {
        Box::new(
            self.timestamps
                .iter_t()
                .zip(self.props.iter())
                .filter_map(|(t, v)| v.map(|v| (t, v.into()))),
        )
    }

    fn iter_window(&self, r: Range<i64>) -> Box<dyn Iterator<Item = (i64, Prop)> + '_> {
        let (start, end) = self.timestamps.partition_points(r.clone());
        Box::new(
            self.timestamps
                .slice(start..end)
                .into_iter_t()
                .zip(self.props.slice(start..end).into_iter())
                .filter_map(|(t, v)| v.map(|v| (t, v.into()))),
        )
    }

    fn iter_window_te(
        &self,
        r: Range<TimeIndexEntry>,
    ) -> Box<dyn Iterator<Item = (i64, Prop)> + '_> {
        let start = self.timestamps.position(&r.start);
        let end = self.timestamps.position(&r.end);

        Box::new(
            self.timestamps
                .slice(start..end)
                .into_iter_t()
                .zip(self.props.slice(start..end).into_iter())
                .filter_map(|(t, v)| v.map(|v| (t, v.into()))),
        )
    }

    fn at(&self, ti: &TimeIndexEntry) -> Option<Prop> {
        let t_index = self.timestamps.position(ti);
        self.props.get(t_index).map(|v| v.into())
    }
}

fn new_tprop_column<T: NativeType>(edge: Edge, id: usize) -> Option<Box<dyn TPropOps + '_>>
where
    Prop: From<T>,
{
    let props = edge.prop_values::<T>(id)?;
    let timestamps = TimeStamps::new(edge.timestamp_slice(), None);
    Some(Box::new(TPropColumn { props, timestamps }))
}

pub fn read_tprop_column(id: usize, field: Field, edge: Edge) -> Option<Box<dyn TPropOps + '_>> {
    match field.data_type() {
        DataType::Int64 => new_tprop_column::<i64>(edge, id),
        DataType::Int32 => new_tprop_column::<i32>(edge, id),
        DataType::UInt32 => new_tprop_column::<u32>(edge, id),
        DataType::UInt64 => new_tprop_column::<u64>(edge, id),
        DataType::Float32 => new_tprop_column::<f32>(edge, id),
        DataType::Float64 => new_tprop_column::<f64>(edge, id),
        DataType::Utf8 => {
            let props = edge.prop_str_values::<i32>(id)?;
            let timestamps = TimeStamps::new(edge.timestamp_slice(), None);
            Some(Box::new(TPropColumn { props, timestamps }))
        }
        DataType::LargeUtf8 => {
            let props = edge.prop_str_values::<i64>(id)?;
            let timestamps = TimeStamps::new(edge.timestamp_slice(), None);
            Some(Box::new(TPropColumn { props, timestamps }))
        }
        _ => todo!(),
    }
}
