use std::ops::Range;

use arrow2::{
    datatypes::{DataType, Field},
    types::{NativeType, Offset},
};

use crate::{
    arrow::{
        chunked_array::{
            array_ops::{ArrayOps, BaseArrayOps},
            col::ChunkedPrimitiveCol,
            utf8_col::GenericChunkedUtf8Col,
            ChunkedArraySlice,
        },
        edge::Edge,
        timestamps::TimeStamps,
    },
    core::{entities::properties::tprop::LayeredTProp, storage::timeindex::TimeIndexOps},
    prelude::{Prop, TimeIndexEntry},
};

pub struct TPropColumn<'a, A> {
    props: ChunkedArraySlice<'a, A>,
    timestamps: TimeStamps<'a, TimeIndexEntry>,
}

impl<T: NativeType + Into<Prop>> LayeredTProp for TPropColumn<'_, ChunkedPrimitiveCol<'_, T>> {
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

impl<I: Offset> LayeredTProp for TPropColumn<'_, GenericChunkedUtf8Col<'_, I>> {
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

fn new_tprop_column<T: NativeType>(edge: Edge, id: usize) -> Option<Box<dyn LayeredTProp + '_>>
where
    Prop: From<T>,
{
    let props = edge.prop_values::<T>(id)?;
    let timestamps = TimeStamps::new(edge.timestamp_slice(), None);
    Some(Box::new(TPropColumn { props, timestamps }))
}

pub fn read_tprop_column(
    id: usize,
    field: Field,
    edge: Edge,
) -> Option<Box<dyn LayeredTProp + '_>> {
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
