use crate::core::{storage::TColumns, Prop};
#[cfg(feature = "storage")]
use pometry_storage::{
    graph::TemporalGraph, properties::TemporalProps, timestamps::TimeStamps, tprops::DiskTProp,
};
#[cfg(feature = "storage")]
use raphtory_api::core::{entities::VID, storage::timeindex::TimeIndexEntry};

#[derive(Debug, Copy, Clone)]
pub enum Row<'a> {
    Mem(MemRow<'a>),
    #[cfg(feature = "storage")]
    Disk(DiskRow<'a>),
}

impl<'a> IntoIterator for Row<'a> {
    type Item = (usize, Option<Prop>);

    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Row::Mem(mem_row) => mem_row.into_iter(),
            #[cfg(feature = "storage")]
            Row::Disk(disk_row) => disk_row.into_iter(),
        }
    }
}

#[cfg(feature = "storage")]
#[derive(Debug, Copy, Clone)]
pub struct DiskRow<'a> {
    graph: &'a TemporalGraph,
    ts: TimeStamps<'a, TimeIndexEntry>,
    layer: usize,
    row: usize,
}

#[cfg(feature = "storage")]
impl<'a> DiskRow<'a> {
    pub fn new(
        graph: &'a TemporalGraph,
        ts: TimeStamps<'a, TimeIndexEntry>,
        row: usize,
        layer: usize,
    ) -> Self {
        Self {
            graph,
            ts,
            row,
            layer,
        }
    }

    pub fn temporal_props(&'a self) -> &'a TemporalProps<VID> {
        &self.graph.node_properties().temporal_props()[self.layer]
    }
}

#[cfg(feature = "storage")]
impl<'a> IntoIterator for DiskRow<'a> {
    type Item = (usize, Option<Prop>);

    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        let props = self.temporal_props();
        let iter = (0..props.prop_dtypes().len()).filter_map(move |prop_id| {
            let global_prop = self
                .graph
                .prop_mapping()
                .globalise_node_prop_id(self.layer, prop_id)?;
            let props = self.temporal_props();
            Some((
                global_prop,
                get(
                    &props.prop_for_ts::<TimeIndexEntry>(self.ts, prop_id),
                    self.row,
                ),
            ))
        });
        Box::new(iter)
    }
}

#[cfg(feature = "storage")]
use polars_arrow::datatypes::ArrowDataType;

#[cfg(feature = "storage")]
fn get<'a>(disk_col: &DiskTProp<'a, TimeIndexEntry>, row: usize) -> Option<Prop> {
    use bigdecimal::BigDecimal;
    use num_traits::FromPrimitive;

    match disk_col {
        DiskTProp::Empty(_) => None,
        DiskTProp::Bool(tprop_column) => tprop_column.get(row).map(|p| p.into()),
        DiskTProp::Str64(tprop_column) => tprop_column.get(row).map(|p| p.into()),
        DiskTProp::Str32(tprop_column) => tprop_column.get(row).map(|p| p.into()),
        DiskTProp::Str(tprop_column) => tprop_column.get(row).map(|p| p.into()),
        DiskTProp::I32(tprop_column) => tprop_column.get(row).map(|p| p.into()),
        DiskTProp::I64(tprop_column) => tprop_column.get(row).map(|p| p.into()),
        DiskTProp::U8(tprop_column) => tprop_column.get(row).map(|p| p.into()),
        DiskTProp::U16(tprop_column) => tprop_column.get(row).map(|p| p.into()),
        DiskTProp::U32(tprop_column) => tprop_column.get(row).map(|p| p.into()),
        DiskTProp::U64(tprop_column) => tprop_column.get(row).map(|p| p.into()),
        DiskTProp::F32(tprop_column) => tprop_column.get(row).map(|p| p.into()),
        DiskTProp::F64(tprop_column) => tprop_column.get(row).map(|p| p.into()),
        DiskTProp::I128(tprop_column) => {
            let d_type = tprop_column.data_type()?;
            match d_type {
                ArrowDataType::Decimal(_, scale) => tprop_column.get(row).map(|p| {
                    BigDecimal::from_i128(p)
                        .unwrap()
                        .with_scale(*scale as i64)
                        .into()
                }),
                _ => unimplemented!("{d_type:?} not supported as disk_graph property"),
            }
        }
    }
}
