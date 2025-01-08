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

#[derive(Debug, Copy, Clone)]
pub struct MemRow<'a> {
    cols: &'a TColumns,
    row: Option<usize>,
}

impl<'a> MemRow<'a> {
    pub fn new(cols: &'a TColumns, row: Option<usize>) -> Self {
        Self { cols, row }
    }
}

impl<'a> IntoIterator for MemRow<'a> {
    type Item = (usize, Option<Prop>);

    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(
            self.cols
                .iter()
                .enumerate()
                .map(move |(i, col)| (i, self.row.and_then(|row| col.get(row)))),
        )
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
fn get<'a>(disk_col: &DiskTProp<'a, TimeIndexEntry>, row: usize) -> Option<Prop> {
    match disk_col {
        DiskTProp::Empty(_) => None,
        DiskTProp::Bool(tprop_column) => tprop_column.get(row).map(|p| p.into()),
        DiskTProp::Str64(tprop_column) => tprop_column.get(row).map(|p| p.into()),
        DiskTProp::Str32(tprop_column) => tprop_column.get(row).map(|p| p.into()),
        DiskTProp::I32(tprop_column) => tprop_column.get(row).map(|p| p.into()),
        DiskTProp::I64(tprop_column) => tprop_column.get(row).map(|p| p.into()),
        DiskTProp::U8(tprop_column) => tprop_column.get(row).map(|p| p.into()),
        DiskTProp::U16(tprop_column) => tprop_column.get(row).map(|p| p.into()),
        DiskTProp::U32(tprop_column) => tprop_column.get(row).map(|p| p.into()),
        DiskTProp::U64(tprop_column) => tprop_column.get(row).map(|p| p.into()),
        DiskTProp::F32(tprop_column) => tprop_column.get(row).map(|p| p.into()),
        DiskTProp::F64(tprop_column) => tprop_column.get(row).map(|p| p.into()),
    }
}
