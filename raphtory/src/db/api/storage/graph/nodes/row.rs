use pometry_storage::{properties::TemporalProps, tprops::DiskTProp};
use raphtory_api::core::{entities::VID, storage::timeindex::TimeIndexEntry};

use crate::{
    core::{storage::TColumns, Prop},
    disk_graph::storage_interface::node::DiskNode,
};

use super::node_storage_ops::NodeStorageOps;

#[derive(Debug, Copy, Clone)]
pub enum Row<'a> {
    Mem(MemRow<'a>),
    Disk(DiskRow<'a>),
}

impl<'a> IntoIterator for Row<'a> {
    type Item = (usize, Option<Prop>);

    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Row::Mem(mem_row) => mem_row.into_iter(),
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

#[derive(Debug, Copy, Clone)]
pub struct DiskRow<'a> {
    node: DiskNode<'a>,
    props: &'a TemporalProps<VID>,
    layer: usize,
    row: usize,
}

impl<'a> DiskRow<'a> {
    pub fn new(
        node: DiskNode<'a>,
        props: &'a TemporalProps<VID>,
        row: usize,
        layer: usize,
    ) -> Self {
        Self {
            node,
            props,
            row,
            layer,
        }
    }
}

impl<'a> IntoIterator for DiskRow<'a> {
    type Item = (usize, Option<Prop>);

    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        let iter = (0..self.node.graph().node_meta().temporal_prop_meta().len())
            .flat_map(move |global_prop_id| {
                self.node
                    .graph()
                    .prop_mapping()
                    .localise_node_prop_id(global_prop_id)
                    .map(|(layer, local_prop_id)| (global_prop_id, layer, local_prop_id))
            })
            .filter_map(move |(l_layer, local, global_prop)| {
                (self.layer == l_layer).then(|| (local, global_prop))
            })
            .map(move |(global_prop, local_prop)| {
                let prop_col = self
                    .props
                    .prop::<TimeIndexEntry>(self.node.vid(), local_prop);
                (global_prop, get(&prop_col, self.row))
            });
        Box::new(iter)
    }
}

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
