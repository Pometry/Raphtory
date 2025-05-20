use super::TColumns;
use crate::entities::{nodes::node_store::NodeStore, properties::tprop::TPropCell};
use itertools::Itertools;
use raphtory_api::core::{
    entities::{
        edges::edge_ref::EdgeRef,
        properties::{prop::Prop, tprop::TPropOps},
        LayerIds,
    },
    storage::timeindex::TimeIndexEntry,
    Direction,
};
use std::{
    fmt::{Debug, Formatter},
    ops::Range,
};

#[derive(Copy, Clone)]
pub struct MemRow<'a> {
    cols: &'a TColumns,
    row: Option<usize>,
}

impl<'a> Debug for MemRow<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.into_iter()).finish()
    }
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

#[derive(Copy, Clone)]
pub struct NodePtr<'a> {
    pub node: &'a NodeStore,
    t_props_log: &'a TColumns,
}

impl<'a> NodePtr<'a> {
    pub fn edges_iter(
        self,
        layers: &LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + 'a {
        self.node.edge_tuples(layers, dir)
    }
}

impl<'a> Debug for NodePtr<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("gid", self.node.global_id())
            .field("vid", &self.node.vid)
            .field("node_type", &self.node.node_type)
            .field(
                "constant_properties",
                &self
                    .node
                    .const_prop_ids()
                    .filter_map(|i| Some((i, self.node.constant_property(i)?)))
                    .collect_vec(),
            )
            .field("temporal_properties", &self.into_rows().collect_vec())
            .finish()
    }
}

impl<'a> NodePtr<'a> {
    pub fn new(node: &'a NodeStore, t_props_log: &'a TColumns) -> Self {
        Self { node, t_props_log }
    }

    pub fn node(self) -> &'a NodeStore {
        self.node
    }

    pub fn t_prop(self, prop_id: usize) -> TPropCell<'a> {
        TPropCell::new(
            &self.node.timestamps().props_ts,
            self.t_props_log.get(prop_id),
        )
    }

    pub fn temporal_prop_ids(self) -> impl Iterator<Item = usize> + 'a {
        self.t_props_log
            .t_props_log
            .iter()
            .enumerate()
            .filter_map(|(id, col)| (!col.is_empty()).then(|| id))
    }

    pub fn into_rows(self) -> impl Iterator<Item = (TimeIndexEntry, MemRow<'a>)> {
        self.node
            .timestamps()
            .props_ts
            .iter()
            .map(move |(t, &row)| (*t, MemRow::new(self.t_props_log, row)))
    }

    pub fn last_before_row(self, t: TimeIndexEntry) -> Vec<(usize, Prop)> {
        self.t_props_log
            .iter()
            .enumerate()
            .filter_map(|(prop_id, _)| {
                let t_prop = self.t_prop(prop_id);
                t_prop.last_before(t).map(|(_, v)| (prop_id, v))
            })
            .collect()
    }

    pub fn into_rows_window(
        self,
        w: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, MemRow<'a>)> + Send + Sync {
        let tcell = &self.node.timestamps().props_ts;
        tcell
            .iter_window(w)
            .map(move |(t, row)| (*t, MemRow::new(self.t_props_log, *row)))
    }
}
