use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::core::{timeindex::TimeIndex, Direction, Prop};

use super::{adj::Adj, props::Props, EID, VID};

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
pub(crate) struct NodeStore<const N: usize> {
    global_id: u64,
    // all the timestamps that have been seen by this vertex
    timestamps: TimeIndex,
    // each layer represents a separate view of the graph
    layers: Vec<Adj>,
    // props for vertex
    props: Props,
}

impl<const N: usize> NodeStore<N> {
    pub fn new(global_id: u64, t: i64) -> Self {
        Self {
            global_id,
            timestamps: TimeIndex::one(t),
            layers: vec![Adj::Solo],
            props: Props::new(),
        }
    }

    pub fn global_id(&self) -> u64 {
        self.global_id
    }

    pub fn timestamps(&self) -> &TimeIndex {
        &self.timestamps
    }

    pub fn update_time(&mut self, t: i64) {
        self.timestamps.insert(t);
    }

    pub fn add_prop(&mut self, t: i64, prop_id: usize, prop: Prop) {
        self.props.add_prop(t, prop_id, prop);
    }

    pub(crate) fn find_edge(&self, dst: VID, layer_id: Option<usize>) -> Option<super::EID> {
        match layer_id {
            Some(layer_id) => {
                let layer_adj = self.layers.get(layer_id)?;
                return layer_adj.get_edge(dst, Direction::OUT);
            }
            None => {
                for layer in self.layers.iter() {
                    if let Some(eid) = layer.get_edge(dst, Direction::OUT) {
                        return Some(eid);
                    }
                }
            }
        }
        None
    }

    pub(crate) fn find_edge_on_layer(&self, dst: VID, layer_id: usize) -> Option<super::EID> {
        let layer_adj = self.layers.get(layer_id)?;
        layer_adj.get_edge(dst, Direction::OUT)
    }

    pub(crate) fn add_edge(
        &mut self,
        v_id: VID,
        dir: Direction,
        layer: usize,
        edge_id: super::EID,
    ) {
        if layer >= self.layers.len() {
            self.layers.resize_with(layer + 1, || Adj::Solo);
        }

        match dir {
            Direction::IN => self.layers[layer].add_edge_into(v_id, edge_id),
            Direction::OUT => self.layers[layer].add_edge_out(v_id, edge_id),
            _ => {}
        }
    }

    // pub(crate) fn has_time_window(&self, window: Range<i64>) -> bool {
    //     self.timestamps.range(window).next().is_some()
    // }

    pub(crate) fn temporal_properties<'a>(
        &'a self,
        prop_id: usize,
    ) -> impl Iterator<Item = (i64, Prop)> + 'a {
        self.props.temporal_props(prop_id)
    }

    pub(crate) fn static_property(&self, prop_id: usize) -> Option<&Prop> {
        self.props.static_prop(prop_id)
    }

    pub(crate) fn edge_tuples<'a>(
        &'a self,
        layer_id: usize,
        d: Direction,
    ) -> impl Iterator<Item = (VID, super::EID)> + Send + 'a {
        self.layers[layer_id].iter(d)
    }

    pub(crate) fn edges_from_last<'a>(
        &'a self,
        layer_id: usize,
        dir: Direction,
        last: Option<VID>,
        page_size: usize,
    ) -> Vec<(VID, EID)> {
        self.layers[layer_id].get_page_vec(last, page_size, dir)
    }
}
