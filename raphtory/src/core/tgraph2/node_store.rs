use std::ops::Range;

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::core::{
    edge_ref::EdgeRef, errors::MutateGraphError, timeindex::TimeIndex, tprop::TProp, Direction,
    Prop,
};

use super::{adj::Adj, props::Props, EID, VID};

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
pub(crate) struct NodeStore<const N: usize> {
    global_id: u64,
    pub(crate) vid: VID,
    // all the timestamps that have been seen by this vertex
    timestamps: TimeIndex,
    // each layer represents a separate view of the graph
    layers: Vec<Adj>,
    // props for vertex
    props: Option<Props>,
}

impl<const N: usize> NodeStore<N> {
    pub fn new(global_id: u64, t: i64) -> Self {
        let mut layers = Vec::with_capacity(1);
        layers.push(Adj::Solo);
        Self {
            global_id,
            vid: 0.into(),
            timestamps: TimeIndex::one(t),
            layers ,
            props: None,
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
        // let props = self.props.get_or_insert_with(|| Props::new());
        // props.add_prop(t, prop_id, prop);
    }

    pub fn add_static_prop(
        &mut self,
        prop_id: usize,
        name: &str,
        prop: Prop,
    ) -> Result<(), MutateGraphError> {
        // let props = self.props.get_or_insert_with(|| Props::new());
        // props.add_static_prop(prop_id, name, prop)?;
        Ok(())
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

    pub(crate) fn temporal_properties<'a>(
        &'a self,
        prop_id: usize,
        window: Option<Range<i64>>,
    ) -> impl Iterator<Item = (i64, Prop)> + 'a {
        if let Some(window) = window {
            self.props
                .as_ref()
                .map(|ps| ps.temporal_props_window(prop_id, window.start, window.end))
                .unwrap_or_else(|| Box::new(std::iter::empty()))
        } else {
            self.props
                .as_ref()
                .map(|ps| ps.temporal_props(prop_id))
                .unwrap_or_else(|| Box::new(std::iter::empty()))
        }
    }

    pub(crate) fn static_property(&self, prop_id: usize) -> Option<&Prop> {
        self.props.as_ref().and_then(|ps| ps.static_prop(prop_id))
    }

    pub(crate) fn edge_tuples<'a>(
        &'a self,
        layer_id: Option<usize>,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send + 'a> {
        let self_id = self.vid;
        match layer_id {
            Some(layer_id) => {
                if let Some(layer) = self.layers.get(layer_id) {
                    match d {
                        Direction::IN => {
                            Box::new(
                                layer
                                    .iter(d)
                                    .map(move |(src_pid, e_id)| EdgeRef::LocalInto {
                                        e_pid: e_id,
                                        src_pid,
                                        dst_pid: self_id,
                                        layer_id: layer_id,
                                        time: None,
                                    }),
                            )
                        }
                        Direction::OUT => {
                            Box::new(layer.iter(d).map(move |(dst_pid, e_id)| EdgeRef::LocalOut {
                                e_pid: e_id,
                                layer_id: layer_id,
                                src_pid: self_id,
                                dst_pid,
                                time: None,
                            }))
                        }
                        Direction::BOTH => {
                            Box::new(self.edge_tuples(Some(layer_id), Direction::OUT).merge_by(
                                self.edge_tuples(Some(layer_id), Direction::IN),
                                |e1, e2| e1.remote() < e2.remote(),
                            ))
                        }
                    }
                } else {
                    Box::new(std::iter::empty())
                }
            }
            None => {
                let iter = self
                    .layers
                    .iter()
                    .enumerate()
                    .flat_map(move |(layer_id, _)| self.edge_tuples(Some(layer_id), d));
                Box::new(iter)
            }
        }
    }

    // every neighbour apears once in the iterator
    // this is important because it calculates degree
    pub(crate) fn neighbours<'a>(
        &'a self,
        layer_id: Option<usize>,
        d: Direction,
    ) -> Box<dyn Iterator<Item = VID> + Send + 'a> {
        match layer_id {
            Some(layer_id) => {
                if let Some(layer) = self.layers.get(layer_id) {
                    match d {
                        Direction::IN => Box::new(layer.iter(d).map(|(from_v, _)| from_v)),
                        Direction::OUT => Box::new(layer.iter(d).map(|(to_v, _)| to_v)),
                        Direction::BOTH => Box::new(
                            self.neighbours(Some(layer_id), Direction::OUT)
                                .merge(self.neighbours(Some(layer_id), Direction::IN))
                                .dedup(),
                        ),
                    }
                } else {
                    Box::new(std::iter::empty())
                }
            }
            None => {
                let iter = self
                    .layers
                    .iter()
                    .enumerate()
                    .map(|(layer_id, _)| self.neighbours(Some(layer_id), d))
                    .kmerge()
                    .dedup();
                Box::new(iter)
            }
        }
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

    pub(crate) fn static_prop_ids(&self) -> Vec<usize> {
        self.props
            .as_ref()
            .map(|ps| ps.static_prop_ids())
            .unwrap_or_default()
    }

    pub(crate) fn temporal_property(&self, prop_id: usize) -> Option<&TProp> {
        self.props.as_ref().and_then(|ps| ps.temporal_prop(prop_id))
    }

    pub(crate) fn temp_prop_ids(&self) -> Vec<usize> {
        self.props
            .as_ref()
            .map(|ps| ps.temporal_prop_ids())
            .unwrap_or_default()
    }
}
