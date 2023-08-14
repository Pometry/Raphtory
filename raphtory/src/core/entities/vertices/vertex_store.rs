use crate::core::{
    entities::{
        edges::edge_ref::EdgeRef,
        properties::{props::Props, tprop::TProp},
        vertices::structure::{adj, adj::Adj},
        LayerIds, EID, VID,
    },
    storage::{
        lazy_vec::IllegalSet,
        timeindex::{AsTime, TimeIndex, TimeIndexEntry},
    },
    utils::errors::MutateGraphError,
    Direction, Prop,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{iter, ops::Range, sync::Arc};

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
pub(crate) struct VertexStore<const N: usize> {
    global_id: u64,
    pub(crate) vid: VID,
    // all the timestamps that have been seen by this vertex
    timestamps: TimeIndex<i64>,
    // each layer represents a separate view of the graph
    layers: Vec<Adj>,
    // props for vertex
    pub(crate) props: Option<Props>,
}

impl<const N: usize> VertexStore<N> {
    pub fn new(global_id: u64, t: TimeIndexEntry) -> Self {
        let mut layers = Vec::with_capacity(1);
        layers.push(Adj::Solo);
        Self {
            global_id,
            vid: 0.into(),
            timestamps: TimeIndex::one(*t.t()),
            layers,
            props: None,
        }
    }

    pub fn global_id(&self) -> u64 {
        self.global_id
    }

    pub fn timestamps(&self) -> &TimeIndex<i64> {
        &self.timestamps
    }

    pub fn update_time(&mut self, t: TimeIndexEntry) {
        self.timestamps.insert(*t.t());
    }

    pub fn add_prop(&mut self, t: TimeIndexEntry, prop_id: usize, prop: Prop) {
        let props = self.props.get_or_insert_with(|| Props::new());
        props.add_prop(t, prop_id, prop);
    }

    pub fn add_static_prop(
        &mut self,
        prop_id: usize,
        prop: Prop,
    ) -> Result<(), IllegalSet<Option<Prop>>> {
        let props = self.props.get_or_insert_with(|| Props::new());
        props.add_static_prop(prop_id, prop)
    }

    pub(crate) fn find_edge(&self, dst: VID, layer_id: LayerIds) -> Option<EID> {
        match layer_id {
            LayerIds::All => self
                .layers
                .iter()
                .find_map(|layer| layer.get_edge(dst, Direction::OUT)),
            LayerIds::One(layer_id) => self
                .layers
                .get(layer_id)
                .and_then(|layer| layer.get_edge(dst, Direction::OUT)),
            LayerIds::Multiple(layers) => layers.iter().find_map(|layer_id| {
                self.layers
                    .get(*layer_id)
                    .and_then(|layer| layer.get_edge(dst, Direction::OUT))
            }),
            LayerIds::None => None,
        }
    }

    pub(crate) fn add_edge(&mut self, v_id: VID, dir: Direction, layer: usize, edge_id: EID) {
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
        layers: LayerIds,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send + 'a> {
        let self_id = self.vid;
        let iter: Box<dyn Iterator<Item = EdgeRef> + Send> = match d {
            Direction::OUT => self.merge_layers(layers, Direction::OUT, self_id),
            Direction::IN => self.merge_layers(layers, Direction::IN, self_id),
            Direction::BOTH => Box::new(
                self.edge_tuples(layers.clone(), Direction::OUT)
                    .merge_by(self.edge_tuples(layers, Direction::IN), |e1, e2| {
                        e1.remote() < e2.remote()
                    }),
            ),
        };
        iter
    }

    fn merge_layers(
        &self,
        layers: LayerIds,
        d: Direction,
        self_id: VID,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send + '_> {
        match layers {
            LayerIds::All => Box::new(
                self.layers
                    .iter()
                    .map(|adj| self.iter_adj(adj, d, self_id))
                    .kmerge_by(|e1, e2| e1.remote() < e2.remote())
                    .dedup(),
            ),
            LayerIds::One(id) => {
                if let Some(layer) = self.layers.get(id) {
                    Box::new(self.iter_adj(layer, d, self_id))
                } else {
                    Box::new(std::iter::empty())
                }
            }
            LayerIds::Multiple(ids) => Box::new(
                ids.iter()
                    .filter_map(|id| self.layers.get(*id))
                    .map(|layer| self.iter_adj(layer, d, self_id))
                    .kmerge_by(|e1, e2| e1.remote() < e2.remote())
                    .dedup(),
            ),
            LayerIds::None => Box::new(iter::empty()),
        }
    }

    fn iter_adj<'a>(
        &'a self,
        layer: &'a Adj,
        d: Direction,
        self_id: VID,
    ) -> impl Iterator<Item = EdgeRef> + Send + '_ {
        let iter: Box<dyn Iterator<Item = EdgeRef> + Send> = match d {
            Direction::IN => Box::new(
                layer
                    .iter(d)
                    .map(move |(src_pid, e_id)| EdgeRef::new_incoming(e_id, src_pid, self_id)),
            ),
            Direction::OUT => Box::new(
                layer
                    .iter(d)
                    .map(move |(dst_pid, e_id)| EdgeRef::new_outgoing(e_id, self_id, dst_pid)),
            ),
            _ => Box::new(iter::empty()),
        };
        iter
    }

    pub(crate) fn degree(&self, layers: LayerIds, d: Direction) -> usize {
        match layers {
            LayerIds::All => match self.layers.len() {
                0 => 0,
                1 => self.layers[0].degree(d),
                _ => self
                    .layers
                    .iter()
                    .map(|l| l.vertex_iter(d))
                    .kmerge()
                    .dedup()
                    .count(),
            },
            LayerIds::One(l) => self.layers.get(l).map(|layer| layer.degree(d)).unwrap_or(0),
            LayerIds::None => 0,
            LayerIds::Multiple(ids) => ids
                .iter()
                .flat_map(|l_id| self.layers.get(*l_id).map(|layer| layer.vertex_iter(d)))
                .kmerge()
                .dedup()
                .count(),
        }
    }

    // every neighbour apears once in the iterator
    // this is important because it calculates degree
    pub(crate) fn neighbours<'a>(
        &'a self,
        layers: LayerIds,
        d: Direction,
    ) -> Box<dyn Iterator<Item = VID> + Send + 'a> {
        match layers {
            LayerIds::All => {
                let iter = self
                    .layers
                    .iter()
                    .enumerate()
                    .map(|(layer_id, _)| self.neighbours(layer_id.into(), d))
                    .kmerge()
                    .dedup();
                Box::new(iter)
            }
            LayerIds::One(one) => {
                let iter = self
                    .layers
                    .get(one)
                    .map(|layer| self.neighbours_from_adj(layer, d, layers))
                    .unwrap_or(Box::new(std::iter::empty()));
                Box::new(iter)
            }
            LayerIds::Multiple(layers) => {
                let iter = layers
                    .iter()
                    .filter_map(|l| self.layers.get(*l))
                    .map(|layer| self.neighbours_from_adj(layer, d, layers.clone().into()))
                    .kmerge()
                    .dedup();
                Box::new(iter)
            }
            LayerIds::None => Box::new(iter::empty()),
        }
    }

    fn neighbours_from_adj<'a>(
        &'a self,
        layer: &'a Adj,
        d: Direction,
        layers: LayerIds,
    ) -> Box<dyn Iterator<Item = VID> + Send + '_> {
        let iter: Box<dyn Iterator<Item = VID> + Send> = match d {
            Direction::IN => Box::new(layer.iter(d).map(|(from_v, _)| from_v)),
            Direction::OUT => Box::new(layer.iter(d).map(|(to_v, _)| to_v)),
            Direction::BOTH => Box::new(
                self.neighbours(layers.clone().into(), Direction::OUT)
                    .merge(self.neighbours(layers.clone().into(), Direction::IN))
                    .dedup(),
            ),
        };
        iter
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
