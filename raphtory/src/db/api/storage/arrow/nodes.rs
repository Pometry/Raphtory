use crate::{
    arrow::{
        graph::TemporalGraph, graph_fragment::TempColGraphFragment, properties::Properties,
        timestamps::TimeStamps,
    },
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, EID, VID},
        storage::timeindex::TimeIndexOps,
        Direction,
    },
    db::api::{
        storage::{direction_variants::DirectionVariants, layer_variants::LayerVariants},
        view::{internal::NodeAdditions, IntoDynBoxed},
    },
};
use itertools::Itertools;
use rayon::prelude::*;
use std::{ops::Range, sync::Arc};

#[derive(Copy, Clone, Debug)]
pub struct ArrowNodesRef<'a> {
    num_nodes: usize,
    properties: Option<&'a Properties<VID>>,
    layers: &'a Arc<[TempColGraphFragment]>,
}

#[derive(Clone, Debug)]
pub struct ArrowNodesOwned {
    num_nodes: usize,
    properties: Option<Properties<VID>>,
    layers: Arc<[TempColGraphFragment]>,
}

impl ArrowNodesOwned {
    pub(crate) fn new(graph: &TemporalGraph) -> Self {
        Self {
            num_nodes: graph.num_nodes(),
            properties: graph.node_properties.clone(),
            layers: graph.layers.clone(),
        }
    }

    pub fn node(&self, vid: VID) -> ArrowNode {
        ArrowNode {
            properties: self.properties.as_ref(),
            layers: &self.layers,
            vid,
        }
    }

    pub fn as_ref(&self) -> ArrowNodesRef {
        ArrowNodesRef {
            num_nodes: self.num_nodes,
            properties: self.properties.as_ref(),
            layers: &self.layers,
        }
    }
}

impl<'a> ArrowNodesRef<'a> {
    pub(crate) fn new(graph: &'a TemporalGraph) -> Self {
        Self {
            num_nodes: graph.num_nodes(),
            properties: graph.node_properties.as_ref(),
            layers: &graph.layers,
        }
    }

    pub fn node(self, vid: VID) -> ArrowNode<'a> {
        ArrowNode {
            properties: self.properties,
            layers: self.layers,
            vid,
        }
    }

    pub fn par_iter(self) -> impl IndexedParallelIterator<Item = ArrowNode<'a>> {
        (0..self.num_nodes)
            .into_par_iter()
            .map(move |vid| self.node(VID(vid)))
    }

    pub fn iter(self) -> impl Iterator<Item = ArrowNode<'a>> {
        (0..self.num_nodes).map(move |vid| self.node(VID(vid)))
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ArrowNode<'a> {
    properties: Option<&'a Properties<VID>>,
    layers: &'a Arc<[TempColGraphFragment]>,
    vid: VID,
}

impl<'a> ArrowNode<'a> {
    pub(crate) fn new(graph: &'a TemporalGraph, vid: VID) -> Self {
        Self {
            properties: graph.node_properties(),
            layers: &graph.layers,
            vid,
        }
    }

    pub fn vid(&self) -> VID {
        self.vid
    }

    pub fn out_edges(self, layers: &'a LayerIds) -> impl Iterator<Item = EdgeRef> + 'a {
        match layers {
            LayerIds::None => LayerVariants::None,
            LayerIds::All => LayerVariants::All(
                self.layers
                    .iter()
                    .enumerate()
                    .map(|(layer_id, layer)| {
                        layer.nodes.out_adj_list(self.vid).map(move |(eid, dst)| {
                            EdgeRef::new_outgoing(eid, self.vid, dst).at_layer(layer_id)
                        })
                    })
                    .kmerge_by(|e1, e2| e1.remote() <= e2.remote()),
            ),
            LayerIds::One(layer_id) => {
                LayerVariants::One(self.layers[*layer_id].nodes.out_adj_list(self.vid).map(
                    move |(eid, dst)| EdgeRef::new_outgoing(eid, self.vid, dst).at_layer(*layer_id),
                ))
            }
            LayerIds::Multiple(ids) => LayerVariants::Multiple(
                ids.iter()
                    .map(|&layer_id| {
                        self.layers[layer_id]
                            .nodes
                            .out_adj_list(self.vid)
                            .map(move |(eid, dst)| {
                                EdgeRef::new_outgoing(eid, self.vid, dst).at_layer(layer_id)
                            })
                    })
                    .kmerge_by(|e1, e2| e1.remote() <= e2.remote()),
            ),
        }
    }

    pub fn into_out_edges(self, layers: LayerIds) -> impl Iterator<Item = EdgeRef> {
        match layers {
            LayerIds::None => LayerVariants::None,
            LayerIds::All => {
                let layers = self.layers.clone();
                LayerVariants::All(
                    (0..layers.len())
                        .map(move |layer_id| {
                            let layer = &layers[layer_id];
                            let adj = layer.nodes.adj_out.clone().into_value(self.vid.index());
                            let eids = adj.range().clone().map(EID);
                            let nbrs = adj.map(|i| VID(i as usize));
                            eids.zip(nbrs).map(move |(eid, dst)| {
                                EdgeRef::new_outgoing(eid, self.vid, dst).at_layer(layer_id)
                            })
                        })
                        .kmerge_by(|e1, e2| e1.remote() <= e2.remote()),
                )
            }
            LayerIds::One(layer_id) => {
                let adj = self.layers[layer_id]
                    .nodes
                    .adj_out
                    .clone()
                    .into_value(self.vid.index());
                let eids = adj.range().clone().map(EID);
                let nbrs = adj.map(|i| VID(i as usize));
                LayerVariants::One(eids.zip(nbrs).map(move |(eid, dst)| {
                    EdgeRef::new_outgoing(eid, self.vid, dst).at_layer(layer_id)
                }))
            }
            LayerIds::Multiple(ids) => LayerVariants::Multiple(
                (0..ids.len())
                    .map(move |i| {
                        let layer_id = ids[i];
                        let adj = self.layers[layer_id]
                            .nodes
                            .adj_out
                            .clone()
                            .into_value(self.vid.index());
                        let eids = adj.range().clone().map(EID);
                        let nbrs = adj.map(|i| VID(i as usize));
                        let src = self.vid;
                        eids.zip(nbrs).map(move |(eid, dst)| {
                            EdgeRef::new_outgoing(eid, src, dst).at_layer(layer_id)
                        })
                    })
                    .kmerge_by(|e1, e2| e1.remote() <= e2.remote()),
            ),
        }
    }

    pub fn in_edges(self, layers: &'a LayerIds) -> impl Iterator<Item = EdgeRef> + 'a {
        match layers {
            LayerIds::None => LayerVariants::None,
            LayerIds::All => LayerVariants::All(
                self.layers
                    .iter()
                    .enumerate()
                    .map(|(layer_id, layer)| {
                        layer.nodes.in_adj_list(self.vid).map(move |(eid, src)| {
                            EdgeRef::new_incoming(eid, src, self.vid).at_layer(layer_id)
                        })
                    })
                    .kmerge_by(|e1, e2| e1.remote() <= e2.remote()),
            ),
            LayerIds::One(layer_id) => {
                LayerVariants::One(self.layers[*layer_id].nodes.in_adj_list(self.vid).map(
                    move |(eid, src)| EdgeRef::new_incoming(eid, src, self.vid).at_layer(*layer_id),
                ))
            }
            LayerIds::Multiple(ids) => LayerVariants::Multiple(
                ids.iter()
                    .map(|&layer_id| {
                        self.layers[layer_id]
                            .nodes
                            .in_adj_list(self.vid)
                            .map(move |(eid, src)| {
                                EdgeRef::new_incoming(eid, src, self.vid).at_layer(layer_id)
                            })
                    })
                    .kmerge_by(|e1, e2| e1.remote() <= e2.remote()),
            ),
        }
    }

    pub fn into_in_edges(self, layers: LayerIds) -> impl Iterator<Item = EdgeRef> {
        match layers {
            LayerIds::None => LayerVariants::None,
            LayerIds::All => {
                let layers = self.layers.clone();
                LayerVariants::All(
                    (0..layers.len())
                        .map(move |layer_id| {
                            let layer = &layers[layer_id];
                            let eids = layer
                                .nodes
                                .adj_in_edges
                                .clone()
                                .into_value(self.vid.index())
                                .map(|i| EID(i as usize));
                            let nbrs = layer
                                .nodes
                                .adj_in_neighbours
                                .clone()
                                .into_value(self.vid.index())
                                .map(|i| VID(i as usize));
                            let src = self.vid;
                            eids.zip(nbrs).map(move |(eid, dst)| {
                                EdgeRef::new_incoming(eid, src, dst).at_layer(layer_id)
                            })
                        })
                        .kmerge_by(|e1, e2| e1.remote() <= e2.remote()),
                )
            }
            LayerIds::One(layer_id) => {
                let layer = &self.layers[layer_id];
                let eids = layer
                    .nodes
                    .adj_in_edges
                    .clone()
                    .into_value(self.vid.index())
                    .map(|i| EID(i as usize));
                let nbrs = layer
                    .nodes
                    .adj_in_neighbours
                    .clone()
                    .into_value(self.vid.index())
                    .map(|i| VID(i as usize));
                let src = self.vid;
                LayerVariants::One(
                    eids.zip(nbrs).map(move |(eid, dst)| {
                        EdgeRef::new_incoming(eid, src, dst).at_layer(layer_id)
                    }),
                )
            }
            LayerIds::Multiple(ids) => LayerVariants::Multiple(
                (0..ids.len())
                    .map(move |i| {
                        let layer_id = ids[i];
                        let layer = &self.layers[layer_id];
                        let eids = layer
                            .nodes
                            .adj_in_edges
                            .clone()
                            .into_value(self.vid.index())
                            .map(|i| EID(i as usize));
                        let nbrs = layer
                            .nodes
                            .adj_in_neighbours
                            .clone()
                            .into_value(self.vid.index())
                            .map(|i| VID(i as usize));
                        let src = self.vid;
                        eids.zip(nbrs).map(move |(eid, dst)| {
                            EdgeRef::new_incoming(eid, src, dst).at_layer(layer_id)
                        })
                    })
                    .kmerge_by(|e1, e2| e1.remote() <= e2.remote()),
            ),
        }
    }

    pub fn edges(self, layers: &'a LayerIds) -> impl Iterator<Item = EdgeRef> + 'a {
        self.in_edges(layers)
            .merge_by(self.out_edges(layers), |e1, e2| e1.remote() <= e2.remote())
    }

    pub fn into_edges(self, layers: LayerIds) -> impl Iterator<Item = EdgeRef> {
        self.into_in_edges(layers.clone())
            .merge_by(self.into_out_edges(layers), |e1, e2| {
                e1.remote() <= e2.remote()
            })
    }

    pub fn degree(self, layers: &LayerIds, dir: Direction) -> usize {
        let single_layer = match layers {
            LayerIds::None => return 0,
            LayerIds::All => match self.layers.len() {
                0 => return 0,
                1 => Some(&self.layers[0]),
                _ => None,
            },
            LayerIds::One(id) => Some(&self.layers[*id]),
            LayerIds::Multiple(ids) => match ids.len() {
                0 => return 0,
                1 => Some(&self.layers[ids[0]]),
                _ => None,
            },
        };
        match dir {
            Direction::OUT => match single_layer {
                None => self
                    .out_edges(layers)
                    .dedup_by(|e1, e2| e1.remote() <= e2.remote())
                    .count(),
                Some(layer) => layer.nodes.out_degree(self.vid),
            },
            Direction::IN => match single_layer {
                None => self
                    .in_edges(layers)
                    .dedup_by(|e1, e2| e1.remote() <= e2.remote())
                    .count(),
                Some(layer) => layer.nodes.in_degree(self.vid),
            },
            Direction::BOTH => match single_layer {
                None => self
                    .edges(layers)
                    .dedup_by(|e1, e2| e1.remote() <= e2.remote())
                    .count(),
                Some(layer) => layer
                    .nodes
                    .in_neighbours_iter(self.vid)
                    .merge(layer.nodes.out_neighbours_iter(self.vid))
                    .dedup()
                    .count(),
            },
        }
    }

    fn additions(&self, layer_ids: &LayerIds) -> NodeAdditions {
        let mut additions = match layer_ids {
            LayerIds::None => Vec::with_capacity(1),
            LayerIds::All => {
                let mut additions = Vec::with_capacity(self.layers.len() + 1);
                self.layers
                    .par_iter()
                    .map(|l| TimeStamps::new(l.nodes.additions.value(self.vid.index()), None))
                    .collect_into_vec(&mut additions);
                additions
            }
            LayerIds::One(id) => {
                vec![TimeStamps::new(
                    self.layers[*id].nodes.additions.value(self.vid.index()),
                    None,
                )]
            }
            LayerIds::Multiple(ids) => {
                let mut additions = Vec::with_capacity(ids.len() + 1);
                ids.par_iter()
                    .map(|l| TimeStamps::new(self.layers[*l].nodes.additions.value(self.vid.index()), None))
                    .collect_into_vec(&mut additions);
                additions
            }
        };
        if let Some(props) = self.properties {
            let timestamps = props.temporal_props.timestamps(self.vid);
            if timestamps.len() > 0 {
                let mut additions = Vec::with_capacity(self.layers.len() + 1);
                let ts = timestamps.times();
                additions.push(ts);
            }
        }
        NodeAdditions::Col(additions)
    }
}
