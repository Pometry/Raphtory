use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, EID, VID},
        Direction,
    },
    db::api::{
        storage::{
            nodes::node_storage_ops::{NodeStorageIntoOps, NodeStorageOps},
            tprop_storage_ops::TPropOps,
            variants::{direction_variants::DirectionVariants, layer_variants::LayerVariants},
        },
        view::internal::NodeAdditions,
    },
};
use itertools::Itertools;
use raphtory_arrow::{
    graph::TemporalGraph, graph_fragment::TempColGraphFragment, properties::Properties,
    timestamps::TimeStamps,
};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::{iter, sync::Arc};

#[derive(Copy, Clone, Debug)]
pub struct ArrowNode<'a> {
    pub(super) properties: Option<&'a Properties<VID>>,
    pub(super) layers: &'a Arc<[TempColGraphFragment]>,
    pub(super) vid: VID,
}

impl<'a> ArrowNode<'a> {
    pub(crate) fn new(graph: &'a TemporalGraph, vid: VID) -> Self {
        Self {
            properties: graph.node_properties(),
            layers: graph.arc_layers(),
            vid,
        }
    }

    pub fn out_edges(self, layers: &'a LayerIds) -> impl Iterator<Item = EdgeRef> + 'a {
        match layers {
            LayerIds::None => LayerVariants::None(iter::empty()),
            LayerIds::All => LayerVariants::All(
                self.layers
                    .iter()
                    .enumerate()
                    .map(|(layer_id, layer)| {
                        layer
                            .nodes_storage()
                            .out_adj_list(self.vid)
                            .map(move |(eid, dst)| {
                                EdgeRef::new_outgoing(eid, self.vid, dst)
                                    .at_layer(layer_id)
                            })
                    })
                    .kmerge_by(|e1, e2| e1.remote() <= e2.remote()),
            ),
            LayerIds::One(layer_id) => LayerVariants::One(
                self.layers[*layer_id]
                    .nodes_storage()
                    .out_adj_list(self.vid)
                    .map(move |(eid, dst)| {
                        EdgeRef::new_outgoing(eid, self.vid, dst)
                            .at_layer(*layer_id)
                    }),
            ),
            LayerIds::Multiple(ids) => LayerVariants::Multiple(
                ids.iter()
                    .map(|&layer_id| {
                        self.layers[layer_id]
                            .nodes_storage()
                            .out_adj_list(self.vid)
                            .map(move |(eid, dst)| {
                                EdgeRef::new_outgoing(eid, self.vid, dst)
                                    .at_layer(layer_id)
                            })
                    })
                    .kmerge_by(|e1, e2| e1.remote() <= e2.remote()),
            ),
        }
    }

    pub fn in_edges(self, layers: &'a LayerIds) -> impl Iterator<Item = EdgeRef> + 'a {
        match layers {
            LayerIds::None => LayerVariants::None(iter::empty()),
            LayerIds::All => LayerVariants::All(
                self.layers
                    .iter()
                    .enumerate()
                    .map(|(layer_id, layer)| {
                        layer
                            .nodes_storage()
                            .in_adj_list(self.vid)
                            .map(move |(eid, src)| {
                                EdgeRef::new_incoming(eid, src, self.vid)
                                    .at_layer(layer_id)
                            })
                    })
                    .kmerge_by(|e1, e2| e1.remote() <= e2.remote()),
            ),
            LayerIds::One(layer_id) => LayerVariants::One(
                self.layers[*layer_id]
                    .nodes_storage()
                    .in_adj_list(self.vid)
                    .map(move |(eid, src)| {
                        EdgeRef::new_incoming(eid, src, self.vid)
                            .at_layer(*layer_id)
                    }),
            ),
            LayerIds::Multiple(ids) => LayerVariants::Multiple(
                ids.iter()
                    .map(|&layer_id| {
                        self.layers[layer_id]
                            .nodes_storage()
                            .in_adj_list(self.vid)
                            .map(move |(eid, src)| {
                                EdgeRef::new_incoming(eid, src, self.vid)
                                    .at_layer(layer_id)
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

    pub fn additions_for_layers(self, layer_ids: &LayerIds) -> NodeAdditions<'a> {
        let mut additions = match layer_ids {
            LayerIds::None => Vec::with_capacity(1),
            LayerIds::All => {
                let mut additions = Vec::with_capacity(self.layers.len() + 1);
                self.layers
                    .par_iter()
                    .map(|l| {
                        TimeStamps::new(l.nodes_storage().additions().value(self.vid.index()), None)
                    })
                    .collect_into_vec(&mut additions);
                additions
            }
            LayerIds::One(id) => {
                vec![TimeStamps::new(
                    self.layers[*id]
                        .nodes_storage()
                        .additions()
                        .value(self.vid.index()),
                    None,
                )]
            }
            LayerIds::Multiple(ids) => {
                let mut additions = Vec::with_capacity(ids.len() + 1);
                ids.par_iter()
                    .map(|l| {
                        TimeStamps::new(
                            self.layers[*l]
                                .nodes_storage()
                                .additions()
                                .value(self.vid.index()),
                            None,
                        )
                    })
                    .collect_into_vec(&mut additions);
                additions
            }
        };
        if let Some(props) = self.properties {
            let timestamps = props.temporal_props.timestamps::<i64>(self.vid);
            if timestamps.len() > 0 {
                let ts = timestamps.times();
                additions.push(ts);
            }
        }
        NodeAdditions::Col(additions)
    }
}

impl<'a> NodeStorageOps<'a> for ArrowNode<'a> {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize {
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
                Some(layer) => layer.nodes_storage().out_degree(self.vid),
            },
            Direction::IN => match single_layer {
                None => self
                    .in_edges(layers)
                    .dedup_by(|e1, e2| e1.remote() <= e2.remote())
                    .count(),
                Some(layer) => layer.nodes_storage().in_degree(self.vid),
            },
            Direction::BOTH => match single_layer {
                None => self
                    .edges(layers)
                    .dedup_by(|e1, e2| e1.remote() <= e2.remote())
                    .count(),
                Some(layer) => layer
                    .nodes_storage()
                    .in_neighbours_iter(self.vid)
                    .merge(layer.nodes_storage().out_neighbours_iter(self.vid))
                    .dedup()
                    .count(),
            },
        }
    }

    fn additions(self) -> NodeAdditions<'a> {
        self.additions_for_layers(&LayerIds::All)
    }

    fn tprop(self, prop_id: usize) -> impl TPropOps<'a> {
        self.properties
            .unwrap()
            .temporal_props
            .prop(self.vid, prop_id)
    }

    fn edges_iter(
        self,
        layers: &'a LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + 'a {
        match dir {
            Direction::OUT => DirectionVariants::Out(self.out_edges(layers)),
            Direction::IN => DirectionVariants::In(self.in_edges(layers)),
            Direction::BOTH => DirectionVariants::Both(self.edges(layers)),
        }
    }

    fn node_type_id(self) -> usize {
        0
    }

    fn vid(self) -> VID {
        self.vid
    }

    fn name(self) -> Option<&'a str> {
        todo!()
    }

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef> {
        match layer_ids {
            LayerIds::None => None,
            LayerIds::All => match self.layers.len() {
                0 => None,
                1 => {
                    let eid = self.layers[0].nodes_storage().find_edge(self.vid, dst)?;
                    Some(EdgeRef::new_outgoing(eid, self.vid, dst).at_layer(0))
                }
                _ => todo!("multilayer edge views not implemented in arrow yet"),
            },
            LayerIds::One(id) => {
                let eid = self.layers[*id].nodes_storage().find_edge(self.vid, dst)?;
                Some(EdgeRef::new_outgoing(eid, self.vid, dst).at_layer(*id))
            }
            LayerIds::Multiple(ids) => match ids.len() {
                0 => None,
                1 => {
                    let layer = ids[0];
                    let eid = self.layers[layer]
                        .nodes_storage()
                        .find_edge(self.vid, dst)?;
                    Some(
                        EdgeRef::new_outgoing(eid, self.vid, dst)
                            .at_layer(layer),
                    )
                }
                _ => todo!("multtilayer edge views not implemented in arrow yet"),
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct ArrowOwnedNode {
    properties: Option<Properties<VID>>,
    layers: Arc<[TempColGraphFragment]>,
    vid: VID,
}

impl ArrowOwnedNode {
    pub(crate) fn new(graph: &TemporalGraph, vid: VID) -> Self {
        Self {
            properties: graph.node_properties().cloned(),
            layers: graph.arc_layers().clone(),
            vid,
        }
    }
    pub fn as_ref(&self) -> ArrowNode {
        ArrowNode {
            properties: self.properties.as_ref(),
            layers: &self.layers,
            vid: self.vid,
        }
    }

    fn out_edges(self, layers: LayerIds) -> impl Iterator<Item = EdgeRef> {
        match layers {
            LayerIds::None => LayerVariants::None(iter::empty()),
            LayerIds::All => {
                let layers = self.layers;
                LayerVariants::All(
                    (0..layers.len())
                        .map(move |layer_id| {
                            let layer = &layers[layer_id];
                            let adj = layer
                                .nodes_storage()
                                .adj_out()
                                .clone()
                                .into_value(self.vid.index());
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
                    .nodes_storage()
                    .adj_out()
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
                            .nodes_storage()
                            .adj_out()
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

    pub fn in_edges(self, layers: LayerIds) -> impl Iterator<Item = EdgeRef> {
        match layers {
            LayerIds::None => LayerVariants::None(iter::empty()),
            LayerIds::All => {
                let layers = self.layers;
                LayerVariants::All(
                    (0..layers.len())
                        .map(move |layer_id| {
                            let layer = &layers[layer_id];
                            let eids = layer
                                .nodes_storage()
                                .adj_in_edges()
                                .clone()
                                .into_value(self.vid.index())
                                .map(|i| EID(i as usize));
                            let nbrs = layer
                                .nodes_storage()
                                .adj_in_neighbours()
                                .clone()
                                .into_value(self.vid.index())
                                .map(|i| VID(i as usize));
                            let dst = self.vid;
                            eids.zip(nbrs).map(move |(eid, src)| {
                                EdgeRef::new_incoming(eid, src, dst).at_layer(layer_id)
                            })
                        })
                        .kmerge_by(|e1, e2| e1.remote() <= e2.remote()),
                )
            }
            LayerIds::One(layer_id) => {
                let layer = &self.layers[layer_id];
                let eids = layer
                    .nodes_storage()
                    .adj_in_edges()
                    .clone()
                    .into_value(self.vid.index())
                    .map(|i| EID(i as usize));
                let nbrs = layer
                    .nodes_storage()
                    .adj_in_neighbours()
                    .clone()
                    .into_value(self.vid.index())
                    .map(|i| VID(i as usize));
                let dst = self.vid;
                LayerVariants::One(
                    eids.zip(nbrs).map(move |(eid, src)| {
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
                            .nodes_storage()
                            .adj_in_edges()
                            .clone()
                            .into_value(self.vid.index())
                            .map(|i| EID(i as usize));
                        let nbrs = layer
                            .nodes_storage()
                            .adj_in_neighbours()
                            .clone()
                            .into_value(self.vid.index())
                            .map(|i| VID(i as usize));
                        let dst = self.vid;
                        eids.zip(nbrs).map(move |(eid, src)| {
                            EdgeRef::new_incoming(eid, src, dst).at_layer(layer_id)
                        })
                    })
                    .kmerge_by(|e1, e2| e1.remote() <= e2.remote()),
            ),
        }
    }

    pub fn edges(self, layers: LayerIds) -> impl Iterator<Item = EdgeRef> {
        self.clone().in_edges(layers.clone()).merge_by(
            self.out_edges(layers).filter(|e| e.src() != e.dst()),
            |e1, e2| e1.remote() <= e2.remote(),
        )
    }
}

impl<'a> NodeStorageOps<'a> for &'a ArrowOwnedNode {
    #[inline]
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize {
        self.as_ref().degree(layers, dir)
    }

    #[inline]
    fn additions(self) -> NodeAdditions<'a> {
        self.as_ref().additions()
    }

    #[inline]
    fn tprop(self, prop_id: usize) -> impl TPropOps<'a> {
        self.as_ref().tprop(prop_id)
    }

    #[inline]
    fn edges_iter(
        self,
        layers: &'a LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + 'a {
        match dir {
            Direction::OUT => DirectionVariants::Out(self.as_ref().out_edges(layers)),
            Direction::IN => DirectionVariants::In(self.as_ref().in_edges(layers)),
            Direction::BOTH => DirectionVariants::Both(self.as_ref().edges(layers)),
        }
    }

    #[inline]
    fn node_type_id(self) -> usize {
        self.as_ref().node_type_id()
    }

    fn vid(self) -> VID {
        self.vid
    }

    fn name(self) -> Option<&'a str> {
        self.as_ref().name()
    }

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef> {
        self.as_ref().find_edge(dst, layer_ids)
    }
}

impl NodeStorageIntoOps for ArrowOwnedNode {
    fn into_edges_iter(self, layers: LayerIds, dir: Direction) -> impl Iterator<Item = EdgeRef> {
        match dir {
            Direction::OUT => DirectionVariants::Out(self.out_edges(layers)),
            Direction::IN => DirectionVariants::In(self.in_edges(layers)),
            Direction::BOTH => DirectionVariants::Both(self.edges(layers)),
        }
    }
}
