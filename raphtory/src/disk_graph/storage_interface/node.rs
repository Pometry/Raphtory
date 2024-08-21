use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, EID, VID},
        Direction,
    },
    db::api::{
        storage::graph::{
            nodes::node_storage_ops::{NodeStorageIntoOps, NodeStorageOps},
            tprop_storage_ops::TPropOps,
            variants::{direction_variants::DirectionVariants, layer_variants::LayerVariants},
        },
        view::internal::NodeAdditions,
    },
    prelude::Prop,
};
use itertools::Itertools;
use polars_arrow::datatypes::ArrowDataType;
use pometry_storage::{graph::TemporalGraph, timestamps::TimeStamps, GidRef};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::{borrow::Cow, iter, sync::Arc};

#[derive(Copy, Clone, Debug)]
pub struct DiskNode<'a> {
    graph: &'a TemporalGraph,
    pub(super) vid: VID,
}

impl<'a> DiskNode<'a> {
    pub fn constant_node_prop_ids(self) -> Box<dyn Iterator<Item = usize> + 'a> {
        match &self.graph.node_properties().const_props {
            None => Box::new(std::iter::empty()),
            Some(props) => {
                Box::new((0..props.num_props()).filter(move |id| props.has_prop(self.vid, *id)))
            }
        }
    }

    pub fn temporal_node_prop_ids(self) -> Box<dyn Iterator<Item = usize> + 'a> {
        Box::new(std::iter::empty())
    }

    pub(crate) fn new(graph: &'a TemporalGraph, vid: VID) -> Self {
        Self { graph, vid }
    }

    pub fn out_edges(self, layers: &'a LayerIds) -> impl Iterator<Item = EdgeRef> + 'a {
        match layers {
            LayerIds::None => LayerVariants::None(iter::empty()),
            LayerIds::All => LayerVariants::All(
                self.graph
                    .layers()
                    .iter()
                    .enumerate()
                    .map(|(layer_id, layer)| {
                        layer
                            .nodes_storage()
                            .out_adj_list(self.vid)
                            .map(move |(eid, dst)| {
                                EdgeRef::new_outgoing(eid, self.vid, dst).at_layer(layer_id)
                            })
                    })
                    .kmerge_by(|e1, e2| e1.remote() <= e2.remote()),
            ),
            LayerIds::One(layer_id) => LayerVariants::One(
                self.graph.layers()[*layer_id]
                    .nodes_storage()
                    .out_adj_list(self.vid)
                    .map(move |(eid, dst)| {
                        EdgeRef::new_outgoing(eid, self.vid, dst).at_layer(*layer_id)
                    }),
            ),
            LayerIds::Multiple(ids) => LayerVariants::Multiple(
                ids.iter()
                    .map(|&layer_id| {
                        self.graph.layers()[layer_id]
                            .nodes_storage()
                            .out_adj_list(self.vid)
                            .map(move |(eid, dst)| {
                                EdgeRef::new_outgoing(eid, self.vid, dst).at_layer(layer_id)
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
                self.graph
                    .layers()
                    .iter()
                    .enumerate()
                    .map(|(layer_id, layer)| {
                        layer
                            .nodes_storage()
                            .in_adj_list(self.vid)
                            .map(move |(eid, src)| {
                                EdgeRef::new_incoming(eid, src, self.vid).at_layer(layer_id)
                            })
                    })
                    .kmerge_by(|e1, e2| e1.remote() <= e2.remote()),
            ),
            LayerIds::One(layer_id) => LayerVariants::One(
                self.graph.layers()[*layer_id]
                    .nodes_storage()
                    .in_adj_list(self.vid)
                    .map(move |(eid, src)| {
                        EdgeRef::new_incoming(eid, src, self.vid).at_layer(*layer_id)
                    }),
            ),
            LayerIds::Multiple(ids) => LayerVariants::Multiple(
                ids.iter()
                    .map(|&layer_id| {
                        self.graph.layers()[layer_id]
                            .nodes_storage()
                            .in_adj_list(self.vid)
                            .map(move |(eid, src)| {
                                EdgeRef::new_incoming(eid, src, self.vid).at_layer(layer_id)
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

    pub fn additions_for_layers(&self, layer_ids: &LayerIds) -> NodeAdditions<'a> {
        let mut additions = match layer_ids {
            LayerIds::None => Vec::with_capacity(1),
            LayerIds::All => {
                let mut additions = Vec::with_capacity(self.graph.layers().len() + 1);
                self.graph
                    .layers()
                    .par_iter()
                    .map(|l| {
                        TimeStamps::new(l.nodes_storage().additions().value(self.vid.index()), None)
                    })
                    .collect_into_vec(&mut additions);
                additions
            }
            LayerIds::One(id) => {
                vec![TimeStamps::new(
                    self.graph.layers()[*id]
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
                            self.graph.layers()[*l]
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

        if let Some(props) = &self.graph.node_properties().temporal_props {
            let timestamps = props.timestamps::<i64>(self.vid);
            if timestamps.len() > 0 {
                let ts = timestamps.times();
                additions.push(ts);
            }
        }
        NodeAdditions::Col(additions)
    }
}

impl<'a> NodeStorageOps<'a> for DiskNode<'a> {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize {
        let single_layer = match layers {
            LayerIds::None => return 0,
            LayerIds::All => match self.graph.layers().len() {
                0 => return 0,
                1 => Some(&self.graph.layers()[0]),
                _ => None,
            },
            LayerIds::One(id) => Some(&self.graph.layers()[*id]),
            LayerIds::Multiple(ids) => match ids.len() {
                0 => return 0,
                1 => Some(&self.graph.layers()[ids[0]]),
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
        self.graph
            .node_properties()
            .temporal_props
            .as_ref()
            .unwrap()
            .prop(self.vid, prop_id)
    }

    fn prop(self, prop_id: usize) -> Option<Prop> {
        let cprops = self.graph.node_properties().const_props.as_ref()?;
        let prop_type = cprops.prop_dtype(prop_id);
        match prop_type.data_type {
            ArrowDataType::Int32 => cprops.prop_native::<i32>(self.vid, prop_id).map(Prop::I32),
            ArrowDataType::Int64 => cprops.prop_native::<i64>(self.vid, prop_id).map(Prop::I64),
            ArrowDataType::UInt32 => cprops.prop_native::<u32>(self.vid, prop_id).map(Prop::U32),
            ArrowDataType::UInt64 => cprops.prop_native::<u64>(self.vid, prop_id).map(Prop::U64),
            ArrowDataType::Float32 => cprops.prop_native::<f32>(self.vid, prop_id).map(Prop::F32),
            ArrowDataType::Float64 => cprops.prop_native::<f64>(self.vid, prop_id).map(Prop::F64),
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => {
                cprops.prop_str(self.vid, prop_id).map(Prop::str)
            }
            // Add cases for other types, including special handling for complex types
            _ => None, // Placeholder for unhandled types
        }
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
        self.graph.node_type_id(self.vid)
    }

    fn vid(self) -> VID {
        self.vid
    }

    fn id(self) -> GidRef<'a> {
        self.graph.node_gid(self.vid).unwrap()
    }

    fn name(self) -> Option<Cow<'a, str>> {
        match self.graph.node_gid(self.vid).unwrap() {
            GidRef::U64(_) => None,
            GidRef::Str(v) => Some(Cow::from(v)),
        }
    }

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef> {
        match layer_ids {
            LayerIds::None => None,
            LayerIds::All => match self.graph.layers().len() {
                0 => None,
                1 => {
                    let eid = self.graph.layers()[0]
                        .nodes_storage()
                        .find_edge(self.vid, dst)?;
                    Some(EdgeRef::new_outgoing(eid, self.vid, dst).at_layer(0))
                }
                _ => todo!("multilayer edge views not implemented in diskgraph yet"),
            },
            LayerIds::One(id) => {
                let eid = self.graph.layers()[*id]
                    .nodes_storage()
                    .find_edge(self.vid, dst)?;
                Some(EdgeRef::new_outgoing(eid, self.vid, dst).at_layer(*id))
            }
            LayerIds::Multiple(ids) => match ids.len() {
                0 => None,
                1 => {
                    let layer = ids[0];
                    let eid = self.graph.layers()[layer]
                        .nodes_storage()
                        .find_edge(self.vid, dst)?;
                    Some(EdgeRef::new_outgoing(eid, self.vid, dst).at_layer(layer))
                }
                _ => todo!("multtilayer edge views not implemented in diskgraph yet"),
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct DiskOwnedNode {
    graph: Arc<TemporalGraph>,
    vid: VID,
}

impl DiskOwnedNode {
    pub(crate) fn new(graph: Arc<TemporalGraph>, vid: VID) -> Self {
        Self { graph, vid }
    }
    pub fn as_ref(&self) -> DiskNode {
        DiskNode {
            graph: &self.graph,
            vid: self.vid,
        }
    }

    fn out_edges(self, layers: LayerIds) -> impl Iterator<Item = EdgeRef> {
        match layers {
            LayerIds::None => LayerVariants::None(iter::empty()),
            LayerIds::All => {
                let layers = self.graph.arc_layers().clone();
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
                let adj = self.graph.layers()[layer_id]
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
                        let adj = self.graph.layers()[layer_id]
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
                let layers = self.graph.arc_layers().clone();
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
                let layer = self.graph.layer(layer_id);
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
                        let layer = self.graph.layer(layer_id);
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

impl<'a> NodeStorageOps<'a> for &'a DiskOwnedNode {
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

    #[inline]
    fn id(self) -> GidRef<'a> {
        self.as_ref().id()
    }

    fn name(self) -> Option<Cow<'a, str>> {
        self.as_ref().name()
    }

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef> {
        self.as_ref().find_edge(dst, layer_ids)
    }

    fn prop(self, prop_id: usize) -> Option<Prop> {
        self.as_ref().prop(prop_id)
    }
}

impl NodeStorageIntoOps for DiskOwnedNode {
    fn into_edges_iter(self, layers: LayerIds, dir: Direction) -> impl Iterator<Item = EdgeRef> {
        match dir {
            Direction::OUT => DirectionVariants::Out(self.out_edges(layers)),
            Direction::IN => DirectionVariants::In(self.in_edges(layers)),
            Direction::BOTH => DirectionVariants::Both(self.edges(layers)),
        }
    }
}
