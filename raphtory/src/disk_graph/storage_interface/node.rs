use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
        utils::iter::GenLockedIter,
        Direction,
    },
    db::api::{
        storage::graph::{
            nodes::{
                node_storage_ops::{NodeStorageIntoOps, NodeStorageOps},
                row::{DiskRow, Row},
            },
            tprop_storage_ops::TPropOps,
            variants::{direction_variants::DirectionVariants, layer_variants::LayerVariants},
        },
        view::{internal::NodeAdditions, BoxedLIter},
    },
    prelude::Prop,
};
use itertools::Itertools;
use polars_arrow::datatypes::ArrowDataType;
use pometry_storage::{
    graph::TemporalGraph, timestamps::LayerAdditions, tprops::DiskTProp, GidRef,
};
use raphtory_api::{
    core::storage::timeindex::{TimeIndexEntry, TimeIndexIntoOps},
    iter::IntoDynBoxed,
};
use std::{borrow::Cow, iter, ops::Range, sync::Arc};

#[derive(Copy, Clone, Debug)]
pub struct DiskNode<'a> {
    graph: &'a TemporalGraph,
    pub(super) vid: VID,
}

impl<'a> DiskNode<'a> {
    pub fn into_rows(self) -> impl Iterator<Item = (TimeIndexEntry, Row<'a>)> {
        self.graph
            .node_properties()
            .temporal_props()
            .into_iter()
            .enumerate()
            .flat_map(move |(layer, props)| {
                let ts = props.timestamps::<TimeIndexEntry>(self.vid);
                ts.into_iter().zip(0..ts.len()).map(move |(t, row)| {
                    let row = DiskRow::new(&self.graph, ts, row, layer);
                    (t, Row::Disk(row))
                })
            })
    }

    pub fn into_rows_window(
        self,
        window: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Row<'a>)> {
        self.graph
            .node_properties()
            .temporal_props()
            .into_iter()
            .enumerate()
            .flat_map(move |(layer, props)| {
                let ts = props.timestamps::<TimeIndexEntry>(self.vid);
                let ts = ts.into_range(window.clone());
                ts.into_iter().zip(0..ts.len()).map(move |(t, row)| {
                    let row = DiskRow::new(self.graph, ts, row, layer);
                    (t, Row::Disk(row))
                })
            })
    }

    pub fn last_before_row(self, t: TimeIndexEntry) -> Vec<(usize, Prop)> {
        self.graph
            .prop_mapping()
            .nodes()
            .into_iter()
            .enumerate()
            .filter_map(|(prop_id, &location)| {
                let (layer, local_prop_id) = location?;
                let layer = self.graph().node_properties().temporal_props().get(layer)?;
                let t_prop = layer.prop::<TimeIndexEntry>(self.vid, local_prop_id);
                t_prop.last_before(t).map(|(_, p)| (prop_id, p))
            })
            .collect()
    }

    pub fn constant_node_prop_ids(self) -> BoxedLIter<'a, usize> {
        match &self.graph.node_properties().const_props {
            None => Box::new(std::iter::empty()),
            Some(props) => {
                Box::new((0..props.num_props()).filter(move |id| props.has_prop(self.vid, *id)))
            }
        }
    }

    pub fn temporal_node_prop_ids(self) -> impl Iterator<Item = usize> + 'a {
        self.graph
            .prop_mapping()
            .nodes()
            .into_iter()
            .enumerate()
            .filter(|(_, exists)| exists.is_some())
            .map(|(id, _)| id)
    }

    pub(crate) fn new(graph: &'a TemporalGraph, vid: VID) -> Self {
        Self { graph, vid }
    }

    pub fn out_edges(self, layers: &LayerIds) -> impl Iterator<Item = EdgeRef> + 'a {
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
            LayerIds::One(layer_id) => {
                let layer_id = *layer_id;
                LayerVariants::One(
                    self.graph.layers()[layer_id]
                        .nodes_storage()
                        .out_adj_list(self.vid)
                        .map(move |(eid, dst)| {
                            EdgeRef::new_outgoing(eid, self.vid, dst).at_layer(layer_id)
                        }),
                )
            }
            LayerIds::Multiple(ids) => LayerVariants::Multiple(
                ids.into_iter()
                    .map(|layer_id| {
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

    pub fn in_edges(self, layers: &LayerIds) -> impl Iterator<Item = EdgeRef> + 'a {
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
            LayerIds::One(layer_id) => {
                let layer_id = *layer_id;
                LayerVariants::One(
                    self.graph.layers()[layer_id]
                        .nodes_storage()
                        .in_adj_list(self.vid)
                        .map(move |(eid, src)| {
                            EdgeRef::new_incoming(eid, src, self.vid).at_layer(layer_id)
                        }),
                )
            }
            LayerIds::Multiple(ids) => LayerVariants::Multiple(
                ids.into_iter()
                    .map(|layer_id| {
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

    pub fn edges(self, layers: &LayerIds) -> impl Iterator<Item = EdgeRef> + 'a {
        self.in_edges(layers)
            .merge_by(self.out_edges(layers), |e1, e2| e1.remote() <= e2.remote())
    }

    pub fn additions_for_layers(self, layer_ids: LayerIds) -> NodeAdditions<'a> {
        NodeAdditions::Col(LayerAdditions::new(self.graph, self.vid, layer_ids, None))
    }

    pub fn graph(&self) -> &TemporalGraph {
        self.graph
    }
}

impl<'a> NodeStorageOps<'a> for DiskNode<'a> {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize {
        let single_layer = match &layers {
            LayerIds::None => return 0,
            LayerIds::All => match self.graph.layers().len() {
                0 => return 0,
                1 => Some(&self.graph.layers()[0]),
                _ => None,
            },
            LayerIds::One(id) => Some(&self.graph.layers()[*id]),
            LayerIds::Multiple(ids) => match ids.len() {
                0 => return 0,
                1 => Some(&self.graph.layers()[ids.find(0).unwrap()]),
                _ => None,
            },
        };
        match dir {
            Direction::OUT => match single_layer {
                None => self
                    .out_edges(layers)
                    .dedup_by(|e1, e2| e1.remote() == e2.remote())
                    .count(),
                Some(layer) => layer.nodes_storage().out_degree(self.vid),
            },
            Direction::IN => match single_layer {
                None => self
                    .in_edges(layers)
                    .dedup_by(|e1, e2| e1.remote() == e2.remote())
                    .count(),
                Some(layer) => layer.nodes_storage().in_degree(self.vid),
            },
            Direction::BOTH => match single_layer {
                None => self
                    .edges(layers)
                    .dedup_by(|e1, e2| e1.remote() == e2.remote())
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
        self.additions_for_layers(LayerIds::All)
    }

    fn tprop(self, prop_id: usize) -> impl TPropOps<'a> {
        self.graph
            .prop_mapping()
            .localise_node_prop_id(prop_id)
            .and_then(|(layer, local_prop_id)| {
                self.graph
                    .node_properties()
                    .temporal_props()
                    .get(layer)
                    .map(|t_props| t_props.prop(self.vid, local_prop_id))
            })
            .unwrap_or(DiskTProp::empty())
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
        layers: &LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + Send + 'a {
        //FIXME: something is capturing the &LayerIds lifetime when using impl Iterator
        Box::new(match dir {
            Direction::OUT => DirectionVariants::Out(self.out_edges(layers)),
            Direction::IN => DirectionVariants::In(self.in_edges(layers)),
            Direction::BOTH => DirectionVariants::Both(self.edges(layers)),
        })
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
            LayerIds::All => self
                .graph
                .find_edge(self.vid, dst)
                .map(|e| EdgeRef::new_outgoing(e.pid(), self.vid, dst)),
            LayerIds::One(id) => {
                let eid = self.graph.layers()[*id]
                    .nodes_storage()
                    .find_edge(self.vid, dst)?;
                Some(EdgeRef::new_outgoing(eid, self.vid, dst))
            }
            LayerIds::Multiple(ids) => ids
                .iter()
                .filter_map(|layer_id| {
                    self.graph.layers()[layer_id]
                        .nodes_storage()
                        .find_edge(self.vid, dst)
                        .map(|eid| EdgeRef::new_outgoing(eid, self.vid, dst))
                })
                .next(),
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
                let iter = GenLockedIter::from(self, |owned_node| {
                    let layers = owned_node.graph.arc_layers();
                    (0..layers.len())
                        .map(move |layer_id| {
                            let layer = &layers[layer_id];
                            let eids = layer.nodes_storage().out_edges_iter(owned_node.vid);
                            let nbrs = layer.nodes_storage().out_neighbours_iter(owned_node.vid);
                            eids.zip(nbrs).map(move |(eid, dst)| {
                                EdgeRef::new_outgoing(eid, owned_node.vid, dst)
                            })
                        })
                        .kmerge_by(|e1, e2| e1.remote() <= e2.remote())
                        .dedup_by(|e1, e2| e1.remote() == e2.remote())
                        .into_dyn_boxed()
                });
                LayerVariants::All(iter)
            }
            LayerIds::One(layer_id) => {
                let iter = GenLockedIter::from(self, |owned_node| {
                    let layer = owned_node.graph.layer(layer_id);
                    let eids = layer.nodes_storage().out_edges_iter(owned_node.vid);
                    let nbrs = layer.nodes_storage().out_neighbours_iter(owned_node.vid);
                    eids.zip(nbrs)
                        .map(move |(eid, dst)| EdgeRef::new_outgoing(eid, owned_node.vid, dst))
                        .into_dyn_boxed()
                });
                LayerVariants::One(iter)
            }
            LayerIds::Multiple(ids) => {
                LayerVariants::Multiple(GenLockedIter::from(self, |owned_node| {
                    ids.into_iter()
                        .map(move |layer_id| {
                            let layer = owned_node.graph.layer(layer_id);
                            let eids = layer.nodes_storage().out_edges_iter(owned_node.vid);
                            let nbrs = layer.nodes_storage().out_neighbours_iter(owned_node.vid);
                            let src = owned_node.vid;
                            eids.zip(nbrs)
                                .map(move |(eid, dst)| EdgeRef::new_outgoing(eid, src, dst))
                        })
                        .kmerge_by(|e1, e2| e1.remote() <= e2.remote())
                        .dedup_by(|e1, e2| e1.remote() == e2.remote())
                        .into_dyn_boxed()
                }))
            }
        }
    }

    pub fn in_edges(self, layers: LayerIds) -> impl Iterator<Item = EdgeRef> {
        match layers {
            LayerIds::None => LayerVariants::None(iter::empty()),
            LayerIds::All => {
                let iter = GenLockedIter::from(self, |owned_node| {
                    let layers = owned_node.graph.arc_layers();
                    (0..layers.len())
                        .map(move |layer_id| {
                            let layer = &layers[layer_id];
                            let eids = layer.nodes_storage().in_edges_iter(owned_node.vid);
                            let nbrs = layer.nodes_storage().in_neighbours_iter(owned_node.vid);
                            let dst = owned_node.vid;
                            eids.zip(nbrs)
                                .map(move |(eid, src)| EdgeRef::new_incoming(eid, src, dst))
                        })
                        .kmerge_by(|e1, e2| e1.remote() <= e2.remote())
                        .dedup_by(|e1, e2| e1.remote() == e2.remote())
                        .into_dyn_boxed()
                });
                LayerVariants::All(iter)
            }
            LayerIds::One(layer_id) => {
                let iter = GenLockedIter::from(self, |owned_node| {
                    let layer = owned_node.graph.layer(layer_id);
                    let eids = layer.nodes_storage().in_edges_iter(owned_node.vid);
                    let nbrs = layer.nodes_storage().in_neighbours_iter(owned_node.vid);
                    let dst = owned_node.vid;
                    eids.zip(nbrs)
                        .map(move |(eid, src)| EdgeRef::new_incoming(eid, src, dst))
                        .into_dyn_boxed()
                });
                LayerVariants::One(iter)
            }
            LayerIds::Multiple(ids) => {
                let iter = GenLockedIter::from(self, |owned_node| {
                    ids.into_iter()
                        .map(move |layer_id| {
                            let layer = owned_node.graph.layer(layer_id);
                            let eids = layer.nodes_storage().in_edges_iter(owned_node.vid);
                            let nbrs = layer.nodes_storage().in_neighbours_iter(owned_node.vid);
                            let dst = owned_node.vid;
                            eids.zip(nbrs)
                                .map(move |(eid, src)| EdgeRef::new_incoming(eid, src, dst))
                        })
                        .kmerge_by(|e1, e2| e1.remote() <= e2.remote())
                        .dedup_by(|e1, e2| e1.remote() == e2.remote())
                        .into_dyn_boxed()
                });

                LayerVariants::Multiple(iter)
            }
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
    fn edges_iter(self, layers: &LayerIds, dir: Direction) -> impl Iterator<Item = EdgeRef> + 'a {
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
