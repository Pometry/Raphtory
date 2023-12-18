use crate::{
    core::{
        entities::{
            edges::edge_ref::{Dir, EdgeRef},
            graph::tgraph::InnerTemporalGraph,
            nodes::node_ref::NodeRef,
            LayerIds, EID, VID,
        },
        Direction,
    },
    db::api::view::{
        internal::{EdgeFilter, GraphOps},
        BoxedLIter,
    },
};
use itertools::Itertools;
use rayon::prelude::*;
use std::{iter, ops::Deref};

impl<'graph, const N: usize> GraphOps<'graph> for InnerTemporalGraph<N> {
    fn node_refs(
        &self,
        _layers: LayerIds,
        _filter: Option<&EdgeFilter>,
    ) -> Box<dyn Iterator<Item = VID> + Send> {
        Box::new(self.inner().node_ids())
    }

    fn edge_refs(
        &self,
        layers: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> BoxedLIter<'graph, EdgeRef> {
        let filter = filter.cloned();
        match layers {
            LayerIds::None => Box::new(iter::empty()),
            LayerIds::All => {
                let iter = self
                    .inner()
                    .storage
                    .edges
                    .read_lock()
                    .into_iter()
                    .filter(move |e| {
                        filter
                            .as_ref()
                            .map(|f| f(e.deref(), &layers))
                            .unwrap_or(true)
                    })
                    .map_into();
                Box::new(iter)
            }
            _ => Box::new(
                self.inner()
                    .storage
                    .edges
                    .read_lock()
                    .into_iter()
                    .filter(move |edge| {
                        filter
                            .as_ref()
                            .map(|f| f(edge.deref(), &layers))
                            .unwrap_or_else(|| edge.has_layer(&layers))
                    })
                    .map(|edge| edge.into()),
            ),
        }
    }

    fn node_edges(
        &self,
        v: VID,
        d: Direction,
        layers: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> BoxedLIter<'graph, EdgeRef> {
        let entry = self.inner().storage.nodes.entry_arc(v.into());
        match d {
            Direction::OUT => {
                let iter: Box<dyn Iterator<Item = EdgeRef> + Send> =
                    match &layers {
                        LayerIds::None => Box::new(iter::empty()),
                        LayerIds::All => Box::new(
                            entry
                                .into_layers()
                                .map(move |layer| {
                                    layer
                                        .into_tuples(Dir::Out)
                                        .map(move |(n, e)| EdgeRef::new_outgoing(e, v, n))
                                })
                                .kmerge()
                                .dedup(),
                        ),
                        LayerIds::One(layer) => {
                            Box::new(entry.into_layer(*layer).into_iter().flat_map(move |it| {
                                it.into_tuples(Dir::Out)
                                    .map(move |(n, e)| EdgeRef::new_outgoing(e, v, n))
                            }))
                        }
                        LayerIds::Multiple(ids) => Box::new(
                            ids.iter()
                                .map(move |&layer| {
                                    entry.clone().into_layer(layer).into_iter().flat_map(
                                        move |it| {
                                            it.into_tuples(Dir::Out)
                                                .map(move |(n, e)| EdgeRef::new_outgoing(e, v, n))
                                        },
                                    )
                                })
                                .kmerge()
                                .dedup(),
                        ),
                    };
                match filter.cloned() {
                    None => iter,
                    Some(filter) => {
                        let edge_store = self.inner().storage.edges.read_lock();
                        Box::new(
                            iter.filter(move |eref| {
                                filter(edge_store.get(eref.pid().into()), &layers)
                            }),
                        )
                    }
                }
            }
            Direction::IN => {
                let iter: Box<dyn Iterator<Item = EdgeRef> + Send> =
                    match &layers {
                        LayerIds::None => Box::new(iter::empty()),
                        LayerIds::All => Box::new(
                            entry
                                .into_layers()
                                .map(move |layer| {
                                    layer
                                        .into_tuples(Dir::Into)
                                        .map(move |(n, e)| EdgeRef::new_incoming(e, n, v))
                                })
                                .kmerge()
                                .dedup(),
                        ),
                        LayerIds::One(layer) => {
                            Box::new(entry.into_layer(*layer).into_iter().flat_map(move |it| {
                                it.into_tuples(Dir::Into)
                                    .map(move |(n, e)| EdgeRef::new_incoming(e, n, v))
                            }))
                        }
                        LayerIds::Multiple(ids) => Box::new(
                            ids.iter()
                                .map(move |&layer| {
                                    entry.clone().into_layer(layer).into_iter().flat_map(
                                        move |it| {
                                            it.into_tuples(Dir::Into)
                                                .map(move |(n, e)| EdgeRef::new_incoming(e, n, v))
                                        },
                                    )
                                })
                                .kmerge()
                                .dedup(),
                        ),
                    };
                match filter.cloned() {
                    None => iter,
                    Some(filter) => {
                        let edge_store = self.inner().storage.edges.read_lock();
                        Box::new(
                            iter.filter(move |eref| {
                                filter(edge_store.get(eref.pid().into()), &layers)
                            }),
                        )
                    }
                }
            }
            Direction::BOTH => Box::new(
                self.node_edges(v, Direction::IN, layers.clone(), filter)
                    .merge(self.node_edges(v, Direction::OUT, layers, filter)),
            ),
        }
    }

    fn neighbours(
        &self,
        v: VID,
        d: Direction,
        layers: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> BoxedLIter<'graph, VID> {
        let iter = self.node_edges(v, d, layers, filter).map(|e| e.remote());
        if matches!(d, Direction::BOTH) {
            Box::new(iter.dedup())
        } else {
            Box::new(iter)
        }
    }
    fn internal_node_ref(
        &self,
        v: NodeRef,
        _layer_ids: &LayerIds,
        _filter: Option<&EdgeFilter>,
    ) -> Option<VID> {
        match v {
            NodeRef::Internal(l) => Some(l),
            NodeRef::External(_) => {
                let vid = self.inner().resolve_node_ref(v)?;
                Some(vid)
            }
        }
    }

    fn find_edge_id(
        &self,
        e_id: EID,
        layer_ids: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Option<EdgeRef> {
        let e_id_usize: usize = e_id.into();
        if e_id_usize >= self.inner().storage.edges.len() {
            return None;
        }
        let e = self.inner().storage.edges.get(e_id_usize);
        filter
            .map(|f| f(&*e, layer_ids))
            .unwrap_or(true)
            .then(|| EdgeRef::new_outgoing(e_id, e.src(), e.dst()))
    }

    fn nodes_len(&self, _layer_ids: LayerIds, _filter: Option<&EdgeFilter>) -> usize {
        self.inner().internal_num_nodes()
    }

    fn edges_len(&self, layers: LayerIds, filter: Option<&EdgeFilter>) -> usize {
        self.inner().num_edges(&layers, filter)
    }

    fn temporal_edges_len(&self, layers: LayerIds, filter: Option<&EdgeFilter>) -> usize {
        let edges = self.inner().storage.edges.read_lock();
        edges
            .par_iter()
            .filter(|&e| e.has_layer(&layers) && filter.map(|f| f(e, &layers)).unwrap_or(true))
            .map(|e| e.additions_iter(&layers).map(|ts| ts.len()).sum::<usize>())
            .sum()
    }

    #[inline]
    fn degree(
        &self,
        v: VID,
        d: Direction,
        layers: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> usize {
        self.inner().degree(v, d, layers, filter)
    }

    fn edge_ref(
        &self,
        src: VID,
        dst: VID,
        layer: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Option<EdgeRef> {
        self.inner()
            .find_edge(src, dst, layer)
            .filter(|eid| {
                filter
                    .map(|f| f(&*self.inner().storage.edges.get((*eid).into()), layer))
                    .unwrap_or(true)
            })
            .map(|e_id| EdgeRef::new_outgoing(e_id, src, dst))
    }
}
