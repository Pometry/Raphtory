use crate::{
    core::{
        entities::{
            edges::edge_ref::{Dir, EdgeRef},
            graph::tgraph::InnerTemporalGraph,
            vertices::vertex_ref::VertexRef,
            LayerIds, EID, VID,
        },
        Direction,
    },
    db::api::view::internal::{EdgeFilter, GraphOps},
};
use itertools::Itertools;
use std::iter;

impl<const N: usize> GraphOps for InnerTemporalGraph<N> {
    fn internal_vertex_ref(
        &self,
        v: VertexRef,
        _layer_ids: &LayerIds,
        _filter: Option<&EdgeFilter>,
    ) -> Option<VID> {
        match v {
            VertexRef::Internal(l) => Some(l),
            VertexRef::External(_) => {
                let vid = self.inner().resolve_vertex_ref(v)?;
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
            .map(|f| f(&e, layer_ids))
            .unwrap_or(true)
            .then(|| EdgeRef::new_outgoing(e_id, e.src(), e.dst()))
    }

    fn vertices_len(&self, _layer_ids: LayerIds, _filter: Option<&EdgeFilter>) -> usize {
        self.inner().internal_num_vertices()
    }

    fn edges_len(&self, layers: LayerIds, filter: Option<&EdgeFilter>) -> usize {
        self.inner().num_edges(&layers, filter)
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

    fn vertex_refs(
        &self,
        _layers: LayerIds,
        _filter: Option<&EdgeFilter>,
    ) -> Box<dyn Iterator<Item = VID> + Send> {
        Box::new(self.inner().vertex_ids())
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
                    .map(|f| f(&self.inner().storage.edges.get((*eid).into()), layer))
                    .unwrap_or(true)
            })
            .map(|e_id| EdgeRef::new_outgoing(e_id, src, dst))
    }

    fn edge_refs(
        &self,
        layers: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
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
                    .filter(move |e| filter.as_ref().map(|f| f(e, &layers)).unwrap_or(true))
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
                            .map(|f| f(edge, &layers))
                            .unwrap_or_else(|| edge.has_layer(&layers))
                    })
                    .map(|edge| edge.into()),
            ),
        }
    }

    fn vertex_edges(
        &self,
        v: VID,
        d: Direction,
        layers: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
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
                        Box::new(iter.filter(move |eref| {
                            filter(&edge_store.get(eref.pid().into()), &layers)
                        }))
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
                        Box::new(iter.filter(move |eref| {
                            filter(&edge_store.get(eref.pid().into()), &layers)
                        }))
                    }
                }
            }
            Direction::BOTH => Box::new(
                self.vertex_edges(v, Direction::IN, layers.clone(), filter)
                    .merge(self.vertex_edges(v, Direction::OUT, layers, filter)),
            ),
        }
    }

    fn neighbours(
        &self,
        v: VID,
        d: Direction,
        layers: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Box<dyn Iterator<Item = VID> + Send> {
        let iter = self.vertex_edges(v, d, layers, filter).map(|e| e.remote());
        if matches!(d, Direction::BOTH) {
            Box::new(iter.dedup())
        } else {
            Box::new(iter)
        }
    }
}
