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
    db::api::view::{internal::GraphOps, Layer},
    prelude::GraphViewOps,
};
use genawaiter::sync::GenBoxed;
use itertools::Itertools;
use std::{iter, ops::Deref};

impl<const N: usize> GraphOps for InnerTemporalGraph<N> {
    fn layer_ids(&self) -> LayerIds {
        LayerIds::All
    }

    fn local_vertex_ref(&self, v: VertexRef) -> Option<VID> {
        match v {
            VertexRef::Local(l) => Some(l),
            VertexRef::Remote(_) => {
                let vid = self.inner().resolve_vertex_ref(v)?;
                Some(vid)
            }
        }
    }

    fn find_edge_id(&self, e_id: EID) -> Option<EdgeRef> {
        let e_id_usize: usize = e_id.into();
        if e_id_usize >= self.num_edges() {
            return None;
        }
        let edge_view = self.inner().edge(e_id);
        Some(EdgeRef::new_outgoing(
            e_id,
            edge_view.src_id(),
            edge_view.dst_id(),
        ))
    }

    fn layer_ids_from_names(&self, key: Layer) -> LayerIds {
        self.inner().layer_id(key)
    }

    fn edge_layer_ids(&self, e_id: EID) -> LayerIds {
        let edge = self.inner().edge(e_id);
        edge.layer_ids()
    }

    fn vertices_len(&self) -> usize {
        self.inner().internal_num_vertices()
    }

    fn edges_len(&self, layers: LayerIds) -> usize {
        self.inner().internal_num_edges(layers)
    }

    fn edges_len_window(&self, t_start: i64, t_end: i64, layers: LayerIds) -> usize {
        self.inner()
            .internal_num_edges_window(layers, t_start..t_end)
    }

    fn degree(&self, v: VID, d: Direction, layers: LayerIds) -> usize {
        self.inner().degree(v, d, layers)
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VID> + Send> {
        Box::new(self.inner().vertex_ids())
    }

    fn edge_ref(&self, src: VID, dst: VID, layer: LayerIds) -> Option<EdgeRef> {
        self.inner()
            .find_edge(src, dst, layer)
            .map(|e_id| EdgeRef::new_outgoing(e_id, src, dst))
    }

    fn edge_refs(&self, layers: LayerIds) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        match layers {
            LayerIds::None => Box::new(iter::empty()),
            LayerIds::All => {
                let iter = self
                    .inner()
                    .storage
                    .edges
                    .read_lock()
                    .into_iter()
                    .map_into();
                Box::new(iter)
            }
            _ => Box::new(
                self.inner()
                    .storage
                    .edges
                    .read_lock()
                    .into_iter()
                    .filter(move |edge| edge.has_layer(&layers))
                    .map(|edge| edge.into()),
            ),
        }
    }

    fn vertex_edges(
        &self,
        v: VID,
        d: Direction,
        layers: LayerIds,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        let entry = self.inner().storage.nodes.entry_arc(v.into());
        match d {
            Direction::OUT => match layers {
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
                LayerIds::One(layer) => Box::new(
                    entry
                        .into_layer(layer)
                        .into_tuples(Dir::Out)
                        .map(move |(n, e)| EdgeRef::new_outgoing(e, v, n)),
                ),
                LayerIds::Multiple(ids) => Box::new(
                    ids.iter()
                        .map(move |&layer| {
                            entry
                                .clone()
                                .into_layer(layer)
                                .into_tuples(Dir::Out)
                                .map(move |(n, e)| EdgeRef::new_outgoing(e, v, n))
                        })
                        .kmerge()
                        .dedup(),
                ),
            },
            Direction::IN => match layers {
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
                LayerIds::One(layer) => Box::new(
                    entry
                        .into_layer(layer)
                        .into_tuples(Dir::Into)
                        .map(move |(n, e)| EdgeRef::new_incoming(e, n, v)),
                ),
                LayerIds::Multiple(ids) => Box::new(
                    ids.iter()
                        .map(move |&layer| {
                            entry
                                .clone()
                                .into_layer(layer)
                                .into_tuples(Dir::Into)
                                .map(move |(n, e)| EdgeRef::new_incoming(e, n, v))
                        })
                        .kmerge()
                        .dedup(),
                ),
            },
            Direction::BOTH => Box::new(
                self.vertex_edges(v, Direction::IN, layers.clone())
                    .merge(self.vertex_edges(v, Direction::OUT, layers)),
            ),
        }
    }

    fn neighbours(
        &self,
        v: VID,
        d: Direction,
        layers: LayerIds,
    ) -> Box<dyn Iterator<Item = VID> + Send> {
        let iter = self.vertex_edges(v, d, layers).map(|e| e.remote());
        if matches!(d, Direction::BOTH) {
            Box::new(iter.dedup())
        } else {
            Box::new(iter)
        }
    }
}
