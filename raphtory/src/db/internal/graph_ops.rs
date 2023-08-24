use crate::{
    core::{
        entities::{
            edges::edge_ref::EdgeRef, graph::tgraph::InnerTemporalGraph,
            vertices::vertex_ref::VertexRef, LayerIds, EID, VID,
        },
        Direction,
    },
    db::api::view::{internal::GraphOps, Layer},
    prelude::GraphViewOps,
};
use genawaiter::sync::GenBoxed;
use std::{iter, ops::Deref};

impl<const N: usize> GraphOps for InnerTemporalGraph<N> {
    fn layer_ids(&self) -> LayerIds {
        LayerIds::All
    }

    fn local_vertex_ref(&self, v: VertexRef) -> Option<VID> {
        match v {
            VertexRef::Local(l) => Some(l),
            VertexRef::Remote(_) => {
                let vid = self.inner().resolve_vertex_ref(&v)?;
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
            LayerIds::All => {
                let iter = self.inner().locked_edges().map(|edge| edge.deref().into());
                Box::new(iter)
            }
            _ => Box::new(
                self.inner()
                    .locked_edges()
                    .filter(move |edge| edge.has_layer(&layers))
                    .map(|edge| edge.deref().into()),
            ),
        }
    }

    fn vertex_edges(
        &self,
        v: VID,
        d: Direction,
        layers: LayerIds,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        let vid = self
            .inner()
            .resolve_vertex_ref(&VertexRef::Local(v))
            .unwrap();
        let v = self.inner().vertex_arc(vid);
        let option_edge = v.edge_tuples(layers.clone(), d).next();
        match option_edge {
            None => Box::new(iter::empty()),
            Some(_) => {
                let iter: GenBoxed<EdgeRef> = GenBoxed::new_boxed(|co| async move {
                    for e_ref in v.edge_tuples(layers, d) {
                        co.yield_(e_ref).await;
                    }
                });
                Box::new(iter.into_iter())
            }
        }
    }

    fn neighbours(
        &self,
        v: VID,
        d: Direction,
        layers: LayerIds,
    ) -> Box<dyn Iterator<Item = VID> + Send> {
        let v = self.inner().vertex_arc(v);

        let iter: GenBoxed<VID> = GenBoxed::new_boxed(|co| async move {
            for v_id in v.neighbours(layers, d) {
                co.yield_(v_id).await;
            }
        });

        Box::new(iter.into_iter())
    }
}
