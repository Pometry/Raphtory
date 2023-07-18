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
use std::ops::Deref;

impl<const N: usize> GraphOps for InnerTemporalGraph<N> {
    fn find_edge_id(&self, e_id: EID) -> Option<EdgeRef> {
        let e_id_usize: usize = e_id.into();
        if e_id_usize >= self.num_edges() {
            return None;
        }
        let edge_view = self.edge(e_id);
        Some(EdgeRef::new_outgoing(
            e_id,
            edge_view.src_id(),
            edge_view.dst_id(),
        ))
    }

    fn local_vertex_ref(&self, v: VertexRef) -> Option<VID> {
        match v {
            VertexRef::Local(l) => Some(l),
            VertexRef::Remote(_) => {
                let vid = self.resolve_vertex_ref(&v)?;
                Some(vid)
            }
        }
    }

    fn get_unique_layers_internal(&self) -> Vec<usize> {
        self.get_all_layers()
    }

    fn get_layer_id(&self, key: Layer) -> Option<LayerIds> {
        self.layer_id(key)
    }

    fn vertices_len(&self) -> usize {
        self.internal_num_vertices()
    }

    fn edges_len(&self, layers: LayerIds) -> usize {
        self.internal_num_edges(layers)
    }

    fn degree(&self, v: VID, d: Direction, layers: LayerIds) -> usize {
        self.degree(v, d, layers)
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VID> + Send> {
        Box::new(self.vertex_ids())
    }

    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: LayerIds) -> Option<EdgeRef> {
        let src = self.resolve_vertex_ref(&src)?;
        let dst = self.resolve_vertex_ref(&dst)?;

        self.find_edge(src, dst, layer)
            .map(|e_id| EdgeRef::new_outgoing(e_id, src, dst))
    }

    fn edge_refs(&self, layers: LayerIds) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        match layers {
            LayerIds::All => {
                let iter = self.locked_edges().map(|edge| edge.deref().into());
                Box::new(iter)
            }
            _ => {
                Box::new(self
                    .locked_edges()
                    .filter(move |edge| edge.has_layer(&layers))
                    .map(|edge| edge.deref().into()))
            }
        }
    }

    fn vertex_edges(
        &self,
        v: VID,
        d: Direction,
        layers: LayerIds,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        let vid = self.resolve_vertex_ref(&VertexRef::Local(v)).unwrap();
        let v = self.vertex_arc(vid);

        let iter: GenBoxed<EdgeRef> = GenBoxed::new_boxed(|co| async move {
            for e_ref in v.edge_tuples(layers, d) {
                co.yield_(e_ref).await;
            }
        });

        Box::new(iter.into_iter())
    }

    fn neighbours(
        &self,
        v: VID,
        d: Direction,
        layers: LayerIds,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        let vid = self.resolve_vertex_ref(&VertexRef::Local(v)).unwrap();
        let v = self.vertex_arc(vid);

        let iter: GenBoxed<VertexRef> = GenBoxed::new_boxed(|co| async move {
            for v_id in v.neighbours(layers, d) {
                co.yield_(VertexRef::Local(v_id)).await;
            }
        });

        Box::new(iter.into_iter())
    }
}
