use crate::{
    core::{
        entities::{
            edges::edge_ref::EdgeRef, graph::tgraph::InnerTemporalGraph,
            vertices::vertex_ref::VertexRef, EID, VID,
        },
        Direction,
    },
    db::api::view::internal::GraphOps,
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
        Some(EdgeRef::new_outgoing(e_id, edge_view.src_id(), edge_view.dst_id()))
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

    fn get_layer_id(&self, key: Option<&str>) -> Option<usize> {
        self.layer_id(key)
    }

    fn vertices_len(&self) -> usize {
        self.internal_num_vertices()
    }

    fn edges_len(&self, layer: Option<usize>) -> usize {
        let layers = layer.into_iter().collect::<Vec<_>>();
        self.internal_num_edges(&layers)
    }

    fn degree(&self, v: VID, d: Direction, layer: Option<usize>) -> usize {
        let layers = layer.into_iter().collect::<Vec<_>>();
        self.degree(v, d, &layers)
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VID> + Send> {
        Box::new(self.vertex_ids())
    }

    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> Option<EdgeRef> {
        let src = self.resolve_vertex_ref(&src)?;
        let dst = self.resolve_vertex_ref(&dst)?;

        self.find_edge(src, dst, Some(layer))
            .map(|e_id| EdgeRef::new_outgoing(e_id, src, dst) )
    }

    fn edge_refs(&self, layer: Option<usize>) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        let layers = layer.into_iter().collect::<Vec<_>>();
        if let Some(layer_id) = layer {
            let iter = self
                .locked_edges()
                .filter_map(move |edge| {
                    if edge.has_layer(&layers) {
                        Some((layer_id, edge))
                    } else {
                        None
                    }
                })
                .map(|(layer_id, edge)| {
                    edge.deref().into()
                });
            Box::new(iter)
        } else {
            let iter = self.locked_edges().map(|edge| {
                edge.deref().into()
            });
            Box::new(iter)
        }
    }

    fn vertex_edges(
        &self,
        v: VID,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        let vid = self.resolve_vertex_ref(&VertexRef::Local(v)).unwrap();
        let v = self.vertex_arc(vid);

        let iter: GenBoxed<EdgeRef> = GenBoxed::new_boxed(|co| async move {
            let layers = layer.into_iter().collect::<Vec<_>>();
            for e_ref in v.edge_tuples(&layers, d) {
                co.yield_(e_ref).await;
            }
        });

        Box::new(iter.into_iter())
    }

    fn neighbours(
        &self,
        v: VID,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        let vid = self.resolve_vertex_ref(&VertexRef::Local(v)).unwrap();
        let v = self.vertex_arc(vid);

        let iter: GenBoxed<VertexRef> = GenBoxed::new_boxed(|co| async move {
            let layers = layer.into_iter().collect::<Vec<_>>();
            for v_id in v.neighbours(&layers, d) {
                co.yield_(VertexRef::Local(v_id)).await;
            }
        });

        Box::new(iter.into_iter())
    }
}
