use std::ops::Deref;

use genawaiter::sync::GenBoxed;

use crate::{
    core::{
        tgraph2::{tgraph::InnerTemporalGraph, VID},
        vertex_ref::VertexRef, Direction, edge_ref::EdgeRef,
    },
    db::view_api::internal::GraphOps,
};

impl<const N: usize> GraphOps for InnerTemporalGraph<N> {
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
        self.internal_num_edges(layer)
    }

    fn degree(&self, v: VID, d: Direction, layer: Option<usize>) -> usize {
        self.degree(v, d, layer)
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VID> + Send> {
        Box::new(self.vertex_ids())
    }

    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> Option<EdgeRef> {
        let src = self.resolve_vertex_ref(&src)?;
        let dst = self.resolve_vertex_ref(&dst)?;

        self.find_edge(src, dst, Some(layer))
            .map(|e_id| EdgeRef::LocalOut {
                e_pid: e_id,
                layer_id: layer,
                src_pid: src,
                dst_pid: dst,
                time: None,
            })
    }

    fn edge_refs(&self, layer: Option<usize>) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        if let Some(layer_id) = layer {
            let iter = self
                .locked_edges()
                .filter_map(move |edge| {
                    if edge.has_layer(layer_id) {
                        Some((layer_id, edge))
                    } else {
                        None
                    }
                })
                .map(|(layer_id, edge)| {
                    let e_ref: EdgeRef = edge.deref().into();
                    e_ref.at_layer(layer_id)
                });
            Box::new(iter)
        } else {
            let iter = self.locked_edges().map(|edge| {
                let e_ref: EdgeRef = edge.deref().into();
                e_ref.at_layer(0)
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
            for e_ref in v.edge_tuples(layer, d) {
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
            for v_id in v.neighbours(layer, d) {
                co.yield_(VertexRef::Local(v_id)).await;
            }
        });

        Box::new(iter.into_iter())
    }
}
