use rayon::iter::ParallelIterator;

use crate::{
    arrow::GID,
    core::{
        self,
        entities::{edges::edge_ref::EdgeRef, nodes::node_ref::NodeRef, LayerIds, EID, VID},
    },
    db::api::view::{
        self,
        internal::{EdgeFilter, GraphOps},
    },
};

use super::Graph2;

impl<'graph> GraphOps<'graph> for Graph2 {
    fn internal_node_ref(
        &self,
        v: NodeRef,
        _layer_ids: &LayerIds,
        _filter: Option<&EdgeFilter>,
    ) -> Option<VID> {
        match v {
            NodeRef::Internal(vid) => Some(vid),
            NodeRef::External(vid) => self.find_node(&GID::U64(vid)),
            //FIXME: when the original GID was string this will fail
        }
    }

    fn find_edge_id(
        &self,
        e_id: EID,
        layer_ids: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Option<EdgeRef> {
        match layer_ids {
            LayerIds::One(layer_id) => {
                if self.num_edges(*layer_id) <= e_id.into() {
                    return None;
                }
                let edge = self.edge(e_id, *layer_id);

                filter
                    .map(|f| f(&edge, layer_ids))
                    .unwrap_or(true)
                    .then(|| EdgeRef::new_outgoing(e_id, edge.src(), edge.dst()))
            }
            _ => todo!(),
        }
    }

    fn nodes_len(&self, _layer_ids: LayerIds, _filter: Option<&EdgeFilter>) -> usize {
        self.num_nodes()
    }

    fn edges_len(&self, layers: LayerIds, filter: Option<&EdgeFilter>) -> usize {
        match &layers {
            layer @ LayerIds::One(layer_id) => {
                if let Some(ef) = filter {
                    self.all_edges_par(*layer_id)
                        .filter(|edge| ef(edge, &layer))
                        .count()
                } else {
                    self.num_edges(*layer_id)
                }
            }
            _ => todo!(),
        }
    }

    fn temporal_edges_len(&self, layers: LayerIds, filter: Option<&EdgeFilter>) -> usize {
        match &layers {
            layer @ LayerIds::One(layer_id) => {
                if let Some(ef) = filter {
                    self.all_edges_par(*layer_id)
                        .filter(|edge| ef(edge, &layer))
                        .map(|edge| edge.timestamps().len())
                        .sum::<usize>()
                } else {
                    self.t_len(*layer_id)
                }
            }
            _ => todo!(),
        }
    }

    fn degree(
        &self,
        v: VID,
        d: core::Direction,
        layers: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> usize {
        let layer_id = match layers {
            LayerIds::One(layer_id) => *layer_id,
            _ => todo!(),
        };
        if let Some(ef) = filter {
            self.edges_par(v, d, layer_id)
                .filter(|(e_id, _)| {
                    let edge = self.edge(*e_id, layer_id);
                    ef(&edge, layers)
                })
                .count()
        } else {
            let iter = self.edges_iter(v, d, layer_id);
            let (low, up) = iter.size_hint();
            assert_eq!(Some(low), up);
            low
        }
    }

    fn edge_ref(
        &self,
        src: VID,
        dst: VID,
        layers: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Option<EdgeRef> {
        let layer_id = match layers {
            LayerIds::One(layer_id) => *layer_id,
            _ => todo!(),
        };
        let (vs, es) = self.edges_slice(src, core::Direction::OUT, layer_id)?;
        let needle = dst.as_u64();
        let pos = vs.binary_search_by(|v| v.cmp(&needle)).ok()?;

        let e_id = es[pos];
        let edge = self.edge(EID(e_id as usize), layer_id);

        filter
            .map(|f| f(&edge, layers))
            .unwrap_or(true)
            .then(|| EdgeRef::new_outgoing(EID(e_id as usize), src, dst))
    }

    fn node_refs(
        &self,
        _layers: LayerIds,
        _filter: Option<&EdgeFilter>,
    ) -> view::BoxedLIter<'graph, VID> {
        Box::new(self.all_nodes())
    }

    fn edge_refs(
        &self,
        layers: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> view::BoxedLIter<'graph, EdgeRef> {
        let layer_id = match layers {
            LayerIds::One(layer_id) => layer_id,
            _ => todo!(),
        };

        let filter = filter.cloned();
        let self_clone = self.clone();
        let iter = self.all_edge_ids(layer_id).filter_map(move |eid| {
            let edge = self_clone.edge(eid, layer_id);
            filter
                .as_ref()
                .map(|f| f(&edge, &layers))
                .unwrap_or(true)
                .then(|| EdgeRef::new_outgoing(eid, edge.src(), edge.dst()))
        });

        Box::new(iter)
    }

    fn node_edges(
        &self,
        src: VID,
        d: core::Direction,
        layer: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> view::BoxedLIter<'graph, EdgeRef> {
        let layer_id = match layer {
            LayerIds::One(layer_id) => layer_id,
            _ => todo!(),
        };
        let filter = filter.cloned();
        let self_cloned = self.clone();
        let iter = self.edges(src, d, layer_id).map(move |(e_id, v)| match d {
            core::Direction::OUT => EdgeRef::new_outgoing(e_id, src, v),
            core::Direction::IN => EdgeRef::new_incoming(e_id, v, src),
            core::Direction::BOTH => todo!("BOTH"),
        });

        if filter.is_some() {
            Box::new(iter.filter(move |e_ref| {
                let edge = self_cloned.edge(e_ref.pid(), layer_id);
                filter.as_ref().map(|f| f(&edge, &layer)).unwrap_or(true)
            }))
        } else {
            Box::new(iter)
        }
    }

    fn neighbours(
        &self,
        src: VID,
        d: core::Direction,
        layers: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> view::BoxedLIter<'graph, VID> {
        let layer_id = match layers {
            LayerIds::One(layer_id) => layer_id,
            _ => todo!(),
        };
        let filter = filter.cloned();
        let self_cloned = self.clone();
        let iter = self.edges(src, d, layer_id);

        if filter.is_some() {
            Box::new(
                iter.filter(move |(e_id, _)| {
                    let edge = self_cloned.edge(*e_id, layer_id);
                    filter.as_ref().map(|f| f(&edge, &layers)).unwrap_or(true)
                })
                .map(|(_, v)| v),
            )
        } else {
            Box::new(iter.map(|(_, v)| v))
        }
    }
}
