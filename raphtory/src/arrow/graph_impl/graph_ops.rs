use super::ArrowGraph;
use crate::{
    arrow::GID,
    core::{
        entities::{edges::edge_ref::EdgeRef, nodes::node_ref::NodeRef, LayerIds, EID, VID},
        Direction,
    },
    db::api::view::{
        self,
        internal::{EdgeFilter, GraphOps},
        IntoDynBoxed,
    },
};
use itertools::{EitherOrBoth, Itertools};
use rayon::{iter::ParallelIterator, prelude::*};
use std::iter;

impl<'graph> GraphOps<'graph> for ArrowGraph {
    fn internal_node_ref(
        &self,
        v: NodeRef,
        _layer_ids: &LayerIds,
        _filter: Option<&EdgeFilter>,
    ) -> Option<VID> {
        match v {
            NodeRef::Internal(vid) => Some(vid),
            NodeRef::External(vid) => self.inner.find_node(&GID::U64(vid)),
            NodeRef::ExternalStr(str) => self.inner.find_node(&GID::Str(str.into())),
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
                if self.inner.num_edges(*layer_id) <= e_id.into() {
                    return None;
                }
                let edge = self.inner.edge(e_id, *layer_id);

                filter
                    .map(|f| f(&edge, layer_ids))
                    .unwrap_or(true)
                    .then(|| EdgeRef::new_outgoing(e_id, edge.src(), edge.dst()))
            }
            _ => todo!(),
        }
    }

    fn nodes_len(&self, _layer_ids: LayerIds, _filter: Option<&EdgeFilter>) -> usize {
        self.inner.num_nodes()
    }

    fn edges_len(&self, layers: LayerIds, filter: Option<&EdgeFilter>) -> usize {
        let layer_id = self.inner.layer_from_ids(&layers);
        match layer_id {
            None => {
                todo!("Multilayer edges not yet supported on arrow")
            }
            Some(layer_id) => {
                if let Some(ef) = filter {
                    self.inner
                        .all_edges_par(layer_id)
                        .filter(|edge| ef(edge, &layers))
                        .count()
                } else {
                    self.inner.num_edges(layer_id)
                }
            }
        }
    }

    fn temporal_edges_len(&self, layers: LayerIds, filter: Option<&EdgeFilter>) -> usize {
        match &layers {
            LayerIds::None => 0,
            LayerIds::All => {
                if let Some(ef) = filter {
                    self.inner
                        .layers
                        .par_iter()
                        .map(|layer| {
                            layer
                                .edges
                                .par_iter()
                                .filter(|edge| ef(edge, &layers))
                                .map(|edge| edge.timestamp_slice().len())
                                .sum::<usize>()
                        })
                        .sum()
                } else {
                    self.inner
                        .layers
                        .par_iter()
                        .map(|layer| layer.num_temporal_edges())
                        .sum()
                }
            }
            LayerIds::One(layer_id) => {
                if let Some(ef) = filter {
                    self.inner
                        .layer(*layer_id)
                        .edges
                        .par_iter()
                        .filter(|edge| ef(edge, &layers))
                        .map(|edge| edge.timestamp_slice().len())
                        .sum()
                } else {
                    self.inner.layer(*layer_id).num_temporal_edges()
                }
            }
            LayerIds::Multiple(ids) => {
                if let Some(ef) = filter {
                    ids.par_iter()
                        .map(|layer_id| {
                            self.inner
                                .layer(*layer_id)
                                .edges
                                .par_iter()
                                .filter(|edge| ef(edge, &layers))
                                .map(|edge| edge.timestamp_slice().len())
                                .sum::<usize>()
                        })
                        .sum()
                } else {
                    ids.par_iter()
                        .map(|layer_id| self.inner.layer(*layer_id).num_temporal_edges())
                        .sum()
                }
            }
        }
    }

    fn degree(
        &self,
        v: VID,
        d: Direction,
        layers: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> usize {
        if let Some(layer_id) = self.inner.layer_from_ids(layers) {
            let nodes = self.inner.layers[layer_id].nodes();
            if let Some(ef) = filter {
                match d {
                    Direction::OUT => nodes
                        .out_edges_par(v)
                        .filter(|eid| self.inner.filter_edge(*eid, ef, layer_id))
                        .count(),
                    Direction::IN => nodes
                        .in_edges_par(v)
                        .filter(|eid| self.inner.filter_edge(*eid, ef, layer_id))
                        .count(),
                    Direction::BOTH => nodes
                        .out_adj_list(v)
                        .merge_join_by(nodes.in_adj_list(v), |(_, v_l), (_, v_r)| v_l.cmp(v_r))
                        .filter(|merged| match merged {
                            EitherOrBoth::Both((e_l, _), (e_r, _)) => {
                                self.inner.filter_edge(*e_l, ef, layer_id)
                                    || self.inner.filter_edge(*e_r, ef, layer_id)
                            }
                            EitherOrBoth::Left((eid, _)) => {
                                self.inner.filter_edge(*eid, ef, layer_id)
                            }
                            EitherOrBoth::Right((eid, _)) => {
                                self.inner.filter_edge(*eid, ef, layer_id)
                            }
                        })
                        .count(),
                }
            } else {
                match d {
                    Direction::OUT => nodes.out_degree(v),
                    Direction::IN => nodes.in_degree(v),
                    Direction::BOTH => nodes
                        .out_neighbours_iter(v)
                        .merge(nodes.in_neighbours_iter(v))
                        .dedup()
                        .count(),
                }
            }
        } else {
            0
        }
    }

    fn edge_ref(
        &self,
        src: VID,
        dst: VID,
        layers: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Option<EdgeRef> {
        let layer_id = self.inner.layer_from_ids(layers)?;
        let eid = self.inner.layers[layer_id].nodes().find_edge(src, dst)?;
        match filter {
            None => Some(EdgeRef::new_outgoing(eid, src, dst).at_layer(layer_id)),
            Some(ef) => {
                if self.inner.filter_edge(eid, ef, layer_id) {
                    Some(EdgeRef::new_outgoing(eid, src, dst).at_layer(layer_id))
                } else {
                    None
                }
            }
        }
    }

    fn node_refs(
        &self,
        _layers: LayerIds,
        _filter: Option<&EdgeFilter>,
    ) -> view::BoxedLIter<'graph, VID> {
        Box::new(self.inner.all_nodes())
    }

    fn edge_refs(
        &self,
        layers: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> view::BoxedLIter<'graph, EdgeRef> {
        let layer = self.inner.layer_from_ids(&layers);
        let filter = filter.cloned();
        let self_clone = self.clone();
        let iter = layer
            .into_iter()
            .filter_map(move |layer_id| {
                if layers.contains(&layer_id) {
                    let filter = filter.clone();
                    let self_clone = self_clone.clone();
                    Some(
                        self_clone
                            .inner
                            .all_edge_ids(layer_id)
                            .filter_map(move |eid| {
                                let edge = self_clone.inner.edge(eid, layer_id);
                                filter
                                    .as_ref()
                                    .map(|f| f(&edge, &LayerIds::One(layer_id)))
                                    .unwrap_or(true)
                                    .then(|| {
                                        EdgeRef::new_outgoing(eid, edge.src(), edge.dst())
                                            .at_layer(layer_id)
                                    })
                            }),
                    )
                } else {
                    None
                }
            })
            .flatten();

        Box::new(iter)
    }

    fn node_edges(
        &self,
        src: VID,
        d: Direction,
        layer: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> view::BoxedLIter<'graph, EdgeRef> {
        let layer_id = self.inner.layer_from_ids(&layer);
        if matches!(d, Direction::BOTH) {
            Box::new(
                self.node_edges(src, Direction::OUT, layer.clone(), filter)
                    .merge_by(
                        self.node_edges(src, Direction::IN, layer.clone(), filter)
                            .filter(|e| e.src() != e.dst()),
                        |e1, e2| e1.remote() <= e2.remote(),
                    ),
            )
        } else {
            match layer_id {
                Some(layer_id) => {
                    let filter = filter.cloned();
                    let self_cloned = self.clone();
                    let iter = self
                        .inner
                        .edges_cloned(src, d, layer_id)
                        .map(move |(e_id, v)| match d {
                            Direction::OUT => {
                                EdgeRef::new_outgoing(e_id, src, v).at_layer(layer_id)
                            }
                            Direction::IN => EdgeRef::new_incoming(e_id, v, src).at_layer(layer_id),
                            Direction::BOTH => unreachable!(),
                        });

                    if filter.is_some() {
                        Box::new(iter.filter(move |e_ref| {
                            let edge = self_cloned.inner.edge(e_ref.pid(), layer_id);
                            filter.as_ref().map(|f| f(&edge, &layer)).unwrap_or(true)
                        }))
                    } else {
                        Box::new(iter)
                    }
                }
                None => Box::new(iter::empty()),
            }
        }
    }

    fn neighbours(
        &self,
        src: VID,
        d: Direction,
        layers: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> view::BoxedLIter<'graph, VID> {
        if let Some(layer_id) = self.inner.layer_from_ids(&layers) {
            let filter = filter.cloned();
            let self_cloned = self.clone();
            let iter = self.inner.edges_cloned(src, d, layer_id);

            let iter = if filter.is_some() {
                iter.filter(move |(e_id, _)| {
                    let edge = self_cloned.inner.edge(*e_id, layer_id);
                    filter.as_ref().map(|f| f(&edge, &layers)).unwrap_or(true)
                })
                .map(|(_, v)| v)
                .into_dyn_boxed()
            } else {
                iter.map(|(_, v)| v).into_dyn_boxed()
            };
            if matches!(d, Direction::BOTH) {
                Box::new(iter.dedup())
            } else {
                iter
            }
        } else {
            Box::new(iter::empty())
        }
    }
}
