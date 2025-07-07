use crate::{
    db::api::view::internal::{FilterOps, FilterState, FilterVariants, GraphView},
    prelude::GraphViewOps,
};
use either::Either;
use raphtory_api::core::{
    entities::{
        properties::{prop::Prop, tprop::TPropOps},
        LayerIds, ELID,
    },
    storage::timeindex::{TimeIndexEntry, TimeIndexOps},
};
use raphtory_storage::graph::edges::{
    edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps, edges::EdgesStorage,
};
use rayon::iter::ParallelIterator;
use std::ops::Range;

#[derive(Clone)]
pub struct FilteredEdgeTimeIndex<'graph, G, TS> {
    eid: ELID,
    time_index: TS,
    view: G,
    _marker: std::marker::PhantomData<&'graph ()>,
}

impl<
        'a,
        'graph: 'a,
        TS: TimeIndexOps<'a, IndexType = TimeIndexEntry, RangeType = TS>,
        G: GraphViewOps<'graph>,
    > TimeIndexOps<'a> for FilteredEdgeTimeIndex<'graph, G, TS>
{
    type IndexType = TimeIndexEntry;
    type RangeType = Self;

    #[inline]
    fn active(&self, w: Range<Self::IndexType>) -> bool {
        if self.view.edge_history_filtered() {
            self.time_index
                .range(w)
                .iter()
                .find(|t| {
                    self.view
                        .filter_edge_history(self.eid, *t, self.view.layer_ids())
                })
                .is_some()
        } else {
            self.time_index.active(w)
        }
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType {
        Self {
            eid: self.eid,
            time_index: self.time_index.range(w),
            view: self.view.clone(),
            _marker: std::marker::PhantomData,
        }
    }

    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        if self.view.edge_history_filtered() {
            let view = self.view.clone();
            let eid = self.eid;
            Either::Left(
                self.time_index
                    .iter()
                    .filter(move |t| view.filter_edge_history(eid, *t, view.layer_ids())),
            )
        } else {
            Either::Right(self.time_index.iter())
        }
    }

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        if self.view.edge_history_filtered() {
            let view = self.view.clone();
            let eid = self.eid;
            Either::Left(
                self.time_index
                    .iter_rev()
                    .filter(move |t| view.filter_edge_history(eid, *t, view.layer_ids())),
            )
        } else {
            Either::Right(self.time_index.iter_rev())
        }
    }

    fn len(&self) -> usize {
        if self.view.edge_history_filtered() {
            self.clone().iter().count()
        } else {
            self.time_index.len()
        }
    }
}

#[derive(Copy, Clone)]
pub struct FilteredEdgeTProp<G, P> {
    eid: ELID,
    view: G,
    props: P,
}

impl<'graph, G: GraphViewOps<'graph>, P: TPropOps<'graph>> TPropOps<'graph>
    for FilteredEdgeTProp<G, P>
{
    // fn iter(
    //     self,
    // ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'graph {
    //     let view = self.view.clone();
    //     let eid = self.eid;
    //     self.props
    //         .iter()
    //         .filter(move |(t, _)| view.filter_edge_history(eid, *t, view.layer_ids()))
    // }

    // fn iter_window(
    //     self,
    //     r: Range<TimeIndexEntry>,
    // ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'graph {
    //     let view = self.view.clone();
    //     let eid = self.eid;
    //     self.props
    //         .iter_window(r)
    //         .filter(move |(t, _)| view.filter_edge_history(eid, *t, view.layer_ids()))
    // }

    fn at(&self, ti: &TimeIndexEntry) -> Option<Prop> {
        if self
            .view
            .filter_edge_history(self.eid, *ti, self.view.layer_ids())
        {
            self.props.at(ti)
        } else {
            None
        }
    }

    fn iter_inner(
        self,
        range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'graph {
        let view = self.view.clone();
        let eid = self.eid;
        self.props
            .iter_inner(range)
            .filter(move |(t, _)| view.filter_edge_history(eid, *t, view.layer_ids()))
    }

    fn iter_inner_rev(
        self,
        range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'graph {
        let view = self.view.clone();
        let eid = self.eid;
        self.props
            .iter_inner_rev(range)
            .filter(move |(t, _)| view.filter_edge_history(eid, *t, view.layer_ids()))
    }
}

pub trait FilteredEdgeStorageOps<'a>: EdgeStorageOps<'a> {
    fn filtered_additions_iter<G: GraphView + 'a>(
        self,
        view: G,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<
        Item = (
            usize,
            FilteredEdgeTimeIndex<'a, G, storage::EdgeAdditions<'a>>,
        ),
    > {
        let eid = self.eid();
        self.additions_iter(layer_ids)
            .map(move |(layer_id, additions)| {
                (
                    layer_id,
                    FilteredEdgeTimeIndex {
                        eid: eid.with_layer(layer_id),
                        time_index: additions,
                        view: view.clone(),
                        _marker: std::marker::PhantomData,
                    },
                )
            })
    }

    fn filtered_deletions_iter<G: GraphViewOps<'a>>(
        self,
        view: G,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<
        Item = (
            usize,
            FilteredEdgeTimeIndex<'a, G, storage::EdgeDeletions<'a>>,
        ),
    > {
        let eid = self.eid();
        self.deletions_iter(layer_ids)
            .map(move |(layer_id, deletions)| {
                (
                    layer_id,
                    FilteredEdgeTimeIndex {
                        eid: eid.with_layer_deletion(layer_id),
                        time_index: deletions,
                        view: view.clone(),
                        _marker: std::marker::PhantomData,
                    },
                )
            })
    }

    fn filtered_updates_iter<G: GraphViewOps<'a>>(
        self,
        view: G,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<
        Item = (
            usize,
            FilteredEdgeTimeIndex<'a, G, storage::EdgeAdditions<'a>>,
            FilteredEdgeTimeIndex<'a, G, storage::EdgeDeletions<'a>>,
        ),
    > + 'a {
        self.layer_ids_iter(layer_ids).map(move |layer_id| {
            (
                layer_id,
                self.filtered_additions(layer_id, view.clone()),
                self.filtered_deletions(layer_id, view.clone()),
            )
        })
    }

    fn filtered_additions<G: GraphViewOps<'a>>(
        self,
        layer_id: usize,
        view: G,
    ) -> FilteredEdgeTimeIndex<'a, G, storage::EdgeAdditions<'a>> {
        FilteredEdgeTimeIndex {
            eid: self.eid().with_layer(layer_id),
            time_index: self.additions(layer_id),
            view,
            _marker: std::marker::PhantomData,
        }
    }

    fn filtered_deletions<G: GraphViewOps<'a>>(
        self,
        layer_id: usize,
        view: G,
    ) -> FilteredEdgeTimeIndex<'a, G, storage::EdgeDeletions<'a>> {
        FilteredEdgeTimeIndex {
            eid: self.eid().with_layer_deletion(layer_id),
            time_index: self.deletions(layer_id),
            view,
            _marker: std::marker::PhantomData,
        }
    }

    fn filtered_temporal_prop_layer<G: GraphViewOps<'a>>(
        self,
        layer_id: usize,
        prop_id: usize,
        view: G,
    ) -> impl TPropOps<'a> + Sync + 'a {
        FilteredEdgeTProp {
            eid: self.eid().with_layer(layer_id),
            view,
            props: self.temporal_prop_layer(layer_id, prop_id),
        }
    }

    fn filtered_temporal_prop_iter<G: GraphView + 'a>(
        self,
        prop_id: usize,
        view: G,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, impl TPropOps<'a>)> + 'a {
        self.layer_ids_iter(layer_ids).map(move |layer_id| {
            (
                layer_id,
                self.filtered_temporal_prop_layer(layer_id, prop_id, view.clone()),
            )
        })
    }
}

impl<'a, T: EdgeStorageOps<'a>> FilteredEdgeStorageOps<'a> for T {}

pub trait FilteredEdgesStorageOps {
    fn filtered_par_iter<'a, G: GraphView + 'a>(
        &'a self,
        view: G,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = EdgeStorageRef<'a>> + 'a;
}

impl FilteredEdgesStorageOps for EdgesStorage {
    fn filtered_par_iter<'a, G: GraphView + 'a>(
        &'a self,
        view: G,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = EdgeStorageRef<'a>> + 'a {
        let par_iter = self.par_iter(layer_ids);
        match view.filter_state() {
            FilterState::Neither => FilterVariants::Neither(par_iter),
            FilterState::Both => {
                let nodes = view.core_nodes();
                FilterVariants::Both(par_iter.filter(move |&e| {
                    view.filter_edge(e, layer_ids)
                        && view.filter_node(nodes.node_entry(e.src()))
                        && view.filter_node(nodes.node_entry(e.dst()))
                }))
            }
            FilterState::Nodes => {
                let nodes = view.core_nodes();
                FilterVariants::Nodes(par_iter.filter(move |&e| {
                    view.filter_node(nodes.node_entry(e.src()))
                        && view.filter_node(nodes.node_entry(e.dst()))
                }))
            }
            FilterState::Edges | FilterState::BothIndependent => {
                FilterVariants::Edges(par_iter.filter(move |&e| view.filter_edge(e, layer_ids)))
            }
        }
    }
}
