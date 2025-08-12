use crate::{
    db::api::view::internal::{FilterOps, FilterState, FilterVariants, GraphView},
    prelude::{GraphViewOps, LayerOps},
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
    edge_ref::EdgeStorageRef,
    edge_storage_ops::{EdgeStorageOps, TimeIndexRef},
    edges::EdgesStorage,
};
use rayon::iter::ParallelIterator;
use std::{iter, ops::Range};

#[derive(Clone)]
pub struct FilteredEdgeTimeIndex<'graph, G> {
    eid: ELID,
    time_index: TimeIndexRef<'graph>,
    view: G,
}

impl<'graph, G> FilteredEdgeTimeIndex<'graph, G> {
    pub fn invert(self) -> InvertedFilteredEdgeTimeIndex<'graph, G> {
        InvertedFilteredEdgeTimeIndex {
            eid: self.eid,
            time_index: self.time_index,
            view: self.view,
        }
    }

    pub fn unfiltered(&self) -> TimeIndexRef<'graph> {
        self.time_index.clone()
    }
}

impl<'a, 'graph: 'a, G: GraphViewOps<'graph>> TimeIndexOps<'a>
    for FilteredEdgeTimeIndex<'graph, G>
{
    type IndexType = TimeIndexEntry;
    type RangeType = Self;

    #[inline]
    fn active(&self, w: Range<Self::IndexType>) -> bool {
        if self.view.internal_exploded_edge_filtered() {
            self.time_index
                .range(w)
                .iter()
                .find(|t| {
                    self.view
                        .internal_filter_exploded_edge(self.eid, *t, self.view.layer_ids())
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
        }
    }

    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        if self.view.internal_exploded_edge_filtered() {
            let view = self.view.clone();
            let eid = self.eid;
            Either::Left(
                self.time_index
                    .iter()
                    .filter(move |t| view.internal_filter_exploded_edge(eid, *t, view.layer_ids())),
            )
        } else {
            Either::Right(self.time_index.iter())
        }
    }

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        if self.view.internal_exploded_edge_filtered() {
            let view = self.view.clone();
            let eid = self.eid;
            Either::Left(
                self.time_index
                    .iter_rev()
                    .filter(move |t| view.internal_filter_exploded_edge(eid, *t, view.layer_ids())),
            )
        } else {
            Either::Right(self.time_index.iter_rev())
        }
    }

    fn len(&self) -> usize {
        if self.view.internal_exploded_edge_filtered() {
            self.iter().count()
        } else {
            self.time_index.len()
        }
    }
}

#[derive(Clone)]
pub struct InvertedFilteredEdgeTimeIndex<'graph, G> {
    eid: ELID,
    time_index: TimeIndexRef<'graph>,
    view: G,
}

impl<'a, 'graph: 'a, G: GraphViewOps<'graph>> TimeIndexOps<'a>
    for InvertedFilteredEdgeTimeIndex<'graph, G>
{
    type IndexType = TimeIndexEntry;
    type RangeType = Self;

    #[inline]
    fn active(&self, w: Range<Self::IndexType>) -> bool {
        if self.view.internal_exploded_edge_filtered() {
            self.time_index
                .range(w)
                .iter()
                .find(|t| {
                    !self
                        .view
                        .internal_filter_exploded_edge(self.eid, *t, self.view.layer_ids())
                })
                .is_some()
        } else {
            false
        }
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType {
        Self {
            eid: self.eid,
            time_index: self.time_index.range(w),
            view: self.view.clone(),
        }
    }

    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        if self.view.internal_exploded_edge_filtered() {
            let view = self.view.clone();
            let eid = self.eid;
            Either::Left(
                self.time_index.iter().filter(move |t| {
                    !view.internal_filter_exploded_edge(eid, *t, view.layer_ids())
                }),
            )
        } else {
            Either::Right(iter::empty())
        }
    }

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        if self.view.internal_exploded_edge_filtered() {
            let view = self.view.clone();
            let eid = self.eid;
            Either::Left(
                self.time_index.iter_rev().filter(move |t| {
                    !view.internal_filter_exploded_edge(eid, *t, view.layer_ids())
                }),
            )
        } else {
            Either::Right(iter::empty())
        }
    }

    fn len(&self) -> usize {
        if self.view.internal_exploded_edge_filtered() {
            self.iter().count()
        } else {
            0
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
    fn iter(
        self,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'graph {
        let view = self.view.clone();
        let eid = self.eid;
        self.props
            .iter()
            .filter(move |(t, _)| view.internal_filter_exploded_edge(eid, *t, view.layer_ids()))
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'graph {
        let view = self.view.clone();
        let eid = self.eid;
        self.props
            .iter_window(r)
            .filter(move |(t, _)| view.internal_filter_exploded_edge(eid, *t, view.layer_ids()))
    }

    fn at(&self, ti: &TimeIndexEntry) -> Option<Prop> {
        if self
            .view
            .internal_filter_exploded_edge(self.eid, *ti, self.view.layer_ids())
        {
            self.props.at(ti)
        } else {
            None
        }
    }
}

pub trait FilteredEdgeStorageOps<'a> {
    fn filtered_additions_iter<G: GraphView + 'a>(
        self,
        view: G,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, FilteredEdgeTimeIndex<'a, G>)>;

    fn filtered_deletions_iter<G: GraphViewOps<'a>>(
        self,
        view: G,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, FilteredEdgeTimeIndex<'a, G>)>;

    fn filtered_updates_iter<G: GraphViewOps<'a>>(
        self,
        view: G,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<
        Item = (
            usize,
            FilteredEdgeTimeIndex<'a, G>,
            FilteredEdgeTimeIndex<'a, G>,
        ),
    > + 'a;

    fn filtered_additions<G: GraphViewOps<'a>>(
        self,
        layer_id: usize,
        view: G,
    ) -> FilteredEdgeTimeIndex<'a, G>;

    fn filtered_deletions<G: GraphViewOps<'a>>(
        self,
        layer_id: usize,
        view: G,
    ) -> FilteredEdgeTimeIndex<'a, G>;

    fn filtered_temporal_prop_layer<G: GraphViewOps<'a>>(
        self,
        layer_id: usize,
        prop_id: usize,
        view: G,
    ) -> impl TPropOps<'a> + Sync + 'a;

    fn filtered_temporal_prop_iter<G: GraphView + 'a>(
        self,
        prop_id: usize,
        view: G,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, impl TPropOps<'a>)> + 'a;

    fn filtered_edge_metadata<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
        prop_id: usize,
        layer_filter: impl Fn(usize) -> bool,
    ) -> Option<Prop>;
}

impl<'a> FilteredEdgeStorageOps<'a> for EdgeStorageRef<'a> {
    fn filtered_additions_iter<G: GraphView + 'a>(
        self,
        view: G,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, FilteredEdgeTimeIndex<'a, G>)> {
        self.layer_ids_iter(layer_ids).filter_map(move |layer| {
            let view = view.clone();
            view.internal_filter_edge_layer(self, layer)
                .then(move || (layer, self.filtered_additions(layer, view.clone())))
        })
    }

    fn filtered_deletions_iter<G: GraphViewOps<'a>>(
        self,
        view: G,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, FilteredEdgeTimeIndex<'a, G>)> {
        self.layer_ids_iter(layer_ids).filter_map(move |layer| {
            let view = view.clone();
            view.internal_filter_edge_layer(self, layer)
                .then(move || (layer, self.filtered_deletions(layer, view.clone())))
        })
    }

    fn filtered_updates_iter<G: GraphViewOps<'a>>(
        self,
        view: G,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<
        Item = (
            usize,
            FilteredEdgeTimeIndex<'a, G>,
            FilteredEdgeTimeIndex<'a, G>,
        ),
    > + 'a {
        self.layer_ids_iter(layer_ids).filter_map(move |layer_id| {
            let view = view.clone();
            view.internal_filter_edge_layer(self, layer_id)
                .then(move || {
                    (
                        layer_id,
                        self.filtered_additions(layer_id, view.clone()),
                        self.filtered_deletions(layer_id, view.clone()),
                    )
                })
        })
    }

    fn filtered_additions<G: GraphViewOps<'a>>(
        self,
        layer_id: usize,
        view: G,
    ) -> FilteredEdgeTimeIndex<'a, G> {
        FilteredEdgeTimeIndex {
            eid: self.eid().with_layer(layer_id),
            time_index: self.additions(layer_id),
            view,
        }
    }

    fn filtered_deletions<G: GraphViewOps<'a>>(
        self,
        layer_id: usize,
        view: G,
    ) -> FilteredEdgeTimeIndex<'a, G> {
        FilteredEdgeTimeIndex {
            eid: self.eid().with_layer_deletion(layer_id),
            time_index: self.deletions(layer_id),
            view,
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
        self.layer_ids_iter(layer_ids).filter_map(move |layer_id| {
            let view = view.clone();
            view.internal_filter_edge_layer(self, layer_id)
                .then(move || {
                    (
                        layer_id,
                        self.filtered_temporal_prop_layer(layer_id, prop_id, view.clone()),
                    )
                })
        })
    }

    fn filtered_edge_metadata<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
        prop_id: usize,
        layer_filter: impl Fn(usize) -> bool,
    ) -> Option<Prop> {
        let layer_ids = view.layer_ids();
        let mut values = self
            .metadata_iter(layer_ids, prop_id)
            .filter(|(layer, _)| layer_filter(*layer));
        if view.num_layers() > 1 {
            let mut values = values.peekable();
            if values.peek().is_some() {
                Some(Prop::map(
                    values.map(|(layer_id, v)| (view.get_layer_name(layer_id), v)),
                ))
            } else {
                None
            }
        } else {
            values.next().map(|(_, v)| v)
        }
    }
}

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
            FilterState::Both => FilterVariants::Both(par_iter.filter(move |&e| {
                view.filter_edge(e)
                    && view.filter_node(view.core_node(e.src()).as_ref())
                    && view.filter_node(view.core_node(e.dst()).as_ref())
            })),
            FilterState::Nodes => FilterVariants::Nodes(par_iter.filter(move |&e| {
                view.filter_node(view.core_node(e.src()).as_ref())
                    && view.filter_node(view.core_node(e.dst()).as_ref())
            })),
            FilterState::Edges | FilterState::BothIndependent => {
                FilterVariants::Edges(par_iter.filter(move |&e| view.filter_edge(e)))
            }
        }
    }
}
