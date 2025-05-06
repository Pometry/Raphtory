use crate::{
    core::{
        entities::{
            edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
            properties::{props::Props, tprop::TProp},
            LayerIds, VID,
        },
        storage::{
            raw_edges::EdgeShard,
            timeindex::{TimeIndex, TimeIndexOps, TimeIndexWindow},
        },
        Prop,
    },
    db::api::storage::graph::{
        tprop_storage_ops::TPropOps, variants::layer_variants::LayerVariants,
    },
    prelude::GraphViewOps,
};
use either::Either;
use iter_enum::{DoubleEndedIterator, ExactSizeIterator, FusedIterator, Iterator};
#[cfg(feature = "storage")]
use pometry_storage::timestamps::TimeStamps;
use raphtory_api::{
    core::{
        entities::{edges::edge_ref::Dir, EID, ELID},
        storage::timeindex::TimeIndexEntry,
    },
    iter::{BoxedLDIter, IntoDynDBoxed},
};
use rayon::prelude::*;
use std::ops::Range;

#[derive(Clone)]
pub enum TimeIndexRef<'a> {
    Ref(&'a TimeIndex<TimeIndexEntry>),
    Range(TimeIndexWindow<'a, TimeIndexEntry, TimeIndex<TimeIndexEntry>>),
    #[cfg(feature = "storage")]
    External(TimeStamps<'a, TimeIndexEntry>),
}

#[derive(Iterator, DoubleEndedIterator, ExactSizeIterator, FusedIterator, Debug, Clone)]
pub enum TimeIndexRefVariants<Ref, Range, #[cfg(feature = "storage")] External> {
    Ref(Ref),
    Range(Range),
    #[cfg(feature = "storage")]
    External(External),
}

impl<'a> TimeIndexOps<'a> for TimeIndexRef<'a> {
    type IndexType = TimeIndexEntry;
    type RangeType = Self;

    #[inline(always)]
    fn active(&self, w: Range<TimeIndexEntry>) -> bool {
        match self {
            TimeIndexRef::Ref(t) => t.active(w),
            TimeIndexRef::Range(ref t) => t.active(w),
            #[cfg(feature = "storage")]
            TimeIndexRef::External(ref t) => t.active(w),
        }
    }

    fn range(&self, w: Range<TimeIndexEntry>) -> Self {
        match self {
            TimeIndexRef::Ref(t) => TimeIndexRef::Range(t.range(w)),
            TimeIndexRef::Range(ref t) => TimeIndexRef::Range(t.range(w)),
            #[cfg(feature = "storage")]
            TimeIndexRef::External(ref t) => TimeIndexRef::External(t.range(w)),
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        match self {
            TimeIndexRef::Ref(t) => t.first(),
            TimeIndexRef::Range(ref t) => t.first(),
            #[cfg(feature = "storage")]
            TimeIndexRef::External(ref t) => t.first(),
        }
    }

    fn last(&self) -> Option<Self::IndexType> {
        match self {
            TimeIndexRef::Ref(t) => t.last(),
            TimeIndexRef::Range(ref t) => t.last(),
            #[cfg(feature = "storage")]
            TimeIndexRef::External(ref t) => t.last(),
        }
    }

    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        match self {
            TimeIndexRef::Ref(t) => TimeIndexRefVariants::Ref(t.iter()),
            TimeIndexRef::Range(t) => TimeIndexRefVariants::Range(t.iter()),
            #[cfg(feature = "storage")]
            TimeIndexRef::External(t) => TimeIndexRefVariants::External(t.iter()),
        }
    }

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        match self {
            TimeIndexRef::Ref(t) => TimeIndexRefVariants::Ref(t.iter_rev()),
            TimeIndexRef::Range(t) => TimeIndexRefVariants::Range(t.iter_rev()),
            #[cfg(feature = "storage")]
            TimeIndexRef::External(t) => TimeIndexRefVariants::External(t.iter_rev()),
        }
    }

    fn len(&self) -> usize {
        match self {
            TimeIndexRef::Ref(ts) => ts.len(),
            TimeIndexRef::Range(ts) => ts.len(),
            #[cfg(feature = "storage")]
            TimeIndexRef::External(ref t) => t.len(),
        }
    }
}

#[derive(Clone)]
pub struct FilteredEdgeTimeIndex<'graph, G> {
    eid: ELID,
    time_index: TimeIndexRef<'graph>,
    view: G,
}

impl<'a, 'graph: 'a, G: GraphViewOps<'graph>> TimeIndexOps<'a>
    for FilteredEdgeTimeIndex<'graph, G>
{
    type IndexType = TimeIndexEntry;
    type RangeType = Self;

    #[inline]
    fn active(&self, w: Range<Self::IndexType>) -> bool {
        if self.view.edge_history_filtered() {
            self.time_index
                .range(w)
                .iter()
                .filter(|t| {
                    self.view
                        .filter_edge_history(self.eid, *t, self.view.layer_ids())
                })
                .next()
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
            self.iter().count()
        } else {
            self.time_index.len()
        }
    }
}

pub struct FilteredEdgeTProp<G, P> {
    eid: ELID,
    view: G,
    props: P,
}

impl<'graph, G: GraphViewOps<'graph>, P: TPropOps<'graph>> TPropOps<'graph>
    for FilteredEdgeTProp<G, P>
{
    fn iter(&self) -> BoxedLDIter<'graph, (TimeIndexEntry, Prop)> {
        let view = self.view.clone();
        let eid = self.eid;
        self.props
            .iter()
            .filter(move |(t, _)| view.filter_edge_history(eid, *t, view.layer_ids()))
            .into_dyn_dboxed()
    }

    fn iter_window(&self, r: Range<TimeIndexEntry>) -> BoxedLDIter<'graph, (TimeIndexEntry, Prop)> {
        let view = self.view.clone();
        let eid = self.eid;
        self.props
            .iter_window(r)
            .filter(move |(t, _)| view.filter_edge_history(eid, *t, view.layer_ids()))
            .into_dyn_dboxed()
    }

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
}

pub trait EdgeStorageOps<'a>: Copy + Sized + Send + Sync + 'a {
    fn edge_ref(self, dir: Dir) -> EdgeRef {
        EdgeRef::new(self.eid(), self.src(), self.dst(), dir)
    }
    fn out_ref(self) -> EdgeRef {
        self.edge_ref(Dir::Out)
    }

    /// Check if the edge was added in any of the layers during the time interval
    fn added(self, layer_ids: &LayerIds, w: Range<i64>) -> bool;

    /// Check if the edge was deleted in any of the layers during the time interval
    fn deleted(self, layer_ids: &LayerIds, w: Range<i64>) -> bool {
        self.deletions_iter(layer_ids.clone())
            .any(|(_, deletions)| deletions.active_t(w.clone()))
    }

    fn has_layer(self, layer_ids: &LayerIds) -> bool;
    fn src(self) -> VID;
    fn dst(self) -> VID;
    fn eid(self) -> EID;

    fn layer_ids_iter(self, layer_ids: LayerIds) -> impl Iterator<Item = usize> + Send + Sync + 'a;

    fn layer_ids_par_iter(self, layer_ids: &LayerIds) -> impl ParallelIterator<Item = usize> + 'a;

    fn additions_iter(
        self,
        layer_ids: LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexRef<'a>)> + Send + Sync + 'a {
        self.layer_ids_iter(layer_ids)
            .map(move |id| (id, self.additions(id)))
    }

    fn filtered_additions_iter<'graph: 'a, G: GraphViewOps<'graph>>(
        self,
        view: G,
    ) -> impl Iterator<Item = (usize, FilteredEdgeTimeIndex<'a, G>)> {
        let eid = self.eid();
        self.additions_iter(view.layer_ids().clone())
            .map(move |(layer_id, additions)| {
                (
                    layer_id,
                    FilteredEdgeTimeIndex {
                        eid: eid.with_layer(layer_id),
                        time_index: additions,
                        view: view.clone(),
                    },
                )
            })
    }

    fn additions_par_iter(
        self,
        layer_ids: &LayerIds,
    ) -> impl ParallelIterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        self.layer_ids_par_iter(layer_ids)
            .map(move |id| (id, self.additions(id)))
    }
    fn deletions_iter(
        self,
        layer_ids: LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        self.layer_ids_iter(layer_ids)
            .map(move |id| (id, self.deletions(id)))
    }

    fn filtered_deletions_iter<G: GraphViewOps<'a>>(
        self,
        view: G,
    ) -> impl Iterator<Item = (usize, FilteredEdgeTimeIndex<'a, G>)> {
        let eid = self.eid();
        self.deletions_iter(view.layer_ids().clone())
            .map(move |(layer_id, deletions)| {
                (
                    layer_id,
                    FilteredEdgeTimeIndex {
                        eid: eid.with_layer_deletion(layer_id),
                        time_index: deletions,
                        view: view.clone(),
                    },
                )
            })
    }

    fn deletions_par_iter(
        self,
        layer_ids: &LayerIds,
    ) -> impl ParallelIterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        self.layer_ids_par_iter(layer_ids)
            .map(move |id| (id, self.deletions(id)))
    }

    fn updates_iter(
        self,
        layer_ids: LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexRef<'a>, TimeIndexRef<'a>)> + 'a {
        self.layer_ids_iter(layer_ids)
            .map(move |id| (id, self.additions(id), self.deletions(id)))
    }

    fn filtered_updates_iter<G: GraphViewOps<'a>>(
        self,
        view: G,
    ) -> impl Iterator<
        Item = (
            usize,
            FilteredEdgeTimeIndex<'a, G>,
            FilteredEdgeTimeIndex<'a, G>,
        ),
    > + 'a {
        self.layer_ids_iter(view.layer_ids().clone())
            .map(move |layer_id| {
                (
                    layer_id,
                    self.filtered_additions(layer_id, view.clone()),
                    self.filtered_deletions(layer_id, view.clone()),
                )
            })
    }

    fn updates_par_iter(
        self,
        layer_ids: &LayerIds,
    ) -> impl ParallelIterator<Item = (usize, TimeIndexRef<'a>, TimeIndexRef<'a>)> + 'a {
        self.layer_ids_par_iter(layer_ids)
            .map(move |id| (id, self.additions(id), self.deletions(id)))
    }

    fn additions(self, layer_id: usize) -> TimeIndexRef<'a>;

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
    fn deletions(self, layer_id: usize) -> TimeIndexRef<'a>;

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

    fn temporal_prop_layer(self, layer_id: usize, prop_id: usize) -> impl TPropOps<'a> + Sync + 'a;

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

    fn temporal_prop_iter(
        self,
        layer_ids: LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (usize, impl TPropOps<'a>)> + 'a {
        self.layer_ids_iter(layer_ids)
            .map(move |id| (id, self.temporal_prop_layer(id, prop_id)))
    }

    fn filtered_temporal_prop_iter<G: GraphViewOps<'a>>(
        self,
        prop_id: usize,
        view: G,
    ) -> impl Iterator<Item = (usize, impl TPropOps<'a>)> + 'a {
        self.layer_ids_iter(view.layer_ids().clone())
            .map(move |layer_id| {
                (
                    layer_id,
                    self.filtered_temporal_prop_layer(layer_id, prop_id, view.clone()),
                )
            })
    }

    fn temporal_prop_par_iter(
        self,
        layer_ids: &LayerIds,
        prop_id: usize,
    ) -> impl ParallelIterator<Item = (usize, impl TPropOps<'a>)> + 'a {
        self.layer_ids_par_iter(layer_ids)
            .map(move |id| (id, self.temporal_prop_layer(id, prop_id)))
    }

    fn constant_prop_layer(self, layer_id: usize, prop_id: usize) -> Option<Prop>;

    fn constant_prop_iter(
        self,
        layer_ids: LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (usize, Prop)> + 'a {
        self.layer_ids_iter(layer_ids)
            .filter_map(move |id| Some((id, self.constant_prop_layer(id, prop_id)?)))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct MemEdge<'a> {
    edges: &'a EdgeShard,
    offset: usize,
}

impl<'a> MemEdge<'a> {
    pub fn new(edges: &'a EdgeShard, offset: usize) -> Self {
        MemEdge { edges, offset }
    }

    pub fn edge_store(&self) -> &EdgeStore {
        self.edges.edge_store(self.offset)
    }

    #[inline]
    pub fn props(self, layer_id: usize) -> Option<&'a Props> {
        self.edges
            .props(self.offset, layer_id)
            .and_then(|el| el.props())
    }

    pub fn eid(self) -> EID {
        self.edge_store().eid
    }

    pub fn as_edge_ref(&self) -> EdgeRef {
        EdgeRef::new_outgoing(self.eid(), self.src(), self.dst())
    }

    pub fn internal_num_layers(self) -> usize {
        self.edges.internal_num_layers()
    }

    fn get_additions(self, layer_id: usize) -> Option<&'a TimeIndex<TimeIndexEntry>> {
        self.edges.additions(self.offset, layer_id)
    }

    fn get_deletions(self, layer_id: usize) -> Option<&'a TimeIndex<TimeIndexEntry>> {
        self.edges.deletions(self.offset, layer_id)
    }

    pub fn has_layer_inner(self, layer_id: usize) -> bool {
        self.get_additions(layer_id)
            .filter(|t_index| !t_index.is_empty())
            .is_some()
            || self
                .get_deletions(layer_id)
                .filter(|t_index| !t_index.is_empty())
                .is_some()
    }
}

impl<'a> EdgeStorageOps<'a> for MemEdge<'a> {
    fn added(self, layer_ids: &LayerIds, w: Range<i64>) -> bool {
        match layer_ids {
            LayerIds::None => false,
            LayerIds::All => self
                .additions_iter(LayerIds::All)
                .any(|(_, t_index)| t_index.active_t(w.clone())),
            LayerIds::One(l_id) => self
                .get_additions(*l_id)
                .filter(|a| a.active_t(w))
                .is_some(),
            LayerIds::Multiple(layers) => layers
                .iter()
                .any(|l_id| self.added(&LayerIds::One(l_id), w.clone())),
        }
    }

    fn has_layer(self, layer_ids: &LayerIds) -> bool {
        match layer_ids {
            LayerIds::None => false,
            LayerIds::All => true,
            LayerIds::One(id) => self.has_layer_inner(*id),
            LayerIds::Multiple(ids) => ids.iter().any(|id| self.has_layer_inner(id)),
        }
    }

    fn src(self) -> VID {
        self.edge_store().src
    }

    fn dst(self) -> VID {
        self.edge_store().dst
    }

    fn eid(self) -> EID {
        self.eid()
    }

    fn layer_ids_iter(self, layer_ids: LayerIds) -> impl Iterator<Item = usize> + 'a {
        match layer_ids {
            LayerIds::None => LayerVariants::None(std::iter::empty()),
            LayerIds::All => LayerVariants::All(
                (0..self.internal_num_layers()).filter(move |&l| self.has_layer_inner(l)),
            ),
            LayerIds::One(id) => {
                LayerVariants::One(self.has_layer_inner(id).then_some(id).into_iter())
            }
            LayerIds::Multiple(ids) => {
                LayerVariants::Multiple(ids.into_iter().filter(move |&id| self.has_layer_inner(id)))
            }
        }
    }

    fn layer_ids_par_iter(self, layer_ids: &LayerIds) -> impl ParallelIterator<Item = usize> + 'a {
        match layer_ids {
            LayerIds::None => LayerVariants::None(rayon::iter::empty()),
            LayerIds::All => LayerVariants::All(
                (0..self.internal_num_layers())
                    .into_par_iter()
                    .filter(move |&l| self.has_layer_inner(l)),
            ),
            LayerIds::One(id) => {
                LayerVariants::One(self.has_layer_inner(*id).then_some(*id).into_par_iter())
            }
            LayerIds::Multiple(ids) => {
                LayerVariants::Multiple(ids.par_iter().filter(move |&id| self.has_layer_inner(id)))
            }
        }
    }

    fn additions(self, layer_id: usize) -> TimeIndexRef<'a> {
        TimeIndexRef::Ref(self.get_additions(layer_id).unwrap_or(&TimeIndex::Empty))
    }

    fn deletions(self, layer_id: usize) -> TimeIndexRef<'a> {
        TimeIndexRef::Ref(self.get_deletions(layer_id).unwrap_or(&TimeIndex::Empty))
    }

    #[inline(always)]
    fn temporal_prop_layer(self, layer_id: usize, prop_id: usize) -> impl TPropOps<'a> + 'a {
        self.props(layer_id)
            .and_then(|props| props.temporal_prop(prop_id))
            .unwrap_or(&TProp::Empty)
    }

    fn constant_prop_layer(self, layer_id: usize, prop_id: usize) -> Option<Prop> {
        self.props(layer_id)
            .and_then(|props| props.const_prop(prop_id).cloned())
    }
}
