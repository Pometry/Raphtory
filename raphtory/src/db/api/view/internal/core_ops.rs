use crate::{
    core::{
        entities::{nodes::node_store::NodeTimestamps, properties::props::Meta},
        storage::timeindex::{TimeIndexOps, TimeIndexWindow, TimeIndexWindowVariants},
        Prop,
    },
    db::api::{
        storage::graph::{
            edges::edge_storage_ops::EdgeStorageOps, nodes::node_storage_ops::NodeStorageOps,
            variants::filter_variants::FilterVariants,
        },
        view::internal::{Base, FilterOps, FilterState},
    },
    prelude::GraphViewOps,
};
use iter_enum::{DoubleEndedIterator, ExactSizeIterator, FusedIterator, Iterator};
use itertools::Itertools;
#[cfg(feature = "storage")]
use pometry_storage::timestamps::LayerAdditions;
use raphtory_api::{
    core::{
        entities::ELID,
        storage::timeindex::{TimeIndexEntry, TimeIndexLike},
    },
    iter::IntoDynBoxed,
};
use raphtory_core::entities::graph::tgraph_storage::GraphStorage;
use std::{iter, ops::Range};

pub trait InheritCoreOps: Base {}

impl<G: InheritCoreOps> DelegateCoreOps for G
where
    G::Base: CoreGraphOps,
{
    type Internal = G::Base;

    #[inline]
    fn graph(&self) -> &Self::Internal {
        self.base()
    }
}

pub trait DelegateCoreOps {
    type Internal: CoreGraphOps + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: DelegateCoreOps + ?Sized + Send + Sync> CoreGraphOps for G {
    #[inline]
    fn core_graph(&self) -> &GraphStorage {
        self.graph().core_graph()
    }
}

impl<'a> TimeIndexOps<'a> for &'a NodeTimestamps {
    type IndexType = TimeIndexEntry;
    type RangeType = TimeIndexWindow<'a, TimeIndexEntry, NodeTimestamps>;

    #[inline]
    fn active(&self, w: Range<Self::IndexType>) -> bool {
        self.edge_ts().active(w.clone()) || self.props_ts().active(w)
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType {
        TimeIndexWindow::Range {
            timeindex: *self,
            range: w,
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        let first = self.edge_ts().first();
        let other = self.props_ts().first();

        first
            .zip(other)
            .map(|(a, b)| a.min(b))
            .or_else(|| first.or(other))
    }

    fn last(&self) -> Option<Self::IndexType> {
        let last = self.edge_ts().last();
        let other = self.props_ts().last();

        last.zip(other)
            .map(|(a, b)| a.max(b))
            .or_else(|| last.or(other))
    }

    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        self.edge_ts
            .iter()
            .map(|(t, _)| *t)
            .merge(self.props_ts.iter().map(|(t, _)| *t))
    }

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        self.edge_ts
            .iter()
            .rev()
            .map(|(t, _)| *t)
            .merge_by(self.props_ts.iter().rev().map(|(t, _)| *t), |lt, rt| {
                lt >= rt
            })
    }

    fn len(&self) -> usize {
        self.edge_ts.len() + self.props_ts.len()
    }
}

impl<'a> TimeIndexLike<'a> for &'a NodeTimestamps {
    fn range_iter(
        self,
        w: Range<Self::IndexType>,
    ) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        self.edge_ts()
            .range_iter(w.clone())
            .merge(self.props_ts().range_iter(w))
    }

    fn range_iter_rev(
        self,
        w: Range<Self::IndexType>,
    ) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        self.edge_ts()
            .range_iter_rev(w.clone())
            .merge_by(self.props_ts().range_iter_rev(w), |lt, rt| lt >= rt)
    }

    fn range_count(&self, w: Range<Self::IndexType>) -> usize {
        self.edge_ts().range_count(w.clone()) + self.props_ts().range_count(w)
    }

    fn first_range(&self, w: Range<Self::IndexType>) -> Option<Self::IndexType> {
        let first = self
            .edge_ts()
            .iter_window(w.clone())
            .next()
            .map(|(t, _)| *t);
        let other = self.props_ts().iter_window(w).next().map(|(t, _)| *t);

        first
            .zip(other)
            .map(|(a, b)| a.min(b))
            .or_else(|| first.or(other))
    }

    fn last_range(&self, w: Range<Self::IndexType>) -> Option<Self::IndexType> {
        let last = self
            .edge_ts
            .iter_window(w.clone())
            .next_back()
            .map(|(t, _)| *t);
        let other = self.props_ts.iter_window(w).next_back().map(|(t, _)| *t);

        last.zip(other)
            .map(|(a, b)| a.max(b))
            .or_else(|| last.or(other))
    }
}

#[derive(Clone, Debug)]
pub enum NodeAdditions<'a> {
    Mem(&'a NodeTimestamps),
    Range(TimeIndexWindow<'a, TimeIndexEntry, NodeTimestamps>),
    #[cfg(feature = "storage")]
    Col(LayerAdditions<'a>),
}

#[cfg(feature = "storage")]
#[derive(Iterator, DoubleEndedIterator, ExactSizeIterator, FusedIterator, Debug)]
pub enum AdditionVariants<Mem, Range, Col> {
    Mem(Mem),
    Range(Range),
    Col(Col),
}

#[cfg(not(feature = "storage"))]
#[derive(Iterator, DoubleEndedIterator, ExactSizeIterator, FusedIterator, Debug)]
pub enum AdditionVariants<Mem, Range> {
    Mem(Mem),
    Range(Range),
}

impl<'a> NodeAdditions<'a> {
    #[inline]
    pub fn prop_events(&self) -> impl Iterator<Item = TimeIndexEntry> + use<'a> {
        match self {
            NodeAdditions::Mem(index) => {
                AdditionVariants::Mem(index.props_ts.iter().map(|(t, _)| *t))
            }
            NodeAdditions::Range(index) => AdditionVariants::Range(match index {
                TimeIndexWindow::Empty => TimeIndexWindowVariants::Empty(iter::empty()),
                TimeIndexWindow::Range { timeindex, range } => TimeIndexWindowVariants::Range(
                    timeindex
                        .props_ts
                        .iter_window(range.clone())
                        .map(|(t, _)| *t),
                ),
                TimeIndexWindow::All(index) => TimeIndexWindowVariants::All(
                    index.props_ts.iter().map(|(t, _)| *t).into_dyn_boxed(),
                ),
            }),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => {
                AdditionVariants::Col(index.clone().prop_events().map(|t| t.into_iter()).kmerge())
            }
        }
    }

    #[inline]
    pub fn prop_events_rev(&self) -> impl Iterator<Item = TimeIndexEntry> + use<'a> {
        match self {
            NodeAdditions::Mem(index) => {
                AdditionVariants::Mem(index.props_ts.iter().map(|(t, _)| *t).rev())
            }
            NodeAdditions::Range(index) => AdditionVariants::Range(match index {
                TimeIndexWindow::Empty => TimeIndexWindowVariants::Empty(iter::empty()),
                TimeIndexWindow::Range { timeindex, range } => TimeIndexWindowVariants::Range(
                    timeindex
                        .props_ts
                        .iter_window(range.clone())
                        .map(|(t, _)| *t)
                        .rev(),
                ),
                TimeIndexWindow::All(index) => {
                    TimeIndexWindowVariants::All(index.props_ts.iter().map(|(t, _)| *t).rev())
                }
            }),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => AdditionVariants::Col(
                index
                    .clone()
                    .prop_events()
                    .map(|t| t.into_iter().rev())
                    .kmerge_by(|t1, t2| t1 >= t2),
            ),
        }
    }

    #[inline]
    pub fn edge_events(&self) -> impl Iterator<Item = (TimeIndexEntry, ELID)> + use<'a> {
        match self {
            NodeAdditions::Mem(index) => {
                AdditionVariants::Mem(index.edge_ts.iter().map(|(t, e)| (*t, *e)))
            }
            NodeAdditions::Range(index) => AdditionVariants::Range(match index {
                TimeIndexWindow::Empty => TimeIndexWindowVariants::Empty(iter::empty()),
                TimeIndexWindow::Range { timeindex, range } => TimeIndexWindowVariants::Range(
                    timeindex
                        .edge_ts
                        .iter_window(range.clone())
                        .map(|(t, e)| (*t, *e)),
                ),
                TimeIndexWindow::All(index) => {
                    TimeIndexWindowVariants::All(index.edge_ts.iter().map(|(t, e)| (*t, *e)))
                }
            }),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => AdditionVariants::Col(index.edge_history()),
        }
    }

    #[inline]
    pub fn edge_events_rev(&self) -> impl Iterator<Item = (TimeIndexEntry, ELID)> + use<'a> {
        match self {
            NodeAdditions::Mem(index) => {
                AdditionVariants::Mem(index.edge_ts.iter().map(|(t, e)| (*t, *e)).rev())
            }
            NodeAdditions::Range(index) => AdditionVariants::Range(match index {
                TimeIndexWindow::Empty => TimeIndexWindowVariants::Empty(iter::empty()),
                TimeIndexWindow::Range { timeindex, range } => TimeIndexWindowVariants::Range(
                    timeindex
                        .edge_ts
                        .iter_window(range.clone())
                        .map(|(t, e)| (*t, *e))
                        .rev(),
                ),
                TimeIndexWindow::All(index) => {
                    TimeIndexWindowVariants::All(index.edge_ts.iter().map(|(t, e)| (*t, *e)).rev())
                }
            }),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => AdditionVariants::Col(index.edge_history_rev()),
        }
    }
}

impl<'b> TimeIndexOps<'b> for NodeAdditions<'b> {
    type IndexType = TimeIndexEntry;
    type RangeType = Self;

    #[inline]
    fn active(&self, w: Range<TimeIndexEntry>) -> bool {
        match self {
            NodeAdditions::Mem(index) => index.active(w),
            NodeAdditions::Range(index) => index.active(w),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => index.iter().any(|index| index.active(w.clone())),
        }
    }

    fn range(&self, w: Range<TimeIndexEntry>) -> Self {
        match self {
            NodeAdditions::Mem(index) => NodeAdditions::Range(index.range(w)),
            NodeAdditions::Range(index) => NodeAdditions::Range(index.range(w)),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => NodeAdditions::Col(index.with_range(w)),
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        match self {
            NodeAdditions::Mem(index) => index.first(),
            NodeAdditions::Range(index) => index.first(),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => index.iter().flat_map(|index| index.first()).min(),
        }
    }

    fn last(&self) -> Option<Self::IndexType> {
        match self {
            NodeAdditions::Mem(index) => index.last(),
            NodeAdditions::Range(index) => index.last(),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => index.iter().flat_map(|index| index.last()).max(),
        }
    }

    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'b {
        match self {
            NodeAdditions::Mem(index) => AdditionVariants::Mem(index.iter()),
            NodeAdditions::Range(index) => AdditionVariants::Range(index.iter()),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => {
                AdditionVariants::Col(index.iter().map(|index| index.into_iter()).kmerge())
            }
        }
    }

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'b {
        match self {
            NodeAdditions::Mem(index) => AdditionVariants::Mem(index.iter_rev()),
            NodeAdditions::Range(index) => AdditionVariants::Range(index.iter_rev()),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => AdditionVariants::Col(
                index
                    .iter()
                    .map(|index| index.into_iter().rev())
                    .kmerge_by(|lt, rt| lt >= rt),
            ),
        }
    }

    fn len(&self) -> usize {
        match self {
            NodeAdditions::Mem(index) => index.len(),
            NodeAdditions::Range(range) => range.len(),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(col) => col.len(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NodeHistory<'a, G> {
    pub(crate) additions: NodeAdditions<'a>,
    pub(crate) view: G,
}

#[derive(Debug, Clone)]
pub struct NodeEdgeHistory<'a, G> {
    pub(crate) additions: NodeAdditions<'a>,
    pub(crate) view: G,
}

#[derive(Debug, Clone)]
pub struct NodePropHistory<'a, G> {
    pub(crate) additions: NodeAdditions<'a>,
    pub(crate) view: G,
}

impl<'a, G: Clone> NodeHistory<'a, G> {
    pub fn edge_history(&self) -> NodeEdgeHistory<'a, G> {
        NodeEdgeHistory {
            additions: self.additions.clone(),
            view: self.view.clone(),
        }
    }

    pub fn prop_history(&self) -> NodePropHistory<'a, G> {
        NodePropHistory {
            additions: self.additions.clone(),
            view: self.view.clone(),
        }
    }
}

impl<'a, G: GraphViewOps<'a>> NodeEdgeHistory<'a, G> {
    pub fn history(&self) -> impl Iterator<Item = (TimeIndexEntry, ELID)> + use<'a, G> {
        let view = self.view.clone();
        let iter = self.additions.edge_events();
        match view.filter_state() {
            FilterState::Neither => FilterVariants::Neither(iter),
            FilterState::Both => {
                let nodes = view.core_nodes();
                let edges = view.core_edges();
                FilterVariants::Both(iter.filter(move |(t, e)| {
                    view.filter_edge_history(*e, *t, view.layer_ids()) && {
                        let edge = edges.edge(e.edge);
                        view.internal_filter_node(nodes.node_entry(edge.src()), view.layer_ids())
                            && view.internal_filter_node(
                                nodes.node_entry(edge.dst()),
                                view.layer_ids(),
                            )
                    }
                }))
            }
            FilterState::Nodes => {
                let nodes = view.core_nodes();
                let edges = view.core_edges();
                FilterVariants::Nodes(iter.filter(move |(_, e)| {
                    let edge = edges.edge(e.edge);
                    view.internal_filter_node(nodes.node_entry(edge.src()), view.layer_ids())
                        && view.internal_filter_node(nodes.node_entry(edge.dst()), view.layer_ids())
                }))
            }
            FilterState::Edges | FilterState::BothIndependent => FilterVariants::Edges(
                iter.filter(move |(t, e)| view.filter_edge_history(*e, *t, view.layer_ids())),
            ),
        }
    }

    pub fn history_rev(&self) -> impl Iterator<Item = (TimeIndexEntry, ELID)> + use<'a, G> {
        let view = self.view.clone();
        let iter = self.additions.edge_events_rev();
        match view.filter_state() {
            FilterState::Neither => FilterVariants::Neither(iter),
            FilterState::Both => {
                let nodes = view.core_nodes();
                let edges = view.core_edges();
                FilterVariants::Both(iter.filter(move |(t, e)| {
                    view.filter_edge_history(*e, *t, view.layer_ids()) && {
                        let edge = edges.edge(e.edge);
                        view.internal_filter_node(nodes.node_entry(edge.src()), view.layer_ids())
                            && view.internal_filter_node(
                                nodes.node_entry(edge.dst()),
                                view.layer_ids(),
                            )
                    }
                }))
            }
            FilterState::Nodes => {
                let nodes = view.core_nodes();
                let edges = view.core_edges();
                FilterVariants::Nodes(iter.filter(move |(_, e)| {
                    let edge = edges.edge(e.edge);
                    view.internal_filter_node(nodes.node_entry(edge.src()), view.layer_ids())
                        && view.internal_filter_node(nodes.node_entry(edge.dst()), view.layer_ids())
                }))
            }
            FilterState::Edges | FilterState::BothIndependent => FilterVariants::Edges(
                iter.filter(move |(t, e)| view.filter_edge_history(*e, *t, view.layer_ids())),
            ),
        }
    }
}

impl<'a, G: GraphViewOps<'a>> TimeIndexOps<'a> for NodePropHistory<'a, G> {
    type IndexType = TimeIndexEntry;
    type RangeType = Self;

    fn active(&self, w: Range<Self::IndexType>) -> bool {
        let history = &self.additions;
        match history {
            NodeAdditions::Mem(h) => h.props_ts().active(w),
            NodeAdditions::Range(h) => match h {
                TimeIndexWindow::Empty => false,
                TimeIndexWindow::Range { timeindex, range } => {
                    let start = range.start.max(w.start);
                    let end = range.end.min(w.end).max(start);
                    timeindex.props_ts().active(start..end)
                }
                TimeIndexWindow::All(h) => h.props_ts().active(w),
            },
            #[cfg(feature = "storage")]
            NodeAdditions::Col(h) => h.with_range(w).prop_events().any(|t| !t.is_empty()),
        }
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType {
        let additions = self.additions.range(w);
        NodePropHistory {
            additions,
            view: self.view.clone(),
        }
    }

    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        self.additions.prop_events()
    }

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        self.additions.prop_events_rev()
    }

    fn len(&self) -> usize {
        match &self.additions {
            NodeAdditions::Mem(additions) => additions.props_ts.len(),
            NodeAdditions::Range(additions) => match additions {
                TimeIndexWindow::Empty => 0,
                TimeIndexWindow::Range { timeindex, range } => {
                    (&timeindex.props_ts).range(range.clone()).len()
                }
                TimeIndexWindow::All(timeindex) => timeindex.props_ts.len(),
            },
            #[cfg(feature = "storage")]
            NodeAdditions::Col(additions) => additions.clone().prop_events().map(|t| t.len()).sum(),
        }
    }

    fn is_empty(&self) -> bool {
        match &self.additions {
            NodeAdditions::Mem(additions) => additions.props_ts.is_empty(),
            NodeAdditions::Range(additions) => match additions {
                TimeIndexWindow::Empty => true,
                TimeIndexWindow::Range { timeindex, range } => {
                    (&timeindex.props_ts).range(range.clone()).is_empty()
                }
                TimeIndexWindow::All(timeindex) => timeindex.props_ts.is_empty(),
            },
            #[cfg(feature = "storage")]
            NodeAdditions::Col(additions) => additions.clone().prop_events().all(|t| t.is_empty()),
        }
    }
}

impl<'a, G: GraphViewOps<'a>> TimeIndexOps<'a> for NodeEdgeHistory<'a, G> {
    type IndexType = TimeIndexEntry;
    type RangeType = Self;

    fn active(&self, w: Range<Self::IndexType>) -> bool {
        !self.range(w).is_empty()
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType {
        let additions = self.additions.range(w);
        NodeEdgeHistory {
            additions,
            view: self.view.clone(),
        }
    }

    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        self.history().map(|(t, _)| t)
    }

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        self.history_rev().map(|(t, _)| t)
    }

    fn len(&self) -> usize {
        if matches!(self.view.filter_state(), FilterState::Neither) {
            match &self.additions {
                NodeAdditions::Mem(additions) => additions.edge_ts.len(),
                NodeAdditions::Range(additions) => match additions {
                    TimeIndexWindow::Empty => 0,
                    TimeIndexWindow::Range { timeindex, range } => {
                        (&timeindex.edge_ts).range(range.clone()).len()
                    }
                    TimeIndexWindow::All(timeindex) => timeindex.edge_ts.len(),
                },
                #[cfg(feature = "storage")]
                NodeAdditions::Col(additions) => {
                    additions.clone().edge_events().map(|t| t.len()).sum()
                }
            }
        } else {
            self.history().count()
        }
    }

    fn is_empty(&self) -> bool {
        if matches!(self.view.filter_state(), FilterState::Neither) {
            match &self.additions {
                NodeAdditions::Mem(additions) => additions.edge_ts.is_empty(),
                NodeAdditions::Range(additions) => match additions {
                    TimeIndexWindow::Empty => true,
                    TimeIndexWindow::Range { timeindex, range } => {
                        (&timeindex.edge_ts).range(range.clone()).is_empty()
                    }
                    TimeIndexWindow::All(timeindex) => timeindex.edge_ts.is_empty(),
                },
                #[cfg(feature = "storage")]
                NodeAdditions::Col(additions) => {
                    additions.clone().edge_events().all(|t| t.is_empty())
                }
            }
        } else {
            self.history().next().is_none()
        }
    }
}

impl<'b, G: GraphViewOps<'b>> TimeIndexOps<'b> for NodeHistory<'b, G> {
    type IndexType = TimeIndexEntry;
    type RangeType = Self;

    fn active(&self, w: Range<Self::IndexType>) -> bool {
        self.prop_history().active(w.clone()) || self.edge_history().active(w)
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self {
        let additions = self.additions.range(w);
        let view = self.view.clone();
        NodeHistory { additions, view }
    }

    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'b {
        self.prop_history().iter().merge(self.edge_history().iter())
    }

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'b {
        self.prop_history()
            .iter_rev()
            .merge_by(self.edge_history().iter_rev(), |t1, t2| t1 >= t2)
    }

    fn len(&self) -> usize {
        self.prop_history().len() + self.edge_history().len()
    }

    fn is_empty(&self) -> bool {
        self.prop_history().is_empty() && self.edge_history().is_empty()
    }
}
