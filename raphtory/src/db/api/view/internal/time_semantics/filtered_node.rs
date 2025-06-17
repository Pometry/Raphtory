use crate::{
    db::api::view::internal::{FilterOps, FilterState, FilterVariants, GraphView},
    prelude::GraphViewOps,
};
use itertools::Itertools;
use raphtory_api::core::{
    entities::{edges::edge_ref::EdgeRef, LayerIds, ELID, VID},
    storage::timeindex::{TimeIndexEntry, TimeIndexOps},
    Direction,
};
use raphtory_core::storage::timeindex::TimeIndexWindow;
use raphtory_storage::graph::{
    edges::edge_storage_ops::EdgeStorageOps,
    nodes::{node_additions::NodeAdditions, node_storage_ops::NodeStorageOps},
};
use std::ops::Range;

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
                NodeAdditions::Col(additions) => additions.edge_history().count(),
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

pub trait FilteredNodeStorageOps<'a>: NodeStorageOps<'a> {
    /// Get a filtered view of the update history of the node
    ///
    /// Note that this is an internal API that does not apply the window filtering!
    fn history<G: GraphView + 'a>(self, view: G) -> NodeHistory<'a, G> {
        let additions = self.additions();
        NodeHistory { additions, view }
    }

    fn filtered_edges_iter<G: GraphViewOps<'a>>(
        self,
        view: G,
        layer_ids: &'a LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + 'a {
        let iter = self.edges_iter(layer_ids, dir);
        match view.filter_state() {
            FilterState::Neither => FilterVariants::Neither(iter),
            FilterState::Both => {
                let nodes = view.core_nodes();
                let edges = view.core_edges();
                FilterVariants::Both(iter.filter(move |e| {
                    view.filter_edge(edges.edge(e.pid()), view.layer_ids())
                        && view.filter_node(nodes.node_entry(e.remote()))
                }))
            }
            FilterState::Nodes => {
                let nodes = view.core_nodes();
                FilterVariants::Nodes(
                    iter.filter(move |e| view.filter_node(nodes.node_entry(e.remote()))),
                )
            }
            FilterState::Edges | FilterState::BothIndependent => {
                let edges = view.core_edges();
                FilterVariants::Edges(
                    iter.filter(move |e| view.filter_edge(edges.edge(e.pid()), view.layer_ids())),
                )
            }
        }
    }

    fn filtered_neighbours_iter<G: GraphViewOps<'a>>(
        self,
        view: G,
        layer_ids: &'a LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = VID> + 'a {
        self.filtered_edges_iter(view, layer_ids, dir)
            .map(|e| e.remote())
            .dedup()
    }
}

impl<'a, T: NodeStorageOps<'a>> FilteredNodeStorageOps<'a> for T {}
