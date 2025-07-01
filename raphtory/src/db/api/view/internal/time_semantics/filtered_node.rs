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
use raphtory_storage::graph::{
    edges::edge_storage_ops::EdgeStorageOps, nodes::node_storage_ops::NodeStorageOps,
};
use std::ops::Range;

#[derive(Debug, Clone)]
pub struct NodeHistory<'a, G> {
    pub(crate) edge_history: storage::NodeEdgeAdditions<'a>,
    pub(crate) additions: storage::NodePropAdditions<'a>,
    pub(crate) view: G,
}

#[derive(Debug, Clone)]
pub struct NodeEdgeHistory<'a, G> {
    pub(crate) additions: storage::NodeEdgeAdditions<'a>,
    pub(crate) view: G,
}

#[derive(Debug, Clone)]
pub struct NodePropHistory<'a, G> {
    pub(crate) additions: storage::NodePropAdditions<'a>,
    pub(crate) view: G,
}

impl<'a, G: Clone> NodeHistory<'a, G> {
    pub fn edge_history(&self) -> NodeEdgeHistory<'a, G> {
        NodeEdgeHistory {
            additions: self.edge_history.clone(),
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
        self.additions.active(w)
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType {
        let additions = self.additions.range(w);
        NodePropHistory {
            additions,
            view: self.view.clone(),
        }
    }

    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        self.additions.iter()
    }

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        self.additions.iter_rev()
    }

    fn len(&self) -> usize {
        self.additions.len()
    }

    fn is_empty(&self) -> bool {
        self.additions.is_empty()
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
            self.additions.len()
        } else {
            self.history().count()
        }
    }

    fn is_empty(&self) -> bool {
        if matches!(self.view.filter_state(), FilterState::Neither) {
            self.additions.is_empty()
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
        let edge_history = self.edge_history.range(w.clone());
        let additions = self.additions.range(w);
        let view = self.view.clone();
        NodeHistory {
            edge_history,
            additions,
            view,
        }
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
    fn history<G: GraphView + 'a>(self, view: G) -> NodeHistory<'a, G> {
        // FIXME: new storage supports multiple layers, but this is hardcoded to layer 0 as there is no information in history about which layer the node belongs to.
        let additions = self.node_additions(0);
        let edge_history = self.node_edge_additions(0);
        NodeHistory {
            edge_history,
            additions,
            view,
        }
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
