use crate::{
    db::api::view::internal::{
        EdgeTimeSemanticsOps, FilterOps, FilterState, FilterVariants, GraphView,
    },
    prelude::GraphViewOps,
};
use either::Either;
use itertools::Itertools;
use raphtory_api::core::{
    entities::{edges::edge_ref::EdgeRef, LayerIds, ELID, VID},
    storage::timeindex::{EventTime, TimeIndexOps},
    Direction,
};
use raphtory_storage::{core_ops::CoreGraphOps, graph::nodes::node_storage_ops::NodeStorageOps};
use std::ops::Range;
use storage::gen_ts::ALL_LAYERS;

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
            additions: self.edge_history,
            view: self.view.clone(),
        }
    }

    pub fn prop_history(&self) -> NodePropHistory<'a, G> {
        NodePropHistory {
            additions: self.additions,
            view: self.view.clone(),
        }
    }
}

fn handle_update_iter<'graph, G: GraphViewOps<'graph>>(
    iter: impl Iterator<Item = (EventTime, ELID)> + 'graph,
    view: G,
) -> impl Iterator<Item = (EventTime, ELID)> + 'graph {
    if view.filtered() {
        let time_semantics = view.edge_time_semantics();
        Either::Left(
            iter.filter_map(move |(t, e)| time_semantics.handle_edge_update_filter(t, e, &view)),
        )
    } else {
        Either::Right(iter)
    }
}

impl<'a, G: GraphViewOps<'a>> NodeEdgeHistory<'a, G> {
    pub fn history(&self) -> impl Iterator<Item = (EventTime, ELID)> + use<'a, G> {
        handle_update_iter(self.additions.edge_events(), self.view.clone())
    }

    pub fn history_rev(&self) -> impl Iterator<Item = (EventTime, ELID)> + use<'a, G> {
        handle_update_iter(self.additions.edge_events_rev(), self.view.clone())
    }
}

impl<'a, G: GraphViewOps<'a>> TimeIndexOps<'a> for NodePropHistory<'a, G> {
    type IndexType = EventTime;
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
    type IndexType = EventTime;
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
    type IndexType = EventTime;
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
    /// Get a filtered view of the update history of the node
    ///
    /// Note that this is an internal API that does not apply the window filtering!
    fn history<G: GraphView + 'a>(self, view: G, layer_ids: &'a LayerIds) -> NodeHistory<'a, G> {
        // FIXME: new storage supports multiple layers, we can be specific about the layers here once NodeStorageOps is updated
        let additions = self.node_additions(ALL_LAYERS);
        let edge_history = self.node_edge_additions(layer_ids);
        NodeHistory {
            edge_history,
            additions,
            view,
        }
    }

    fn edge_history<G: GraphView + 'a>(
        self,
        view: G,
        layer_ids: &'a LayerIds,
    ) -> NodeEdgeHistory<'a, G> {
        self.history(view, layer_ids).edge_history()
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
            FilterState::Both => FilterVariants::Both(iter.filter(move |e| {
                let gs = view.core_graph();
                view.filter_edge(gs.core_edge(e.pid()).as_ref())
                    && view.filter_node(gs.core_node(e.remote()).as_ref())
            })),
            FilterState::Nodes => FilterVariants::Nodes(iter.filter(move |e| {
                let gs = view.core_graph();
                view.filter_node(gs.core_node(e.remote()).as_ref())
            })),
            FilterState::Edges | FilterState::BothIndependent => {
                FilterVariants::Edges(iter.filter(move |e| {
                    let gs = view.core_graph();
                    view.filter_edge(gs.core_edge(e.pid()).as_ref())
                }))
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
