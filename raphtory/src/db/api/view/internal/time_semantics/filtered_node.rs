use crate::{
    db::api::view::internal::{
        EdgeTimeSemanticsOps, FilterOps, FilterState, FilterVariants, GraphView,
    },
    prelude::GraphViewOps,
};
use either::Either;
use itertools::Itertools;
use raphtory_api::core::{
    entities::{
        edges::edge_ref::EdgeRef, layers::Multiple, properties::meta::STATIC_GRAPH_LAYER_ID,
        LayerIds, ELID, VID,
    },
    storage::timeindex::{EventTime, TimeIndexOps},
    Direction,
};
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::{edges::edge_storage_ops::EdgeStorageOps, nodes::node_storage_ops::NodeStorageOps},
};
use std::{ops::Range, sync::Arc};
use storage::{api::edges::EdgeRefOps, gen_ts::LayerIter, EdgeEntryRef};

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
        handle_update_iter(self.additions.clone().edge_events(), self.view.clone())
    }

    pub fn history_rev(&self) -> impl Iterator<Item = (EventTime, ELID)> + use<'a, G> {
        handle_update_iter(self.additions.clone().edge_events_rev(), self.view.clone())
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

/// Build a `LayerIter` that includes `STATIC_GRAPH_LAYER_ID` in addition to any explicitly
/// requested layers. Nodes added without a specific layer are stored in STATIC_GRAPH_LAYER_ID
/// and should be visible in every layer-restricted view.
fn layer_ids_with_static(layer_ids: &LayerIds) -> LayerIter<'_> {
    match layer_ids {
        // All layers already includes STATIC
        LayerIds::All => LayerIter::LRef(layer_ids),
        // No layers + static = just static
        LayerIds::None => LayerIter::One(STATIC_GRAPH_LAYER_ID),
        LayerIds::One(id) => {
            if *id == STATIC_GRAPH_LAYER_ID {
                LayerIter::One(*id)
            } else {
                // Return both the static layer and the requested layer, sorted for binary search
                let mut ids = [STATIC_GRAPH_LAYER_ID, *id];
                ids.sort();
                LayerIter::Multiple(Multiple(Arc::from(ids.as_slice())))
            }
        }
        LayerIds::Multiple(ids) => {
            if ids.contains(STATIC_GRAPH_LAYER_ID) {
                LayerIter::LRef(layer_ids)
            } else {
                let mut combined: Vec<_> = std::iter::once(STATIC_GRAPH_LAYER_ID)
                    .chain(ids.iter())
                    .collect();
                combined.sort();
                LayerIter::Multiple(Multiple(Arc::from(combined.as_slice())))
            }
        }
    }
}

pub trait FilteredNodeStorageOps<'a>: NodeStorageOps<'a> {
    /// Get a filtered view of the update history of the node
    ///
    /// Note that this is an internal API that does not apply the window filtering!
    fn history<G: GraphView + 'a>(self, view: G, layer_ids: &'a LayerIds) -> NodeHistory<'a, G> {
        // Nodes added without a specific layer go to STATIC_GRAPH_LAYER_ID and should appear
        // active in any layer-restricted view. Nodes added with an explicit layer only appear
        // in that layer's view.
        let additions = self.node_additions(layer_ids_with_static(layer_ids));
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

    fn filtered_edges_iter<G: GraphView + 'a>(
        self,
        view: &'a G,
        layer_ids: &'a LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + 'a {
        let iter = self.edges_iter(layer_ids, dir);
        match view.filter_state() {
            FilterState::Neither => FilterVariants::Neither(iter),
            FilterState::Both => FilterVariants::Both(iter.filter(move |e| {
                let gs = view.core_graph();
                view.filter_edge(gs.core_edge(Either::Right(*e)).as_ref())
                    && view.filter_node(gs.core_node(e.remote()).as_ref())
            })),
            FilterState::Nodes => FilterVariants::Nodes(iter.filter(move |e| {
                let gs = view.core_graph();
                view.filter_node(gs.core_node(e.remote()).as_ref())
            })),
            FilterState::Edges | FilterState::BothIndependent => {
                FilterVariants::Edges(iter.filter(move |e| {
                    let gs = view.core_graph();
                    view.filter_edge(gs.core_edge(Either::Right(*e)).as_ref())
                }))
            }
        }
    }

    fn internal_filtered_edges_iter<G: GraphView + 'a>(
        self,
        view: &'a G,
        layer_ids: &'a LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + 'a {
        let iter = self.edges_iter(layer_ids, dir);
        match view.filter_state() {
            FilterState::Neither => FilterVariants::Neither(iter),
            FilterState::Both => FilterVariants::Both(iter.filter(move |e| {
                let gs = view.core_graph();
                view.internal_filter_node(gs.core_node(e.remote()).as_ref(), layer_ids)
                    && filter_edge_impl(view, gs.core_edge(Either::Right(*e)).as_ref(), layer_ids)
            })),
            FilterState::Nodes | FilterState::BothIndependent => {
                FilterVariants::Nodes(iter.filter(move |e| {
                    let gs = view.core_graph();
                    view.internal_filter_node(gs.core_node(e.remote()).as_ref(), layer_ids)
                }))
            }
            FilterState::Edges => FilterVariants::Edges(iter.filter(move |e| {
                let gs = view.core_graph();
                filter_edge_impl(view, gs.core_edge(Either::Right(*e)).as_ref(), layer_ids)
            })),
        }
    }

    fn filtered_neighbours_iter<G: GraphView + 'a>(
        self,
        view: &'a G,
        layer_ids: &'a LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = VID> + 'a {
        self.filtered_edges_iter(view, layer_ids, dir)
            .map(|e| e.remote())
            .dedup()
    }
}

fn filter_edge_impl<G: GraphView>(view: &G, edge: EdgeEntryRef, layer_ids: &LayerIds) -> bool {
    (!view.internal_edge_filtered() || view.internal_filter_edge(edge, layer_ids))
        && (!view.internal_edge_layer_filtered()
            || view.edge_filter_includes_edge_layer_filter()
            || edge
                .layer_ids_iter(layer_ids)
                .any(|layer_id| view.internal_filter_edge_layer(edge, layer_id)))
        && (!view.internal_exploded_edge_filtered()
            || view.edge_filter_includes_exploded_edge_filter()
            || view.edge_layer_filter_includes_exploded_edge_filter()
            || {
                let eid = edge.edge_id();
                edge.additions_iter(layer_ids).any(|(layer_id, additions)| {
                    additions.iter().any(|t| {
                        view.internal_filter_exploded_edge(eid.with_layer(layer_id), t, layer_ids)
                    })
                })
            })
}

impl<'a, T: NodeStorageOps<'a>> FilteredNodeStorageOps<'a> for T {}
