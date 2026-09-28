//! Reads on one edge, given its storage entry and which part of the edge the
//! question is about: the edge as a whole, the edge in one layer, or one
//! exploded instance. `EdgeView` and the compiled edge ops share them, so both
//! answer the same way and neither looks the entry up a second time.

use crate::{
    core::{entities::LayerIds, utils::iter::GenLockedIter},
    db::{
        api::view::{
            internal::{EdgeTimeSemanticsOps, GraphView},
            BoxedLIter, IntoDynBoxed,
        },
        graph::views::layer_graph::LayeredGraph,
    },
};
use raphtory_api::core::{
    entities::{edges::edge_ref::EdgeRef, properties::prop::Prop, LayerId},
    storage::timeindex::EventTime,
};
use std::iter;
use storage::EdgeEntryRef;

/// Which part of an edge a read is about.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum EdgeAt {
    Whole,
    Layer(LayerId),
    Exploded(LayerId, EventTime),
}

impl EdgeAt {
    /// The part an edge reference points at: its layer and time stamps, if any.
    pub(crate) fn of(edge: EdgeRef) -> Self {
        match (edge.layer(), edge.time()) {
            (Some(layer), Some(t)) => EdgeAt::Exploded(layer, t),
            (Some(layer), None) => EdgeAt::Layer(layer),
            (None, _) => EdgeAt::Whole,
        }
    }

    /// A read through a layer the view does not show has no answer.
    fn visible<G: GraphView>(self, graph: &G) -> bool {
        match self {
            EdgeAt::Whole => true,
            EdgeAt::Layer(layer) | EdgeAt::Exploded(layer, _) => graph.layer_ids().contains(&layer),
        }
    }
}

/// The latest value of temporal property `id`.
pub(crate) fn temporal_value<G: GraphView>(
    graph: &G,
    edge: EdgeEntryRef,
    at: EdgeAt,
    id: usize,
) -> Option<Prop> {
    if !at.visible(graph) {
        return None;
    }
    let time_semantics = graph.edge_time_semantics();
    match at {
        EdgeAt::Whole => time_semantics.temporal_edge_prop_last(edge, graph, id),
        EdgeAt::Layer(layer) => time_semantics.temporal_edge_prop_last(
            edge,
            LayeredGraph::new(graph, LayerIds::One(layer)),
            id,
        ),
        EdgeAt::Exploded(layer, t) => {
            time_semantics.temporal_edge_prop_exploded(edge, graph, id, t, layer)
        }
    }
}

/// The history of temporal property `id`, oldest first.
pub(crate) fn temporal_hist<'a, G: GraphView>(
    graph: &'a G,
    edge: EdgeEntryRef<'a>,
    at: EdgeAt,
    id: usize,
) -> BoxedLIter<'a, (EventTime, Prop)> {
    if !at.visible(graph) {
        return iter::empty().into_dyn_boxed();
    }
    let time_semantics = graph.edge_time_semantics();
    match at {
        EdgeAt::Whole => time_semantics
            .temporal_edge_prop_hist(edge, graph, graph.layer_ids(), id)
            .map(|(t, _, v)| (t, v))
            .into_dyn_boxed(),
        EdgeAt::Layer(layer) => {
            GenLockedIter::from((edge, LayerIds::One(layer)), move |(edge, layer_ids)| {
                time_semantics
                    .temporal_edge_prop_hist(*edge, graph, layer_ids, id)
                    .map(|(t, _, v)| (t, v))
                    .into_dyn_boxed()
            })
            .into_dyn_boxed()
        }
        EdgeAt::Exploded(layer, t) => time_semantics
            .temporal_edge_prop_exploded(edge, graph, id, t, layer)
            .map(|v| (t, v))
            .into_iter()
            .into_dyn_boxed(),
    }
}

/// The history of temporal property `id`, newest first.
pub(crate) fn temporal_hist_rev<'a, G: GraphView>(
    graph: &'a G,
    edge: EdgeEntryRef<'a>,
    at: EdgeAt,
    id: usize,
) -> BoxedLIter<'a, (EventTime, Prop)> {
    if !at.visible(graph) {
        return iter::empty().into_dyn_boxed();
    }
    let time_semantics = graph.edge_time_semantics();
    match at {
        EdgeAt::Whole => time_semantics
            .temporal_edge_prop_hist_rev(edge, graph, graph.layer_ids(), id)
            .map(|(t, _, v)| (t, v))
            .into_dyn_boxed(),
        EdgeAt::Layer(layer) => {
            GenLockedIter::from((edge, LayerIds::One(layer)), move |(edge, layer_ids)| {
                time_semantics
                    .temporal_edge_prop_hist_rev(*edge, graph, layer_ids, id)
                    .map(|(t, _, v)| (t, v))
                    .into_dyn_boxed()
            })
            .into_dyn_boxed()
        }
        EdgeAt::Exploded(layer, t) => time_semantics
            .temporal_edge_prop_exploded(edge, graph, id, t, layer)
            .map(|v| (t, v))
            .into_iter()
            .into_dyn_boxed(),
    }
}

/// The value of metadata entry `id`; an exploded instance shares its layer's.
pub(crate) fn metadata<G: GraphView>(
    graph: &G,
    edge: EdgeEntryRef,
    at: EdgeAt,
    id: usize,
) -> Option<Prop> {
    if !at.visible(graph) {
        return None;
    }
    let time_semantics = graph.edge_time_semantics();
    match at {
        EdgeAt::Whole => time_semantics.edge_metadata(edge, graph, id),
        EdgeAt::Layer(layer) | EdgeAt::Exploded(layer, _) => {
            time_semantics.edge_metadata(edge, LayeredGraph::new(graph, LayerIds::One(layer)), id)
        }
    }
}

pub(crate) fn is_active<G: GraphView>(graph: &G, edge: EdgeEntryRef, at: EdgeAt) -> bool {
    if !at.visible(graph) {
        return false;
    }
    let time_semantics = graph.edge_time_semantics();
    match at {
        EdgeAt::Whole => time_semantics.edge_is_active(edge, graph),
        EdgeAt::Layer(layer) => {
            time_semantics.edge_is_active(edge, LayeredGraph::new(graph, LayerIds::One(layer)))
        }
        EdgeAt::Exploded(layer, t) => time_semantics.edge_is_active_exploded(edge, graph, t, layer),
    }
}

pub(crate) fn is_valid<G: GraphView>(graph: &G, edge: EdgeEntryRef, at: EdgeAt) -> bool {
    if !at.visible(graph) {
        return false;
    }
    let time_semantics = graph.edge_time_semantics();
    match at {
        EdgeAt::Whole => time_semantics.edge_is_valid(edge, graph),
        EdgeAt::Layer(layer) => {
            time_semantics.edge_is_valid(edge, LayeredGraph::new(graph, LayerIds::One(layer)))
        }
        EdgeAt::Exploded(layer, t) => time_semantics.edge_is_valid_exploded(edge, graph, t, layer),
    }
}

pub(crate) fn is_deleted<G: GraphView>(graph: &G, edge: EdgeEntryRef, at: EdgeAt) -> bool {
    if !at.visible(graph) {
        return false;
    }
    let time_semantics = graph.edge_time_semantics();
    match at {
        EdgeAt::Whole => time_semantics.edge_is_deleted(edge, graph),
        EdgeAt::Layer(layer) => {
            time_semantics.edge_is_deleted(edge, LayeredGraph::new(graph, LayerIds::One(layer)))
        }
        EdgeAt::Exploded(layer, t) => {
            time_semantics.edge_is_deleted_exploded(edge, graph, t, layer)
        }
    }
}
