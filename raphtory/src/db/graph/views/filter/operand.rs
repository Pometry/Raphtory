//! How a combinator graph asks one of its operands about an entity. An operand is a
//! view over the combinator's graph and keeps an entity when its public check
//! does: its filter hooks, its layers and its time semantics together. A
//! window or layer view keeps its restriction out of its hooks, so asking the
//! hooks alone would let everything outside the window or the layers through.
//!
//! At each level an operand that cannot drop anything keeps everything, so a
//! combinator only asks the operands that restrict that level.

use crate::db::api::view::internal::{FilterOps, GraphView, InnerFilterOps};
use either::Either;
use raphtory_api::core::{
    entities::{LayerId, ELID},
    storage::timeindex::EventTime,
};
use raphtory_storage::graph::{edges::edge_ref::EdgeEntryRef, nodes::node_ref::NodeStorageRef};

/// Whether `operand` restricts by time or by layer, which none of its hooks
/// reports. Its node and edge lists are not trusted then, since they are the
/// lists of the graph below the window or the layers.
pub(super) fn restricts_view<L: GraphView>(operand: &L) -> bool {
    operand.window_filtered() || operand.is_layer_filtered()
}

/// Whether every operand trusts its own list at a level, by `trusted`, and none
/// restricts by time or by layer, so the lists of the operands are the lists of
/// the combinator.
pub(super) fn lists_trusted<L: GraphView>(operands: &[L], trusted: impl Fn(&L) -> bool) -> bool {
    operands
        .iter()
        .all(|operand| trusted(operand) && !restricts_view(operand))
}

/// Whether `operand` can drop a node.
pub(super) fn restricts_nodes<L: GraphView>(operand: &L) -> bool {
    operand.internal_nodes_filtered() || restricts_view(operand)
}

/// Whether `operand` can drop an edge.
pub(super) fn restricts_edges<L: GraphView>(operand: &L) -> bool {
    operand.internal_edge_filtered() || restricts_view(operand)
}

/// Whether `operand` can drop a layer of an edge it keeps.
pub(super) fn restricts_edge_layers<L: GraphView>(operand: &L) -> bool {
    operand.internal_edge_layer_filtered() || restricts_view(operand)
}

/// Whether `operand` can drop an update of an edge it keeps.
pub(super) fn restricts_exploded_edges<L: GraphView>(operand: &L) -> bool {
    operand.internal_exploded_edge_filtered() || restricts_view(operand)
}

/// Whether `operand` keeps `node`.
pub(super) fn keeps_node<L: GraphView>(operand: &L, node: NodeStorageRef) -> bool {
    !restricts_nodes(operand) || operand.filter_node(node)
}

/// Whether `operand` can drop an edge, by itself or by dropping one of its ends.
pub(super) fn restricts_edges_or_ends<L: GraphView>(operand: &L) -> bool {
    restricts_edges(operand) || restricts_nodes(operand)
}

/// Whether `operand` keeps `layer` of an edge it keeps.
fn keeps_layer<L: GraphView>(operand: &L, edge: EdgeEntryRef, layer: LayerId) -> bool {
    !restricts_edge_layers(operand)
        || (operand.layer_ids().contains(&layer) && operand.filter_edge_layer(edge, layer))
}

/// Whether `operand` keeps the update `eid` of an edge it keeps at `t`.
fn keeps_update<L: GraphView>(operand: &L, eid: ELID, t: EventTime) -> bool {
    !restricts_exploded_edges(operand) || operand.filter_exploded_edge(eid, t)
}

/// Whether `operand` keeps `edge`.
pub(super) fn keeps_edge<L: GraphView>(operand: &L, edge: EdgeEntryRef) -> bool {
    !restricts_edges(operand) || operand.filter_edge(edge)
}

/// Whether `operand` keeps `edge` and both its ends. An operand that restricts only
/// nodes keeps the edges between the nodes it keeps.
pub(super) fn keeps_edge_and_ends<L: GraphView>(operand: &L, edge: EdgeEntryRef) -> bool {
    !restricts_edges_or_ends(operand) || operand.filter_edge(edge)
}

/// Whether `operand` keeps `layer` of `edge`. A layer belongs to an operand through
/// the edge, so an operand that drops the edge drops its layers too.
pub(super) fn keeps_edge_layer<L: GraphView>(
    operand: &L,
    edge: EdgeEntryRef,
    layer: LayerId,
) -> bool {
    keeps_edge(operand, edge) && keeps_layer(operand, edge, layer)
}

/// Whether `operand` keeps `layer` of `edge` and both ends of `edge`.
pub(super) fn keeps_edge_layer_and_ends<L: GraphView>(
    operand: &L,
    edge: EdgeEntryRef,
    layer: LayerId,
) -> bool {
    keeps_edge_and_ends(operand, edge) && keeps_layer(operand, edge, layer)
}

/// Whether `operand` keeps the update `eid` of an edge at `t`. An update belongs
/// to an operand through the edge, so an operand that drops the edge drops its updates
/// too.
pub(super) fn keeps_exploded_edge<L: GraphView>(operand: &L, eid: ELID, t: EventTime) -> bool {
    (!restricts_edges(operand)
        || operand.filter_edge(operand.core_edge(Either::Left(eid.eid())).as_ref()))
        && keeps_update(operand, eid, t)
}

/// Whether `operand` keeps the update `eid` of an edge at `t` and both ends of the edge.
pub(super) fn keeps_exploded_edge_and_ends<L: GraphView>(
    operand: &L,
    eid: ELID,
    t: EventTime,
) -> bool {
    (!restricts_edges_or_ends(operand)
        || operand.filter_edge(operand.core_edge(Either::Left(eid.eid())).as_ref()))
        && keeps_update(operand, eid, t)
}
