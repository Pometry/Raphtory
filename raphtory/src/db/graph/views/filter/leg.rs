//! How a combinator graph asks one of its legs about an entity. A leg is a
//! view over the combinator's graph and keeps an entity when its public check
//! does: its filter hooks, its layers and its time semantics together. A
//! window or layer view keeps its restriction out of its hooks, so asking the
//! hooks alone would let everything outside the window or the layers through.
//!
//! At each level a leg that cannot drop anything keeps everything, so a
//! combinator only asks the legs that restrict that level.

use crate::db::api::view::internal::{FilterOps, GraphView, InnerFilterOps};
use either::Either;
use raphtory_api::core::{
    entities::{LayerId, ELID},
    storage::timeindex::EventTime,
};
use raphtory_storage::graph::{edges::edge_ref::EdgeEntryRef, nodes::node_ref::NodeStorageRef};

/// Whether `leg` restricts by time or by layer, which none of its hooks
/// reports. Its node and edge lists are not trusted then, since they are the
/// lists of the graph below the window or the layers.
pub(super) fn restricts_view<L: GraphView>(leg: &L) -> bool {
    leg.window_filtered() || leg.is_layer_filtered()
}

/// Whether `leg` can drop a node.
pub(super) fn restricts_nodes<L: GraphView>(leg: &L) -> bool {
    leg.internal_nodes_filtered() || restricts_view(leg)
}

/// Whether `leg` can drop an edge.
pub(super) fn restricts_edges<L: GraphView>(leg: &L) -> bool {
    leg.internal_edge_filtered() || restricts_view(leg)
}

/// Whether `leg` can drop a layer of an edge it keeps.
pub(super) fn restricts_edge_layers<L: GraphView>(leg: &L) -> bool {
    leg.internal_edge_layer_filtered() || restricts_view(leg)
}

/// Whether `leg` can drop an update of an edge it keeps.
pub(super) fn restricts_exploded_edges<L: GraphView>(leg: &L) -> bool {
    leg.internal_exploded_edge_filtered() || restricts_view(leg)
}

/// Whether `leg` keeps `node`.
pub(super) fn keeps_node<L: GraphView>(leg: &L, node: NodeStorageRef) -> bool {
    !restricts_nodes(leg) || leg.filter_node(node)
}

/// Whether `leg` keeps `edge`.
pub(super) fn keeps_edge<L: GraphView>(leg: &L, edge: EdgeEntryRef) -> bool {
    !restricts_edges(leg) || leg.filter_edge(edge)
}

/// Whether `leg` keeps `layer` of `edge`. A layer belongs to a leg through
/// the edge, so a leg that drops the edge drops its layers too.
pub(super) fn keeps_edge_layer<L: GraphView>(leg: &L, edge: EdgeEntryRef, layer: LayerId) -> bool {
    keeps_edge(leg, edge)
        && (!restricts_edge_layers(leg)
            || (leg.layer_ids().contains(&layer) && leg.filter_edge_layer(edge, layer)))
}

/// Whether `leg` keeps the update `eid` of an edge at `t`. An update belongs
/// to a leg through the edge, so a leg that drops the edge drops its updates
/// too.
pub(super) fn keeps_exploded_edge<L: GraphView>(leg: &L, eid: ELID, t: EventTime) -> bool {
    (!restricts_edges(leg) || leg.filter_edge(leg.core_edge(Either::Left(eid.eid())).as_ref()))
        && (!restricts_exploded_edges(leg) || leg.filter_exploded_edge(eid, t))
}
