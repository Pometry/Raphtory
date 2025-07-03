use crate::{
    db::api::view::internal::{
        EdgeTimeSemanticsOps, GraphTimeSemanticsOps, GraphView, InternalEdgeFilterOps,
        InternalEdgeLayerFilterOps, InternalExplodedEdgeFilterOps, InternalNodeFilterOps,
        NodeTimeSemanticsOps,
    },
    prelude::LayerOps,
};
use iter_enum::{
    DoubleEndedIterator, ExactSizeIterator, FusedIterator, IndexedParallelIterator, Iterator,
    ParallelIterator,
};
use raphtory_api::core::{
    entities::ELID,
    storage::timeindex::{TimeIndexEntry, TimeIndexOps},
};
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::{
        edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
        nodes::node_ref::NodeStorageRef,
    },
    layer_ops::InternalLayerOps,
};

pub enum FilterState {
    Neither,
    Both,
    BothIndependent,
    Nodes,
    Edges,
}

#[derive(
    Iterator,
    DoubleEndedIterator,
    ExactSizeIterator,
    FusedIterator,
    ParallelIterator,
    IndexedParallelIterator,
)]
pub enum FilterVariants<Neither, Nodes, Edges, Both> {
    Neither(Neither),
    Nodes(Nodes),
    Edges(Edges),
    Both(Both),
}

pub trait FilterOps {
    fn filter_node(&self, node: NodeStorageRef) -> bool;
    fn filter_state(&self) -> FilterState;
    fn node_and_edge_filters_independent(&self) -> bool;

    fn filtered(&self) -> bool;

    fn node_list_trusted(&self) -> bool;

    fn filter_edge(&self, edge: EdgeStorageRef) -> bool;

    fn filter_edge_layer(&self, edge: EdgeStorageRef, layer: usize) -> bool;

    fn filter_exploded_edge(&self, eid: ELID, t: TimeIndexEntry) -> bool;

    fn edge_list_trusted(&self) -> bool;

    fn exploded_filter_independent(&self) -> bool;
    fn filter_edge_from_nodes(&self, edge: EdgeStorageRef) -> bool;
}

/// Implements all the filtering except for time semantics as it is used to define the time semantics
pub trait InnerFilterOps {
    fn filter_node_inner(&self, node: NodeStorageRef) -> bool;

    fn filtered_inner(&self) -> bool;

    fn filter_edge_inner(&self, edge: EdgeStorageRef) -> bool;

    /// handles edge and edge layer filter (not exploded edge filter or windows)
    fn filter_edge_layer_inner(&self, edge: EdgeStorageRef, layer: usize) -> bool;

    fn filter_exploded_edge_inner(&self, eid: ELID, t: TimeIndexEntry) -> bool;
}

impl<G: GraphView> InnerFilterOps for G {
    fn filter_node_inner(&self, node: NodeStorageRef) -> bool {
        self.internal_filter_node(node, self.layer_ids())
    }

    fn filtered_inner(&self) -> bool {
        self.internal_nodes_filtered()
            || self.internal_edge_filtered()
            || self.internal_edge_layer_filtered()
            || self.internal_exploded_edge_filtered()
    }

    fn filter_edge_inner(&self, edge: EdgeStorageRef) -> bool {
        self.internal_filter_edge(edge, self.layer_ids())
            && (self.edge_filter_includes_edge_layer_filter()
                || edge
                    .layer_ids_iter(self.layer_ids())
                    .any(|layer_id| self.internal_filter_edge_layer(edge, layer_id)))
            && filter_edge_from_exploded_filter(self, edge)
            && self.filter_edge_from_nodes(edge)
    }

    fn filter_edge_layer_inner(&self, edge: EdgeStorageRef, layer: usize) -> bool {
        self.layer_ids().contains(&layer)
            && self.internal_filter_edge_layer(edge, layer)
            && (self.edge_layer_filter_includes_edge_filter()
                || self.internal_filter_edge(edge, self.layer_ids()))
            && filter_edge_from_exploded_filter(self, edge)
            && self.filter_edge_from_nodes(edge)
    }

    fn filter_exploded_edge_inner(&self, eid: ELID, t: TimeIndexEntry) -> bool {
        self.layer_ids().contains(&eid.layer())
            && self.internal_filter_exploded_edge(eid, t, self.layer_ids())
            && (self.exploded_filter_independent() || {
                let edge = self.core_edge(eid.edge);
                (self.exploded_edge_filter_includes_edge_layer_filter()
                    || self.internal_filter_edge_layer(edge.as_ref(), eid.layer()))
                    && (self.exploded_edge_filter_includes_edge_filter()
                        || self.internal_filter_edge(edge.as_ref(), self.layer_ids()))
                    && self.filter_edge_from_nodes(edge.as_ref())
            })
    }
}

impl<G: GraphView> FilterOps for G {
    #[inline]
    fn filter_node(&self, node: NodeStorageRef) -> bool {
        if self.filtered() {
            let time_semantics = self.node_time_semantics();
            self.internal_filter_node(node, self.layer_ids())
                && time_semantics.node_valid(node, self)
        } else {
            true
        }
    }

    #[inline]
    fn filter_state(&self) -> FilterState {
        match (
            self.internal_nodes_filtered(),
            self.internal_edge_filtered()
                || self.internal_edge_layer_filtered()
                || self.internal_exploded_edge_filtered(),
        ) {
            (false, false) => FilterState::Neither,
            (true, false) => FilterState::Nodes,
            (false, true) => FilterState::Edges,
            (true, true) => {
                if self.node_and_edge_filters_independent() {
                    FilterState::BothIndependent
                } else {
                    FilterState::Both
                }
            }
        }
    }

    fn node_and_edge_filters_independent(&self) -> bool {
        self.edge_filter_includes_node_filter()
            && self.edge_layer_filter_includes_node_filter()
            && self.exploded_edge_filter_includes_node_filter()
            && self.node_filter_includes_edge_filter()
            && self.node_filter_includes_edge_layer_filter()
            && self.node_filter_includes_exploded_edge_filter()
    }

    #[inline]
    fn filtered(&self) -> bool {
        self.internal_nodes_filtered()
            || self.internal_edge_filtered()
            || self.internal_edge_layer_filtered()
            || self.internal_exploded_edge_filtered()
    }

    #[inline]
    fn node_list_trusted(&self) -> bool {
        self.internal_node_list_trusted()
            && self.node_filter_includes_edge_filter()
            && self.node_filter_includes_edge_layer_filter()
            && self.node_filter_includes_exploded_edge_filter()
    }

    fn filter_edge(&self, edge: EdgeStorageRef) -> bool {
        self.internal_filter_edge(edge, self.layer_ids()) && self.filter_edge_from_nodes(edge) && {
            let time_semantics = self.edge_time_semantics();
            edge.layer_ids_iter(self.layer_ids()).any(|layer_id| {
                self.internal_filter_edge_layer(edge, layer_id)
                    && time_semantics.include_edge(edge, self, layer_id)
            })
        }
    }

    fn filter_edge_layer(&self, edge: EdgeStorageRef, layer: usize) -> bool {
        self.internal_filter_edge_layer(edge, layer)
            && (self.edge_layer_filter_includes_edge_filter()
                || self.internal_filter_edge(edge, self.layer_ids()))
            && self.filter_edge_from_nodes(edge)
            && self.edge_time_semantics().include_edge(edge, self, layer)
    }

    fn filter_exploded_edge(&self, eid: ELID, t: TimeIndexEntry) -> bool {
        self.edge_time_semantics()
            .include_exploded_edge(eid, t, self)
    }

    fn edge_list_trusted(&self) -> bool {
        self.internal_edge_list_trusted() && self.node_and_edge_filters_independent()
    }

    fn exploded_filter_independent(&self) -> bool {
        self.exploded_edge_filter_includes_node_filter()
            && self.exploded_edge_filter_includes_edge_filter()
            && self.exploded_edge_filter_includes_edge_layer_filter()
    }

    fn filter_edge_from_nodes(&self, edge: EdgeStorageRef) -> bool {
        self.exploded_edge_filter_includes_node_filter()
            || self.edge_layer_filter_includes_node_filter()
            || self.edge_filter_includes_node_filter()
            || (self.internal_filter_node(self.core_node(edge.src()).as_ref(), self.layer_ids())
                && self.internal_filter_node(self.core_node(edge.dst()).as_ref(), self.layer_ids()))
    }
}

fn filter_edge_from_exploded_filter<G: GraphView>(view: &G, edge: EdgeStorageRef) -> bool {
    view.edge_filter_includes_exploded_edge_filter()
        || view.edge_layer_filter_includes_exploded_edge_filter()
        || {
            let time_semantics = view.edge_time_semantics();
            let eid = edge.eid();
            edge.additions_iter(view.layer_ids())
                .any(|(layer, additions)| {
                    let elid = eid.with_layer(layer);
                    additions.iter().any(|t| {
                        time_semantics
                            .handle_edge_update_filter(t, elid, view)
                            .is_some()
                    })
                })
                || edge
                    .deletions_iter(view.layer_ids())
                    .any(|(layer, deletions)| {
                        let elid = eid.with_layer_deletion(layer);
                        deletions.iter().any(|t| {
                            time_semantics
                                .handle_edge_update_filter(t, elid, view)
                                .is_some()
                        })
                    })
        }
}
