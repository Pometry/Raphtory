use crate::{
    core::{
        entities::{
            nodes::node_ref::{AsNodeRef, NodeRef},
            properties::{graph_meta::GraphMeta, props::Meta, tprop::TProp},
            LayerIds, EID, GID, VID,
        },
        storage::{
            locked_view::LockedView, raw_edges::WriteLockedEdges, timeindex::TimeIndexEntry,
            WriteLockedNodes,
        },
        utils::errors::{GraphError, GraphError::EventGraphDeletionsNotSupported},
        PropType,
    },
    db::{
        api::{
            mutation::internal::{
                InternalAdditionOps, InternalDeletionOps, InternalPropertyAdditionOps,
            },
            properties::internal::{
                ConstPropertiesOps, TemporalPropertiesOps, TemporalPropertyViewOps,
            },
            storage::graph::{
                edges::{
                    edge_entry::EdgeStorageEntry, edge_ref::EdgeStorageRef, edges::EdgesStorage,
                },
                locked::WriteLockedGraph,
                nodes::{node_entry::NodeStorageEntry, nodes::NodesStorage},
                storage_ops::GraphStorage,
            },
            view::{internal::*, BoxedIter, BoxedLIter},
        },
        graph::{graph::Graph, views::deletion_graph::PersistentGraph},
    },
    prelude::*,
};
use chrono::{DateTime, Utc};
use enum_dispatch::enum_dispatch;
use raphtory_api::{
    core::{
        entities::GidType,
        storage::{arc_str::ArcStr, dict_mapper::MaybeNew},
    },
    GraphType,
};
use serde::{Deserialize, Serialize};
use std::ops::Range;

#[enum_dispatch(CoreGraphOps)]
#[enum_dispatch(InternalLayerOps)]
#[enum_dispatch(ListOps)]
#[enum_dispatch(TimeSemantics)]
#[enum_dispatch(EdgeFilterOps)]
#[enum_dispatch(NodeFilterOps)]
#[enum_dispatch(InternalMaterialize)]
#[enum_dispatch(TemporalPropertiesOps)]
#[enum_dispatch(TemporalPropertyViewOps)]
#[enum_dispatch(ConstPropertiesOps)]
#[enum_dispatch(InternalAdditionOps)]
#[enum_dispatch(InternalPropertyAdditionOps)]
#[derive(Serialize, Deserialize, Clone)]
pub enum MaterializedGraph {
    EventGraph(Graph),
    PersistentGraph(PersistentGraph),
}

impl Static for MaterializedGraph {}

impl MaterializedGraph {
    pub fn storage(&self) -> &GraphStorage {
        match self {
            MaterializedGraph::EventGraph(g) => g.core_graph(),
            MaterializedGraph::PersistentGraph(g) => g.core_graph(),
        }
    }

    pub fn into_events(self) -> Option<Graph> {
        match self {
            MaterializedGraph::EventGraph(g) => Some(g),
            MaterializedGraph::PersistentGraph(_) => None,
        }
    }
    pub fn into_persistent(self) -> Option<PersistentGraph> {
        match self {
            MaterializedGraph::EventGraph(_) => None,
            MaterializedGraph::PersistentGraph(g) => Some(g),
        }
    }
}

impl InternalStorageOps for MaterializedGraph {
    fn get_storage(&self) -> Option<&Storage> {
        match self {
            MaterializedGraph::EventGraph(g) => g.get_storage(),
            MaterializedGraph::PersistentGraph(g) => g.get_storage(),
        }
    }
}

impl InternalDeletionOps for MaterializedGraph {
    fn internal_delete_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<MaybeNew<EID>, GraphError> {
        match self {
            MaterializedGraph::EventGraph(_) => Err(EventGraphDeletionsNotSupported),
            MaterializedGraph::PersistentGraph(g) => g.internal_delete_edge(t, src, dst, layer),
        }
    }

    fn internal_delete_existing_edge(
        &self,
        t: TimeIndexEntry,
        eid: EID,
        layer: usize,
    ) -> Result<(), GraphError> {
        match self {
            MaterializedGraph::EventGraph(_) => Err(EventGraphDeletionsNotSupported),
            MaterializedGraph::PersistentGraph(g) => g.internal_delete_existing_edge(t, eid, layer),
        }
    }
}

impl DeletionOps for MaterializedGraph {}

impl NodeHistoryFilter for MaterializedGraph {
    fn is_node_prop_update_available(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
    ) -> bool {
        match self {
            MaterializedGraph::EventGraph(g) => {
                g.is_node_prop_update_available(prop_id, node_id, time)
            }
            MaterializedGraph::PersistentGraph(g) => {
                g.is_node_prop_update_available(prop_id, node_id, time)
            }
        }
    }

    fn is_node_prop_update_available_window(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        match self {
            MaterializedGraph::EventGraph(g) => {
                g.is_node_prop_update_available_window(prop_id, node_id, time, w)
            }
            MaterializedGraph::PersistentGraph(g) => {
                g.is_node_prop_update_available_window(prop_id, node_id, time, w)
            }
        }
    }

    fn is_node_prop_update_latest(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
    ) -> bool {
        match self {
            MaterializedGraph::EventGraph(g) => {
                g.is_node_prop_update_latest(prop_id, node_id, time)
            }
            MaterializedGraph::PersistentGraph(g) => {
                g.is_node_prop_update_latest(prop_id, node_id, time)
            }
        }
    }

    fn is_node_prop_update_latest_window(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        match self {
            MaterializedGraph::EventGraph(g) => {
                g.is_node_prop_update_latest_window(prop_id, node_id, time, w)
            }
            MaterializedGraph::PersistentGraph(g) => {
                g.is_node_prop_update_latest_window(prop_id, node_id, time, w)
            }
        }
    }
}

impl EdgeHistoryFilter for MaterializedGraph {
    fn is_edge_prop_update_available(
        &self,
        layer_id: usize,
        prop_id: usize,
        edge_id: EID,
        time: TimeIndexEntry,
    ) -> bool {
        match self {
            MaterializedGraph::EventGraph(g) => {
                g.is_edge_prop_update_available(layer_id, prop_id, edge_id, time)
            }
            MaterializedGraph::PersistentGraph(g) => {
                g.is_edge_prop_update_available(layer_id, prop_id, edge_id, time)
            }
        }
    }

    fn is_edge_prop_update_available_window(
        &self,
        layer_id: usize,
        prop_id: usize,
        edge_id: EID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        match self {
            MaterializedGraph::EventGraph(g) => {
                g.is_edge_prop_update_available_window(layer_id, prop_id, edge_id, time, w)
            }
            MaterializedGraph::PersistentGraph(g) => {
                g.is_edge_prop_update_available_window(layer_id, prop_id, edge_id, time, w)
            }
        }
    }

    fn is_edge_prop_update_latest(
        &self,
        layer_ids: &LayerIds,
        layer_id: usize,
        prop_id: usize,
        edge_id: EID,
        time: TimeIndexEntry,
    ) -> bool {
        match self {
            MaterializedGraph::EventGraph(g) => {
                g.is_edge_prop_update_latest(layer_ids, layer_id, prop_id, edge_id, time)
            }
            MaterializedGraph::PersistentGraph(g) => {
                g.is_edge_prop_update_latest(layer_ids, layer_id, prop_id, edge_id, time)
            }
        }
    }

    fn is_edge_prop_update_latest_window(
        &self,
        layer_ids: &LayerIds,
        layer_id: usize,
        prop_id: usize,
        edge_id: EID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        match self {
            MaterializedGraph::EventGraph(g) => {
                g.is_edge_prop_update_latest_window(layer_ids, layer_id, prop_id, edge_id, time, w)
            }
            MaterializedGraph::PersistentGraph(g) => {
                g.is_edge_prop_update_latest_window(layer_ids, layer_id, prop_id, edge_id, time, w)
            }
        }
    }
}

#[enum_dispatch]
pub trait InternalMaterialize {
    fn new_base_graph(&self, graph: GraphStorage) -> MaterializedGraph {
        match self.graph_type() {
            GraphType::EventGraph => {
                MaterializedGraph::EventGraph(Graph::from_internal_graph(graph))
            }
            GraphType::PersistentGraph => {
                MaterializedGraph::PersistentGraph(PersistentGraph::from_internal_graph(graph))
            }
        }
    }

    fn graph_type(&self) -> GraphType;

    fn include_deletions(&self) -> bool;
}

pub trait InheritMaterialize: Base {}

impl<G: InheritMaterialize> InternalMaterialize for G
where
    G::Base: InternalMaterialize,
{
    #[inline]
    fn new_base_graph(&self, graph: GraphStorage) -> MaterializedGraph {
        self.base().new_base_graph(graph)
    }

    #[inline]
    fn graph_type(&self) -> GraphType {
        self.base().graph_type()
    }

    #[inline]
    fn include_deletions(&self) -> bool {
        self.base().include_deletions()
    }
}

#[cfg(test)]
mod test_materialised_graph_dispatch {
    use raphtory_api::core::entities::GID;

    use crate::{
        core::entities::LayerIds,
        db::api::view::internal::{
            CoreGraphOps, EdgeFilterOps, InternalLayerOps, InternalMaterialize, MaterializedGraph,
            TimeSemantics,
        },
        prelude::*,
    };

    #[test]
    fn materialised_graph_has_core_ops() {
        let mg = MaterializedGraph::from(Graph::new());
        assert_eq!(mg.unfiltered_num_nodes(), 0);
    }

    #[test]
    fn materialised_graph_has_graph_ops() {
        let mg = MaterializedGraph::from(Graph::new());
        assert_eq!(mg.count_nodes(), 0);
    }

    #[test]
    fn materialised_graph_has_edge_filter_ops() {
        let mg = MaterializedGraph::from(Graph::new());
        assert!(!mg.edges_filtered());
    }

    #[test]
    fn materialised_graph_has_layer_ops() {
        let mg = MaterializedGraph::from(Graph::new());
        assert!(matches!(mg.layer_ids(), LayerIds::All));
    }

    #[test]
    fn materialised_graph_has_time_semantics() {
        let mg = MaterializedGraph::from(Graph::new());
        assert!(mg.view_start().is_none());
    }

    #[test]
    fn materialised_graph_has_internal_materialise() {
        let mg = MaterializedGraph::from(Graph::new());
        assert!(!mg.include_deletions());
    }

    #[test]
    fn materialised_graph_can_be_used_directly() {
        let g = Graph::new();

        let mg = g.materialize().unwrap();

        let v = mg.add_node(0, 1, NO_PROPS, None).unwrap();
        assert_eq!(v.id(), GID::U64(1))
    }
}
