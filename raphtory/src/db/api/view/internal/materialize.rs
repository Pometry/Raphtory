use crate::{
    api::core::storage::arc_str::ArcStr,
    core::{
        entities::{LayerIds, EID, VID},
        storage::timeindex::TimeIndexEntry,
    },
    db::{
        api::{
            properties::internal::{
                ConstantPropertiesOps, TemporalPropertiesOps, TemporalPropertyViewOps,
            },
            view::internal::*,
        },
        graph::{graph::Graph, views::deletion_graph::PersistentGraph},
    },
    prelude::*,
};
use chrono::{DateTime, Utc};
use enum_dispatch::enum_dispatch;
use raphtory_api::{core::entities::properties::prop::PropType, iter::BoxedLIter, GraphType};
use raphtory_storage::{graph::graph::GraphStorage, mutation::InheritMutationOps};
use std::ops::Range;

#[enum_dispatch(GraphTimeSemanticsOps)]
#[enum_dispatch(TemporalPropertiesOps)]
#[enum_dispatch(TemporalPropertyViewOps)]
#[enum_dispatch(ConstantPropertiesOps)]
#[derive(Clone)]
pub enum MaterializedGraph {
    EventGraph(Graph),
    PersistentGraph(PersistentGraph),
}

impl Base for MaterializedGraph {
    type Base = Arc<Storage>;

    fn base(&self) -> &Self::Base {
        match self {
            MaterializedGraph::EventGraph(g) => &g.inner,
            MaterializedGraph::PersistentGraph(g) => &g.0,
        }
    }
}

impl InheritCoreGraphOps for MaterializedGraph {}
impl InheritLayerOps for MaterializedGraph {}
impl InheritListOps for MaterializedGraph {}

impl InheritNodeFilterOps for MaterializedGraph {}
impl InheritEdgeFilterOps for MaterializedGraph {}

impl InternalMaterialize for MaterializedGraph {
    fn graph_type(&self) -> GraphType {
        match self {
            MaterializedGraph::EventGraph(_) => GraphType::EventGraph,
            MaterializedGraph::PersistentGraph(_) => GraphType::PersistentGraph,
        }
    }
}

impl InheritMutationOps for MaterializedGraph {}

impl Debug for MaterializedGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut builder = f.debug_tuple("MaterializedGraph");
        match self {
            MaterializedGraph::EventGraph(g) => builder.field(g),
            MaterializedGraph::PersistentGraph(g) => builder.field(g),
        }
        .finish()
    }
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
}

#[cfg(test)]
mod test_materialised_graph_dispatch {
    use crate::{
        core::entities::LayerIds,
        db::api::view::internal::{
            EdgeFilterOps, GraphTimeSemanticsOps, InternalLayerOps, MaterializedGraph,
        },
        prelude::*,
    };
    use raphtory_api::core::entities::GID;
    use raphtory_storage::core_ops::CoreGraphOps;

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
    fn materialised_graph_can_be_used_directly() {
        let g = Graph::new();

        let mg = g.materialize().unwrap();

        let v = mg.add_node(0, 1, NO_PROPS, None).unwrap();
        assert_eq!(v.id(), GID::U64(1))
    }
}
