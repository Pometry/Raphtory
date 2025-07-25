use crate::{
    core::{
        entities::{LayerIds, EID, VID},
        storage::timeindex::TimeIndexEntry,
    },
    db::{
        api::view::internal::*,
        graph::{graph::Graph, views::deletion_graph::PersistentGraph},
    },
    prelude::*,
};
use raphtory_api::{iter::BoxedLDIter, GraphType};
use raphtory_storage::{graph::graph::GraphStorage, mutation::InheritMutationOps};
use serde::{Deserialize, Serialize};
use std::ops::Range;

#[derive(Serialize, Deserialize, Clone)]
pub enum MaterializedGraph {
    EventGraph(Graph),
    PersistentGraph(PersistentGraph),
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            MaterializedGraph::EventGraph($pattern) => $result,
            MaterializedGraph::PersistentGraph($pattern) => $result,
        }
    };
}

impl From<Graph> for MaterializedGraph {
    fn from(value: Graph) -> Self {
        MaterializedGraph::EventGraph(value)
    }
}

impl From<PersistentGraph> for MaterializedGraph {
    fn from(value: PersistentGraph) -> Self {
        MaterializedGraph::PersistentGraph(value)
    }
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

impl InheritPropertiesOps for MaterializedGraph {}

impl InheritNodeFilterOps for MaterializedGraph {}
impl InheritAllEdgeFilterOps for MaterializedGraph {}

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
        for_all!(self, g => builder.field(g)).finish()
    }
}

impl Static for MaterializedGraph {}

impl MaterializedGraph {
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
        for_all!(self, g => g.get_storage())
    }
}

impl GraphTimeSemanticsOps for MaterializedGraph {
    fn node_time_semantics(&self) -> TimeSemantics {
        for_all!(self, g => g.node_time_semantics())
    }

    fn edge_time_semantics(&self) -> TimeSemantics {
        for_all!(self, g => g.edge_time_semantics())
    }

    fn view_start(&self) -> Option<i64> {
        for_all!(self, g => g.view_start())
    }

    fn view_end(&self) -> Option<i64> {
        for_all!(self, g => g.view_end())
    }

    fn earliest_time_global(&self) -> Option<i64> {
        for_all!(self, g => g.earliest_time_global())
    }

    fn latest_time_global(&self) -> Option<i64> {
        for_all!(self, g => g.latest_time_global())
    }

    fn earliest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        for_all!(self, g => g.earliest_time_window(start, end))
    }

    fn latest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        for_all!(self, g => g.latest_time_window(start, end))
    }

    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        for_all!(self, g => g.has_temporal_prop(prop_id))
    }

    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLDIter<(TimeIndexEntry, Prop)> {
        for_all!(self, g => g.temporal_prop_iter(prop_id))
    }

    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<i64>) -> bool {
        for_all!(self, g => g.has_temporal_prop_window(prop_id, w))
    }

    fn temporal_prop_iter_window(
        &self,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLDIter<(TimeIndexEntry, Prop)> {
        for_all!(self, g => g.temporal_prop_iter_window(prop_id, start, end))
    }

    fn temporal_prop_last_at(
        &self,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<(TimeIndexEntry, Prop)> {
        for_all!(self, g => g.temporal_prop_last_at(prop_id, t))
    }

    fn temporal_prop_last_at_window(
        &self,
        prop_id: usize,
        t: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<(TimeIndexEntry, Prop)> {
        for_all!(self, g => g.temporal_prop_last_at_window(prop_id, t, w))
    }
}

impl NodeHistoryFilter for MaterializedGraph {
    fn is_node_prop_update_available(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
    ) -> bool {
        for_all!(self, g => g.is_node_prop_update_available(prop_id, node_id, time))
    }

    fn is_node_prop_update_available_window(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        for_all!(self, g => g.is_node_prop_update_available_window(prop_id, node_id, time, w))
    }

    fn is_node_prop_update_latest(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
    ) -> bool {
        for_all!(self, g =>g.is_node_prop_update_latest(prop_id, node_id, time))
    }

    fn is_node_prop_update_latest_window(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        for_all!(self, g => g.is_node_prop_update_latest_window(prop_id, node_id, time, w))
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
        for_all!(self, g => g.is_edge_prop_update_available(layer_id, prop_id, edge_id, time))
    }

    fn is_edge_prop_update_available_window(
        &self,
        layer_id: usize,
        prop_id: usize,
        edge_id: EID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        for_all!(self, g => g.is_edge_prop_update_available_window(layer_id, prop_id, edge_id, time, w))
    }

    fn is_edge_prop_update_latest(
        &self,
        layer_ids: &LayerIds,
        layer_id: usize,
        prop_id: usize,
        edge_id: EID,
        time: TimeIndexEntry,
    ) -> bool {
        for_all!(self, g => g.is_edge_prop_update_latest(layer_ids, layer_id, prop_id, edge_id, time))
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
        for_all!(self, g => g.is_edge_prop_update_latest_window(layer_ids, layer_id, prop_id, edge_id, time, w))
    }
}

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
            GraphTimeSemanticsOps, InternalEdgeFilterOps, InternalLayerOps, MaterializedGraph,
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
        assert!(!mg.internal_edge_filtered());
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
