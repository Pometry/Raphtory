use crate::{
    core::{
        entities::{
            edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
            properties::{
                graph_props::GraphProps,
                props::Meta,
                tprop::{LockedLayeredTProp, TProp},
            },
            vertices::{vertex_ref::VertexRef, vertex_store::VertexStore},
            LayerIds, EID, VID,
        },
        storage::{
            locked_view::LockedView,
            timeindex::{LockedLayeredIndex, TimeIndex, TimeIndexEntry},
            ArcEntry,
        },
        utils::errors::GraphError,
        ArcStr, Direction, PropType,
    },
    db::{
        api::{
            mutation::internal::{InternalAdditionOps, InternalPropertyAdditionOps},
            properties::internal::{
                ConstPropertiesOps, TemporalPropertiesOps, TemporalPropertyViewOps,
            },
            view::{internal::*, BoxedIter, BoxedLIter},
        },
        graph::{
            graph::{Graph, InternalGraph},
            views::deletion_graph::GraphWithDeletions,
        },
    },
    prelude::{Layer, Prop},
};
use enum_dispatch::enum_dispatch;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[enum_dispatch(CoreGraphOps)]
#[enum_dispatch(GraphOpsBase)]
#[enum_dispatch(InternalLayerOps)]
#[enum_dispatch(IntoDynamic)]
#[enum_dispatch(TimeSemantics)]
#[enum_dispatch(InternalMaterialize)]
#[enum_dispatch(TemporalPropertiesOps)]
#[enum_dispatch(TemporalPropertyViewOps)]
#[enum_dispatch(ConstPropertiesOps)]
#[enum_dispatch(InternalAdditionOps)]
#[enum_dispatch(InternalPropertyAdditionOps)]
#[derive(Serialize, Deserialize, Clone)]
pub enum MaterializedGraph {
    EventGraph(Graph),
    PersistentGraph(GraphWithDeletions),
}

impl<'graph> EdgeFilterOps<'graph> for MaterializedGraph {
    fn edge_filter(&self) -> Option<&EdgeFilter<'graph>> {
        match self {
            MaterializedGraph::EventGraph(g) => g.edge_filter(),
            MaterializedGraph::PersistentGraph(g) => g.edge_filter(),
        }
    }

    fn edge_filter_window(&self) -> Option<&EdgeFilter<'graph>> {
        match self {
            MaterializedGraph::EventGraph(g) => g.edge_filter_window(),
            MaterializedGraph::PersistentGraph(g) => g.edge_filter_window(),
        }
    }
}

impl<'graph> GraphOps<'graph> for MaterializedGraph {
    fn internal_vertex_ref(
        &self,
        v: VertexRef,
        layer_ids: &LayerIds,
        filter: Option<&EdgeFilter<'graph>>,
    ) -> Option<VID> {
        todo!()
    }

    fn find_edge_id(
        &self,
        e_id: EID,
        layer_ids: &LayerIds,
        filter: Option<&EdgeFilter<'graph>>,
    ) -> Option<EdgeRef> {
        todo!()
    }

    fn vertices_len(&self, layer_ids: LayerIds, filter: Option<&EdgeFilter<'graph>>) -> usize {
        todo!()
    }

    fn edges_len(&self, layers: LayerIds, filter: Option<&EdgeFilter<'graph>>) -> usize {
        todo!()
    }

    fn temporal_edges_len(&self, layers: LayerIds, filter: Option<&EdgeFilter<'graph>>) -> usize {
        todo!()
    }

    fn degree(
        &self,
        v: VID,
        d: Direction,
        layers: &LayerIds,
        filter: Option<&EdgeFilter<'graph>>,
    ) -> usize {
        todo!()
    }

    fn edge_ref(
        &self,
        src: VID,
        dst: VID,
        layer: &LayerIds,
        filter: Option<&EdgeFilter<'graph>>,
    ) -> Option<EdgeRef> {
        todo!()
    }

    fn vertex_refs(
        &self,
        layers: LayerIds,
        filter: Option<&EdgeFilter<'graph>>,
    ) -> BoxedLIter<'graph, VID> {
        match self {
            MaterializedGraph::EventGraph(g) => g.vertex_refs(layers, filter),
            MaterializedGraph::PersistentGraph(g) => g.vertex_refs(layers, filter),
        }
    }

    fn edge_refs(
        &self,
        layers: LayerIds,
        filter: Option<&EdgeFilter<'graph>>,
    ) -> BoxedLIter<'graph, EdgeRef> {
        match self {
            MaterializedGraph::EventGraph(g) => g.edge_refs(layers, filter),
            MaterializedGraph::PersistentGraph(g) => g.edge_refs(layers, filter),
        }
    }

    fn vertex_edges(
        &self,
        v: VID,
        d: Direction,
        layer: LayerIds,
        filter: Option<&EdgeFilter<'graph>>,
    ) -> BoxedLIter<'graph, EdgeRef> {
        match self {
            MaterializedGraph::EventGraph(g) => g.vertex_edges(v, d, layer, filter),
            MaterializedGraph::PersistentGraph(g) => g.vertex_edges(v, d, layer, filter),
        }
    }

    fn neighbours(
        &self,
        v: VID,
        d: Direction,
        layers: LayerIds,
        filter: Option<&EdgeFilter<'graph>>,
    ) -> BoxedLIter<'graph, VID> {
        match self {
            MaterializedGraph::EventGraph(g) => g.neighbours(v, d, layers, filter),
            MaterializedGraph::PersistentGraph(g) => g.neighbours(v, d, layers, filter),
        }
    }
}

impl MaterializedGraph {
    pub fn into_events(self) -> Option<Graph> {
        match self {
            MaterializedGraph::EventGraph(g) => Some(g),
            MaterializedGraph::PersistentGraph(_) => None,
        }
    }
    pub fn into_persistent(self) -> Option<GraphWithDeletions> {
        match self {
            MaterializedGraph::EventGraph(_) => None,
            MaterializedGraph::PersistentGraph(g) => Some(g),
        }
    }

    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, GraphError> {
        let f = std::fs::File::open(path)?;
        let mut reader = std::io::BufReader::new(f);
        Ok(bincode::deserialize_from(&mut reader)?)
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), GraphError> {
        let f = std::fs::File::create(path)?;
        let mut writer = std::io::BufWriter::new(f);
        Ok(bincode::serialize_into(&mut writer, self)?)
    }

    pub fn bincode(&self) -> Result<Vec<u8>, GraphError> {
        let encoded = bincode::serialize(self)?;
        Ok(encoded)
    }

    pub fn from_bincode(b: &[u8]) -> Result<Self, GraphError> {
        let g = bincode::deserialize(b)?;
        Ok(g)
    }
}

#[enum_dispatch]
pub trait InternalMaterialize {
    fn new_base_graph(&self, graph: InternalGraph) -> MaterializedGraph;

    fn include_deletions(&self) -> bool;
}

pub trait InheritMaterialize: Base {}

impl<G: InheritMaterialize> InternalMaterialize for G
where
    G::Base: InternalMaterialize,
{
    fn new_base_graph(&self, graph: InternalGraph) -> MaterializedGraph {
        self.base().new_base_graph(graph)
    }

    fn include_deletions(&self) -> bool {
        self.base().include_deletions()
    }
}

#[cfg(test)]
mod test_materialised_graph_dispatch {
    use crate::{
        core::entities::LayerIds,
        db::api::view::internal::{
            CoreGraphOps, EdgeFilterOps, GraphOps, GraphOpsBase, InternalLayerOps,
            InternalMaterialize, MaterializedGraph, TimeSemantics,
        },
        prelude::*,
    };

    #[test]
    fn materialised_graph_has_core_ops() {
        let mg = MaterializedGraph::from(Graph::new());
        assert_eq!(mg.unfiltered_num_vertices(), 0);
    }

    #[test]
    fn materialised_graph_has_graph_ops() {
        let mg = MaterializedGraph::from(Graph::new());
        assert_eq!(mg.vertices_len(mg.layer_ids(), mg.edge_filter()), 0);
    }
    #[test]
    fn materialised_graph_has_edge_filter_ops() {
        let mg = MaterializedGraph::from(Graph::new());
        assert!(mg.edge_filter().is_none());
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

        let v = mg.add_vertex(0, 1, NO_PROPS).unwrap();
        assert_eq!(v.id(), 1)
    }
}
