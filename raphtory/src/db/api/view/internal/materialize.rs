use crate::{
    core::{
        entities::{
            edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
            nodes::{node_ref::NodeRef, node_store::NodeStore},
            properties::{
                graph_meta::GraphMeta,
                props::Meta,
                tprop::{LockedLayeredTProp, TProp},
            },
            LayerIds, EID, VID,
        },
        storage::{
            locked_view::LockedView,
            timeindex::{LockedLayeredIndex, TimeIndex, TimeIndexEntry},
            ArcEntry, ReadLockedStorage,
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
    BINCODE_VERSION,
};
use chrono::{DateTime, Utc};
use enum_dispatch::enum_dispatch;
use serde::{de::Error, Deserialize, Deserializer, Serialize};
use std::path::Path;

#[enum_dispatch(CoreGraphOps)]
#[enum_dispatch(InternalLayerOps)]
#[enum_dispatch(TimeSemantics)]
#[enum_dispatch(EdgeFilterOps)]
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

fn version_deserialize<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let version = u32::deserialize(deserializer)?;
    if version != BINCODE_VERSION {
        return Err(D::Error::custom(GraphError::BincodeVersionError(
            version,
            BINCODE_VERSION,
        )));
    };
    Ok(version)
}

#[derive(Serialize, Deserialize)]
struct VersionedGraph<T> {
    #[serde(deserialize_with = "version_deserialize")]
    version: u32,
    graph: T,
}

impl Static for MaterializedGraph {}

impl<'graph> GraphOps<'graph> for MaterializedGraph {
    fn internal_node_ref(
        &self,
        v: NodeRef,
        layer_ids: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Option<VID> {
        match self {
            MaterializedGraph::EventGraph(g) => g.internal_node_ref(v, layer_ids, filter),
            MaterializedGraph::PersistentGraph(g) => g.internal_node_ref(v, layer_ids, filter),
        }
    }

    fn find_edge_id(
        &self,
        e_id: EID,
        layer_ids: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Option<EdgeRef> {
        match self {
            MaterializedGraph::EventGraph(g) => g.find_edge_id(e_id, layer_ids, filter),
            MaterializedGraph::PersistentGraph(g) => g.find_edge_id(e_id, layer_ids, filter),
        }
    }

    fn nodes_len(&self, layer_ids: LayerIds, filter: Option<&EdgeFilter>) -> usize {
        match self {
            MaterializedGraph::EventGraph(g) => g.nodes_len(layer_ids, filter),
            MaterializedGraph::PersistentGraph(g) => g.nodes_len(layer_ids, filter),
        }
    }

    fn edges_len(&self, layers: LayerIds, filter: Option<&EdgeFilter>) -> usize {
        match self {
            MaterializedGraph::EventGraph(g) => g.edges_len(layers, filter),
            MaterializedGraph::PersistentGraph(g) => g.edges_len(layers, filter),
        }
    }

    fn temporal_edges_len(&self, layers: LayerIds, filter: Option<&EdgeFilter>) -> usize {
        match self {
            MaterializedGraph::EventGraph(g) => g.temporal_edges_len(layers, filter),
            MaterializedGraph::PersistentGraph(g) => g.temporal_edges_len(layers, filter),
        }
    }

    fn degree(
        &self,
        v: VID,
        d: Direction,
        layers: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> usize {
        match self {
            MaterializedGraph::EventGraph(g) => g.degree(v, d, layers, filter),
            MaterializedGraph::PersistentGraph(g) => g.degree(v, d, layers, filter),
        }
    }

    fn edge_ref(
        &self,
        src: VID,
        dst: VID,
        layer: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Option<EdgeRef> {
        match self {
            MaterializedGraph::EventGraph(g) => g.edge_ref(src, dst, layer, filter),
            MaterializedGraph::PersistentGraph(g) => g.edge_ref(src, dst, layer, filter),
        }
    }

    fn node_refs(&self, layers: LayerIds, filter: Option<&EdgeFilter>) -> BoxedLIter<'graph, VID> {
        match self {
            MaterializedGraph::EventGraph(g) => g.node_refs(layers, filter),
            MaterializedGraph::PersistentGraph(g) => g.node_refs(layers, filter),
        }
    }

    fn edge_refs(
        &self,
        layers: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> BoxedLIter<'graph, EdgeRef> {
        match self {
            MaterializedGraph::EventGraph(g) => g.edge_refs(layers, filter),
            MaterializedGraph::PersistentGraph(g) => g.edge_refs(layers, filter),
        }
    }

    fn node_edges(
        &self,
        v: VID,
        d: Direction,
        layer: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> BoxedLIter<'graph, EdgeRef> {
        match self {
            MaterializedGraph::EventGraph(g) => g.node_edges(v, d, layer, filter),
            MaterializedGraph::PersistentGraph(g) => g.node_edges(v, d, layer, filter),
        }
    }

    fn neighbours(
        &self,
        v: VID,
        d: Direction,
        layers: LayerIds,
        filter: Option<&EdgeFilter>,
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

    pub fn load_from_file<P: AsRef<Path>>(path: P, force: bool) -> Result<Self, GraphError> {
        let f = std::fs::File::open(path)?;
        let mut reader = std::io::BufReader::new(f);
        if force {
            let _: String = bincode::deserialize_from(&mut reader)?;
            let data: Self = bincode::deserialize_from(&mut reader)?;
            Ok(data)
        } else {
            let version: u32 = bincode::deserialize_from(&mut reader)?;
            if version != BINCODE_VERSION {
                return Err(GraphError::BincodeVersionError(version, BINCODE_VERSION));
            }
            let data: Self = bincode::deserialize_from(&mut reader)?;
            Ok(data)
        }
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), GraphError> {
        let f = std::fs::File::create(path)?;
        let mut writer = std::io::BufWriter::new(f);
        let versioned_data = VersionedGraph {
            version: BINCODE_VERSION,
            graph: self.clone(),
        };
        Ok(bincode::serialize_into(&mut writer, &versioned_data)?)
    }

    pub fn bincode(&self) -> Result<Vec<u8>, GraphError> {
        let versioned_data = VersionedGraph {
            version: BINCODE_VERSION,
            graph: self.clone(),
        };
        let encoded = bincode::serialize(&versioned_data)?;
        Ok(encoded)
    }

    pub fn from_bincode(b: &[u8]) -> Result<Self, GraphError> {
        let version: u32 = bincode::deserialize(b)?;
        if version != BINCODE_VERSION {
            return Err(GraphError::BincodeVersionError(version, BINCODE_VERSION));
        }
        let g: VersionedGraph<MaterializedGraph> = bincode::deserialize(b)?;
        Ok(g.graph)
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
            CoreGraphOps, EdgeFilterOps, GraphOps, InternalLayerOps, InternalMaterialize,
            MaterializedGraph, TimeSemantics,
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
        assert_eq!(mg.nodes_len(mg.layer_ids(), mg.edge_filter()), 0);
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

        let v = mg.add_node(0, 1, NO_PROPS, None).unwrap();
        assert_eq!(v.id(), 1)
    }
}
