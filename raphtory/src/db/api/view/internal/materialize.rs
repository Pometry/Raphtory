use crate::{
    core::{
        entities::{
            edges::edge_ref::EdgeRef,
            graph::tgraph::InternalGraph,
            nodes::node_ref::NodeRef,
            properties::{graph_meta::GraphMeta, props::Meta, tprop::TProp},
            LayerIds, EID, ELID, VID,
        },
        storage::{locked_view::LockedView, timeindex::TimeIndexEntry},
        utils::errors::{
            GraphError,
            GraphError::{EventGraphDeletionsNotSupported, ImmutableDiskGraph},
        },
        ArcStr, PropType,
    },
    db::{
        api::{
            mutation::internal::{
                InternalAdditionOps, InternalDeletionOps, InternalPropertyAdditionOps,
            },
            properties::internal::{
                ConstPropertiesOps, TemporalPropertiesOps, TemporalPropertyViewOps,
            },
            storage::{
                edges::{
                    edge_entry::EdgeStorageEntry, edge_owned_entry::EdgeOwnedEntry,
                    edge_ref::EdgeStorageRef, edges::EdgesStorage,
                },
                nodes::{
                    node_entry::NodeStorageEntry, node_owned_entry::NodeOwnedEntry,
                    nodes::NodesStorage,
                },
                storage_ops::GraphStorage,
            },
            view::{internal::*, BoxedIter, StaticGraphViewOps},
        },
        graph::{
            edge::EdgeView, graph::Graph, node::NodeView, views::deletion_graph::PersistentGraph,
        },
    },
    prelude::*,
    BINCODE_VERSION,
};
use chrono::{DateTime, Utc};
use enum_dispatch::enum_dispatch;
use serde::{de::Error, Deserialize, Deserializer, Serialize};
use std::{fs, io, path::Path};

#[cfg(feature = "storage")]
use crate::disk_graph::graph_impl::DiskGraph;

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
    #[cfg(feature = "storage")]
    DiskEventGraph(DiskGraph),
}

fn version_deserialize<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let version = u32::deserialize(deserializer)?;
    if version != BINCODE_VERSION {
        return Err(Error::custom(GraphError::BincodeVersionError(
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

impl MaterializedGraph {
    pub fn into_events(self) -> Option<Graph> {
        match self {
            MaterializedGraph::EventGraph(g) => Some(g),
            MaterializedGraph::PersistentGraph(_) => None,
            #[cfg(feature = "storage")]
            MaterializedGraph::DiskEventGraph(_) => None,
        }
    }
    pub fn into_persistent(self) -> Option<PersistentGraph> {
        match self {
            MaterializedGraph::EventGraph(_) => None,
            MaterializedGraph::PersistentGraph(g) => Some(g),
            #[cfg(feature = "storage")]
            MaterializedGraph::DiskEventGraph(_) => None,
        }
    }

    #[cfg(feature = "storage")]
    pub fn into_disk_graph(self) -> Option<DiskGraph> {
        match self {
            MaterializedGraph::EventGraph(_) => None,
            MaterializedGraph::PersistentGraph(_) => None,
            #[cfg(feature = "storage")]
            MaterializedGraph::DiskEventGraph(g) => Some(g),
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
        let f = fs::File::create(path)?;
        let mut writer = io::BufWriter::new(f);
        let versioned_data = VersionedGraph {
            version: BINCODE_VERSION,
            graph: self.clone(),
        };
        Ok(bincode::serialize_into(&mut writer, &versioned_data)?)
    }

    pub fn save_to_path(&self, path: &Path) -> Result<(), GraphError> {
        match self {
            MaterializedGraph::EventGraph(g) => g.save_to_file(&path)?,
            MaterializedGraph::PersistentGraph(g) => g.save_to_file(&path)?,
            #[cfg(feature = "storage")]
            MaterializedGraph::DiskEventGraph(g) => {
                let graph_dir = &g.graph_dir;
                println!("Disk Graph saved to directory= {}", graph_dir.display());
                copy_dir_recursive(graph_dir, path)?;
            }
        };

        Ok(())
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

impl InternalDeletionOps for MaterializedGraph {
    fn internal_delete_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<(), GraphError> {
        match self {
            MaterializedGraph::EventGraph(g) => Err(EventGraphDeletionsNotSupported),
            MaterializedGraph::PersistentGraph(g) => g.internal_delete_edge(t, src, dst, layer),
            #[cfg(feature = "storage")]
            MaterializedGraph::DiskEventGraph(g) => Err(ImmutableDiskGraph),
        }
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

pub fn copy_dir_recursive(source_dir: &Path, target_dir: &Path) -> io::Result<()> {
    if !target_dir.exists() {
        fs::create_dir_all(target_dir)?;
    }

    for entry in fs::read_dir(source_dir)? {
        let entry = entry?;
        let entry_path = entry.path();
        let target_path = target_dir.join(entry.file_name());

        if entry_path.is_dir() {
            copy_dir_recursive(&entry_path, &target_path)?;
        } else {
            fs::copy(&entry_path, &target_path)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod test_materialised_graph_dispatch {
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
        assert_eq!(v.id(), 1)
    }
}
