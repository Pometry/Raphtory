use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{self, AtomicUsize},
        Arc,
    },
};

// use crate::entries::node::UnlockedNodeEntry;
use raphtory_api::core::{entities::properties::meta::Meta, input::input_node::InputNode};
use raphtory_core::entities::{
    nodes::node_ref::NodeRef, properties::graph_meta::GraphMeta, GidRef, VID,
};
use storage::{
    persist::strategy::PersistentStrategy, Extension, GIDResolver,
    Layer, ReadLockedLayer, ES, NS,
};

pub mod entries;
pub mod mutation;

#[derive(Debug)]
pub struct TemporalGraph<EXT = Extension> {
    graph_dir: PathBuf,

    // mapping between logical and physical ids
    pub logical_to_physical: GIDResolver,
    pub node_count: AtomicUsize,

    max_page_len_nodes: usize,
    max_page_len_edges: usize,

    storage: Arc<Layer<EXT>>,

    edge_meta: Arc<Meta>,
    node_meta: Arc<Meta>,

    event_counter: AtomicUsize,
    graph_meta: Arc<GraphMeta>,
}

impl<EXT: PersistentStrategy<NS = NS<EXT>, ES = ES<EXT>>> TemporalGraph<EXT> {
    // pub fn node(&self, vid: VID) -> UnlockedNodeEntry<EXT> {
    //     UnlockedNodeEntry::new(vid, self)
    // }

    pub fn read_event_counter(&self) -> usize {
        self.event_counter.load(atomic::Ordering::Relaxed)
    }

    pub fn storage(&self) -> &Arc<Layer<EXT>> {
        &self.storage
    }

    pub fn graph_meta(&self) -> &Arc<GraphMeta> {
        &self.graph_meta
    }

    pub fn num_layers(&self) -> usize {
        self.storage.nodes().num_layers()
    }

    #[inline]
    pub fn resolve_node_ref(&self, v: NodeRef) -> Option<VID> {
        match v {
            NodeRef::Internal(vid) => Some(vid),
            NodeRef::External(GidRef::U64(gid)) => self.logical_to_physical.get_u64(gid),
            NodeRef::External(GidRef::Str(string)) => self
                .logical_to_physical
                .get_str(string)
                .or_else(|| self.logical_to_physical.get_u64(string.id())),
        }
    }

    #[inline]
    pub fn internal_num_nodes(&self) -> usize {
        self.storage.nodes().num_nodes()
    }

    #[inline]
    pub fn internal_num_edges(&self) -> usize {
        self.storage.edges().num_edges()
    }

    pub fn read_locked(self: &Arc<Self>) -> ReadLockedLayer<EXT> {
        self.storage.read_locked()
    }

    pub fn edge_meta(&self) -> &Arc<Meta> {
        &self.edge_meta
    }

    pub fn node_meta(&self) -> &Arc<Meta> {
        &self.node_meta
    }

    pub fn graph_dir(&self) -> &Path {
        &self.graph_dir
    }

    pub fn max_page_len_nodes(&self) -> usize {
        self.max_page_len_nodes
    }

    pub fn max_page_len_edges(&self) -> usize {
        self.max_page_len_edges
    }
}
