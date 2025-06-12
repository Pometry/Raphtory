use std::{
    ops::Deref,
    path::{Path, PathBuf},
    sync::{
        atomic::{self, AtomicUsize},
        Arc,
    },
};

use raphtory_api::core::{entities::properties::meta::Meta, input::input_node::InputNode};
use raphtory_core::entities::{
    graph::logical_to_physical::Mapping, nodes::node_ref::NodeRef,
    properties::graph_meta::GraphMeta, GidRef, VID,
};
use storage::{persist::strategy::PersistentStrategy, Extension, Layer, ES, NS};

use crate::entries::node::{LockedNodeEntry, UnlockedNodeEntry};

pub mod entries;
pub mod mutation;

#[derive(Debug)]
pub struct TemporalGraph<EXT = Extension> {
    graph_dir: PathBuf,
    // mapping between logical and physical ids
    pub logical_to_physical: Mapping,
    pub node_count: AtomicUsize,

    max_page_len_nodes: usize,
    max_page_len_edges: usize,

    static_graph: Arc<Layer<EXT>>,
    layers: boxcar::Vec<Arc<Layer<EXT>>>,

    edge_meta: Arc<Meta>,
    node_meta: Arc<Meta>,

    event_counter: AtomicUsize,
    graph_meta: Arc<GraphMeta>,
}

#[derive(Debug)]
pub struct ReadLockedTemporalGraph<EXT = Extension> {
    pub graph: Arc<TemporalGraph<EXT>>,
    static_graph: storage::ReadLockedLayer<EXT>,
    locked_layers: Box<[storage::ReadLockedLayer<EXT>]>,
}

impl<EXT> ReadLockedTemporalGraph<EXT> {
    pub fn graph(&self) -> &Arc<TemporalGraph<EXT>> {
        &self.graph
    }

    pub fn node(&self, vid: VID) -> LockedNodeEntry<EXT> {
        LockedNodeEntry::new(vid, self)
    }

    pub fn inner(&self) -> &TemporalGraph<EXT> {
        &self.graph
    }
}


impl<EXT: PersistentStrategy<NS = NS<EXT>, ES = ES<EXT>>> TemporalGraph<EXT> {
    pub fn node(&self, vid: VID) -> UnlockedNodeEntry<EXT> {
        UnlockedNodeEntry::new(vid, self)
    }

    pub fn read_event_counter(&self) -> usize {
        self.event_counter.load(atomic::Ordering::Relaxed)
    }

    pub fn static_graph(&self) -> &Arc<Layer<EXT>> {
        &self.static_graph
    }

    pub fn graph_meta(&self) -> &Arc<GraphMeta> {
        &self.graph_meta
    }

    pub fn num_layers(&self) -> usize {
        self.layers.count()
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
        self.static_graph.nodes().num_nodes()
    }

    #[inline]
    pub fn internal_num_edges(&self) -> usize {
        self.static_graph.edges().num_edges()
    }

    pub fn read_locked(self: &Arc<Self>) -> ReadLockedTemporalGraph<EXT> {
        let locked_layers = self
            .layers
            .iter()
            .map(|(_, layer)| layer.read_locked())
            .collect::<Box<_>>();
        ReadLockedTemporalGraph {
            graph: self.clone(),
            static_graph: self.static_graph.read_locked(),
            locked_layers,
        }
    }

    pub fn layers(&self) -> &boxcar::Vec<Arc<Layer<EXT>>> {
        &self.layers
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
