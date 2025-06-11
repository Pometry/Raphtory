use std::{
    path::{Path, PathBuf},
    sync::{atomic::AtomicUsize, Arc},
};

use raphtory_api::core::entities::properties::meta::Meta;
use raphtory_core::entities::graph::logical_to_physical::Mapping;
use storage::{persist::strategy::PersistentStrategy, Extension, Layer, ES, NS};

pub mod mutation;

#[derive(Debug)]
pub struct TemporalGraph<EXT = Extension> {
    graph_dir: PathBuf,
    // mapping between logical and physical ids
    pub logical_to_physical: Mapping,
    pub node_count: AtomicUsize,

    max_page_len_nodes: usize,
    max_page_len_edges: usize,

    layers: boxcar::Vec<Layer<EXT>>,

    edge_meta: Arc<Meta>,
    node_meta: Arc<Meta>,
}

pub struct ReadLockedTemporalGraph<EXT = Extension> {
    graph: Arc<TemporalGraph<EXT>>,
    locked_layers: Box<[storage::ReadLockedLayer<'static, EXT>]>,
}

impl<EXT: PersistentStrategy<NS = NS<EXT>, ES = ES<EXT>>> TemporalGraph<EXT> {
    pub fn read_locked(self: &Arc<Self>) -> ReadLockedTemporalGraph<EXT> {
        let locked_layers = self
            .layers
            .iter()
            .map(|layer| layer.locked())
            .collect::<Box<_>>();
        ReadLockedTemporalGraph {
            graph: self.clone(),
            locked_layers,
        }
    }

    pub fn layers(&self) -> &boxcar::Vec<Layer<EXT>> {
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
