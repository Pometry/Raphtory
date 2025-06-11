use std::{
    ops::DerefMut,
    path::{Path, PathBuf},
    sync::{atomic::AtomicUsize, Arc},
};

use parking_lot::RwLockWriteGuard;
use raphtory_api::core::{
    entities::{
        properties::{
            meta::Meta,
            prop::{Prop, PropType},
        },
        GidRef, EID, VID,
    },
    storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
};
use raphtory_core::{
    entities::{graph::logical_to_physical::Mapping, nodes::node_ref::NodeRef, ELID},
    storage::{raw_edges::WriteLockedEdges, WriteLockedNodes},
};
// use raphtory_storage::mutation::{
//     addition_ops::{AtomicAdditionOps, InternalAdditionOps, SessionAdditionOps},
//     MutationError,
// };
use storage::{
    error::DBV4Error,
    pages::session::WriteSession,
    persist::strategy::PersistentStrategy,
    properties::props_meta_writer::PropsMetaWriter,
    segments::{edge::MemEdgeSegment, node::MemNodeSegment},
    Layer, ES, NS,
};

pub mod mutation;

pub struct TemporalGraph<EXT = ()> {
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

impl<EXT: PersistentStrategy<NS = NS<EXT>, ES = ES<EXT>>> TemporalGraph<EXT> {
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
