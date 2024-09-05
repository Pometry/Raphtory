use crate::{
    core::{utils::errors::GraphError, Prop, PropType},
    db::{
        api::{storage::storage::Storage, view::MaterializedGraph},
        graph::views::deletion_graph::PersistentGraph,
    },
    prelude::Graph,
    serialise::{
        serialise::{CacheOps, StableDecode, StableEncode},
        ProtoGraph,
    },
};
use parking_lot::Mutex;
use prost::Message;
use raphtory_api::core::{
    entities::{GidRef, EID, VID},
    storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
};
use std::{
    fmt::Debug,
    fs::{File, OpenOptions},
    io::Write,
    mem,
    ops::DerefMut,
    path::Path,
};

#[derive(Debug)]
pub struct GraphWriter {
    writer: Mutex<File>,
    proto_delta: Mutex<ProtoGraph>,
}

impl GraphWriter {
    pub fn new(file: File) -> Self {
        Self {
            writer: Mutex::new(file),
            proto_delta: Default::default(),
        }
    }
    pub fn write(&self) -> Result<(), GraphError> {
        let mut proto = mem::take(self.proto_delta.lock().deref_mut());
        let bytes = proto.encode_to_vec();
        if !bytes.is_empty() {
            if let Err(write_err) = self.writer.lock().write_all(&bytes) {
                // If the write fails, try to put the updates back
                let mut new_delta = self.proto_delta.lock();
                let bytes = new_delta.encode_to_vec();
                match proto.merge(&*bytes) {
                    Ok(_) => *new_delta = proto,
                    Err(decode_err) => {
                        // This should never happen, it means that the new delta was an invalid Graph
                        return Err(GraphError::FatalDecodeError {
                            write_err,
                            decode_err,
                        });
                    }
                }
                return Err(write_err.into());
            }
        }
        Ok(())
    }

    #[inline]
    pub fn resolve_layer(&self, layer: Option<&str>, layer_id: MaybeNew<usize>) {
        layer_id.if_new(|id| {
            if let Some(layer) = layer {
                self.proto_delta.lock().new_layer(layer, id)
            }
        });
    }

    pub fn resolve_node(&self, vid: MaybeNew<VID>, gid: GidRef) {
        vid.if_new(|vid| self.proto_delta.lock().new_node(gid, vid, 0));
    }

    pub fn resolve_node_and_type(
        &self,
        node_and_type: MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>,
        node_type: &str,
        gid: GidRef,
    ) {
        if let MaybeNew::New((MaybeNew::Existing(node_id), type_id)) = node_and_type {
            // type assignment changed but node already exists
            self.proto_delta
                .lock()
                .update_node_type(node_id, type_id.inner());
        }
        if let (MaybeNew::New(node_id), type_id) = node_and_type.inner() {
            self.proto_delta
                .lock()
                .new_node(gid, node_id, type_id.inner());
        }
        if let (_, MaybeNew::New(type_id)) = node_and_type.inner() {
            self.proto_delta.lock().new_node_type(node_type, type_id);
        }
    }

    pub fn resolve_graph_property(
        &self,
        prop: &str,
        prop_id: MaybeNew<usize>,
        dtype: PropType,
        is_static: bool,
    ) {
        prop_id.if_new(|id| {
            if is_static {
                self.proto_delta.lock().new_graph_cprop(prop, id);
            } else {
                self.proto_delta.lock().new_graph_tprop(prop, id, &dtype);
            }
        });
    }

    pub fn resolve_node_property(
        &self,
        prop: &str,
        prop_id: MaybeNew<usize>,
        dtype: PropType,
        is_static: bool,
    ) {
        prop_id.if_new(|id| {
            if is_static {
                self.proto_delta.lock().new_node_cprop(prop, id, &dtype);
            } else {
                self.proto_delta.lock().new_node_tprop(prop, id, &dtype);
            }
        });
    }

    pub fn resolve_edge_property(
        &self,
        prop: &str,
        prop_id: MaybeNew<usize>,
        dtype: PropType,
        is_static: bool,
    ) {
        prop_id.if_new(|id| {
            if is_static {
                self.proto_delta.lock().new_edge_cprop(prop, id, &dtype);
            } else {
                self.proto_delta.lock().new_edge_tprop(prop, id, &dtype);
            }
        });
    }

    pub fn add_node_update(&self, t: TimeIndexEntry, v: VID, props: &[(usize, Prop)]) {
        self.proto_delta
            .lock()
            .update_node_tprops(v, t, props.iter().map(|(id, prop)| (*id, prop)))
    }

    pub fn resolve_edge(&self, eid: MaybeNew<EID>, src: VID, dst: VID) {
        eid.if_new(|eid| self.proto_delta.lock().new_edge(src, dst, eid));
    }

    pub fn add_edge_update(
        &self,
        t: TimeIndexEntry,
        edge: EID,
        props: &[(usize, Prop)],
        layer: usize,
    ) {
        self.proto_delta.lock().update_edge_tprops(
            edge,
            t,
            layer,
            props.iter().map(|(id, prop)| (*id, prop)),
        )
    }
    pub fn add_graph_tprops(&self, t: TimeIndexEntry, props: &[(usize, Prop)]) {
        self.proto_delta
            .lock()
            .update_graph_tprops(t, props.iter().map(|(id, prop)| (*id, prop)))
    }

    pub fn add_graph_cprops(&self, props: &[(usize, Prop)]) {
        self.proto_delta
            .lock()
            .update_graph_cprops(props.iter().map(|(id, prop)| (*id, prop)))
    }

    pub fn add_node_cprops(&self, node: VID, props: &[(usize, Prop)]) {
        self.proto_delta
            .lock()
            .update_node_cprops(node, props.iter().map(|(id, prop)| (*id, prop)))
    }

    pub fn add_edge_cprops(&self, edge: EID, layer: usize, props: &[(usize, Prop)]) {
        self.proto_delta.lock().update_edge_cprops(
            edge,
            layer,
            props.iter().map(|(id, prop)| (*id, prop)),
        )
    }

    pub fn delete_edge(&self, edge: EID, t: TimeIndexEntry, layer: usize) {
        self.proto_delta.lock().del_edge(edge, layer, t)
    }
}

pub(crate) trait InternalCache {
    /// Initialise the cache by pointing it at a proto file.
    /// Future updates will be appended to the cache.
    fn init_cache(&self, path: impl AsRef<Path>) -> Result<(), GraphError>;

    /// Get the cache writer if it is initialised.
    fn get_cache(&self) -> Option<&GraphWriter>;
}

impl InternalCache for Storage {
    fn init_cache(&self, path: impl AsRef<Path>) -> Result<(), GraphError> {
        self.cache.get_or_try_init(|| {
            let file = OpenOptions::new().append(true).open(path)?;
            Ok::<_, GraphError>(GraphWriter::new(file))
        })?;
        Ok(())
    }

    fn get_cache(&self) -> Option<&GraphWriter> {
        self.cache.get()
    }
}

impl InternalCache for Graph {
    fn init_cache(&self, path: impl AsRef<Path>) -> Result<(), GraphError> {
        self.inner.init_cache(path)
    }

    fn get_cache(&self) -> Option<&GraphWriter> {
        self.inner.get_cache()
    }
}

impl InternalCache for PersistentGraph {
    fn init_cache(&self, path: impl AsRef<Path>) -> Result<(), GraphError> {
        self.0.init_cache(path)
    }

    fn get_cache(&self) -> Option<&GraphWriter> {
        self.0.get_cache()
    }
}

impl InternalCache for MaterializedGraph {
    fn init_cache(&self, path: impl AsRef<Path>) -> Result<(), GraphError> {
        match self {
            MaterializedGraph::EventGraph(g) => g.init_cache(path),
            MaterializedGraph::PersistentGraph(g) => g.init_cache(path),
        }
    }

    fn get_cache(&self) -> Option<&GraphWriter> {
        match self {
            MaterializedGraph::EventGraph(g) => g.get_cache(),
            MaterializedGraph::PersistentGraph(g) => g.get_cache(),
        }
    }
}

impl<G: InternalCache + StableDecode + StableEncode> CacheOps for G {
    fn cache(&self, path: impl AsRef<Path>) -> Result<(), GraphError> {
        self.encode(path.as_ref())?;
        self.init_cache(path)
    }

    fn write_updates(&self) -> Result<(), GraphError> {
        let cache = self.get_cache().ok_or(GraphError::CacheNotInnitialised)?;
        cache.write()
    }

    fn load_cached(path: impl AsRef<Path>) -> Result<Self, GraphError> {
        let graph = Self::decode(path.as_ref())?;
        graph.init_cache(path)?;
        Ok(graph)
    }
}
