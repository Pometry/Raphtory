use super::GraphFolder;
#[cfg(feature = "search")]
use crate::prelude::IndexMutationOps;
use crate::{
    db::{
        api::{storage::storage::Storage, view::MaterializedGraph},
        graph::views::deletion_graph::PersistentGraph,
    },
    errors::{GraphError, WriteError},
    prelude::{AdditionOps, Graph, StableDecode},
    serialise::{
        serialise::{CacheOps, InternalStableDecode, StableEncode},
        ProtoGraph,
    },
};
use parking_lot::Mutex;
use prost::Message;
use raphtory_api::core::{
    entities::{
        properties::prop::{Prop, PropType},
        GidRef, EID, VID,
    },
    storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
};
use std::{
    fmt::Debug,
    fs::File,
    io::{Seek, SeekFrom, Write},
    mem,
    ops::DerefMut,
    sync::Arc,
};
use tracing::instrument;

#[derive(Debug)]
pub struct GraphWriter {
    write_lock: Arc<Mutex<()>>,
    proto_delta: Mutex<ProtoGraph>,
    pub(crate) folder: GraphFolder,
}

fn try_write(folder: &GraphFolder, bytes: &[u8]) -> Result<(), WriteError> {
    let mut writer = folder.get_appendable_graph_file()?;
    let pos = writer.seek(SeekFrom::End(0))?;
    writer
        .write_all(bytes)
        .map_err(|write_err| match writer.set_len(pos) {
            Ok(_) => WriteError::WriteError(write_err),
            Err(reset_err) => WriteError::FatalWriteError(write_err, reset_err),
        })
}

impl GraphWriter {
    pub fn new(folder: GraphFolder) -> Result<Self, GraphError> {
        Ok(Self {
            write_lock: Arc::new(Mutex::new(())),
            proto_delta: Default::default(),
            folder,
        })
    }

    /// Get an independent writer pointing at the same underlying cache file
    pub fn fork(&self) -> Self {
        GraphWriter {
            write_lock: self.write_lock.clone(),
            proto_delta: Default::default(),
            folder: self.folder.clone(),
        }
    }

    pub fn write(&self) -> Result<(), GraphError> {
        let mut proto = mem::take(self.proto_delta.lock().deref_mut());
        let bytes = proto.encode_to_vec();
        if !bytes.is_empty() {
            let _guard = self.write_lock.lock();
            if let Err(write_err) = try_write(&self.folder, &bytes) {
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
            // should we flush the file?
        }
        Ok(())
    }

    #[inline]
    pub fn resolve_layer(&self, layer: Option<&str>, layer_id: MaybeNew<usize>) {
        layer_id.if_new(|id| {
            let layer = layer.unwrap_or("_default");
            self.proto_delta.lock().new_layer(layer, id)
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
        dtype: &PropType,
        is_static: bool,
    ) {
        prop_id.if_new(|id| {
            if is_static {
                self.proto_delta.lock().new_node_cprop(prop, id, dtype);
            } else {
                self.proto_delta.lock().new_node_tprop(prop, id, dtype);
            }
        });
    }

    pub fn resolve_edge_property(
        &self,
        prop: &str,
        prop_id: MaybeNew<usize>,
        dtype: &PropType,
        is_static: bool,
    ) {
        prop_id.if_new(|id| {
            if is_static {
                self.proto_delta.lock().new_edge_cprop(prop, id, dtype);
            } else {
                self.proto_delta.lock().new_edge_tprop(prop, id, dtype);
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
        if !props.is_empty() {
            self.proto_delta.lock().update_edge_cprops(
                edge,
                layer,
                props.iter().map(|(id, prop)| (*id, prop)),
            )
        }
    }

    pub fn delete_edge(&self, edge: EID, t: TimeIndexEntry, layer: usize) {
        self.proto_delta.lock().del_edge(edge, layer, t)
    }
}

pub trait InternalCache {
    /// Initialise the cache by pointing it at a proto file.
    /// Future updates will be appended to the cache.
    fn init_cache(&self, path: &GraphFolder) -> Result<(), GraphError>;

    /// Get the cache writer if it is initialised.
    fn get_cache(&self) -> Option<&GraphWriter>;
}

impl InternalCache for Storage {
    fn init_cache(&self, path: &GraphFolder) -> Result<(), GraphError> {
        self.cache
            .get_or_try_init(|| GraphWriter::new(path.clone()))?;
        Ok(())
    }

    fn get_cache(&self) -> Option<&GraphWriter> {
        self.cache.get()
    }
}

impl InternalCache for Graph {
    fn init_cache(&self, path: &GraphFolder) -> Result<(), GraphError> {
        self.inner.init_cache(path)
    }

    fn get_cache(&self) -> Option<&GraphWriter> {
        self.inner.get_cache()
    }
}

impl InternalCache for PersistentGraph {
    fn init_cache(&self, path: &GraphFolder) -> Result<(), GraphError> {
        self.0.init_cache(path)
    }

    fn get_cache(&self) -> Option<&GraphWriter> {
        self.0.get_cache()
    }
}

impl InternalCache for MaterializedGraph {
    fn init_cache(&self, path: &GraphFolder) -> Result<(), GraphError> {
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

impl<G: InternalCache + InternalStableDecode + StableEncode + AdditionOps> CacheOps for G {
    fn cache(&self, path: impl Into<GraphFolder>) -> Result<(), GraphError> {
        let folder = path.into();
        self.encode(&folder)?;
        self.init_cache(&folder)
    }

    #[instrument(level = "debug", skip(self))]
    fn write_updates(&self) -> Result<(), GraphError> {
        let cache = self.get_cache().ok_or(GraphError::CacheNotInnitialised)?;
        cache.write()?;
        cache.folder.write_metadata(self)?;
        #[cfg(feature = "search")]
        self.persist_index_to_disk(&cache.folder)?;
        Ok(())
    }

    fn load_cached(path: impl Into<GraphFolder>) -> Result<Self, GraphError> {
        let folder = path.into();
        if folder.is_zip() {
            return Err(GraphError::ZippedGraphCannotBeCached);
        }
        let graph = Self::decode(&folder)?;
        graph.init_cache(&folder)?;
        Ok(graph)
    }
}

#[cfg(test)]
mod test {
    use crate::serialise::{incremental::GraphWriter, GraphFolder};
    use raphtory_api::core::{
        entities::{GidRef, VID},
        storage::dict_mapper::MaybeNew,
        utils::logging::global_info_logger,
    };
    use std::fs::File;
    use tempfile::TempDir;

    // Tests that changes to the cache graph are not thrown away if cache write fails
    // and there is a chance to recover from this.
    #[test]
    fn test_write_failure() {
        global_info_logger();
        let tmp_dir = TempDir::new().unwrap();
        let folder = GraphFolder::from(tmp_dir.path());
        let graph_file_path = folder.get_graph_path();
        let file = File::create(&graph_file_path).unwrap();
        let mut perms = file.metadata().unwrap().permissions();
        perms.set_readonly(true);
        file.set_permissions(perms).unwrap();
        let cache = GraphWriter::new(folder).unwrap();
        cache.resolve_node(MaybeNew::New(VID(0)), GidRef::Str("0"));
        assert_eq!(cache.proto_delta.lock().nodes.len(), 1);
        let res = cache.write();
        assert!(res.is_err());
        assert_eq!(cache.proto_delta.lock().nodes.len(), 1);
    }
}
