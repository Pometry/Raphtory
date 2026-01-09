use std::{
    io,
    path::{Path, PathBuf},
    sync::{atomic::AtomicUsize, Arc},
};

use raphtory_api::core::{
    entities::{self, properties::meta::Meta, GidType},
    input::input_node::InputNode,
};
use raphtory_core::{
    entities::{graph::tgraph::InvalidLayer, nodes::node_ref::NodeRef, GidRef, LayerIds, EID, VID},
    storage::timeindex::TimeIndexEntry,
};
use storage::{
    api::nodes::NodeSegmentOps,
    error::StorageError,
    pages::{
        layer_counter::GraphStats,
        locked::{
            edges::WriteLockedEdgePages, graph_props::WriteLockedGraphPropPages,
            nodes::WriteLockedNodePages,
        },
    },
    persist::strategy::{PersistenceStrategy},
    resolver::GIDResolverOps,
    Extension, GIDResolver, Layer, ReadLockedLayer, transaction::TransactionManager,
    WalImpl, ES, NS, GS, wal::Wal,
};
use tempfile::TempDir;

mod replay;

#[derive(Debug)]
pub struct TemporalGraph<EXT: PersistenceStrategy = Extension> {
    // mapping between logical and physical ids
    pub logical_to_physical: Arc<GIDResolver>,
    pub node_count: AtomicUsize,
    storage: Arc<Layer<EXT>>,
    graph_dir: Option<GraphDir>,
    pub transaction_manager: Arc<TransactionManager>,
    pub wal: Arc<WalImpl>,
}

#[derive(Debug)]
pub enum GraphDir {
    Temp(TempDir),
    Path(PathBuf),
}

impl GraphDir {
    pub fn path(&self) -> &Path {
        match self {
            GraphDir::Temp(dir) => dir.path(),
            GraphDir::Path(path) => path,
        }
    }
    pub fn gid_resolver_dir(&self) -> PathBuf {
        self.path().join("gid_resolver")
    }

    pub fn wal_dir(&self) -> PathBuf {
        self.path().join("wal")
    }

    pub fn create_dir(&self) -> Result<(), io::Error> {
        if let GraphDir::Path(path) = self {
            std::fs::create_dir_all(path)?;
        }
        Ok(())
    }
}

impl AsRef<Path> for GraphDir {
    fn as_ref(&self) -> &Path {
        self.path()
    }
}

impl<'a> From<&'a Path> for GraphDir {
    fn from(path: &'a Path) -> Self {
        GraphDir::Path(path.to_path_buf())
    }
}

impl Default for TemporalGraph<Extension> {
    fn default() -> Self {
        Self::new(Extension::default()).unwrap()
    }
}

impl<EXT: PersistenceStrategy<NS = NS<EXT>, ES = ES<EXT>, GS = GS<EXT>>> TemporalGraph<EXT> {
    pub fn new(ext: EXT) -> Result<Self, StorageError> {
        let node_meta = Meta::new_for_nodes();
        let edge_meta = Meta::new_for_edges();
        let graph_props_meta = Meta::new_for_graph_props();

        Self::new_with_meta(None, node_meta, edge_meta, graph_props_meta, ext)
    }

    pub fn new_with_path(path: impl AsRef<Path>, ext: EXT) -> Result<Self, StorageError> {
        let node_meta = Meta::new_for_nodes();
        let edge_meta = Meta::new_for_edges();
        let graph_props_meta = Meta::new_for_graph_props();

        Self::new_with_meta(
            Some(path.as_ref().into()),
            node_meta,
            edge_meta,
            graph_props_meta,
            ext,
        )
    }

    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref();
        let storage = Layer::load(path)?;
        let id_type = storage.nodes().id_type();

        let gid_resolver_dir = path.join("gid_resolver");
        let resolver = GIDResolver::new_with_path(&gid_resolver_dir, id_type)?;
        let node_count = AtomicUsize::new(storage.nodes().num_nodes());
        let wal_dir = path.join("wal");
        let wal = Arc::new(WalImpl::new(Some(wal_dir))?);

        Ok(Self {
            graph_dir: Some(path.into()),
            logical_to_physical: resolver.into(),
            node_count,
            storage: Arc::new(storage),
            transaction_manager: Arc::new(TransactionManager::new()),
            wal,
        })
    }

    pub fn new_with_meta(
        graph_dir: Option<GraphDir>,
        node_meta: Meta,
        edge_meta: Meta,
        graph_meta: Meta,
        ext: EXT,
    ) -> Result<Self, StorageError> {
        let mut graph_dir = graph_dir;

        // Short-circuit graph_dir to None if disk storage is not enabled
        if !Extension::disk_storage_enabled() {
            graph_dir = None;
        }

        if let Some(dir) = graph_dir.as_ref() {
            std::fs::create_dir_all(dir)?
        }

        let id_type = node_meta
            .metadata_mapper()
            .d_types()
            .first()
            .and_then(|dtype| GidType::from_prop_type(dtype));

        let gid_resolver_dir = graph_dir.as_ref().map(|dir| dir.gid_resolver_dir());
        let logical_to_physical = match gid_resolver_dir {
            Some(gid_resolver_dir) => GIDResolver::new_with_path(gid_resolver_dir, id_type)?,
            None => GIDResolver::new()?,
        }
        .into();

        let storage: Layer<EXT> = Layer::new_with_meta(
            graph_dir.as_ref().map(|p| p.path()),
            node_meta,
            edge_meta,
            graph_meta,
            ext,
        );

        let wal_dir = graph_dir.as_ref().map(|dir| dir.wal_dir());
        let wal = Arc::new(WalImpl::new(wal_dir)?);

        Ok(Self {
            graph_dir,
            logical_to_physical,
            node_count: AtomicUsize::new(0),
            storage: Arc::new(storage),
            transaction_manager: Arc::new(TransactionManager::new()),
            wal,
        })
    }

    pub fn disk_storage_path(&self) -> Option<&Path> {
        self.graph_dir()
            .filter(|_| Extension::disk_storage_enabled())
    }

    pub fn extension(&self) -> &EXT {
        self.storage().extension()
    }

    pub fn read_event_counter(&self) -> usize {
        self.storage().read_event_id()
    }

    pub fn storage(&self) -> &Arc<Layer<EXT>> {
        &self.storage
    }

    pub fn num_layers(&self) -> usize {
        self.storage.nodes().num_layers() - 1
    }

    #[inline]
    pub fn resolve_node_ref(&self, node: NodeRef) -> Option<VID> {
        let vid = match node {
            NodeRef::Internal(vid) => Some(vid),
            NodeRef::External(GidRef::U64(gid)) => self.logical_to_physical.get_u64(gid),
            NodeRef::External(GidRef::Str(string)) => self
                .logical_to_physical
                .get_str(string)
                .or_else(|| self.logical_to_physical.get_u64(string.id())),
        }?;
        // VIDs in the resolver may not be initialised yet, need to double-check the node actually exists!
        let nodes = self.storage().nodes();
        let (page_id, pos) = nodes.resolve_pos(vid);
        let node_page = nodes.segments().get(page_id)?;
        if pos.0 < node_page.num_nodes() {
            Some(vid)
        } else {
            None
        }
    }

    #[inline]
    pub fn internal_num_nodes(&self) -> usize {
        self.logical_to_physical.len()
    }

    #[inline]
    pub fn internal_num_edges(&self) -> usize {
        self.storage.edges().num_edges_layer(0)
    }

    pub fn read_locked(self: &Arc<Self>) -> ReadLockedLayer<EXT> {
        self.storage.read_locked()
    }

    pub fn edge_meta(&self) -> &Meta {
        self.storage().edge_meta()
    }

    pub fn node_meta(&self) -> &Meta {
        self.storage().node_meta()
    }

    pub fn graph_props_meta(&self) -> &Meta {
        self.storage.graph_props_meta()
    }

    pub fn graph_dir(&self) -> Option<&Path> {
        self.graph_dir.as_ref().map(|p| p.path())
    }

    #[inline]
    pub fn graph_earliest_time(&self) -> Option<i64> {
        Some(self.storage().earliest()).filter(|t| *t != i64::MAX)
    }

    #[inline]
    pub fn graph_latest_time(&self) -> Option<i64> {
        Some(self.storage().latest()).filter(|t| *t != i64::MIN)
    }

    pub fn layer_ids(&self, key: entities::Layer) -> Result<LayerIds, InvalidLayer> {
        match key {
            entities::Layer::None => Ok(LayerIds::None),
            entities::Layer::All => Ok(LayerIds::All),
            entities::Layer::Default => Ok(LayerIds::One(1)),
            entities::Layer::One(id) => match self.edge_meta().get_layer_id(&id) {
                Some(id) => Ok(LayerIds::One(id)),
                None => Err(InvalidLayer::new(
                    id,
                    Self::get_valid_layers(self.edge_meta()),
                )),
            },
            entities::Layer::Multiple(ids) => {
                let mut new_layers = ids
                    .iter()
                    .map(|id| {
                        self.edge_meta().get_layer_id(id).ok_or_else(|| {
                            InvalidLayer::new(id.clone(), Self::get_valid_layers(self.edge_meta()))
                        })
                    })
                    .collect::<Result<Vec<_>, InvalidLayer>>()?;
                let num_layers = self.num_layers();
                let num_new_layers = new_layers.len();
                if num_new_layers == 0 {
                    Ok(LayerIds::None)
                } else if num_new_layers == 1 {
                    Ok(LayerIds::One(new_layers[0]))
                } else if num_new_layers == num_layers {
                    Ok(LayerIds::All)
                } else {
                    new_layers.sort_unstable();
                    new_layers.dedup();
                    Ok(LayerIds::Multiple(new_layers.into()))
                }
            }
        }
    }

    fn get_valid_layers(edge_meta: &Meta) -> Vec<String> {
        edge_meta
            .layer_meta()
            .keys()
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
    }

    pub fn valid_layer_ids(&self, key: entities::Layer) -> LayerIds {
        match key {
            entities::Layer::None => LayerIds::None,
            entities::Layer::All => LayerIds::All,
            entities::Layer::Default => LayerIds::One(0),
            entities::Layer::One(id) => match self.edge_meta().get_layer_id(&id) {
                Some(id) => LayerIds::One(id),
                None => LayerIds::None,
            },
            entities::Layer::Multiple(ids) => {
                let mut new_layers = ids
                    .iter()
                    .flat_map(|id| self.edge_meta().get_layer_id(id))
                    .collect::<Vec<_>>();
                let num_layers = self.num_layers();
                let num_new_layers = new_layers.len();
                if num_new_layers == 0 {
                    LayerIds::None
                } else if num_new_layers == 1 {
                    LayerIds::One(new_layers[0])
                } else if num_new_layers == num_layers {
                    LayerIds::All
                } else {
                    new_layers.sort_unstable();
                    new_layers.dedup();
                    LayerIds::Multiple(new_layers.into())
                }
            }
        }
    }

    pub fn write_locked_graph<'a>(&'a self) -> WriteLockedGraph<'a, EXT> {
        WriteLockedGraph::new(self)
    }

    pub fn update_time(&self, earliest: TimeIndexEntry) {
        // self.storage.update_time(earliest);
    }
}

/// Holds write locks across all segments in the graph for fast bulk ingestion.
pub struct WriteLockedGraph<'a, EXT>
where
    EXT: PersistenceStrategy<NS = NS<EXT>, ES = ES<EXT>, GS = GS<EXT>>,
{
    pub nodes: WriteLockedNodePages<'a, storage::NS<EXT>>,
    pub edges: WriteLockedEdgePages<'a, storage::ES<EXT>>,
    pub graph_props: WriteLockedGraphPropPages<'a, storage::GS<EXT>>,
    pub graph: &'a TemporalGraph<EXT>,
}

impl<'a, EXT: PersistenceStrategy<NS = NS<EXT>, ES = ES<EXT>, GS = GS<EXT>>>
    WriteLockedGraph<'a, EXT>
{
    pub fn new(graph: &'a TemporalGraph<EXT>) -> Self {
        WriteLockedGraph {
            nodes: graph.storage.nodes().write_locked(),
            edges: graph.storage.edges().write_locked(),
            graph_props: graph.storage.graph_props().write_locked(),
            graph,
        }
    }

    pub fn graph(&self) -> &TemporalGraph<EXT> {
        self.graph
    }

    pub fn resize_chunks_to_num_nodes(&mut self, num_nodes: usize) {
        if num_nodes == 0 {
            return;
        }
        let (chunks_needed, _) = self.graph.storage.nodes().resolve_pos(VID(num_nodes - 1));
        self.graph.storage().nodes().grow(chunks_needed + 1);
        std::mem::take(&mut self.nodes);
        self.nodes = self.graph.storage.nodes().write_locked();
    }

    pub fn resize_chunks_to_num_edges(&mut self, num_edges: usize) {
        if num_edges == 0 {
            return;
        }
        let (chunks_needed, _) = self.graph.storage.edges().resolve_pos(EID(num_edges - 1));
        self.graph.storage().edges().grow(chunks_needed + 1);
        std::mem::take(&mut self.edges);
        self.edges = self.graph.storage.edges().write_locked();
    }

    pub fn edge_stats(&self) -> &Arc<GraphStats> {
        self.graph.storage().edges().stats()
    }

    pub fn node_stats(&self) -> &Arc<GraphStats> {
        self.graph.storage().nodes().stats()
    }
}
