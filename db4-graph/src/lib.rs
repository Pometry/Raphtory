use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{self, AtomicU64, AtomicUsize},
        Arc,
    },
};

use raphtory_api::core::{
    entities::{self, properties::meta::Meta},
    input::input_node::InputNode,
};
use raphtory_core::{
    entities::{
        graph::tgraph::InvalidLayer, nodes::node_ref::NodeRef, properties::graph_meta::GraphMeta,
        GidRef, LayerIds, EID, VID,
    },
    storage::timeindex::TimeIndexEntry,
};
use storage::{
    pages::{
        layer_counter::GraphStats,
        locked::{edges::WriteLockedEdgePages, nodes::WriteLockedNodePages},
    },
    persist::strategy::PersistentStrategy,
    resolver::GIDResolverOps,
    wal::{TransactionID, WalOps},
    Extension, GIDResolver, Layer, ReadLockedLayer, Wal, ES, NS,
};
use tempfile::TempDir;

pub mod entries;
pub mod mutation;

const DEFAULT_MAX_PAGE_LEN_NODES: usize = 10;
const DEFAULT_MAX_PAGE_LEN_EDGES: usize = 10;

#[derive(Debug)]
pub struct TemporalGraph<EXT = Extension> {
    // mapping between logical and physical ids
    pub logical_to_physical: GIDResolver,
    pub node_count: AtomicUsize,
    storage: Arc<Layer<EXT>>,
    pub graph_meta: Arc<GraphMeta>,
    graph_dir: GraphDir,
    pub transaction_manager: Arc<TransactionManager>,
    pub wal: Arc<Wal>,
}

#[derive(Debug)]
pub enum GraphDir {
    Temp(TempDir),
    Path(PathBuf),
}

impl<'a> From<&'a Path> for GraphDir {
    fn from(path: &'a Path) -> Self {
        GraphDir::Path(path.to_path_buf())
    }
}

impl Default for GraphDir {
    fn default() -> Self {
        GraphDir::Temp(tempfile::tempdir().expect("Failed to create temporary directory"))
    }
}

impl AsRef<Path> for GraphDir {
    fn as_ref(&self) -> &Path {
        match self {
            GraphDir::Temp(temp_dir) => temp_dir.path(),
            GraphDir::Path(path) => path,
        }
    }
}

#[derive(Debug)]
pub struct TransactionManager {
    last_transaction_id: AtomicU64,
}

impl TransactionManager {
    const STARTING_TRANSACTION_ID: TransactionID = 0;

    pub fn new() -> Self {
        Self {
            last_transaction_id: AtomicU64::new(Self::STARTING_TRANSACTION_ID),
        }
    }

    pub fn begin(&self) -> TransactionID {
        self.last_transaction_id
            .fetch_add(1, atomic::Ordering::SeqCst)
    }
}

impl Default for TemporalGraph<Extension> {
    fn default() -> Self {
        Self::new()
    }
}

impl<EXT: PersistentStrategy<NS = NS<EXT>, ES = ES<EXT>>> TemporalGraph<EXT> {
    pub fn new() -> Self {
        let node_meta = Meta::new();
        let edge_meta = Meta::new();
        Self::new_with_meta(GraphDir::default(), node_meta, edge_meta)
    }

    pub fn new_with_path(path: impl AsRef<Path>) -> Self {
        let node_meta = Meta::new();
        let edge_meta = Meta::new();
        Self::new_with_meta(path.as_ref().into(), node_meta, edge_meta)
    }

    pub fn load_from_path(path: impl AsRef<Path>) -> Self {
        let graph_dir: GraphDir = path.as_ref().into();
        let storage = Layer::load(graph_dir.as_ref())
            .unwrap_or_else(|_| panic!("Failed to load graph from path: {graph_dir:?}"));

        let gid_resolver_dir = graph_dir.as_ref().join("gid_resolver");
        let resolver = GIDResolver::new(&gid_resolver_dir).unwrap_or_else(|_| {
            panic!("Failed to load GID resolver from path: {gid_resolver_dir:?}")
        });

        let node_count = AtomicUsize::new(storage.nodes().num_nodes());
        let wal_dir = graph_dir.as_ref().join("wal");
        let wal = Wal::new(&wal_dir).unwrap();

        Self {
            graph_dir,
            logical_to_physical: resolver,
            node_count,
            storage: Arc::new(storage),
            graph_meta: Arc::new(GraphMeta::default()),
            transaction_manager: Arc::new(TransactionManager::new()),
            wal: Arc::new(wal),
        }
    }

    pub fn new_with_meta(graph_dir: GraphDir, node_meta: Meta, edge_meta: Meta) -> Self {
        edge_meta.get_or_create_layer_id(Some("static_graph"));
        std::fs::create_dir_all(&graph_dir)
            .unwrap_or_else(|_| panic!("Failed to create graph directory at {graph_dir:?}"));

        let gid_resolver_dir = graph_dir.as_ref().join("gid_resolver");
        let storage: Layer<EXT> = Layer::new_with_meta(
            graph_dir.as_ref(),
            DEFAULT_MAX_PAGE_LEN_NODES,
            DEFAULT_MAX_PAGE_LEN_EDGES,
            node_meta,
            edge_meta,
        );

        let wal_dir = graph_dir.as_ref().join("wal");
        let wal = Wal::new(&wal_dir).unwrap();

        Self {
            graph_dir,
            logical_to_physical: GIDResolver::new(gid_resolver_dir).unwrap(),
            node_count: AtomicUsize::new(0),
            storage: Arc::new(storage),
            graph_meta: Arc::new(GraphMeta::default()),
            transaction_manager: Arc::new(TransactionManager::new()),
            wal: Arc::new(wal),
        }
    }

    pub fn read_event_counter(&self) -> usize {
        self.storage().read_event_id()
    }

    pub fn storage(&self) -> &Arc<Layer<EXT>> {
        &self.storage
    }

    pub fn graph_meta(&self) -> &Arc<GraphMeta> {
        &self.graph_meta
    }

    pub fn num_layers(&self) -> usize {
        self.storage.nodes().num_layers() - 1
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

    pub fn next_event_id(&self) -> usize {
        self.storage().next_event_id()
    }

    #[inline]
    pub fn internal_num_nodes(&self) -> usize {
        self.storage.nodes().layer_num_nodes(0)
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

    pub fn graph_dir(&self) -> &Path {
        self.graph_dir.as_ref()
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
            .get_keys()
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

pub struct WriteLockedGraph<'a, EXT> {
    pub nodes: WriteLockedNodePages<'a, storage::NS<EXT>>,
    pub edges: WriteLockedEdgePages<'a, storage::ES<EXT>>,
    pub graph: &'a TemporalGraph<EXT>,
}

impl<'a, EXT: PersistentStrategy<NS = NS<EXT>, ES = ES<EXT>>> WriteLockedGraph<'a, EXT> {
    pub fn new(graph: &'a TemporalGraph<EXT>) -> Self {
        WriteLockedGraph {
            nodes: graph.storage.nodes().write_locked(),
            edges: graph.storage.edges().write_locked(),
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
