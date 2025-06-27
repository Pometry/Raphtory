use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{self, AtomicUsize},
        Arc,
    },
};

// use crate::entries::node::UnlockedNodeEntry;
use raphtory_api::core::{
    entities::{self, properties::meta::Meta},
    input::input_node::InputNode,
    storage::dict_mapper::MaybeNew,
};
use raphtory_core::{
    entities::{
        graph::{
            logical_to_physical::{InvalidNodeId, Mapping},
            tgraph::InvalidLayer,
            timer::{MinCounter, TimeCounterTrait},
        },
        nodes::node_ref::NodeRef,
        properties::graph_meta::GraphMeta,
        GidRef, LayerIds, VID,
    },
    storage::timeindex::{AsTime, TimeIndexEntry},
};
use storage::{
    pages::locked::{edges::WriteLockedEdgePages, nodes::WriteLockedNodePages},
    persist::strategy::PersistentStrategy,
    resolver::{GIDResolverError, GIDResolverOps},
    Extension, GIDResolver, Layer,
    ReadLockedLayer, ES, NS
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

    earliest: MinCounter,
    latest: MinCounter,
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

    pub fn layer_ids(&self, key: entities::Layer) -> Result<LayerIds, InvalidLayer> {
        match key {
            entities::Layer::None => Ok(LayerIds::None),
            entities::Layer::All => Ok(LayerIds::All),
            entities::Layer::Default => Ok(LayerIds::One(0)),
            entities::Layer::One(id) => match self.edge_meta.get_layer_id(&id) {
                Some(id) => Ok(LayerIds::One(id)),
                None => Err(InvalidLayer::new(
                    id,
                    Self::get_valid_layers(&self.edge_meta),
                )),
            },
            entities::Layer::Multiple(ids) => {
                let mut new_layers = ids
                    .iter()
                    .map(|id| {
                        self.edge_meta.get_layer_id(id).ok_or_else(|| {
                            InvalidLayer::new(id.clone(), Self::get_valid_layers(&self.edge_meta))
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
            entities::Layer::One(id) => match self.edge_meta.get_layer_id(&id) {
                Some(id) => LayerIds::One(id),
                None => LayerIds::None,
            },
            entities::Layer::Multiple(ids) => {
                let mut new_layers = ids
                    .iter()
                    .flat_map(|id| self.edge_meta.get_layer_id(id))
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
}

pub struct WriteLockedGraph<'a, EXT> {
    pub nodes: WriteLockedNodePages<'a, storage::NS<EXT>>,
    pub edges: WriteLockedEdgePages<'a, storage::ES<EXT>>,
    pub graph: &'a TemporalGraph<EXT>,
    pub num_nodes: Arc<AtomicUsize>,
}

impl<'a, EXT: PersistentStrategy<NS = NS<EXT>, ES = ES<EXT>>> WriteLockedGraph<'a, EXT> {
    pub fn new(graph: &'a TemporalGraph<EXT>) -> Self {
        WriteLockedGraph {
            nodes: graph.storage.nodes().write_locked().into(),
            edges: graph.storage.edges().write_locked().into(),
            graph,
            num_nodes: Arc::new(AtomicUsize::new(graph.internal_num_nodes())),
        }
    }

    pub fn resolve_node(&self, gid: GidRef) -> Result<MaybeNew<VID>, InvalidNodeId> {
        let result = self.graph.logical_to_physical.get_or_init(gid, || {
            VID(self.num_nodes.fetch_add(1, atomic::Ordering::Relaxed))
        });

        match result {
            Ok(vid) => Ok(vid),
            Err(GIDResolverError::Database(e)) => panic!("Database error: {}", e),
            Err(GIDResolverError::InvalidNodeId(e)) => Err(e),
        }
    }

    pub fn num_nodes(&self) -> usize {
        self.num_nodes.load(atomic::Ordering::Relaxed)
    }

    pub fn resolve_node_type(&self, node_type: Option<&str>) -> MaybeNew<usize> {
        node_type
            .map(|node_type| self.graph.node_meta.get_or_create_node_type_id(node_type))
            .unwrap_or_else(|| MaybeNew::Existing(0))
    }

    pub fn resize_chunks_to_num_nodes(&mut self) {
        let num_nodes = self.num_nodes();
        self.graph.storage().nodes().grow_to_num_nodes(num_nodes);
        std::mem::take(&mut self.nodes);
        self.nodes = self.graph.storage.nodes().write_locked().into();
    }

    #[inline]
    pub fn update_time(&self, time: TimeIndexEntry) {
        let t = time.t();
        self.graph.earliest.update(t);
        self.graph.latest.update(t);
    }
}
