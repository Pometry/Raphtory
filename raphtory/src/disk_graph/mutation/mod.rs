use std::{
    hash::Hash,
    ops::Deref,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{
    core::{
        entities::{
            graph::tgraph,
            nodes::node_ref::{AsNodeRef, NodeRef},
        },
        utils::errors::GraphError,
        Prop,
    },
    db::api::{
        mutation::internal::InternalAdditionOps, storage::graph::storage_ops::GraphStorage,
        view::internal::CoreGraphOps,
    },
};
use arc_swap::ArcSwap;
use parking_lot::{Mutex, RwLock, RwLockReadGuard};
use pometry_storage::{graph::TemporalGraph, merge::merge_graph::merge_graphs, GidRef, GID};
use raphtory_api::core::{
    entities::{EID, VID},
    storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry, FxDashMap},
    PropType,
};
use tempfile::TempDir;

use super::graph_impl::prop_conversion::make_node_properties_from_graph;

pub struct MutDiskGraph {
    current: ArcSwap<MappedStorage>,
    disk: ArcSwap<Disk>,

    merging: Mutex<()>,

    to_disk_strategy: Arc<dyn Fn(&GraphStorage) -> bool>,
    graph_dir: PathBuf,
    graph_count: AtomicUsize,
}

#[derive(Clone)]
struct MappedStorage {
    storage: GraphStorage,
    node_count: Arc<AtomicUsize>,
    // local to global mappings
    node_index: SyncIndex<VID, VID>,
    edge_index: SyncIndex<EID, EID>,
}

#[derive(Clone, Default)]
struct SyncIndex<K: Eq + Hash, Index> {
    keys: Arc<RwLock<Vec<K>>>,
    index: FxDashMap<K, Index>,
}

impl<K: Eq + Hash + Default + Copy, I: Copy> SyncIndex<K, I>
where
    usize: From<I>,
{
    fn get(&self, key: &K) -> Option<impl Deref<Target = I> + use<'_, K, I>> {
        self.index.get(key)
    }

    fn index(&self, index: I) -> impl Deref<Target = K> + use<'_, K, I> {
        let i = usize::from(index);
        RwLockReadGuard::map(self.keys.read(), |keys| &keys[i])
    }

    fn insert(&self, key: K, index: I) {
        let mut keys = self.keys.write();
        let i = usize::from(index);
        keys.resize_with(i + 1, Default::default);
        keys[i] = key;
        self.index.insert(key, index);
    }
}

impl MappedStorage {
    pub fn from_empty_storage(storage: GraphStorage, node_count: Arc<AtomicUsize>) -> Self {
        assert!(!storage.is_immutable() && storage.unfiltered_num_nodes() == 0);
        Self {
            storage,
            node_index: SyncIndex::default(),
            edge_index: SyncIndex::default(),
            node_count,
        }
    }

    fn inner(&self) -> &GraphStorage {
        &self.storage
    }

    pub fn lock(&self) -> Self {
        Self {
            storage: self.storage.lock(),
            node_index: self.node_index.clone(),
            edge_index: self.edge_index.clone(),
            node_count: Arc::new(AtomicUsize::new(self.node_count.load(Ordering::Relaxed))),
        }
    }

    // This returns internal node id valid for the current MappedStorage
    pub fn resolve_node(&self, id: NodeRef, lookup: &Disk) -> Result<MaybeNew<VID>, GraphError> {
        match id {
            NodeRef::Internal(global_vid) => match self.node_index.get(&global_vid) {
                Some(local_vid) => Ok(MaybeNew::Existing(*local_vid)),
                None => {
                    let gid = lookup.node_gid(global_vid).unwrap();
                    let local_id = self.inner().resolve_node(gid)?;
                    self.node_index.insert(global_vid, local_id.inner());
                    Ok(local_id)
                }
            },
            NodeRef::External(_) => {
                // disk doesn't know this node_ref
                let internal_id = self.inner().resolve_node(id)?;
                let global_vid = self.node_count.fetch_add(1, Ordering::Relaxed);
                self.node_index.insert(VID(global_vid), internal_id.inner());
                Ok(internal_id)
            }
        }
    }

    pub fn node_id(&self, vid: VID) -> Option<GID> {
        let local_vid = self.node_index.get(&vid)?;
        Some( self.inner().node_id(*local_vid) )
    }
}

enum Disk {
    Empty,
    PreMerge {
        frozen: MappedStorage,
        disk: Option<Arc<TemporalGraph>>,
    },
    PostMerge {
        disk: Arc<TemporalGraph>,
    },
}

impl Disk {
    fn pre_merge(&self, frozen: MappedStorage) -> Self {
        match self {
            Disk::Empty => Disk::PreMerge { frozen, disk: None },
            Disk::PostMerge { disk } => Disk::PreMerge {
                frozen,
                disk: Some(disk.clone()),
            },
            _ => panic!("Cannot pre-merge a disk that is already in a merge state"),
        }
    }

    fn post_merge(&self, disk: Arc<TemporalGraph>) -> Self {
        match self {
            Disk::PreMerge { .. } => Disk::PostMerge { disk },
            _ => panic!("Cannot post-merge a disk that is not in a pre-merge state"),
        }
    }

    // fn find_node(&self, node: NodeRef) -> Option<VID> {
    //     match self {
    //         Disk::Empty => None,
    //         Disk::PreMerge { frozen, disk } => disk
    //             .as_ref()
    //             .and_then(|disk| find_node_ref(node, disk))
    //             .or_else(|| frozen.find_node(node)),
    //         Disk::PostMerge { disk } => find_node_ref(node, disk),
    //     }
    // }

    fn node_gid(&self, vid: VID) -> Option<GID> {
        match self {
            Disk::Empty => None,
            Disk::PreMerge { frozen, disk } => disk
                .as_ref()
                .and_then(|disk| disk.node_gid(vid).map(|g| g.into()))
                .or_else(|| frozen.node_id(vid)),
            Disk::PostMerge { disk } => disk.node_gid(vid).map(|g| g.into()),
        }
    }
}

fn find_node_ref(node: NodeRef<'_>, disk: &Arc<TemporalGraph>) -> Option<VID> {
    match node {
        NodeRef::External(gid) => disk.find_node(gid),
        NodeRef::Internal(vid) => disk.node_gid(vid).map(|_| vid),
    }
}

impl MutDiskGraph {
    pub fn new(graph: GraphStorage, graph_dir: impl AsRef<Path>) -> Self {
        Self {
            current: ArcSwap::new(
                MappedStorage::from_empty_storage(graph, Arc::new(AtomicUsize::new(0))).into(),
            ),
            disk: ArcSwap::new(Disk::Empty.into()),
            merging: Mutex::new(()),
            to_disk_strategy: Arc::new(|gs| gs.unfiltered_num_nodes() > 1), // testing
            graph_dir: graph_dir.as_ref().to_path_buf(),
            graph_count: AtomicUsize::new(0),
        }
    }

    fn spill_to_disk(&self) -> Result<(), GraphError> {
        // first check if we are already merging
        match self.merging.try_lock() {
            None => return Ok(()),
            Some(_guard) => {
                // TODO: spawn a thread to do the swap of current, frozen, disk spill, merge
                // no more writes possible
                let freeze = self.current.load().lock();
                let node_count = freeze.node_count.clone();
                let disk = self.disk.load();
                self.disk.store(disk.pre_merge(freeze.clone()).into());

                // set the current to a new empty graph

                let mut tg = tgraph::TemporalGraph::default();
                tg.node_meta = freeze.inner().node_meta().clone();
                tg.edge_meta = freeze.inner().edge_meta().clone();
                tg.graph_meta = freeze.inner().graph_meta().deep_clone();

                let unlocked_storage = GraphStorage::Unlocked(Arc::new(tg));
                self.current
                    .store(MappedStorage::from_empty_storage(unlocked_storage, node_count).into());

                // Dump the current frozen graph to disk
                let graph_dir = TempDir::new_in(&self.graph_dir)?;
                let t_graph = TemporalGraph::from_graph(freeze.inner(), &graph_dir, || {
                    make_node_properties_from_graph(freeze.inner(), &graph_dir)
                })?;

                // merge the graphs if there is already a disk graph
                let graph_dir = self.graph_dir.join(format!(
                    "graph_{}",
                    self.graph_count
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                ));
                let new_disk_graph = if let Disk::PreMerge {
                    disk: Some(right), ..
                } = self.disk.load().as_ref()
                {
                    merge_graphs(&graph_dir, &t_graph, right)?
                } else {
                    t_graph
                };

                self.disk
                    .store(self.disk.load().post_merge(new_disk_graph.into()).into());
            }
        }
        Ok(())
    }

    fn resolve_node<V: AsNodeRef>(&self, id: V) -> Result<MaybeNew<VID>, GraphError> {
        match id.as_node_ref() {
            NodeRef::Internal(vid) => self.current.load(),
            NodeRef::External(gid_ref) => todo!(),
        }
    }

    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        todo!()
    }

    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<MaybeNew<EID>, GraphError> {
        todo!()
    }

    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        todo!()
    }
}

#[cfg(test)]
mod test {}
