use std::{path::{Path, PathBuf}, sync::{atomic::AtomicUsize, Arc}};

use crate::{
    core::{entities::graph::tgraph, utils::errors::GraphError, Prop},
    db::api::{
        mutation::internal::InternalAdditionOps,
        storage::graph::storage_ops::GraphStorage,
        view::internal::{CoreGraphOps, NodeFilterOps},
    },
    prelude::GraphViewOps,
};
use arc_swap::{ArcSwap, ArcSwapOption};
use parking_lot::Mutex;
use pometry_storage::{graph::TemporalGraph, merge::merge_graph::merge_graphs, properties::Properties};
use raphtory_api::core::{entities::VID, storage::timeindex::TimeIndexEntry};
use rayon::ThreadPool;
use tempfile::TempDir;

use super::{graph_impl::prop_conversion::make_node_properties_from_graph, DiskGraphStorage};

pub struct MutDiskGraph {
    current: ArcSwap<GraphStorage>,
    disk: ArcSwap<Disk>,

    merging: Mutex<()>,

    to_disk_strategy: Arc<dyn Fn(&GraphStorage) -> bool>,
    graph_dir: PathBuf,
    graph_count: AtomicUsize,
}

enum Disk{
    Empty,
    PreMerge{
        frozen: GraphStorage,
        disk: Option<Arc<TemporalGraph>>,
    },
    PostMerge{
        disk: Arc<TemporalGraph>,
    }
}

impl Disk {
    fn pre_merge(&self, frozen: GraphStorage) -> Self {
        match self {
            Disk::Empty => Disk::PreMerge {
                frozen,
                disk: None,
            },
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
}


impl MutDiskGraph {
    pub fn new(graph: GraphStorage, graph_dir: impl AsRef<Path>) -> Self {
        assert!(!graph.is_immutable());
        Self {
            current: ArcSwap::new(graph.into()),
            disk: ArcSwap::new(Disk::Empty.into()),
            merging: Mutex::new(()),
            to_disk_strategy: Arc::new(|gs| gs.unfiltered_num_nodes() > 1), // testing
            graph_dir: graph_dir.as_ref().to_path_buf(),
            graph_count: AtomicUsize::new(0),
        }
    }

    pub fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.current.load().internal_add_node(t, v, props)?;

        if (self.to_disk_strategy)(&self.current.load()) {
            self.spill_to_disk()?;
        }
        Ok(())
    }

    fn spill_to_disk(&self) -> Result<(), GraphError> {
        // first check if we are already merging
        match self.merging.try_lock() {
            None => return Ok(()),
            Some(_guard) => { // TODO: spawn a thread to do the swap of current, frozen, disk spill, merge
                // no more writes possible
                let freeze = self.current.load().lock(); 
                let disk = self.disk.load();
                self.disk.store(disk.pre_merge(freeze.clone()).into());

                // set the current to a new empty graph
                self.current.store(Arc::new(GraphStorage::default()));

                // Dump the current frozen graph to disk
                let graph_dir = TempDir::new_in(&self.graph_dir)?;
                let t_graph = TemporalGraph::from_graph(&freeze, &graph_dir, || {
                    make_node_properties_from_graph(&freeze, &graph_dir)
                })?;

                // merge the graphs if there is already a disk graph
                let graph_dir = self.graph_dir.join(format!("graph_{}", self.graph_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst)));
                let new_disk_graph = if let Disk::PreMerge {  disk:Some(right), .. } = self.disk.load().as_ref() {
                    merge_graphs(&graph_dir, &t_graph, right)?
                } else {
                    t_graph
                };

                self.disk.store(self.disk.load().post_merge(new_disk_graph.into()).into()); 
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {}
