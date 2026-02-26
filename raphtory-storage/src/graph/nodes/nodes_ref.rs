use super::node_ref::NodeStorageRef;
use crate::graph::variants::storage_variants3::StorageVariants3;
use raphtory_api::core::entities::VID;
use raphtory_core::entities::LayerIds;
use rayon::iter::ParallelIterator;
use storage::{Extension, ReadLockedNodes};

#[derive(Debug)]
pub enum NodesStorageEntry<'a> {
    Mem(&'a ReadLockedNodes<Extension>),
    Unlocked(ReadLockedNodes<Extension>),
}

macro_rules! for_all_variants {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            NodesStorageEntry::Mem($pattern) => StorageVariants3::Mem($result),
            NodesStorageEntry::Unlocked($pattern) => StorageVariants3::Unlocked($result),
        }
    };
}

impl<'a> NodesStorageEntry<'a> {
    pub fn node(&self, vid: VID) -> NodeStorageRef<'_> {
        match self {
            NodesStorageEntry::Mem(store) => store.node_ref(vid),
            NodesStorageEntry::Unlocked(store) => store.node_ref(vid),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            NodesStorageEntry::Mem(store) => store.len(),
            NodesStorageEntry::Unlocked(store) => store.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn par_iter(&self) -> impl ParallelIterator<Item = NodeStorageRef<'_>> {
        for_all_variants!(self, nodes => nodes.par_iter())
    }

    pub fn layer_iter(self, layers: LayerIds) -> impl Iterator<Item = VID> {
        for_all_variants!(self, nodes => nodes.layer_iter(layers))
    }

    /// Returns a parallel iterator over nodes row groups
    /// the (usize) part is the row group not the segment
    pub fn row_groups_par_iter(
        &self,
    ) -> impl ParallelIterator<Item = (usize, impl Iterator<Item = VID> + '_)> {
        for_all_variants!(self, nodes => nodes.row_groups_par_iter())
    }
}
