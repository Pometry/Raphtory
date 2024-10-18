use crate::core::storage::raw_edges::EdgeRGuard;

use super::{edge_ref::EdgeStorageRef, mem_edge::MemEdge};

#[derive(Debug)]
pub enum EdgeStorageEntry<'a> {
    Mem(MemEdge<'a>),
    Unlocked(EdgeRGuard<'a>),
    #[cfg(feature = "storage")]
    Disk(DiskEdge<'a>),
}

impl<'a> EdgeStorageEntry<'a> {
    #[inline]
    pub fn as_ref(&self) -> EdgeStorageRef {
        match self {
            EdgeStorageEntry::Mem(edge) => EdgeStorageRef::Mem(*edge),
            EdgeStorageEntry::Unlocked(edge) => EdgeStorageRef::Mem(edge.as_mem_edge()),
            #[cfg(feature = "storage")]
            EdgeStorageEntry::Disk(edge) => EdgeStorageRef::Disk(*edge),
        }
    }
}
