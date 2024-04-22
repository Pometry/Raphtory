use crate::{
    core::{entities::edges::edge_store::EdgeStore, storage::Entry},
    db::api::storage::{arrow::edges::ArrowEdge, edges::edge_ref::EdgeStorageRef},
};

#[derive(Debug)]
pub enum EdgeStorageEntry<'a> {
    Mem(Entry<'a, EdgeStore>),
    #[cfg(feature = "arrow")]
    Arrow(ArrowEdge<'a>),
}

impl<'a> EdgeStorageEntry<'a> {
    pub fn as_ref(&self) -> EdgeStorageRef {
        match self {
            EdgeStorageEntry::Mem(edge) => EdgeStorageRef::Mem(edge),
            #[cfg(feature = "arrow")]
            EdgeStorageEntry::Arrow(edge) => EdgeStorageRef::Arrow(*edge),
        }
    }
}
