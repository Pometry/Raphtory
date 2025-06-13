use std::{
    ops::{Deref, DerefMut},
    path::Path,
    sync::Arc,
};

use crate::{
    error::DBV4Error,
    pages::{GraphStore, ReadLockedGraphStore},
    segments::{edge::EdgeSegmentView, node::NodeSegmentView},
};
use parking_lot::{RwLockReadGuard, RwLockWriteGuard, lock_api::ArcRwLockReadGuard};
use raphtory_api::core::{
    entities::{
        EID, VID,
        properties::{meta::Meta, prop::Prop, tprop::TPropOps},
    },
    storage::timeindex::{TimeIndexEntry, TimeIndexOps},
};
use segments::{edge::MemEdgeSegment, node::MemNodeSegment};

// pub mod loaders;
pub mod pages;
pub mod persist;
pub mod properties;
pub mod segments;

pub type NS<P> = NodeSegmentView<P>;
pub type ES<P> = EdgeSegmentView<P>;

pub type Layer<EXT> = GraphStore<NodeSegmentView<EXT>, EdgeSegmentView<EXT>, EXT>;
pub type ReadLockedLayer<EXT> = ReadLockedGraphStore<NodeSegmentView, EdgeSegmentView, EXT>;

pub trait EdgeSegmentOps: Send + Sync {
    type Extension;

    type Entry<'a>: EdgeEntryOps<'a>
    where
        Self: 'a;

    fn latest(&self) -> Option<TimeIndexEntry>;
    fn earliest(&self) -> Option<TimeIndexEntry>;

    fn t_len(&self) -> usize;

    fn load(
        page_id: usize,
        max_page_len: usize,
        meta: Arc<Meta>,
        path: impl AsRef<Path>,
        ext: Self::Extension,
    ) -> Result<Self, DBV4Error>
    where
        Self: Sized;

    fn new(
        page_id: usize,
        max_page_len: usize,
        meta: Arc<Meta>,
        path: impl AsRef<Path>,
        ext: Self::Extension,
    ) -> Self;

    fn segment_id(&self) -> usize;

    fn num_edges(&self) -> usize;

    fn head(&self) -> RwLockReadGuard<MemEdgeSegment>;

    fn head_arc(&self) -> ArcRwLockReadGuard<parking_lot::RawRwLock, MemEdgeSegment>;

    fn head_mut(&self) -> RwLockWriteGuard<MemEdgeSegment>;

    fn try_head_mut(&self) -> Option<RwLockWriteGuard<MemEdgeSegment>>;

    fn notify_write(
        &self,
        head_lock: impl DerefMut<Target = MemEdgeSegment>,
    ) -> Result<(), DBV4Error>;

    fn increment_num_edges(&self) -> usize;

    fn contains_edge(
        &self,
        edge_pos: LocalPOS,
        locked_head: impl Deref<Target = MemEdgeSegment>,
    ) -> bool;

    fn get_edge(
        &self,
        edge_pos: LocalPOS,
        locked_head: impl Deref<Target = MemEdgeSegment>,
    ) -> Option<(VID, VID)>;

    fn entry<'a, LP: Into<LocalPOS>>(&'a self, edge_pos: LP) -> Self::Entry<'a>;

    fn locked(self: &Arc<Self>) -> ReadLockedES<Self>
    where
        Self: Sized,
    {
        ReadLockedES {
            es: self.clone(),
            head: self.head_arc(),
        }
    }
}

#[derive(Debug)]
pub struct ReadLockedES<ES> {
    es: Arc<ES>,
    head: ArcRwLockReadGuard<parking_lot::RawRwLock, MemEdgeSegment>,
}

pub trait EdgeEntryOps<'a> {
    type Ref<'b>: EdgeRefOps<'b>
    where
        'a: 'b,
        Self: 'b;

    fn as_ref<'b>(&'b self) -> Self::Ref<'b>
    where
        'a: 'b;
}

pub trait EdgeRefOps<'a>: Copy + Clone + Send + Sync {
    type Additions: TimeIndexOps<'a>;
    type TProps: TPropOps<'a>;

    fn edge(self) -> Option<(VID, VID)>;

    fn additions(self) -> Self::Additions;

    fn c_prop(self, prop_id: usize) -> Option<Prop>;

    fn t_prop(self, prop_id: usize) -> Self::TProps;
}

pub trait NodeSegmentOps: Send + Sync {
    type Extension;

    type Entry<'a>: NodeEntryOps<'a>
    where
        Self: 'a;

    fn latest(&self) -> Option<TimeIndexEntry>;
    fn earliest(&self) -> Option<TimeIndexEntry>;

    fn t_len(&self) -> usize;

    fn load(
        page_id: usize,
        max_page_len: usize,
        meta: Arc<Meta>,
        path: impl AsRef<Path>,
        ext: Self::Extension,
    ) -> Result<Self, DBV4Error>
    where
        Self: Sized;
    fn new(
        page_id: usize,
        max_page_len: usize,
        meta: Arc<Meta>,
        path: impl AsRef<Path>,
        ext: Self::Extension,
    ) -> Self;

    fn segment_id(&self) -> usize;

    fn head_arc(&self) -> ArcRwLockReadGuard<parking_lot::RawRwLock, MemNodeSegment>;
    fn head(&self) -> RwLockReadGuard<MemNodeSegment>;

    fn head_mut(&self) -> RwLockWriteGuard<MemNodeSegment>;

    fn num_nodes(&self) -> usize;

    fn increment_num_nodes(&self) -> usize;

    fn notify_write(
        &self,
        head_lock: impl DerefMut<Target = MemNodeSegment>,
    ) -> Result<(), DBV4Error>;

    fn check_node(&self, pos: LocalPOS, layer_id: usize) -> bool;

    fn get_out_edge(
        &self,
        pos: LocalPOS,
        dst: impl Into<VID>,
        layer_id: usize,
        locked_head: impl Deref<Target = MemNodeSegment>,
    ) -> Option<EID>;

    fn get_inb_edge(
        &self,
        pos: LocalPOS,
        src: impl Into<VID>,
        layer_id: usize,
        locked_head: impl Deref<Target = MemNodeSegment>,
    ) -> Option<EID>;

    fn entry<'a>(&'a self, pos: impl Into<LocalPOS>) -> Self::Entry<'a>;

    fn locked(self: &Arc<Self>) -> ReadLockedNS<Self>
    where
        Self: Sized,
    {
        ReadLockedNS {
            ns: self.clone(),
            head: self.head_arc(),
        }
    }
}

#[derive(Debug)]
pub struct ReadLockedNS<NS> {
    ns: Arc<NS>,
    head: ArcRwLockReadGuard<parking_lot::RawRwLock, MemNodeSegment>,
}

pub trait NodeEntryOps<'a> {
    type Ref<'b>: NodeRefOps<'b>
    where
        'a: 'b,
        Self: 'b;

    fn as_ref<'b>(&'b self) -> Self::Ref<'b>
    where
        'a: 'b;
}

pub trait NodeRefOps<'a>: Copy + Clone + Send + Sync {
    type Additions: TimeIndexOps<'a>;

    type TProps: TPropOps<'a>;

    fn out_edges(self, layer_id: usize) -> impl Iterator<Item = (VID, EID)> + 'a;

    fn inb_edges(self, layer_id: usize) -> impl Iterator<Item = (VID, EID)> + 'a;

    fn out_edges_sorted(self, layer_id: usize) -> impl Iterator<Item = (VID, EID)> + 'a;

    fn inb_edges_sorted(self, layer_id: usize) -> impl Iterator<Item = (VID, EID)> + 'a;

    fn out_nbrs(self, layer_id: usize) -> impl Iterator<Item = VID> + 'a
    where
        Self: Sized,
    {
        self.out_edges(layer_id).map(|(v, _)| v)
    }

    fn inb_nbrs(self, layer_id: usize) -> impl Iterator<Item = VID> + 'a
    where
        Self: Sized,
    {
        self.inb_edges(layer_id).map(|(v, _)| v)
    }

    fn out_nbrs_sorted(self, layer_id: usize) -> impl Iterator<Item = VID> + 'a
    where
        Self: Sized,
    {
        self.out_edges_sorted(layer_id).map(|(v, _)| v)
    }

    fn inb_nbrs_sorted(self, layer_id: usize) -> impl Iterator<Item = VID> + 'a
    where
        Self: Sized,
    {
        self.inb_edges_sorted(layer_id).map(|(v, _)| v)
    }

    fn additions(self, layer_id: usize) -> Self::Additions;

    fn c_prop(self, layer_id: usize, prop_id: usize) -> Option<Prop>;

    fn t_prop(self, layer_id: usize, prop_id: usize) -> Self::TProps;
}

pub mod error {
    use std::{path::PathBuf, sync::Arc};

    use raphtory_api::core::entities::properties::prop::PropError;
    use raphtory_core::utils::time::ParseTimeError;

    #[derive(thiserror::Error, Debug)]
    pub enum DBV4Error {
        #[error("External Storage Error {0}")]
        External(#[from] Arc<dyn std::error::Error + Send + Sync>),
        #[error("IO error: {0}")]
        IO(#[from] std::io::Error),
        #[error("Serde error: {0}")]
        Serde(#[from] serde_json::Error),
        // #[error("Load error: {0}")]
        // LoadError(#[from] LoadError),
        #[error("Arrow-rs error: {0}")]
        ArrowRS(#[from] arrow_schema::ArrowError),
        #[error("Parquet error: {0}")]
        Parquet(#[from] parquet::errors::ParquetError),

        #[error("Property error: {0}")]
        PropError(#[from] PropError),
        #[error("Empty Graph: {0}")]
        EmptyGraphDir(PathBuf),
        #[error("Failed to parse time string")]
        ParseTime {
            #[from]
            source: ParseTimeError,
        },
        // #[error("Failed to mutate: {0}")]
        // MutationError(#[from] MutationError),
        #[error("Unnamed Failure: {0}")]
        GenericFailure(String),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct LocalPOS(pub usize);

impl LocalPOS {
    pub fn as_vid(self, page_id: usize, max_page_len: usize) -> VID {
        VID(page_id * max_page_len + self.0)
    }

    pub fn as_eid(self, page_id: usize, max_page_len: usize) -> EID {
        EID(page_id * max_page_len + self.0)
    }
}

impl From<usize> for LocalPOS {
    fn from(pos: usize) -> Self {
        Self(pos)
    }
}

pub fn calculate_size_recursive(path: &Path) -> Result<usize, std::io::Error> {
    let mut size = 0;
    if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                size += calculate_size_recursive(&path)?;
            } else {
                size += path.metadata()?.len() as usize;
            }
        }
    } else {
        size += path.metadata()?.len() as usize;
    }
    Ok(size)
}
