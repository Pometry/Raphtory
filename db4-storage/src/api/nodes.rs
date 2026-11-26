use crate::{
    LocalPOS,
    error::StorageError,
    generic_time_ops::LayerIter,
    pages::node_store::increment_and_clamp,
    segments::node::segment::MemNodeSegment,
    utils::{Iter3, Iter4},
    wal::LSN,
};
use itertools::Itertools;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use raphtory_api::{
    ArcRwLockReadGuard, ArcRwLockWriteGuard,
    core::{
        Direction,
        entities::{
            LayerId,
            properties::{
                meta::{Meta, NODE_ID_PROP_ID, NODE_TYPE_PROP_ID, STATIC_GRAPH_LAYER_ID},
                prop::{Prop, PropRef, PropUnwrap, prop_hashable::HashableProp},
                tprop::TPropOps,
            },
        },
    },
    iter::IntoDynBoxed,
};
use raphtory_api_macros::box_on_debug_lifetime;
use raphtory_core::{
    entities::{EID, GidRef, LayerIds, VID, edges::edge_ref::EdgeRef},
    storage::timeindex::{EventTime, TimeIndexOps},
    utils::iter::GenLockedIter,
};
use raphtory_itertools::FastMergeExt;
use rayon::prelude::*;
use std::{
    borrow::Cow,
    collections::HashSet,
    fmt::Debug,
    ops::{Deref, DerefMut, Range},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

/// A property predicate a storage backend may resolve to candidate rows via a
/// secondary index. String operators use the semantics of the corresponding
/// `str` methods; comparisons use the property value's natural order.
#[derive(Debug, Clone, Copy)]
pub enum PropPredicate<'a> {
    Eq(&'a Prop),
    In(&'a HashSet<HashableProp>),
    Lt(&'a Prop),
    Le(&'a Prop),
    Gt(&'a Prop),
    Ge(&'a Prop),
    StartsWith(&'a str),
    EndsWith(&'a str),
    Contains(&'a str),
}

/// Which value(s) of a property a [`PropPredicate`] is asked about. A
/// historical index can answer `Ever` exactly; `Latest` answers are supersets
/// unless the backend tracks latest values separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropSemantics {
    /// The property's latest value must match.
    Latest,
    /// Any value the property ever held may match.
    Ever,
}

/// Which node property columns an index build considers.
///
/// Named columns are configured by property *name* and persisted, so a
/// selection can name a property that does not exist yet; the names are
/// resolved to prop ids once per build, which is what `columns` carries. A
/// property left out is simply not indexed — filters over it fall back to a
/// scan and must still be correct.
///
/// Two metadata columns are not user properties and are handled here rather
/// than by name:
///
/// - `NODE_TYPE_PROP_ID` is never indexed; nothing queries it through the
///   property filters.
/// - `NODE_ID_PROP_ID` holds the node's external id (its GID, `U64` or `Str`).
///   It has no user-facing name, so it is selected by its own flag and is
///   independent of the named selection: `gid` alone is enough to index it
///   even when `columns` names nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SelectedProps {
    columns: SelectedColumns,
    gid: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
enum SelectedColumns {
    /// Every indexable column, the default for a graph that never configured
    /// a selection.
    #[default]
    All,
    /// Only these prop ids, per id space. Either set may be empty.
    Only {
        temporal: HashSet<usize>,
        metadata: HashSet<usize>,
    },
}

impl SelectedProps {
    /// Every indexable column.
    pub fn all(gid: bool) -> Self {
        Self {
            columns: SelectedColumns::All,
            gid,
        }
    }

    /// Only the given prop ids, per id space.
    pub fn only(temporal: HashSet<usize>, metadata: HashSet<usize>, gid: bool) -> Self {
        Self {
            columns: SelectedColumns::Only { temporal, metadata },
            gid,
        }
    }

    pub fn includes(&self, temporal: bool, prop_id: usize) -> bool {
        if !temporal {
            match prop_id {
                NODE_TYPE_PROP_ID => return false,
                // selected by the flag, not by name, and regardless of it
                NODE_ID_PROP_ID => return self.gid,
                _ => {}
            }
        }
        match &self.columns {
            SelectedColumns::All => true,
            SelectedColumns::Only {
                temporal: t,
                metadata: m,
            } => {
                if temporal {
                    t.contains(&prop_id)
                } else {
                    m.contains(&prop_id)
                }
            }
        }
    }

    /// Whether the node id column is selected.
    pub fn gid(&self) -> bool {
        self.gid
    }

    /// True when nothing at all is selected, so a build has no work to do.
    pub fn is_empty(&self) -> bool {
        if self.gid {
            return false;
        }
        match &self.columns {
            SelectedColumns::All => false,
            SelectedColumns::Only { temporal, metadata } => {
                temporal.is_empty() && metadata.is_empty()
            }
        }
    }
}

/// Global candidate node ids for a [`PropPredicate`].
///
/// `vids` must be a superset of the matching nodes (no false negatives) over
/// the graph's whole history; when `exact` is false the caller must still
/// verify each candidate against the actual predicate semantics.
#[derive(Debug, Clone, Default)]
pub struct GlobalPropCandidates {
    pub vids: Vec<VID>,
    pub exact: bool,
}

pub trait NodeSegmentOps: Send + Sync + Debug + 'static {
    type Extension;

    type Entry<'a>: NodeEntryOps<'a>
    where
        Self: 'a;

    type ArcLockedSegment: LockedNSSegment;

    fn latest(&self) -> Option<EventTime>;

    fn earliest(&self) -> Option<EventTime>;

    fn t_len(&self) -> usize;

    fn load(
        segment_id: usize,
        node_meta: Arc<Meta>,
        edge_meta: Arc<Meta>,
        path: impl AsRef<Path>,
        ext: Self::Extension,
    ) -> Result<Self, StorageError>
    where
        Self: Sized;

    fn new(
        segment_id: usize,
        node_meta: Arc<Meta>,
        edge_meta: Arc<Meta>,
        path: Option<PathBuf>,
        ext: Self::Extension,
    ) -> Self;

    fn segment_id(&self) -> usize;

    fn head_arc(&self) -> ArcRwLockReadGuard<MemNodeSegment>;

    fn head_mut_arc(&self) -> ArcRwLockWriteGuard<MemNodeSegment>;

    fn try_head_mut_arc(&self) -> Option<ArcRwLockWriteGuard<MemNodeSegment>>;

    fn head(&self) -> RwLockReadGuard<'_, MemNodeSegment>;

    fn head_mut(&self) -> RwLockWriteGuard<'_, MemNodeSegment>;

    fn try_head_mut(&self) -> Option<RwLockWriteGuard<'_, MemNodeSegment>>;

    fn try_head_mut_for(&self, timeout: Duration) -> Option<RwLockWriteGuard<'_, MemNodeSegment>>;

    fn notify_write(
        &self,
        head: impl DerefMut<Target = MemNodeSegment>,
    ) -> Result<(), StorageError>;

    fn set_dirty(&self, dirty: bool);

    fn has_node(&self, pos: LocalPOS, layer_id: LayerId) -> bool;

    fn get_out_edge(
        &self,
        pos: LocalPOS,
        dst: impl Into<VID>,
        layer_id: LayerId,
        head: impl Deref<Target = MemNodeSegment>,
    ) -> Option<EID>;

    fn get_inb_edge(
        &self,
        pos: LocalPOS,
        src: impl Into<VID>,
        layer_id: LayerId,
        head: impl Deref<Target = MemNodeSegment>,
    ) -> Option<EID>;

    fn entry(&self, pos: impl Into<LocalPOS>) -> Self::Entry<'_>;

    fn locked(&self) -> Self::ArcLockedSegment;

    fn flush(&self) -> Result<(), StorageError> {
        let mut head = self.head_mut();
        head.sort_deferred();
        self.flush_with_head(head)
    }

    fn flush_with_head(
        &self,
        head: impl DerefMut<Target = MemNodeSegment>,
    ) -> Result<(), StorageError>;

    fn is_dirty(&self) -> bool;

    fn copy_to(&self, dst: &Path) -> Result<(), StorageError>;

    fn vacuum(&self, head: impl DerefMut<Target = MemNodeSegment>) -> Result<(), StorageError>;

    /// Returns the latest lsn for the immutable part of this segment.
    fn immut_lsn(&self) -> LSN;

    fn nodes_counter(&self) -> &AtomicU32;

    fn increment_num_nodes(&self, max_page_len: u32) {
        increment_and_clamp(self.nodes_counter(), 1, max_page_len);
    }

    fn num_nodes(&self) -> u32 {
        self.nodes_counter().load(Ordering::Relaxed)
    }

    fn num_layers(&self) -> usize;

    fn layer_count(&self, layer_id: LayerId) -> u32;

    fn get_metadata_immut(&self, pos: LocalPOS, layer_id: LayerId, prop_id: usize) -> Option<Prop>;

    fn check_metadata_immut(
        &self,
        pos: LocalPOS,
        layer_id: LayerId,
        prop_id: usize,
        prop_ref: PropRef,
    ) -> Result<(), StorageError>;
}

pub trait LockedNSSegment: Debug + Send + Sync {
    type EntryRef<'a>: NodeRefOps<'a>
    where
        Self: 'a;

    fn num_nodes(&self) -> u32;

    fn entry_ref<'a>(&'a self, pos: impl Into<LocalPOS>) -> Self::EntryRef<'a>;

    fn iter_entries<'a>(&'a self) -> impl Iterator<Item = Self::EntryRef<'a>> + Send + Sync + 'a {
        let num_nodes = self.num_nodes();
        (0..num_nodes).map(move |vid| self.entry_ref(LocalPOS(vid)))
    }

    fn par_iter_entries<'a>(
        &'a self,
    ) -> impl ParallelIterator<Item = Self::EntryRef<'a>> + Sync + 'a {
        let num_nodes = self.num_nodes();
        (0..num_nodes)
            .into_par_iter()
            .map(move |vid| self.entry_ref(LocalPOS(vid)))
    }
}

pub trait NodeEntryOps<'a>: Send + Sync + 'a {
    type Ref<'b>: NodeRefOps<'b>
    where
        'a: 'b,
        Self: 'b;

    fn as_ref<'b>(&'b self) -> Self::Ref<'b>
    where
        'a: 'b;

    fn into_edges<'b: 'a>(
        self,
        layers: &'b LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + Send + Sync + 'a
    where
        Self: Sized,
    {
        GenLockedIter::from((self, layers), |(e, layers)| {
            e.as_ref().edges_iter(layers, dir).into_dyn_boxed()
        })
    }
}

pub trait NodeRefOps<'a>: Copy + Clone + Send + Sync + 'a {
    type Additions: TimeIndexOps<'a, IndexType = EventTime>;
    type EdgeAdditions: TimeIndexOps<'a, IndexType = EventTime>;
    type TProps: TPropOps<'a>;

    fn out_edges(self, layer_id: LayerId) -> impl Iterator<Item = (VID, EID)> + Send + Sync + 'a;

    fn inb_edges(self, layer_id: LayerId) -> impl Iterator<Item = (VID, EID)> + Send + Sync + 'a;

    fn out_edges_sorted(
        self,
        layer_id: LayerId,
    ) -> impl Iterator<Item = (VID, EID)> + Send + Sync + 'a;

    fn inb_edges_sorted(
        self,
        layer_id: LayerId,
    ) -> impl Iterator<Item = (VID, EID)> + Send + Sync + 'a;

    fn vid(&self) -> VID;

    #[box_on_debug_lifetime]
    fn edges_dir(
        self,
        layer_id: LayerId,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + Send + Sync + 'a
    where
        Self: Sized,
    {
        let src_pid = self.vid();
        match dir {
            Direction::OUT => Iter3::I(
                self.out_edges(layer_id)
                    .map(move |(v, e)| EdgeRef::new_outgoing(e, src_pid, v)),
            ),
            Direction::IN => Iter3::J(
                self.inb_edges(layer_id)
                    .map(move |(v, e)| EdgeRef::new_incoming(e, v, src_pid)),
            ),
            Direction::BOTH => Iter3::K(
                self.out_edges_sorted(layer_id)
                    .map(move |(v, e)| EdgeRef::new_outgoing(e, src_pid, v))
                    .merge_by(
                        self.inb_edges_sorted(layer_id)
                            .map(move |(v, e)| EdgeRef::new_incoming(e, v, src_pid)),
                        |e1, e2| e1.remote() < e2.remote(),
                    )
                    .dedup_by(|l, r| l.pid() == r.pid()),
            ),
        }
    }

    #[box_on_debug_lifetime]
    fn edges_sorted_dir(
        self,
        layer_id: LayerId,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + Send + Sync + 'a
    where
        Self: Sized,
    {
        let src_pid = self.vid();
        match dir {
            Direction::OUT => Iter3::I(
                self.out_edges_sorted(layer_id)
                    .map(move |(v, e)| EdgeRef::new_outgoing(e, src_pid, v)),
            ),
            Direction::IN => Iter3::J(
                self.inb_edges_sorted(layer_id)
                    .map(move |(v, e)| EdgeRef::new_incoming(e, v, src_pid)),
            ),
            Direction::BOTH => Iter3::K(
                self.out_edges_sorted(layer_id)
                    .map(move |(v, e)| EdgeRef::new_outgoing(e, src_pid, v))
                    .merge_by(
                        self.inb_edges_sorted(layer_id)
                            .map(move |(v, e)| EdgeRef::new_incoming(e, v, src_pid)),
                        |e1, e2| e1.remote() < e2.remote(),
                    )
                    .dedup_by(|l, r| l.pid() == r.pid()),
            ),
        }
    }

    #[box_on_debug_lifetime]
    fn edges_iter<'b>(
        self,
        layers_ids: &'b LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + Send + Sync + 'a
    where
        Self: Sized,
    {
        match layers_ids {
            LayerIds::One(layer_id) => Iter4::I(self.edges_dir(*layer_id, dir)),
            LayerIds::All => Iter4::J(self.edges_dir(STATIC_GRAPH_LAYER_ID, dir)),
            LayerIds::Multiple(layers) => Iter4::K(
                layers
                    .into_iter()
                    .map(|layer_id| self.edges_sorted_dir(layer_id, dir))
                    .fast_merge_by(|e1, e2| e1.remote() < e2.remote())
                    .dedup_by(|l, r| l.pid() == r.pid()),
            ),
            LayerIds::None => Iter4::L(std::iter::empty()),
        }
    }

    fn node_meta(&self) -> &Arc<Meta>;

    fn temp_prop_rows(
        self,
        w: Option<Range<EventTime>>,
        prop_ids: &'a [usize],
        layer_id: LayerId,
    ) -> impl Iterator<Item = (EventTime, Vec<(usize, Prop)>)> + 'a;

    fn out_nbrs(self, layer_id: LayerId) -> impl Iterator<Item = VID> + 'a
    where
        Self: Sized,
    {
        self.out_edges(layer_id).map(|(v, _)| v)
    }

    fn inb_nbrs(self, layer_id: LayerId) -> impl Iterator<Item = VID> + 'a
    where
        Self: Sized,
    {
        self.inb_edges(layer_id).map(|(v, _)| v)
    }

    fn out_nbrs_sorted(self, layer_id: LayerId) -> impl Iterator<Item = VID> + 'a
    where
        Self: Sized,
    {
        self.out_edges_sorted(layer_id).map(|(v, _)| v)
    }

    fn inb_nbrs_sorted(self, layer_id: LayerId) -> impl Iterator<Item = VID> + 'a
    where
        Self: Sized,
    {
        self.inb_edges_sorted(layer_id).map(|(v, _)| v)
    }

    fn edge_additions<L: Into<LayerIter<'a>>>(self, layer_id: L) -> Self::EdgeAdditions;

    fn node_additions<L: Into<LayerIter<'a>>>(self, layer_id: L) -> Self::Additions;

    fn c_prop(self, layer_id: LayerId, prop_id: usize) -> Option<Prop>;

    fn c_prop_str(self, layer_id: LayerId, prop_id: usize) -> Option<&'a str>;

    fn temporal_prop_layer(self, layer_id: LayerId, prop_id: usize) -> Self::TProps;

    fn degree(self, layers: &LayerIds, dir: Direction) -> usize;

    fn find_edge(&self, dst: VID, layers: &LayerIds) -> Option<EdgeRef>;

    fn name(&self) -> Cow<'a, str> {
        self.gid().to_str()
    }

    fn gid(&self) -> GidRef<'a> {
        self.c_prop_str(LayerId(0), NODE_ID_PROP_ID)
            .map(GidRef::Str)
            .or_else(|| {
                self.c_prop(LayerId(0), NODE_ID_PROP_ID)
                    .and_then(|prop| prop.into_u64().map(GidRef::U64))
            })
            .unwrap_or_else(|| panic!("GID should be present, for node {:?}", self.vid()))
    }

    fn node_type_id(&self) -> usize {
        self.c_prop(LayerId(0), NODE_TYPE_PROP_ID)
            .and_then(|prop| prop.into_u64())
            .map_or(0, |id| id as usize)
    }

    fn internal_num_layers(&self) -> usize;

    fn has_layer_inner(self, layer_id: LayerId) -> bool;

    fn layer_ids_iter<L: Into<LayerIter<'a>>>(
        self,
        layer_ids: L,
    ) -> impl Iterator<Item = LayerId> + Send + Sync + 'a {
        layer_ids
            .into()
            .into_iter(self.internal_num_layers())
            .filter(move |layer| self.has_layer_inner(*layer))
    }

    fn layer_ids_iter_with_static<L: Into<LayerIter<'a>>>(
        self,
        layer_ids: L,
    ) -> impl Iterator<Item = LayerId> + Send + Sync + 'a {
        layer_ids
            .into()
            .into_iter_with_static(self.internal_num_layers())
            .filter(move |layer| self.has_layer_inner(*layer))
    }
}
