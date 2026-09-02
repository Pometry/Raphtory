use itertools::Itertools;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard, lock_api::ArcRwLockReadGuard};
use raphtory_api::{
    core::{
        Direction,
        entities::properties::{
            meta::{Meta, NODE_ID_IDX, NODE_TYPE_IDX},
            prop::{AsPropRef, Prop, PropUnwrap},
            tprop::TPropOps,
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
use std::{
    borrow::Cow,
    fmt::Debug,
    ops::{Deref, DerefMut, Range},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use crate::{
    LocalPOS, NodeDeletions,
    error::StorageError,
    generic_time_ops::LayerIter,
    pages::node_store::increment_and_clamp,
    segments::node::segment::MemNodeSegment,
    utils::{Iter2, Iter3, Iter4},
    wal::LSN,
};
use raphtory_api::core::entities::{
    LayerId, LayerVariants, properties::meta::STATIC_GRAPH_LAYER_ID,
};
use raphtory_itertools::FastMergeExt;
use rayon::prelude::*;

pub trait NodeSegmentOps: Send + Sync + Debug + 'static {
    type Extension;

    type Entry<'a>: NodeEntryOps + 'a
    where
        Self: 'a;

    type ArcLockedSegment: LockedNSSegment;

    fn latest(&self) -> Option<EventTime>;

    fn earliest(&self) -> Option<EventTime>;

    fn t_len(&self) -> usize;

    fn load(
        page_id: usize,
        node_meta: Arc<Meta>,
        edge_meta: Arc<Meta>,
        path: impl AsRef<Path>,
        ext: Self::Extension,
    ) -> Result<Self, StorageError>
    where
        Self: Sized;

    fn new(
        page_id: usize,
        node_meta: Arc<Meta>,
        edge_meta: Arc<Meta>,
        path: Option<PathBuf>,
        ext: Self::Extension,
    ) -> Self;

    fn segment_id(&self) -> usize;

    fn head_arc(&self) -> ArcRwLockReadGuard<parking_lot::RawRwLock, MemNodeSegment>;

    fn head(&self) -> RwLockReadGuard<'_, MemNodeSegment>;

    fn head_mut(&self) -> RwLockWriteGuard<'_, MemNodeSegment>;

    fn try_head_mut(&self) -> Option<RwLockWriteGuard<'_, MemNodeSegment>>;

    fn notify_write(
        &self,
        head_lock: impl DerefMut<Target = MemNodeSegment>,
    ) -> Result<(), StorageError>;

    fn set_dirty(&self, dirty: bool);

    fn has_node(&self, pos: LocalPOS, layer_id: LayerId) -> bool;

    fn get_out_edge(
        &self,
        pos: LocalPOS,
        dst: impl Into<VID>,
        layer_id: LayerId,
        locked_head: impl Deref<Target = MemNodeSegment>,
    ) -> Option<EID>;

    fn get_inb_edge(
        &self,
        pos: LocalPOS,
        src: impl Into<VID>,
        layer_id: LayerId,
        locked_head: impl Deref<Target = MemNodeSegment>,
    ) -> Option<EID>;

    fn entry(&self, pos: impl Into<LocalPOS>) -> Self::Entry<'_>;

    fn locked(&self) -> Self::ArcLockedSegment;

    fn flush(&self) -> Result<(), StorageError>;

    fn is_dirty(&self) -> bool;

    fn vacuum(
        &self,
        locked_head: impl DerefMut<Target = MemNodeSegment>,
    ) -> Result<(), StorageError>;

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

    fn check_metadata_immut<P: AsPropRef>(
        &self,
        pos: LocalPOS,
        layer_id: LayerId,
        props: &[(usize, P)],
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

pub trait NodeEntryOps: Send + Sync {
    type Ref<'b>: NodeRefOps<'b>
    where
        Self: 'b;

    fn as_ref<'b>(&'b self) -> Self::Ref<'b>;

    fn out_edges<'a>(
        &'a self,
        layer_id: LayerId,
    ) -> impl Iterator<Item = (VID, EID)> + Send + Sync + 'a {
        self.as_ref().out_edges(layer_id)
    }

    fn inb_edges<'a>(
        &'a self,
        layer_id: LayerId,
    ) -> impl Iterator<Item = (VID, EID)> + Send + Sync + 'a {
        self.as_ref().inb_edges(layer_id)
    }

    fn out_edges_sorted<'a>(
        &'a self,
        layer_id: LayerId,
    ) -> impl Iterator<Item = (VID, EID)> + Send + Sync + 'a {
        self.as_ref().out_edges_sorted(layer_id)
    }

    fn inb_edges_sorted<'a>(
        &'a self,
        layer_id: LayerId,
    ) -> impl Iterator<Item = (VID, EID)> + Send + Sync + 'a {
        self.as_ref().inb_edges_sorted(layer_id)
    }

    fn vid(&self) -> VID {
        self.as_ref().vid()
    }

    fn edges_dir<'a>(
        &'a self,
        layer_id: LayerId,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + Send + Sync + 'a
    where
        Self::Ref<'a>: Sized,
    {
        self.as_ref().edges_dir(layer_id, dir)
    }

    fn edges_sorted_dir<'a>(
        &'a self,
        layer_id: LayerId,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + Send + Sync + 'a
    where
        Self::Ref<'a>: Sized,
    {
        self.as_ref().edges_sorted_dir(layer_id, dir)
    }

    fn edges_iter<'a, 'b: 'a>(
        &'a self,
        layers_ids: &'b LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + Send + Sync + 'a
    where
        Self::Ref<'a>: Sized,
    {
        self.as_ref().edges_iter(layers_ids, dir)
    }

    fn node_meta(&self) -> &Arc<Meta> {
        self.as_ref().node_meta()
    }

    fn t_prop_rows<'a>(
        &'a self,
        w: Option<Range<EventTime>>,
        prop_ids: Arc<[usize]>,
    ) -> impl Iterator<Item = (EventTime, usize, Vec<(usize, Prop)>)> + 'a {
        self.as_ref().t_prop_rows(w, prop_ids)
    }

    fn out_nbrs<'a>(&'a self, layer_id: LayerId) -> impl Iterator<Item = VID> + 'a
    where
        Self::Ref<'a>: Sized,
    {
        self.as_ref().out_nbrs(layer_id)
    }

    fn inb_nbrs<'a>(&'a self, layer_id: LayerId) -> impl Iterator<Item = VID> + 'a
    where
        Self::Ref<'a>: Sized,
    {
        self.as_ref().inb_nbrs(layer_id)
    }

    fn out_nbrs_sorted<'a>(&'a self, layer_id: LayerId) -> impl Iterator<Item = VID> + 'a
    where
        Self::Ref<'a>: Sized,
    {
        self.as_ref().out_nbrs_sorted(layer_id)
    }

    fn inb_nbrs_sorted<'a>(&'a self, layer_id: LayerId) -> impl Iterator<Item = VID> + 'a
    where
        Self::Ref<'a>: Sized,
    {
        self.as_ref().inb_nbrs_sorted(layer_id)
    }

    fn edge_additions<'a, L: Into<LayerIter<'a>>>(
        &'a self,
        layer_id: L,
    ) -> <Self::Ref<'a> as NodeRefOps<'a>>::EdgeAdditions {
        self.as_ref().edge_additions(layer_id)
    }

    fn node_additions<'a, L: Into<LayerIter<'a>>>(
        &'a self,
        layer_id: L,
    ) -> <Self::Ref<'a> as NodeRefOps<'a>>::Additions {
        self.as_ref().node_additions(layer_id)
    }

    fn node_deletions<'a, L: Into<LayerIter<'a>>>(
        &'a self,
        layer_id: L,
    ) -> <Self::Ref<'a> as NodeRefOps<'a>>::Deletions {
        self.as_ref().node_deletions(layer_id)
    }

    fn c_prop(&self, layer_id: LayerId, prop_id: usize) -> Option<Prop> {
        self.as_ref().c_prop(layer_id, prop_id)
    }

    fn c_prop_str(&self, layer_id: LayerId, prop_id: usize) -> Option<&str> {
        self.as_ref().c_prop_str(layer_id, prop_id)
    }

    fn t_prop_layer<'a>(
        &'a self,
        layer_id: LayerId,
        prop_id: usize,
    ) -> <Self::Ref<'a> as NodeRefOps<'a>>::TProps {
        self.as_ref().t_prop_layer(layer_id, prop_id)
    }

    fn degree(&self, layers: &LayerIds, dir: Direction) -> usize {
        self.as_ref().degree(layers, dir)
    }

    fn find_edge(&self, dst: VID, layers: &LayerIds) -> Option<EdgeRef> {
        self.as_ref().find_edge(dst, layers)
    }

    fn name<'a>(&'a self) -> Cow<'a, str> {
        self.as_ref().name()
    }

    fn gid<'a>(&'a self) -> GidRef<'a> {
        self.as_ref().gid()
    }

    fn node_type_id(&self) -> usize {
        self.as_ref().node_type_id()
    }

    fn internal_num_layers(&self) -> usize {
        self.as_ref().num_layers()
    }

    fn has_layer_inner(&self, layer_id: LayerId) -> bool {
        self.as_ref().has_layer(layer_id)
    }

    fn layer_ids_iter<'a, L: Into<LayerIter<'a>>>(
        &'a self,
        layer_ids: L,
    ) -> impl Iterator<Item = LayerId> + Send + Sync + 'a {
        self.as_ref().layer_ids_iter(layer_ids)
    }

    fn has_layers<'a, L: Into<LayerIter<'a>>>(&'a self, layer_ids: L) -> bool {
        self.as_ref().has_layers(layer_ids)
    }
}

pub trait IntoEdges<'a>: NodeEntryOps + Send + Sync + 'a {
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

impl<'a, T: NodeEntryOps + Send + Sync + 'a> IntoEdges<'a> for T {}

pub trait NodeRefOps<'a>: Copy + Clone + Send + Sync + 'a {
    type Additions: TimeIndexOps<'a, IndexType = EventTime>;
    type EdgeAdditions: TimeIndexOps<'a, IndexType = EventTime>;

    type Deletions: TimeIndexOps<'a, IndexType = EventTime>;
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

    fn node_meta(self) -> &'a Arc<Meta>;

    fn t_prop_rows(
        self,
        w: Option<Range<EventTime>>,
        prop_ids: Arc<[usize]>,
    ) -> impl Iterator<Item = (EventTime, usize, Vec<(usize, Prop)>)> + 'a {
        (0..self.num_layers()).flat_map(move |layer_id| {
            let w = w.clone();
            let prop_ids = Arc::clone(&prop_ids);
            let additions = self.node_additions(layer_id);
            let additions = w
                .clone()
                .map(|w| Iter2::I1(additions.range(w).iter()))
                .unwrap_or_else(|| Iter2::I2(additions.iter()));

            let mut time_ordered_iter = prop_ids
                .iter()
                .copied()
                .map(move |prop_id| {
                    self.t_prop_layer(LayerId(layer_id), prop_id)
                        .iter_inner(w.clone())
                        .map(move |(t, prop)| (t, (prop_id, prop)))
                })
                .kmerge_by(|(t1, (p_id1, _)), (t2, (p_id2, _))| (t1, p_id1) < (t2, p_id2))
                .merge_join_by(additions, |(t1, _), t2| t1 <= t2)
                .map(move |result| match result {
                    either::Either::Left((l, (prop_id, prop))) => (l, Some((prop_id, prop))),
                    either::Either::Right(r) => (r, None),
                });

            let mut done = false;
            if let Some((mut current_time, maybe_prop)) = time_ordered_iter.next() {
                let mut current_row = Vec::from_iter(maybe_prop);
                Iter2::I2(std::iter::from_fn(move || {
                    if done {
                        return None;
                    }
                    for (t, maybe_prop) in time_ordered_iter.by_ref() {
                        if t == current_time {
                            current_row.extend(maybe_prop);
                        } else {
                            let mut row = std::mem::take(&mut current_row);
                            row.sort_unstable_by_key(|(a, _)| *a);
                            let out = Some((current_time, layer_id, row));
                            current_row.extend(maybe_prop);
                            current_time = t;
                            return out;
                        }
                    }
                    done = true;
                    let row = std::mem::take(&mut current_row);
                    Some((current_time, layer_id, row))
                }))
            } else {
                Iter2::I1(std::iter::empty())
            }
        })
    }

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

    fn node_deletions<L: Into<LayerIter<'a>>>(self, layer_id: L) -> Self::Deletions;

    fn c_prop(self, layer_id: LayerId, prop_id: usize) -> Option<Prop>;

    fn c_prop_str(self, layer_id: LayerId, prop_id: usize) -> Option<&'a str>;

    fn t_prop_layer(self, layer_id: LayerId, prop_id: usize) -> Self::TProps;

    /// Iterate over `NodeTProps` for each layer specified by `layer_ids`, always
    /// including `STATIC_GRAPH_LAYER_ID` (the layer for nodes added without an
    /// explicit layer name).  This mirrors the behaviour of `layer_ids_with_static`
    /// used for node additions: unlayered nodes must be visible in every view.
    fn t_prop_iter_layers(
        self,
        layer_ids: &LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = Self::TProps> + Send + Sync + 'a {
        let layers = match layer_ids {
            LayerIds::None => LayerVariants::None(std::iter::once(STATIC_GRAPH_LAYER_ID)),
            LayerIds::All => LayerVariants::All((0..self.num_layers()).map(LayerId)),
            LayerIds::One(id) => {
                if *id == STATIC_GRAPH_LAYER_ID {
                    LayerVariants::One(std::iter::once(*id))
                } else {
                    LayerVariants::Multiple(Iter3::I([STATIC_GRAPH_LAYER_ID, *id].into_iter()))
                }
            }
            LayerIds::Multiple(ids) => {
                if ids.contains(STATIC_GRAPH_LAYER_ID) {
                    LayerVariants::Multiple(Iter3::J(ids.clone().into_iter()))
                } else {
                    let v = std::iter::once(STATIC_GRAPH_LAYER_ID).chain(ids.clone().into_iter());
                    LayerVariants::Multiple(Iter3::K(v))
                }
            }
        };
        layers.map(move |id| self.t_prop_layer(id, prop_id))
    }

    fn degree(self, layers: &LayerIds, dir: Direction) -> usize;

    fn find_edge(&self, dst: VID, layers: &LayerIds) -> Option<EdgeRef>;

    fn name(&self) -> Cow<'a, str> {
        self.gid().to_str()
    }

    fn gid(&self) -> GidRef<'a> {
        self.c_prop_str(LayerId(0), NODE_ID_IDX)
            .map(GidRef::Str)
            .or_else(|| {
                self.c_prop(LayerId(0), NODE_ID_IDX)
                    .and_then(|prop| prop.into_u64().map(GidRef::U64))
            })
            .unwrap_or_else(|| panic!("GID should be present, for node {:?}", self.vid()))
    }

    fn node_type_id(&self) -> usize {
        self.c_prop(LayerId(0), NODE_TYPE_IDX)
            .and_then(|prop| prop.into_u64())
            .map_or(0, |id| id as usize)
    }

    fn num_layers(&self) -> usize;

    fn has_layer(self, layer_id: LayerId) -> bool;

    fn layer_ids_iter<L: Into<LayerIter<'a>>>(
        self,
        layer_ids: L,
    ) -> impl Iterator<Item = LayerId> + Send + Sync + 'a {
        layer_ids
            .into()
            .into_iter(self.num_layers())
            .filter(move |layer| self.has_layer(*layer))
    }

    fn has_layers<L: Into<LayerIter<'a>>>(self, layer_ids: L) -> bool {
        !self.node_additions(STATIC_GRAPH_LAYER_ID).is_empty()
            || self.layer_ids_iter(layer_ids).next().is_some()
    }
}
