use db4_graph::TemporalGraph;
use parking_lot::RwLock;
use raphtory_api::core::entities::{LayerId, EID};
use raphtory_core::entities::VID;
use std::{sync::Arc, time::Duration};
use storage::{
    api::{edges::EdgeSegmentOps, nodes::NodeSegmentOps},
    pages::{
        edge_store::EdgeStorageInner,
        locked::{edges::LockedEdgeSegment, nodes::LockedNodeSegment},
        resolve_pos,
    },
    Extension, ES, NS,
};

pub(super) type NodeSegmentLock<'a> = LockedNodeSegment<'a, NS<Extension>>;
pub(super) type EdgeSegmentLock<'a> = LockedEdgeSegment<'a, ES<Extension>>;
type EdgeStore = EdgeStorageInner<ES<Extension>, Extension>;

/// Nothing but the load takes segment locks while it runs, so waiting this long means a bug.
pub(super) const LOCK_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Copy)]
pub(super) struct SegmentSizes {
    node: u32,
    edge: u32,
}

/// The owner of each node segment, as the coordinator last published it.
#[derive(Clone, Default)]
pub(super) struct SegmentOwners(Arc<[u16]>);

#[derive(Default)]
pub(super) struct PublishedOwners(RwLock<SegmentOwners>);

/// The node segments an owner writes, by segment id.
pub(super) struct NodeSegments<'a> {
    held: Vec<Option<NodeSegmentLock<'a>>>,
    sizes: SegmentSizes,
}

/// The edge segments an owner writes, and the one it fills with new edges.
pub(super) struct EdgeSegments<'a> {
    held: Vec<Option<EdgeSegmentLock<'a>>>,
    filling: Option<usize>,
    store: &'a EdgeStore,
    sizes: SegmentSizes,
}

impl SegmentSizes {
    pub fn of(graph: &TemporalGraph<Extension>) -> Self {
        Self {
            node: graph.storage().nodes().max_segment_len(),
            edge: graph.storage().edges().max_page_len(),
        }
    }

    pub fn node_segment(self, vid: VID) -> usize {
        resolve_pos(vid.index(), self.node).0
    }

    pub fn edge_segment(self, eid: EID) -> usize {
        resolve_pos(eid, self.edge).0
    }
}

impl SegmentOwners {
    pub fn covers(&self, segments: usize) -> bool {
        segments <= self.0.len()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn of_segment(&self, segment: usize) -> usize {
        self.0[segment] as usize
    }

    pub fn extended_by(&self, owners: impl IntoIterator<Item = usize>) -> Self {
        let owners = owners.into_iter().map(|owner| owner as u16);
        Self(self.0.iter().copied().chain(owners).collect())
    }
}

impl PublishedOwners {
    pub fn latest(&self) -> SegmentOwners {
        self.0.read().clone()
    }

    pub fn publish(&self, owners: SegmentOwners) {
        *self.0.write() = owners;
    }
}

impl<'a> NodeSegments<'a> {
    pub fn new(sizes: SegmentSizes) -> Self {
        Self {
            held: Vec::new(),
            sizes,
        }
    }

    pub fn segment_of(&self, vid: VID) -> usize {
        self.sizes.node_segment(vid)
    }

    pub fn take(&mut self, segment: NodeSegmentLock<'a>) {
        let id = segment.segment_id();
        *slot(&mut self.held, id) = Some(segment);
    }

    pub fn get_mut(&mut self, segment: usize) -> &mut NodeSegmentLock<'a> {
        self.held
            .get_mut(segment)
            .and_then(Option::as_mut)
            .expect("rows are only routed to the owner of their node segment")
    }

    /// Opens a writer on every dirty segment, which flushes it if memory is short.
    pub fn flush_if_memory_is_short(&mut self) {
        for segment in self.held.iter_mut().flatten() {
            if segment.segment().is_dirty() {
                let _writer = segment.writer();
            }
        }
    }
}

impl<'a> EdgeSegments<'a> {
    pub fn new(store: &'a EdgeStore, sizes: SegmentSizes) -> Self {
        Self {
            held: Vec::new(),
            filling: None,
            store,
            sizes,
        }
    }

    pub fn segment_of(&self, eid: EID) -> usize {
        self.sizes.edge_segment(eid)
    }

    pub fn get_mut(&mut self, segment: usize) -> &mut EdgeSegmentLock<'a> {
        self.held
            .get_mut(segment)
            .and_then(Option::as_mut)
            .expect("an owner only adds edges to segments it holds")
    }

    pub fn new_edge_id(&mut self) -> EID {
        loop {
            if let Some(segment) = self.filling {
                if let Some(pos) = self.store.reserve_pos_in(segment) {
                    return pos.as_eid(segment, self.sizes.edge);
                }
            }
            self.start_new_segment();
        }
    }

    pub fn has_edge_in_layer(&self, eid: EID, layer: LayerId) -> bool {
        let (segment, pos) = resolve_pos(eid, self.sizes.edge);
        self.held
            .get(segment)
            .and_then(Option::as_ref)
            .is_some_and(|segment| segment.has_edge(pos, layer))
    }

    pub fn flush_if_memory_is_short(&mut self) {
        for segment in self.held.iter_mut().flatten() {
            if segment.segment().is_dirty() {
                let _writer = segment.writer();
            }
        }
    }

    fn start_new_segment(&mut self) {
        let id = self.store.push_new_segment();
        let segment = self
            .store
            .lock_segment(id, LOCK_TIMEOUT)
            .expect("nobody else locks a segment that was just created");
        *slot(&mut self.held, id) = Some(segment);
        self.filling = Some(id);
    }
}

fn slot<T>(slots: &mut Vec<Option<T>>, index: usize) -> &mut Option<T> {
    if slots.len() <= index {
        slots.resize_with(index + 1, || None);
    }
    &mut slots[index]
}
