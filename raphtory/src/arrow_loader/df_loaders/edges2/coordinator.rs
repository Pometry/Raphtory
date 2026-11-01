use crate::errors::{into_graph_err, GraphError};
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use storage::{pages::node_store::NodeStorageInner, Extension, NS};

use super::{
    owner::OwnerMsg,
    pool::OwnerPool,
    segments::{NodeSegmentLock, SegmentOwners, LOCK_TIMEOUT},
};

type NodeStore = NodeStorageInner<NS<Extension>, Extension>;

/// Decides which owner writes each node segment, locking segments as node resolution creates
/// them. Runs on the calling thread, which holds the graph's write lock.
pub(super) struct Coordinator<'s, 'a> {
    node_store: &'a NodeStore,
    owners: &'s OwnerPool<'a>,
}

/// A reader's line to the coordinator; dropping it tells the coordinator the reader is done.
pub(super) struct CoordinatorHandle(Sender<Request>);

pub(super) enum Request {
    CoverSegments {
        segments: usize,
        reply: Sender<Result<SegmentOwners, GraphError>>,
    },
    ReaderDone,
}

pub(super) fn request_channel() -> (Sender<Request>, Receiver<Request>) {
    unbounded()
}

impl<'s, 'a> Coordinator<'s, 'a> {
    pub fn new(node_store: &'a NodeStore, owners: &'s OwnerPool<'a>) -> Self {
        Self { node_store, owners }
    }

    pub fn hand_out(&self, segments: Vec<NodeSegmentLock<'a>>) {
        let published = &self.owners.segment_owners;
        let owners = segments
            .into_iter()
            .map(|segment| self.give_to_least_busy(segment));
        published.publish(published.latest().extended_by(owners));
    }

    /// Answers readers until every one of them is done.
    pub fn serve(&self, requests: Receiver<Request>, readers: usize) {
        let mut readers_running = readers;
        while readers_running > 0 {
            match requests.recv() {
                Ok(Request::CoverSegments { segments, reply }) => {
                    let _ = reply.send(self.cover(segments));
                }
                Ok(Request::ReaderDone) => readers_running -= 1,
                Err(_) => break,
            }
        }
    }

    fn cover(&self, segments: usize) -> Result<SegmentOwners, GraphError> {
        let current = self.owners.segment_owners.latest();
        if current.covers(segments) {
            return Ok(current);
        }
        let new_segments = (current.len()..segments)
            .map(|id| self.node_store.lock_segment(id, LOCK_TIMEOUT))
            .collect::<Result<Vec<_>, _>>()
            .map_err(into_graph_err)?;
        let owners = new_segments
            .into_iter()
            .map(|segment| self.give_to_least_busy(segment));
        let extended = current.extended_by(owners);
        self.owners.segment_owners.publish(extended.clone());
        Ok(extended)
    }

    /// Every segment gets a share of new nodes for the rest of the load, so segments held
    /// decide; rows routed only break ties, as when the first chunk creates many segments.
    fn give_to_least_busy(&self, segment: NodeSegmentLock<'a>) -> usize {
        let owner = self.owners.workload.least_busy();
        self.owners.workload.add_segment(owner);
        self.owners.send(owner, OwnerMsg::TakeNodeSegment(segment));
        owner
    }
}

impl CoordinatorHandle {
    pub fn new(requests: Sender<Request>) -> Self {
        Self(requests)
    }

    pub fn owners_covering(&self, segments: usize) -> Result<SegmentOwners, GraphError> {
        let (reply, answer) = bounded(1);
        let _ = self.0.send(Request::CoverSegments { segments, reply });
        answer
            .recv()
            .map_err(|_| GraphError::LoadFailure("the load coordinator stopped".to_string()))?
    }
}

impl Drop for CoordinatorHandle {
    fn drop(&mut self) {
        let _ = self.0.send(Request::ReaderDone);
    }
}
