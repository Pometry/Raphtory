use crate::errors::GraphError;
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use std::{cell::RefCell, collections::BTreeMap};
use storage::{
    pages::{
        locked::nodes::{MemNodeSegmentGuards, WriteLockedNodeSegment},
        node_store::NodeStorageInner,
    },
    Extension, NS,
};

use super::{
    owner::OwnerMsg,
    pool::OwnerPool,
    segments::{NodeSegmentLock, SegmentOwners},
};

type NodeStore = NodeStorageInner<NS<Extension>, Extension>;

/// Decides which owner writes each node segment, handing out segments as node resolution
/// creates them. Runs on the calling thread, which holds the graph's write lock.
pub(super) struct Coordinator<'s, 'a> {
    node_store: &'a NodeStore,
    owners: &'s OwnerPool<'a>,
    /// segments created but not handed out yet: those before them haven't arrived
    created: RefCell<BTreeMap<usize, NodeSegmentLock<'a>>>,
}

/// A reader's line to the coordinator; dropping it tells the coordinator the reader is done.
pub(super) struct CoordinatorHandle(Sender<Request>);

pub(super) enum Request {
    CoverSegments {
        segments: usize,
        reply: Sender<Result<SegmentOwners, GraphError>>,
    },
    /// Node segments resolution created, write-locked before they were published.
    NewSegments(MemNodeSegmentGuards),
    ReaderDone,
}

pub(super) fn request_channel() -> (Sender<Request>, Receiver<Request>) {
    unbounded()
}

impl<'s, 'a> Coordinator<'s, 'a> {
    pub fn new(node_store: &'a NodeStore, owners: &'s OwnerPool<'a>) -> Self {
        Self {
            node_store,
            owners,
            created: RefCell::default(),
        }
    }

    pub fn hand_out(&self, segments: Vec<NodeSegmentLock<'a>>) {
        let published = &self.owners.segment_owners;
        let owners = segments
            .into_iter()
            .map(|segment| self.give_to_least_busy(segment));
        published.publish(published.latest().extended_by(owners));
    }

    /// Answers readers until every one of them is done. A reader asking for a segment whose
    /// lock another reader is still sending waits for it: the sender never waits on us.
    pub fn serve(&self, requests: Receiver<Request>, readers: usize) {
        let mut readers_running = readers;
        let mut waiting = Vec::new();
        while readers_running > 0 {
            match requests.recv() {
                Ok(Request::CoverSegments { segments, reply }) => waiting.push((segments, reply)),
                Ok(Request::NewSegments(guards)) => self.take_created(guards),
                Ok(Request::ReaderDone) => readers_running -= 1,
                Err(_) => break,
            }
            let owners = self.owners.segment_owners.latest();
            waiting.retain(|(segments, reply)| {
                let covered = owners.covers(*segments);
                if covered {
                    let _ = reply.send(Ok(owners.clone()));
                }
                !covered
            });
            if !waiting.is_empty() && waiting.len() == readers_running {
                // every reader left is waiting: the locks they wait for will never come
                for (_, reply) in waiting.drain(..) {
                    let _ = reply.send(Err(GraphError::LoadFailure(
                        "node segments were created without handing over their locks".to_string(),
                    )));
                }
            }
        }
    }

    /// Hands out the created segments that follow the ones already handed out.
    fn take_created(&self, guards: MemNodeSegmentGuards) {
        let mut created = self.created.borrow_mut();
        for guard in guards {
            let id = guard.segment_id();
            let segment = self
                .node_store
                .get_segment(id)
                .expect("a segment is published before its creator hands its lock over");
            let lock = WriteLockedNodeSegment::new(
                id,
                self.node_store.stats(),
                self.node_store.max_segment_len(),
                segment,
                guard,
            );
            created.insert(id, lock);
        }
        let published = &self.owners.segment_owners;
        let current = published.latest();
        let mut next = current.len();
        let mut owners = Vec::new();
        while let Some(segment) = created.remove(&next) {
            owners.push(self.give_to_least_busy(segment));
            next += 1;
        }
        if !owners.is_empty() {
            published.publish(current.extended_by(owners));
        }
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
