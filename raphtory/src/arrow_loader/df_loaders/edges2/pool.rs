use crossbeam_channel::{unbounded, Receiver, Sender};
use std::sync::atomic::{AtomicU64, Ordering};

use super::{owner::OwnerMsg, segments::PublishedOwners};

/// What everyone shares about the owners: how to reach each, how busy each is, and which node
/// segments each owns.
pub(super) struct OwnerPool<'a> {
    mailboxes: Vec<Sender<OwnerMsg<'a>>>,
    pub workload: OwnerWorkload,
    pub segment_owners: PublishedOwners,
}

impl<'a> OwnerPool<'a> {
    pub fn new(owners: usize) -> (Self, Vec<Receiver<OwnerMsg<'a>>>) {
        let (mailboxes, receivers) = (0..owners).map(|_| unbounded()).unzip();
        let pool = Self {
            mailboxes,
            workload: OwnerWorkload::new(owners),
            segment_owners: PublishedOwners::default(),
        };
        (pool, receivers)
    }

    pub fn len(&self) -> usize {
        self.mailboxes.len()
    }

    pub fn send(&self, owner: usize, message: OwnerMsg<'a>) {
        let _ = self.mailboxes[owner].send(message);
    }

    /// Sends each owner its bucket, if it isn't empty.
    pub fn send_buckets<T>(&self, buckets: Vec<Vec<T>>, message: impl Fn(Vec<T>) -> OwnerMsg<'a>) {
        for (owner, bucket) in buckets.into_iter().enumerate() {
            if !bucket.is_empty() {
                self.send(owner, message(bucket));
            }
        }
    }

    pub fn shut_down(&self) {
        for owner in 0..self.len() {
            self.send(owner, OwnerMsg::Shutdown);
        }
    }
}

/// How much each owner has been given so far, to give new segments to the least busy.
pub(super) struct OwnerWorkload {
    segments: Vec<AtomicU64>,
    rows: Vec<AtomicU64>,
}

impl OwnerWorkload {
    pub fn new(owners: usize) -> Self {
        let counters = || (0..owners).map(|_| AtomicU64::new(0)).collect();
        Self {
            segments: counters(),
            rows: counters(),
        }
    }

    pub fn add_segment(&self, owner: usize) {
        self.segments[owner].fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_rows(&self, owner: usize, rows: usize) {
        self.rows[owner].fetch_add(rows as u64, Ordering::Relaxed);
    }

    pub fn least_busy(&self) -> usize {
        let load = |owner: usize| {
            (
                self.segments[owner].load(Ordering::Relaxed),
                self.rows[owner].load(Ordering::Relaxed),
            )
        };
        (0..self.segments.len())
            .min_by_key(|&owner| load(owner))
            .unwrap_or(0)
    }
}
